from __future__ import annotations

import asyncio
import contextlib
import contextvars
import datetime
import os
from typing import List, Tuple

from ddtrace import tracer
from mmlib.ops import log, stats

import constants
from app.common import apm
from app.common.gcs import LocalStorage, Storage
from app.dryrun.report import DryRunCsvWriter
from app.eligibility.gcs import EligibilityFileManager
from config import settings
from db.clients import postgres_connector, verification_client
from db.model import BackfillMemberTrackEligibilityData
from db.mono import client as mclient
from db.mono.client import MavenMonoClient
from db.mono.model import (
    MemberTrackBackfillError,
    MemberTrackBackfillRequest,
    MemberTrackBackfillResult,
)

RESOURCE = "backfill_member_track"
logger = log.getLogger(__name__)

_STATS_PREFIX = "eligibility.backfill.member_track"
_POD = constants.POD
_BATCH_SIZE = 10000


def main():
    asyncio.run(backfill(batch_size=_BATCH_SIZE))


# region backfill member track


@tracer.wrap(service=apm.ApmService.ELIGIBILITY_TASKS, resource=RESOURCE)
async def backfill(batch_size: int = _BATCH_SIZE, dry_run=True):
    """
    Backfill eligibility_verification_id and eligibility_member_id
    Can be run in devshell

    from utils import backfill_member_track
    await backfill_member_track.backfill(batch_size=10, dry_run=True)
    """
    logger.info(f"Beginning backfill member track table with batch_size={batch_size}")
    with stats.timed(metric_name=f"{_STATS_PREFIX}.run", pod_name=_POD):
        async with backfill_context():
            mono, verifications = (
                mclient.MavenMonoClient(),
                verification_client.Verifications(),
            )
            batch_no = 1
            last_id = 0
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            report_folder = f"{timestamp}_{str(dry_run)}"
            while True:

                requests = await _get_member_track_backfill_requests(
                    mono, batch_size, last_id
                )
                if not requests:
                    logger.info("No data to backfill. Backfill complete.")
                    break
                logger.info(
                    f"Found {len(requests)} records to backfill batch #{batch_no}, last_id= {last_id}"
                )
                results, errors = await _get_e9y_data(verifications, requests)
                if not dry_run:
                    update_errors = await _update_member_track_table(mono, results)
                    errors.extend(update_errors)

                await _write_back_fill_report(results, errors, batch_no, report_folder)
                last_id = max(
                    requests, key=lambda request: request.member_track_id
                ).member_track_id
                batch_no += 1


async def _get_member_track_backfill_requests(
    mono: MavenMonoClient,
    batch_size: int,
    last_id: int,
) -> List[MemberTrackBackfillRequest]:
    return await mono.get_member_track_back_fill_requests(
        batch_size=batch_size, last_id=last_id
    )


async def _get_e9y_data(
    verifications: verification_client.Verifications,
    requests: List[MemberTrackBackfillRequest],
) -> Tuple[List[MemberTrackBackfillResult], List[MemberTrackBackfillError]]:
    errors = []
    results = []
    for request in requests:
        records: List[
            BackfillMemberTrackEligibilityData
        ] = await verifications.get_e9y_data_for_member_track_backfill(
            user_id=request.user_id
        )
        if not records:
            errors.append(
                MemberTrackBackfillError(
                    request=request,
                    reason="No records Found",
                )
            )
            continue
        match_result = _found_best_match_with_verification_and_member(records, request)
        if isinstance(match_result, MemberTrackBackfillError):
            logger.info(
                f"matching with verification and member failed, trying with verification only for request {request}"
            )
            match_result = _found_best_match_with_verification_only(records, request)

        if isinstance(match_result, MemberTrackBackfillResult):
            results.append(match_result)
        else:
            errors.append(match_result)
    return (results, errors)


def _found_best_match_with_verification_and_member(
    records: List[BackfillMemberTrackEligibilityData],
    request: MemberTrackBackfillRequest,
) -> MemberTrackBackfillResult | MemberTrackBackfillError:

    # matching member organization_id
    filtered = [
        r
        for r in records
        if r.member_id is not None
        and r.member_organization_id == request.organization_id
    ]
    if not filtered:
        return MemberTrackBackfillError(
            request=request,
            reason=f"Match with verification and member: {len(records)} records found, but no matching organization_id",
        )
    if len(filtered) == 1:
        # in case only 1, return it
        return MemberTrackBackfillResult(
            request=request,
            e9y_verification_id=filtered[0].verification_id,
            e9y_member_id=filtered[0].member_id,
        )
    # in case more than 1, found the one before and most closed to track created_at
    filtered.sort(key=lambda r: r.member_created_at)
    before_track_created = [
        r
        for r in filtered
        if r.member_created_at.astimezone() <= request.created_at.astimezone()
    ]
    if before_track_created:
        return MemberTrackBackfillResult(
            request=request,
            e9y_verification_id=before_track_created[-1].verification_id,
            e9y_member_id=before_track_created[-1].member_id,
        )
    # in case no one before track created_at, return the first one
    return MemberTrackBackfillResult(
        request=request,
        e9y_verification_id=filtered[0].verification_id,
        e9y_member_id=filtered[0].member_id,
    )


def _found_best_match_with_verification_only(
    records: List[BackfillMemberTrackEligibilityData],
    request: MemberTrackBackfillRequest,
) -> MemberTrackBackfillResult | MemberTrackBackfillError:
    # matching verification organization_id
    filtered = [
        r for r in records if r.verification_organization_id == request.organization_id
    ]
    if not filtered:
        return MemberTrackBackfillError(
            request=request,
            reason=f"Match with verification only: {len(records)} records found, but no matching organization_id",
        )
    if len(filtered) == 1:
        # in case only 1, return it
        return MemberTrackBackfillResult(
            request=request,
            e9y_verification_id=filtered[0].verification_id,
            e9y_member_id=filtered[0].member_id,
        )
    # in case more than 1, found the one before and most closed to track created_at
    filtered.sort(key=lambda r: r.verification_created_at)
    before_track_created = [
        r
        for r in filtered
        if r.verification_created_at.astimezone() <= request.created_at.astimezone()
    ]
    if before_track_created:
        return MemberTrackBackfillResult(
            request=request,
            e9y_verification_id=before_track_created[-1].verification_id,
            e9y_member_id=before_track_created[-1].member_id,
        )
    # in case no one before track created_at, return the first one
    return MemberTrackBackfillResult(
        request=request,
        e9y_verification_id=filtered[0].verification_id,
        e9y_member_id=filtered[0].member_id,
    )


async def _update_member_track_table(
    mono: MavenMonoClient, results: List[MemberTrackBackfillResult]
) -> List[MemberTrackBackfillError]:
    update_errors = []
    for result in results:
        try:
            await mono.backfill_member_track_record(
                result.request.member_track_id,
                result.e9y_verification_id,
                result.e9y_member_id,
            )
        except Exception as e:
            update_errors.append(
                MemberTrackBackfillError(
                    request=result.request,
                    reason=f"update member track error: {str(e)}",
                )
            )
    return update_errors


async def _write_back_fill_report(
    results: List[MemberTrackBackfillResult],
    errors: List[MemberTrackBackfillError],
    batch_no: int,
    report_folder: str,
):
    BACKFILL_REPORT_FOLDER = "backfill_member_track_report"
    bucket = settings.GCP().census_file_bucket
    project = settings.GCP().project
    if project and project != "local-dev":
        storage = Storage(project)
        encrypted = True
    else:
        encrypted = False
        storage = LocalStorage("local-dev")
    file_manager = EligibilityFileManager(storage, encrypted)

    # write results
    result_writer = DryRunCsvWriter(fieldnames=MemberTrackBackfillResult.get_csv_cols())
    for result in results:
        result_writer.write_row(result.as_csv_dict())

    await file_manager.put(
        data=result_writer.get_value(),
        name=os.path.join(
            BACKFILL_REPORT_FOLDER, report_folder, f"{batch_no:03d}_results.csv"
        ),
        bucket_name=bucket,
    )

    # write errors
    error_writer = DryRunCsvWriter(fieldnames=MemberTrackBackfillError.get_csv_cols())
    for error in errors:
        error_writer.write_row(error.as_csv_dict())

    await file_manager.put(
        data=error_writer.get_value(),
        name=os.path.join(
            BACKFILL_REPORT_FOLDER, report_folder, f"{batch_no:03d}_errors.csv"
        ),
        bucket_name=bucket,
    )


@contextlib.asynccontextmanager
async def backfill_context():
    """A context-manager for managing global state."""

    pg_connections = postgres_connector.cached_connectors()
    pg_main_connection = pg_connections["main"]

    await pg_main_connection.initialize()
    await mclient.initialize()
    try:
        yield contextvars.copy_context()
    finally:
        await pg_main_connection.close()
        await mclient.teardown()
