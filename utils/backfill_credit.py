import asyncio
import contextlib
import contextvars
import datetime
import os
from typing import List, Optional, Tuple

from ddtrace import tracer
from mmlib.ops import log, stats

import constants
from app.common import apm
from app.common.gcs import LocalStorage, Storage
from app.dryrun.report import DryRunCsvWriter
from app.eligibility.gcs import EligibilityFileManager
from config import settings
from db.clients import postgres_connector, verification_client
from db.model import EligibilityVerificationForUser
from db.mono import client as mclient
from db.mono.client import MavenMonoClient
from db.mono.model import (
    CreditBackfillError,
    CreditBackfillRequest,
    CreditBackfillResult,
)

RESOURCE = "backfill_credit"
logger = log.getLogger(__name__)

_STATS_PREFIX = "eligibility.backfill.credit"
_POD = constants.POD
_BATCH_SIZE = 10000


def main():
    asyncio.run(backfill(batch_size=_BATCH_SIZE))


# region backfill credit


@tracer.wrap(service=apm.ApmService.ELIGIBILITY_TASKS, resource=RESOURCE)
async def backfill(batch_size: int = _BATCH_SIZE, dry_run=False):
    """
    Backfill eligibility_verification_id and eligibility_member_id since 2023-11
    Can be run in devshell

    from utils import backfill_credit
    await backfill_credit.backfill(batch_size=10, dry_run=True)
    """
    logger.info(f"Beginning backfill credit table with batch_size={batch_size}")
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

                requests = await _get_credit_backfill_requests(
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
                    update_errors = await _update_credit_table(mono, results)
                    errors.extend(update_errors)

                await _write_back_fill_report(results, errors, batch_no, report_folder)
                last_id = max(requests, key=lambda request: request.id).id
                batch_no += 1


async def _get_credit_backfill_requests(
    mono: MavenMonoClient,
    batch_size: int,
    last_id: int,
) -> List[CreditBackfillRequest]:
    return await mono.get_credit_back_fill_requests(
        batch_size=batch_size, last_id=last_id
    )


async def _get_e9y_data(
    verifications: verification_client.Verifications,
    requests: List[CreditBackfillRequest],
) -> Tuple[List[CreditBackfillResult], List[CreditBackfillError]]:
    errors = []
    results = []
    for request in requests:
        records: Optional[
            List[EligibilityVerificationForUser]
        ] = await verifications.get_all_eligibility_verification_record_for_user(
            user_id=request.user_id
        )
        found = None
        reason = None
        if (not records) or (len(records) == 0):
            found = None
            reason = "No Verification Found"
        elif len(records) >= 1:
            filtered = [
                v for v in records if v.organization_id == request.organization_id
            ]
            found = filtered[0] if len(filtered) == 1 else None
            reason = (
                f"{len(records)} Verifications Found, but {len(filtered)} matching organization_id"
                if not found
                else ""
            )

        if found:
            results.append(
                CreditBackfillResult(
                    request=request,
                    e9y_verification_id=found.verification_id,
                    e9y_member_id=found.eligibility_member_id,
                )
            )
        else:
            errors.append(
                CreditBackfillError(
                    request=request,
                    reason=reason,
                )
            )

    return (results, errors)


async def _update_credit_table(
    mono: MavenMonoClient, results: List[CreditBackfillResult]
) -> List[CreditBackfillError]:
    update_errors = []
    for result in results:
        try:
            await mono.backfill_credit_record(
                result.request.id, result.e9y_verification_id, result.e9y_member_id
            )
        except Exception as e:
            update_errors.append(
                CreditBackfillError(
                    request=result.request,
                    reason=f"update credit error: {str(e)}",
                )
            )
    return update_errors


async def _write_back_fill_report(
    results: List[CreditBackfillResult],
    errors: List[CreditBackfillError],
    batch_no: int,
    report_folder: str,
):
    BACKFILL_REPORT_FOLDER = "backfill_credit_report"
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
    result_writer = DryRunCsvWriter(fieldnames=CreditBackfillResult.get_csv_cols())
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
    error_writer = DryRunCsvWriter(fieldnames=CreditBackfillError.get_csv_cols())
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
