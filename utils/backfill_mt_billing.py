from __future__ import annotations

import csv
import datetime
import os
from io import StringIO
from typing import AnyStr, List

from mmlib.ops import log

from app.common.gcs import LocalStorage, Storage
from app.eligibility.gcs import EligibilityFileManager
from config import settings
from db.clients import verification_client
from db.mono import client as mclient
from db.mono.client import MavenMonoClient
from db.mono.model import MemberTrackBackfillRequest
from utils.backfill_member_track import (
    _get_e9y_data,
    _update_member_track_table,
    _write_back_fill_report,
    backfill_context,
)

logger = log.getLogger(__name__)


async def backfill_for_billing(csv_file: str, batch_size: int = 10_000, dry_run=True):
    async with backfill_context():
        mono, verifications = (
            mclient.MavenMonoClient(),
            verification_client.Verifications(),
        )
        batch_no = 1
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_name = _extract_file_name(csv_file)
        report_folder = f"{timestamp}_{str(dry_run)}_billing_{csv_name}"
        member_track_ids = await _read_mt_ids_from_csv(csv_file)
        for batch in batch_iterator(member_track_ids, batch_size):
            requests = await _get_member_track_backfill_requests_for_billing(
                mono, batch
            )
            if not requests:
                logger.info("No data to backfill. Backfill complete.")
                break
            logger.info(f"Found {len(requests)} records to backfill batch #{batch_no}")
            results, errors = await _get_e9y_data(verifications, requests)
            if not dry_run:
                update_errors = await _update_member_track_table(mono, results)
                errors.extend(update_errors)

            await _write_back_fill_report(results, errors, batch_no, report_folder)
            batch_no += 1


def batch_iterator(ids: List[int], batch_size: int):
    for i in range(0, len(ids), batch_size):
        yield ids[i : i + batch_size]


async def _get_member_track_backfill_requests_for_billing(
    mono: MavenMonoClient,
    member_track_ids: List[int],
) -> List[MemberTrackBackfillRequest]:
    return await mono.get_member_track_back_fill_requests_for_billing(
        member_track_ids=member_track_ids
    )


async def _read_mt_ids_from_csv(csv_file: str) -> List[int]:
    bucket = settings.GCP().census_file_bucket
    project = settings.GCP().project
    if project and project != "local-dev":
        storage = Storage(project)
        encrypted = True
    else:
        encrypted = False
        storage = LocalStorage("local-dev")
    file_manager = EligibilityFileManager(storage, encrypted)
    csv_string = await file_manager.get(csv_file, bucket)
    if csv_string is None:
        return []
    return _parse_csv_string(csv_string)


def _parse_csv_string(csv_string: AnyStr) -> List[int]:
    decoded_csv_string = csv_string
    if not isinstance(csv_string, str):
        decoded_csv_string = csv_string.decode("utf-8")
    csv_file = StringIO(decoded_csv_string)
    reader = csv.reader(csv_file)
    next(reader, None)  # Skip the header row
    member_track_ids = [int(row[0]) for row in reader]
    member_track_ids.sort()
    return member_track_ids


def _extract_file_name(full_path: str) -> str:
    return os.path.splitext(os.path.basename(full_path))[0]
