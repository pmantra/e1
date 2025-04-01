from __future__ import annotations

import asyncio
import contextlib
import contextvars
import dataclasses
import datetime
import os
from typing import List, Tuple

from ddtrace import tracer
from mmlib.ops import log

from app.common import apm
from app.common.gcs import LocalStorage, Storage
from app.dryrun.report import DryRunCsvWriter
from app.eligibility.gcs import EligibilityFileManager
from config import settings
from db.clients import postgres_connector
from db.mono import client as mclient
from db.mono.client import MavenMonoClient

RESOURCE = "backfill_oed"
logger = log.getLogger(__name__)

_BATCH_SIZE = 10000


@dataclasses.dataclass
class OEDBackfillResult:
    oed_id: int
    reimbursement_wallet_id: int

    @staticmethod
    def get_csv_cols():
        return [
            "oed_id",
            "reimbursement_wallet_id",
        ]

    def as_csv_dict(self):
        return {
            "oed_id": self.oed_id,
            "reimbursement_wallet_id": self.reimbursement_wallet_id,
        }


@dataclasses.dataclass
class OEDBackfillError:
    oed_id: int
    reason: str = ""

    @staticmethod
    def get_csv_cols():
        return ["oed_id", "reason"]

    def as_csv_dict(self):
        return {"oed_id": self.oed_id, "reason": self.reason}


def main():
    asyncio.run(backfill(batch_size=_BATCH_SIZE))


# region backfill oed


@tracer.wrap(service=apm.ApmService.ELIGIBILITY_TASKS, resource=RESOURCE)
async def backfill(batch_size: int = _BATCH_SIZE, dry_run=False):
    """
    Backfill reimbursement_wallet_id for OED table
    Can be run in devshell

    from utils import backfill_oed
    await backfill_oed.backfill(batch_size=10, dry_run=True)
    """
    logger.info(f"Beginning backfill oed table with batch_size={batch_size}")

    async with backfill_context():
        mono = mclient.MavenMonoClient()
        batch_no = 1
        last_id = 0
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        report_folder = f"{timestamp}_{str(dry_run)}"
        while True:

            oed_ids = await _get_oed_backfill_requests(mono, batch_size, last_id)
            if not oed_ids:
                logger.info("No data to backfill. Backfill complete.")
                break
            logger.info(
                f"Found {len(oed_ids)} records to backfill batch #{batch_no}, last_id= {last_id}"
            )
            results, errors = await _get_oed_backfill_data(mono, oed_ids)
            if not dry_run:
                update_errors = await _update_oed_table(mono, results)
                errors.extend(update_errors)

            await _write_back_fill_report(results, errors, batch_no, report_folder)
            last_id = max(oed_ids)
            batch_no += 1


async def _get_oed_backfill_requests(
    mono: MavenMonoClient,
    batch_size: int,
    last_id: int,
) -> List[int]:
    return await mono.get_oed_back_fill_requests(batch_size=batch_size, last_id=last_id)


async def _get_oed_backfill_data(
    mono: MavenMonoClient,
    oed_ids: List[int],
) -> Tuple[List[OEDBackfillResult], List[OEDBackfillError]]:
    errors = []
    results = []
    for id in oed_ids:
        rw_id: int | None = await mono.get_rw_id_for_oed(id=id)
        if rw_id is not None:
            results.append(OEDBackfillResult(oed_id=id, reimbursement_wallet_id=rw_id))
        else:
            errors.append(OEDBackfillError(oed_id=id, reason="No RW ID Found"))
    return (results, errors)


async def _update_oed_table(
    mono: MavenMonoClient, results: List[OEDBackfillResult]
) -> List[OEDBackfillError]:
    update_errors = []
    for result in results:
        try:
            await mono.backfill_oed_record(
                id=result.oed_id, reimbursement_wallet_id=result.reimbursement_wallet_id
            )
        except Exception as e:
            update_errors.append(
                OEDBackfillError(
                    oed_id=result.oed_id,
                    reason=f"update oed error: {str(e)}",
                )
            )
    return update_errors


async def _write_back_fill_report(
    results: List[OEDBackfillResult],
    errors: List[OEDBackfillError],
    batch_no: int,
    report_folder: str,
):
    BACKFILL_REPORT_FOLDER = "backfill_oed_report"
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
    result_writer = DryRunCsvWriter(fieldnames=OEDBackfillResult.get_csv_cols())
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
    error_writer = DryRunCsvWriter(fieldnames=OEDBackfillError.get_csv_cols())
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
