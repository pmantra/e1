from __future__ import annotations

import datetime
from typing import List

from mmlib.ops import log

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


async def backfill_for_v2(
    organization_ids: List[int], batch_size: int = 10_0000, dry_run=True
):
    """
    backfill member track for e9y ingestion v2
    backfill member track records that have e9y_verification_id, but no e9y_member_id"""
    async with backfill_context():
        mono, verifications = (
            mclient.MavenMonoClient(),
            verification_client.Verifications(),
        )

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

        for organization_id in organization_ids:
            batch_no = 1
            last_id = 0
            while True:
                report_folder = f"{timestamp}_{str(dry_run)}_v2/{str(organization_id)}"
                requests = await _get_member_track_backfill_requests_for_v2(
                    mono, organization_id, batch_size, last_id
                )
                if not requests:
                    logger.info(
                        f"No data to backfill for organization={organization_id}, batch={batch_no}, last_id= {last_id}. Backfill complete."
                    )
                    break
                logger.info(
                    f"Found {len(requests)} records to backfill for organization={organization_id}, batch={batch_no}, last_id= {last_id}"
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


async def _get_member_track_backfill_requests_for_v2(
    mono: MavenMonoClient,
    organization_id: int,
    batch_size: int,
    last_id: int,
) -> List[MemberTrackBackfillRequest]:
    return await mono.get_member_track_back_fill_requests_for_v2(
        organization_id=organization_id, batch_size=batch_size, last_id=last_id
    )
