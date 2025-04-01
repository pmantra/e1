import asyncio
import logging

from ddtrace import tracer
from mmlib.ops import stats

from app.common import apm, bq
from db.clients import member_client

logger = logging.getLogger("eligibility.jobs.backfill")


def main(batch_size: int = 100_000):
    return asyncio.run(backfill(batch_size=batch_size))


@tracer.wrap(service=apm.ApmService.ELIGIBILITY_JOBS, resource="backfill")
async def backfill(batch_size: int = 100_000):
    members = member_client.Members()
    logger.info("Beginning backfill of eligibility members to Big Query.")
    async with members.iterall(coerce=False) as cursor:
        while batch := (await cursor.fetch(batch_size)):
            stats.gauge(
                metric_name=f"{_STATS_PREFIX}.members.batch.size",
                pod_name=_POD,
                metric_value=len(batch),
            )
            logger.info("Exporting %s records to big query.", len(batch))
            await bq.export_rows_to_table(
                "ao_eligibility_member", [{**r} for r in batch]
            )
    logger.info("Done backfilling eligibility members to Big Query.")


_STATS_PREFIX = logger.name
_POD = stats.PodNames.CORE_SERVICES.value
