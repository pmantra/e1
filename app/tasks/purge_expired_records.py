import asyncio
from typing import Awaitable, List

import structlog
from ddtrace import tracer
from mmlib.ops import stats

import constants
from app.common import apm
from db import model
from db.clients import configuration_client, member_versioned_client, postgres_connector

logger = structlog.getLogger(__name__)
MAX_CONCURRENT = 10


def main():
    return asyncio.run(purge_by_org())


@tracer.wrap(service=apm.ApmService.ELIGIBILITY_TASKS, resource="purge_expired_for_org")
async def purge_expired_for_single_org(
    *,
    organization_id: int,
    members_versioned: member_versioned_client.MembersVersioned,
):
    """Purge expired or invalid records for a single organization"""
    try:
        async with members_versioned.client.connector.transaction() as c:
            logger.info(
                "Starting purge of expired records", organization_id=organization_id
            )
            num_purged: int = await members_versioned.purge_expired_records(
                connection=c, organization_id=organization_id
            )

            stats.increment(
                metric_value=num_purged,
                metric_name="eligibility.tasks.purge_expired_records",
                pod_name=constants.POD,
                tags=[
                    f"organization_id:{organization_id}",
                ],
            )
        logger.info(
            "Expired record purging completed for org",
            organization_id=organization_id,
        )
    except Exception as e:
        logger.exception(
            "Exception encountered while purging records for org",
            organization_id=organization_id,
            error=e,
        )


async def gather_with_concurrency(n, awaitables):
    semaphore = asyncio.Semaphore(n)

    async def with_semaphore(coro):
        async with semaphore:
            return await coro

    return await asyncio.gather(*(with_semaphore(c) for c in awaitables))


@tracer.wrap(service=apm.ApmService.ELIGIBILITY_TASKS, resource="purge_by_org")
async def purge_by_org():
    """Purge expired or invalid records for all organizations - as enabled by feature flag"""
    dsn = postgres_connector.get_dsn()
    pool = postgres_connector.create_pool(dsn=dsn, min_size=10, max_size=20)
    connector = postgres_connector.PostgresConnector(dsn=dsn, pool=pool)
    configs = configuration_client.Configurations(connector=connector)
    member_versioned = member_versioned_client.MembersVersioned(connector=connector)

    all_configs: List[model.Configuration] = await configs.all()

    config: model.Configuration

    tasks_by_org: List[Awaitable] = []
    for config in all_configs:
        # Run our file_purge_logic
        tasks_by_org.append(
            purge_expired_for_single_org(
                organization_id=config.organization_id,
                members_versioned=member_versioned,
            )
        )

    await gather_with_concurrency(MAX_CONCURRENT, tasks_by_org)
