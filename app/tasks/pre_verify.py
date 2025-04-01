from __future__ import annotations

import asyncio
import time
from typing import Awaitable, List

import structlog
from ddtrace import tracer
from mmlib.ops import stats
from structlog import contextvars

import constants
from app.common import apm
from db import model
from db.clients import (
    configuration_client,
    member_versioned_client,
    postgres_connector,
    verification_client,
)

logger = structlog.getLogger(__name__)
MAX_CONCURRENT = 10


def main(batch_size: int = 10_000):
    return asyncio.run(pre_verify(batch_size=batch_size))


@tracer.wrap(service=apm.ApmService.ELIGIBILITY_TASKS, resource="pre_verify_org")
async def pre_verify_org(
    *,
    organization_id: int,
    members_versioned: member_versioned_client.MembersVersioned,
    verifications: verification_client.Verifications,
    file_id: int | None = None,
    batch_size: int = 10_000,
):
    """Pre-verify records for organization_id"""
    contextvars.bind_contextvars(
        organization_id=organization_id, file_id=file_id, batch_size=batch_size
    )
    pre_verified_count = 0
    # Start the timer
    start_time = time.time()
    while True:
        try:
            logger.info(
                "pre_verify query running",
                batch_size=batch_size,
                pre_verified_count=pre_verified_count,
            )
            async with members_versioned.client.connector.transaction() as c:
                num_pre_verified: int = (
                    await verifications.batch_pre_verify_records_by_org(
                        connection=c,
                        organization_id=organization_id,
                        file_id=file_id,
                        batch_size=batch_size,
                    )
                )
        except Exception as e:
            logger.exception(
                "Exception encountered while processing batch",
                error=e,
            )
        else:
            pre_verified_count += num_pre_verified
            stats.increment(
                metric_value=num_pre_verified,
                metric_name="eligibility.tasks.pre_verify.record_pre_verified",
                pod_name=constants.POD,
                tags=[
                    f"organization_id:{organization_id}",
                ],
            )

        if num_pre_verified == 0:
            break

    # Stop the timer
    end_time = time.time()
    delta = end_time - start_time
    elapsed_time = round(delta, 2)

    logger.info(
        "Pre-verification completed for org",
        count=pre_verified_count,
        elapsed_time=elapsed_time,
    )

    contextvars.unbind_contextvars("organization_id", "file_id", "batch_size")


async def gather_with_concurrency(n, awaitables):
    semaphore = asyncio.Semaphore(n)

    async def with_semaphore(coro):
        async with semaphore:
            return await coro

    return await asyncio.gather(*(with_semaphore(c) for c in awaitables))


@tracer.wrap(service=apm.ApmService.ELIGIBILITY_TASKS, resource="pre_verify")
async def pre_verify(batch_size: int = 10_000):
    """Pre-verify records for all organizations - as enabled by feature flag"""
    dsn = postgres_connector.get_dsn()
    pool = postgres_connector.create_pool(dsn=dsn, min_size=10, max_size=20)
    connector = postgres_connector.PostgresConnector(dsn=dsn, pool=pool)
    configs = configuration_client.Configurations(connector=connector)
    member_versioned = member_versioned_client.MembersVersioned(connector=connector)
    verifications = verification_client.Verifications(connector=connector)

    all_configs: List[model.Configuration] = await configs.all()

    config: model.Configuration

    tasks_by_org: List[Awaitable] = []

    for config in all_configs:
        # Run our pre-verification logic
        tasks_by_org.append(
            pre_verify_org(
                organization_id=config.organization_id,
                members_versioned=member_versioned,
                verifications=verifications,
                batch_size=batch_size,
            )
        )

    await gather_with_concurrency(MAX_CONCURRENT, tasks_by_org)
