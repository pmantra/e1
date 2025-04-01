from __future__ import annotations

import asyncio
import contextlib
import contextvars
from typing import List

import aiomysql
import orjson
from ddtrace import tracer
from mmlib.ops import log, stats

from app.common import apm
from app.eligibility import translate
from app.utils import format
from db import model
from db.clients import configuration_client, header_aliases_client, postgres_connector
from db.clients.configuration_client import Configurations
from db.clients.header_aliases_client import HeaderAliases
from db.mono import client as mclient
from db.mono.client import MavenMonoClient

RESOURCE = "sync"
logger = log.getLogger(__name__)


def main(batch_size: int = 1_000):
    asyncio.run(sync(batch_size=batch_size))


# region sync mono


@tracer.wrap(service=apm.ApmService.ELIGIBILITY_TASKS, resource=RESOURCE)
async def sync(batch_size: int = 1_000):
    """Synchronize mono organizations with e9y configurations."""
    logger.info("Beginning org-configuration sync.")
    with inflight():
        with stats.timed(metric_name=f"{_STATS_PREFIX}.run", pod_name=_POD):
            async with sync_context():
                configs, header_aliases, mono = (
                    configuration_client.Configurations(),
                    header_aliases_client.HeaderAliases(),
                    mclient.MavenMonoClient(),
                )
                await sync_all_mono_orgs(
                    configs, header_aliases, mono, batch_size=batch_size
                )
                await sync_all_mono_external_ids(configs, mono)

    logger.info("Done.")


@tracer.wrap(service=apm.ApmService.ELIGIBILITY_TASKS, resource=RESOURCE)
async def sync_all_mono_orgs(
    configs: configuration_client.Configurations,
    header_aliases: header_aliases_client.HeaderAliases,
    mono: mclient.MavenMonoClient,
    batch_size: int = 1_000,
):
    """Pull in organization configurations in mono and persist them in e9y."""

    logger.info("Syncing organizations and header mappings to e9y.")
    cursor: aiomysql.Cursor
    async with mono.get_orgs_for_sync_cursor() as cursor:
        seen = set()
        ignored = set()
        while batch := (await cursor.fetchmany(size=batch_size)):
            logger.info("Handling batch of orgs (%s)", len(batch))
            stats.gauge(
                metric_name=f"{_STATS_PREFIX}.orgs.batch.size",
                pod_name=_POD,
                metric_value=len(batch),
            )

            # Extract and manipulate our configs and headers
            with stats.timed(
                metric_name=f"{_STATS_PREFIX}.orgs.batch.extract",
                pod_name=_POD,
            ):
                headers_by_org: dict[int, dict[str, str]] = {}
                orgs = []
                for mono_org in batch:
                    # Decode the email domains to a set of strings.
                    mono_org_json = format.sanitize_json_input(mono_org["json"])
                    mono_org_email_domains = format.sanitize_json_input(
                        mono_org["email_domains"]
                    )
                    mono_org.update(
                        json=orjson.loads(mono_org_json),
                        email_domains={*orjson.loads(mono_org_email_domains)},
                    )

                    # create a config and header mapping
                    config, headers = translate.org_to_config(
                        mclient.MavenOrganization(**mono_org), config_cls=dict
                    )
                    if config["directory_name"] in seen:
                        logger.warning(
                            "Got a duplicated directory name. Skipping Org %s (%s).",
                            mono_org["name"],
                            mono_org["id"],
                        )
                        stats.increment(
                            metric_name="duplicate_org_directory",
                            pod_name=stats.PodNames.CORE_SERVICES,
                        )
                        ignored.add(config["organization_id"])
                        continue
                    seen.add(config["directory_name"])
                    orgs.append(config)
                    headers_by_org[config["organization_id"]] = headers
            logger.info("Done extracting configs and header mappings for batch.")

            if orgs:
                # Save our configs and headers
                with stats.timed(
                    metric_name=f"{_STATS_PREFIX}.orgs.batch.persist",
                    pod_name=_POD,
                ):
                    # First persist the configs (in the event there are new ones)
                    await configs.bulk_persist(data=orgs, coerce=False)
                    # Then persist the headers.
                    await header_aliases.bulk_refresh(
                        headers_by_org.items(), coerce=False
                    )
                logger.info("Done persisting batch.")

    logger.info("Done syncing organizations and header mappings to e9y.")
    return ignored


@tracer.wrap(service=apm.ApmService.ELIGIBILITY_TASKS, resource=RESOURCE)
async def sync_all_mono_external_ids(
    configs: configuration_client.Configurations, mono: mclient.MavenMonoClient
):
    """Pull in external ID configurations from mono and persist them in e9y."""

    logger.info("Synchronizing external IDs for maven organizations.")
    cursor: aiomysql.Cursor

    # Grab all external IDs from mono
    mono_external_ids = await mono.get_all_external_ids_for_sync()

    # Get latest org ids and check external ids
    all_configs: List[model.Configuration] = await configs.all()
    latest_org_ids = set(map(lambda config: config.organization_id, all_configs))
    valid_external_ids = []
    invalid_external_ids = []
    for external_id in mono_external_ids:
        if not external_id.external_id or not bool(external_id.external_id.strip()):
            logger.warning(
                f"external_id for {external_id.organization_id} is empty. skipped",
                organization_id=external_id.organization_id,
            )
            continue
        if external_id.organization_id in latest_org_ids:
            valid_external_ids.append(external_id)
        else:
            invalid_external_ids.append(external_id)

    if len(invalid_external_ids) > 0:
        logger.warning(
            f"{len(invalid_external_ids)} external IDs does not have matching organizations. skipped",
            invalid_external_ids=invalid_external_ids,
        )
        stats.increment(
            metric_name=f"{_STATS_PREFIX}.sync_external_ids.skipped", pod_name=_POD
        )

    # Enter a transaction- delete all existing external_ids and recreate
    # Because of resolution and indexing, it's easier to just drop the externalIDs and recreate
    # This will hopefully be replaced when we move to syncing org_external_ids a little more tactically for our external integrations
    logger.info("Handling batch of external IDs (%s).", len(valid_external_ids))
    stats.gauge(
        metric_name=f"{_STATS_PREFIX}.external_ids.batch.size",
        pod_name=_POD,
        metric_value=len(valid_external_ids),
    )
    with stats.timed(
        metric_name=f"{_STATS_PREFIX}.external_ids.batch.delete_and_recreate",
        pod_name=_POD,
    ):
        try:
            await configs.delete_and_recreate_all_external_ids(valid_external_ids)
            logger.info("Done recreating batch of external IDs.")
        except Exception as e:
            stats.increment(
                metric_name=f"{_STATS_PREFIX}.sync_external_ids.failed", pod_name=_POD
            )
            logger.error("Failed sync external IDs: ", exc_info=e)
    logger.info("Done syncing external IDs to e9y.")


async def sync_single_mono_org_for_directory(
    configuration_client: Configurations,
    header_client: HeaderAliases,
    mono_client: MavenMonoClient,
    directory: str,
):
    """
    Given a directory, sync the associated organization from mono, it's headers, and external_id mappings to e9y

    """

    # Look for an organization associated to a directory.
    logger.info("Looking for organization associated to directory.")
    org = await mono_client.get_org_from_directory(name=directory)

    if not org:
        logger.warning(
            "Couldn't locate an organization associated to the given directory.",
            directory=directory,
        )
        return None

    external_ids = await mono_client.get_org_external_ids_for_org(org_id=org.id)

    configuration, headers = translate.org_to_config(org)
    logger.info(
        "Extracted configuration and header mapping for org.",
        organization_id=configuration.organization_id,
        headers=headers,
    )

    # Persist the configuration and the headers.
    configuration = await configuration_client.persist(model=configuration)
    header_mapping = await header_client.persist_header_mapping(
        configuration.organization_id,
        headers,
    )

    # HACK: disable sync for Optum Employer Provider for now
    if configuration.organization_id != 2865:
        # Remove existing org_external_ids from our DB- we want to ensure that we are getting the most up-to-date values from mono,
        # and it's a pain to try to reconcile if there are differences or records have been removed
        await configuration_client.delete_external_ids_for_org(organization_id=org.id)
        await configuration_client.delete_external_ids_for_data_provider_org(
            data_provider_organization_id=org.id
        )

        if external_ids:
            # Before we try to save any external IDs, ensure we have the source organization created for them
            # If, for example, we are processing a file from a data provider, we need to ensure we have record of
            # the configs for all sub-orgs belonging to that data provider
            for ext_id in external_ids:
                if ext_id.organization_id != org.id:
                    sub_org = await mono_client.get_org_from_id(
                        id=ext_id.organization_id
                    )
                    sub_config, sub_header_mapping = translate.org_to_config(sub_org)
                    await configuration_client.persist(model=sub_config)

            await configuration_client.bulk_add_external_id(external_ids)

        logger.info(
            "Saved configuration and header mapping for org.",
            organization_id=configuration.organization_id,
            directory_name=configuration.directory_name,
            headers=header_mapping,
        )

    return configuration


# endregion


@contextlib.asynccontextmanager
async def sync_context():
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


@contextlib.contextmanager
def inflight():
    """A context for automatically tracking inflight work."""

    stats.increment(metric_name=f"{_STATS_PREFIX}.inflight", pod_name=_POD)
    try:
        yield
    finally:
        stats.decrement(metric_name=f"{_STATS_PREFIX}.inflight", pod_name=_POD)


_STATS_PREFIX = "eligibility.tasks.sync"
_POD = stats.PodNames.CORE_SERVICES.value
