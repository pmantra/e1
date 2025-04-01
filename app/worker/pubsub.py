import os
from typing import Iterable, List, Optional

import structlog.stdlib
import typic
from mmlib.ops import stats
from mmlib.redis.client import make_dsn
from mmstream import pubsub, redis
from split.utils import helper
from structlog.contextvars import bind_contextvars, unbind_contextvars

import constants
from app.eligibility import convert
from app.tasks.sync import sync_single_mono_org_for_directory
from app.utils import async_ttl_cache, utils
from app.utils.eligibility_validation import is_effective_range_activated
from app.utils.utils import detect_and_sanitize_possible_ssn
from app.worker import types
from config import settings
from constants import APP_NAME
from db import model
from db.clients import (
    configuration_client,
    file_client,
    header_aliases_client,
    member_client,
    member_versioned_client,
)
from db.mono import client as mono_client

logger = structlog.getLogger(__name__)
subscriptions = pubsub.PubSubStreams(APP_NAME)
app_settings = settings.App()
gcp_settings = settings.GCP()
redis_settings = settings.Redis()


@subscriptions.consumer(
    gcp_settings.census_file_topic,
    group=gcp_settings.census_file_group,
    model=types.FileUploadNotification,
    auto_create=app_settings.dev_enabled,
)
async def file_notification_handler(
    stream: pubsub.SubscriptionStream[types.FileUploadNotification],
):
    """Create a handler to read in our files and prep them for later consumption"""
    logger.debug("Initializing external connections.")
    files = file_client.Files()
    configs = configuration_client.Configurations()
    headers = header_aliases_client.HeaderAliases()

    mono = mono_client.MavenMonoClient()
    redis_dsn = make_dsn(redis_settings.host, password=redis_settings.password)
    async with redis.RedisStreamPublisher(
        topic="pending-file", name=subscriptions.name, dsn=redis_dsn
    ) as pending_files:
        async for msg in stream:
            bind_contextvars(filename=msg.data.name)
            logger.info("Got a file.")
            directory, file = os.path.split(msg.data.name)
            if not file:
                logger.info("Got a directory. Ignoring.")
                continue

            # Retrieve the configuration from mono associated with this file and persist its associated headers and external_ids
            configuration = await sync_single_mono_org_for_directory(
                configs, headers, mono, directory
            )

            if configuration is None:
                continue

            if helper.is_parent_org(configuration):
                logger.info("Got parent file, split needed. Ignoring.")
                continue

            # Now, handle the file that we need to process
            file = file_client.File(configuration.organization_id, name=msg.data.name)
            file = await files.persist(model=file)
            logger.info(
                "Saved file event record.",
                file_id=file.id,
            )

            # Publish a notification to our downstream consumers.please
            # Use an `async with` context to ensure we tear everything down when we exit.
            response = await pending_files.publish({"file_id": file.id})
            logger.info("Published pending file notification.", response=response)
            unbind_contextvars("filename")
            stats.increment(
                metric_name="eligibility.process.pubsub.file_received",
                pod_name=constants.POD,
                tags=[
                    "eligibility:error",
                    f"organization_id:{configuration.organization_id}",
                ],
            )
            yield file


# endregion


# region record/message handling


@subscriptions.consumer(
    gcp_settings.integrations_topic,
    group=gcp_settings.integrations_group,
    auto_create=app_settings.dev_enabled,
    model=types.ExternalMemberRecord,
    timeoutms=30_000,
)
async def external_record_notification_handler(
    stream: pubsub.SubscriptionStream[types.ExternalMemberRecord],
):
    """Consume member records, translate them to our internal format, and persist to storage"""
    members = member_client.Members()
    members_versioned = member_versioned_client.MembersVersioned()
    configs = configuration_client.Configurations()

    async for messages in stream.next(count=1_000):
        logger.info("Got external records to persist.", num=len(messages))

        # Extract and validate the records w/ attributes attached.
        records = await _extract_records(messages, configs)
        if records == []:
            logger.warning("No valid records in batch.")
            stats.increment(
                metric_name="eligibility.process.pubsub.no_valid_records_in_batch",
                pod_name=constants.POD,
                tags=[
                    "eligibility:error",
                ],
            )
            continue

        (
            persisted_members,
            persisted_addresses,
        ) = await members.bulk_persist_external_records(external_records=records)

        # Unfortunately we cannot filter our hashing logic by org for external records (we get a mix of records),
        # but the logic to insert hashed values should work for orgs where we did not enable the hash values to be generated
        (
            persisted_members_versioned,
            persisted_addresses_versioned,
        ) = await members_versioned.bulk_persist_external_records_hash(
            external_records=records
        )

        logger.info(
            "Persisted member records.",
            num_members=len(persisted_members),
            num_members_versioned=len(persisted_members_versioned),
        )
        logger.info(
            "Persisted member address records.",
            num_addresses=len(persisted_addresses),
            num_addresses_versioned=len(persisted_addresses_versioned),
        )

        # Calculate the number of records we hashed, and therefore did not insert duplicates for
        records = {}
        for m in persisted_members_versioned:
            if m["organization_id"] not in records.keys():
                records[m["organization_id"]] = {"hashed": 0, "new": 0}

            # Hashed records will not have created_at == updated_at
            if m["created_at"] != m["updated_at"]:
                records[m["organization_id"]]["hashed"] += 1

            else:
                records[m["organization_id"]]["new"] += 1
        for k, v in records.items():
            logger.info(
                "Inserted external records",
                organization_id=k,
                hashed_records=v["hashed"],
                new_records=v["new"],
            )

        # Yield out the saved data.
        yield (
            persisted_members,
            persisted_addresses,
            persisted_members_versioned,
            persisted_addresses_versioned,
        )


async def _extract_records(
    messages: Iterable[pubsub.PubSubEntry[types.ExternalMemberRecord]],
    configs: configuration_client.Configurations,
):
    """Take in a message and attempt to massage it to be an internal record and (optionally, depending on input) an internal record address"""

    results = []
    for msg in messages:
        try:
            attributes = typic.transmute(
                types.ExternalMessageAttributes, msg.attributes
            )
        except (KeyError, ValueError):
            logger.exception(
                "Got an external record without required data attributes.",
                unique_corp_id=msg.data.unique_corp_id,
                has_email=bool(msg.data.email),
                attributes=msg.attributes,
            )
            stats.increment(
                metric_name="eligibility.process.pubsub.record_missing_required_attributes",
                pod_name=constants.POD,
                tags=[
                    "eligibility:error",
                ],
            )
            continue

        logger.debug("Got an external record.", record=msg.data)
        msg.data.record.update(attributes)

        # Extract our addresses from our records
        address = dict(typic.iterate(msg.data.address)) if msg.data.address else None

        # Normalize the country code from our address - remember we default to None
        if address:
            if address["country_code"] not in [None, ""]:
                address["country_code"] = convert.to_country_code(
                    address["country_code"]
                )
            else:
                address["country_code"] = ""

        record = dict(typic.iterate(msg.data))
        record = {**record, **attributes}  # combine our records and attributes dicts

        # region external ID mapping
        external_org_info: model.ExternalMavenOrgInfo = (
            await retrieve_external_org_info(
                client_id=msg.data.client_id,
                customer_id=msg.data.customer_id,
                source=msg.attributes["source"],
                configs=configs,
            )
        )

        if not external_org_info:
            logger.debug(
                "Got an external record that does not have any parts of the compound key configured.",
                unique_corp_id=msg.data.unique_corp_id,
                attributes=msg.attributes,
                client_id=record["client_id"],
                customer_id=record["customer_id"],
            )
            stats.increment(
                metric_name="eligibility.process.pubsub.record_missing_configured_external_id",
                pod_name=constants.POD,
                tags=[
                    "eligibility:error",
                ],
            )
            continue

        # check if the pk resembles a ssn
        sanitized_pk, possible_ssn = detect_and_sanitize_possible_ssn(
            input_string=msg.data.unique_corp_id,
            organization_id=external_org_info.organization_id,
            client_id=record["client_id"],
            customer_id=record["customer_id"],
        )

        if sanitized_pk:
            msg.data.unique_corp_id = sanitized_pk
            record["unique_corp_id"] = sanitized_pk
            record["record"]["unique_corp_id"] = sanitized_pk
            record["record"]["id-resembling-hyphenated-ssn"] = True

        # check if organization is inactive
        if external_org_info.activated_at is None:
            logger.warning(
                "Organization is inactive",
                organization_id=external_org_info.organization_id,
            )
            stats.increment(
                metric_name="eligibility.process.pubsub.inactive_organization",
                pod_name=constants.POD,
                tags=[
                    f"org_id:{external_org_info.organization_id}",
                ],
            )

            continue

        # if effective range ends before activated
        if not is_effective_range_activated(
            external_org_info.activated_at, msg.data.effective_range
        ):
            logger.warning(
                "Got an external record with an effective range before configuration/organization activated.",
                unique_corp_id=msg.data.unique_corp_id,
                attributes=msg.attributes,
                client_id=record["client_id"],
                customer_id=record["customer_id"],
                organization_id=external_org_info.organization_id,
                activated_at=external_org_info.activated_at,
                effective_upper=msg.data.effective_range.upper,
            )
            stats.increment(
                metric_name="eligibility.process.pubsub.record_before_activated",
                pod_name=constants.POD,
                tags=[
                    f"org_id:{external_org_info.organization_id}",
                    f"client_id:{record['client_id']}",
                    f"customer_id:{record['customer_id']}",
                ],
            )
            continue

        record["organization_id"] = external_org_info.organization_id

        # endregion

        # Generate unique hash values for our record and address

        (
            record["hash_value"],
            record["hash_version"],
        ) = utils.generate_hash_for_external_record(record, address)

        # Remove values we do not want to save on the final record in our DB
        for val in ["address", "client_id", "customer_id"]:
            record.pop(val)

        results.append(
            {
                "external_record": record,
                "record_address": address,
            }
        )
    return results


async def retrieve_external_org_info(
    *,
    client_id: str,
    customer_id: str,
    source: str,
    configs: configuration_client.Configurations,
) -> Optional[model.ExternalMavenOrgInfo]:

    # Convert our the customerID and clientID to our internally mapped orgID

    external_compound_id = [client_id, customer_id]
    end_idx = len(external_compound_id)
    external_org_id = None

    # Repeatedly search to see if we have a compound key for the combination of our values
    while (end_idx >= 0) and not external_org_id:
        compound_key = ":".join(external_compound_id[0:end_idx])

        # TODO: This will need to be updated to use the data provider ID when we transition optum records over in the external_id table
        external_org_id: List[
            model.ExternalMavenOrgInfo
        ] = await get_cached_external_org_infos_by_value(
            source=source,
            external_id=compound_key,
            configs=configs,
        )
        end_idx -= 1

    if not external_org_id:
        return None

    return external_org_id[0]


@async_ttl_cache.AsyncTTLCache(time_to_live=30 * 60, max_size=1024)
async def get_cached_external_org_infos_by_value(
    source: str,
    external_id: str,
    configs: configuration_client.Configurations,
) -> List[model.ExternalMavenOrgInfo]:
    """TTL Cache of the external ID mapping. Caches up to 1024 of the most
    recent returned values for 30 minutes.
    """
    # TODO: This will need to be updated to use the data provider ID when we transition optum records over in the external_id table
    # once we have provider ID, call `get_external_org_infos_by_value_and_data_provider`
    return await configs.get_external_org_infos_by_value_and_source(
        source=source, external_id=external_id
    )


# end region
