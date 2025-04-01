import asyncio
import hashlib
from typing import Awaitable, List

from ddtrace import tracer
from mmlib.ops import log

from app.common import apm
from db import model
from db.clients import configuration_client, member_versioned_client, postgres_connector

RESOURCE = "purge_duplicates"
MAX_CONCURRENT = 10
logger = log.getLogger(__name__)


async def gather_with_concurrency(n, awaitables):
    semaphore = asyncio.Semaphore(n)

    async def with_semaphore(coro):
        async with semaphore:
            return await coro

    return await asyncio.gather(*(with_semaphore(c) for c in awaitables))


def main():
    return asyncio.run(optum_hash_and_discard_by_org())


@tracer.wrap(service=apm.ApmService.ELIGIBILITY_TASKS, resource=RESOURCE)
async def optum_hash_and_discard_by_org():
    """For Optum organizations, clear out duplicate records and hash records that do not have a hash associated"""
    dsn = postgres_connector.get_dsn()
    pool = postgres_connector.create_pool(dsn=dsn, min_size=10, max_size=20)
    connector = postgres_connector.PostgresConnector(dsn=dsn, pool=pool)
    configs = configuration_client.Configurations(connector=connector)
    member_versioned = member_versioned_client.MembersVersioned(connector=connector)

    optum_configs: List[model.Configuration] = await configs.get_configs_for_optum()

    config: model.Configuration

    tasks_by_org: List[Awaitable] = []
    for config in optum_configs:
        # Run our optum purge/hash
        tasks_by_org.append(
            remove_dupes_and_rehash_for_organization(
                organization_id=config.organization_id,
                members_versioned=member_versioned,
            )
        )

    await gather_with_concurrency(MAX_CONCURRENT, tasks_by_org)


# This method is a copy of the generate_hash_for_external_records method from the utils file
# It has been slightly modified to allow us to perform these actions on a member_versioned record, rather than
# an external record object
#
def generate_hash_for_optum_mv_record(record) -> (str):

    address_string = ""
    if record.get("address_id", None):

        address_components = [
            "" if record["address_1"] is None else record["address_1"],
            "" if record["city"] is None else record["city"],
            "" if record["state"] is None else record["state"],
            "" if record["postal_code"] is None else record["postal_code"],
            "" if record["address_2"] is None else record["address_2"],
            ""
            if record["postal_code_suffix"] is None
            else record["postal_code_suffix"],
            "" if record["country_code"] is None else record["country_code"],
        ]

        address_string = ",".join(address_components)

    # Smoosh all our values together
    raw_string = ",".join(
        [
            record.get("first_name", ""),
            record.get("last_name", ""),
            str(
                record.get("organization_id", ""),
            ),
            record.get("unique_corp_id", ""),
            str(record["date_of_birth"]),
            record["work_state"] if record["work_state"] else "",
            record.get("email", ""),
            record.get("dependent_id", ""),
            str(  # Remove values *we* generate from our hash
                sorted(
                    {
                        k: record["record"][k]
                        for k in record["record"]
                        if k not in ["received_ts", "mvn_batch_record_id"]
                    }
                ),
            ),  # sort our items, so they will always be parsed in the same order
            address_string,
            record["do_not_contact"] if record["do_not_contact"] else "",
            record["gender_code"] if record["gender_code"] else "",
            record["employer_assigned_id"] if record["employer_assigned_id"] else "",
            str(record["effective_range"].upper)
            if record["effective_range"] and record["effective_range"].upper
            else "",
            str(record["effective_range"].lower)
            if record["effective_range"] and record["effective_range"].lower
            else "",
            str(
                sorted(
                    {
                        k: record["custom_attributes"][k]
                        for k in record["custom_attributes"]
                    }
                ),
            )
            if record["custom_attributes"]
            else "",  # sort our items, so they will always be parsed in the same order
        ]
    )

    hash_result = hashlib.sha256(raw_string.encode()).hexdigest()
    hashed_value = ",".join([hash_result, str(record["organization_id"])])
    return hashed_value


@tracer.wrap(service=apm.ApmService.ELIGIBILITY_TASKS, resource=RESOURCE)
def determine_rows_to_hash_and_discard(
    incoming_records,
) -> (list[(str, str)], list[int]):
    hashed_records = {}
    ids_to_purge = []

    for r in incoming_records:
        hash_value = generate_hash_for_optum_mv_record(r)

        if hash_value in hashed_records.keys():
            # See if the conflicting row was created earlier than what we saw previously in our hash dict
            # if so, replace what is in the dict with the new row
            previous_record = hashed_records[hash_value]

            # Want to keep the earlier row, as it represents the first time we saw an incoming hash row
            if r["created_at"] < previous_record["created_at"]:
                hashed_records[hash_value] = r
                ids_to_purge.append(previous_record["id"])

            # Otherwise, mark current row as one needing removal
            else:
                ids_to_purge.append(r["id"])

        else:
            hashed_records[hash_value] = r

    # Invert our mapping of hash:id to id:hash for easier saving later on
    hashed_records = [(v["id"], k) for k, v in hashed_records.items()]

    return hashed_records, ids_to_purge


@tracer.wrap(service=apm.ApmService.ELIGIBILITY_TASKS, resource=RESOURCE)
async def remove_dupes_and_rehash_for_organization(
    organization_id: int, members_versioned: member_versioned_client.MembersVersioned
) -> (int, int):

    # First, grab a set of unhashed rows for an organization
    records_to_hash = await members_versioned.get_values_to_hash_for_org(
        organization_id=organization_id
    )
    logger.info("Determining rows to hash/discard")

    # Then, for these records, determine which represent records we want to persist and those we want to remove
    hashed_records, ids_to_purge = determine_rows_to_hash_and_discard(records_to_hash)

    # Discard the rows we know we want to delete
    records_removed = await members_versioned.purge_duplicate_non_hash_optum(
        member_ids=ids_to_purge
    )
    logger.info(
        f"organization_id removed records: {organization_id} : {records_removed}"
    )

    # logger.info("Updating remaining rows with new hash")

    # # Attempt to update remaining rows with the new hash values
    # (
    #     records_updated,
    #     duplicate_rows_updated,
    # ) = await members_versioned.update_hash_values_for_optum(
    #     records=hashed_records, organization_id=organization_id
    # )
    #
    # logger.info(
    #     "Purged duplicates/hashed existing Optum rows",
    #     organization_id=organization_id,
    #     rows_purged=records_removed,
    #     rows_hashed=records_updated,
    #     duplicate_rows_updated=duplicate_rows_updated,
    # )

    return records_removed, None  # records_updated
