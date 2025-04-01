import datetime

import pytest
from tests.factories import data_models
from tests.factories import data_models as factory

from app.tasks.purge_duplicate_optum_records import (
    determine_rows_to_hash_and_discard,
    generate_hash_for_optum_mv_record,
    remove_dupes_and_rehash_for_organization,
)
from app.utils.utils import generate_hash_for_external_record
from db.model import ExternalRecordAndAddress

pytestmark = pytest.mark.asyncio


async def test_ensure_same_hash_values_generated(
    member_versioned_test_client, external_record
):
    # Given
    address = factory.AddressFactory.create()
    original_hash, original_hash_version = generate_hash_for_external_record(
        external_record, dict(address)
    )
    records = [
        ExternalRecordAndAddress(
            external_record=external_record, record_address=address
        )
    ]

    await member_versioned_test_client.bulk_persist_external_records(
        external_records=records
    )
    records_to_hash = await member_versioned_test_client.get_values_to_hash_for_org(
        organization_id=external_record["organization_id"]
    )

    # When
    regenerated_hash = generate_hash_for_optum_mv_record(records_to_hash[0])

    # Then
    assert original_hash == regenerated_hash


async def test_ensure_same_hash_values_generated_no_address(
    member_versioned_test_client, external_record
):
    # Given
    original_hash, original_hash_version = generate_hash_for_external_record(
        external_record, None
    )
    records = [
        ExternalRecordAndAddress(external_record=external_record, record_address=None)
    ]

    await member_versioned_test_client.bulk_persist_external_records(
        external_records=records
    )
    records_to_hash = await member_versioned_test_client.get_values_to_hash_for_org(
        organization_id=external_record["organization_id"]
    )

    # When
    regenerated_hash = generate_hash_for_optum_mv_record(records_to_hash[0])

    # Then
    assert original_hash == regenerated_hash


async def test_determine_rows_to_hash_and_discard(
    member_versioned_test_client, external_record
):
    # Given
    number_records = 5
    address = factory.AddressFactory.create()

    # Create records one at a time, rather than in bulk
    # Our original query doesn't handle duplicate records in a batch with addresses well
    persisted_rows = []
    for i in range(number_records):
        row = await member_versioned_test_client.bulk_persist_external_records(
            external_records=[
                ExternalRecordAndAddress(
                    external_record=external_record, record_address=address
                )
            ]
        )
        persisted_rows.append(row)

    # We need to set the first record to be the most recently created one
    await member_versioned_test_client.set_created_at(
        id=persisted_rows[0][0][0]["id"],
        created_at=persisted_rows[0][0][0]["created_at"] - datetime.timedelta(days=10),
    )

    records_to_hash = await member_versioned_test_client.get_values_to_hash_for_org(
        organization_id=external_record["organization_id"]
    )

    # When
    hashed_record, rows_to_purge = determine_rows_to_hash_and_discard(records_to_hash)

    # Then
    # Ensure we saved the most recent row to hash
    assert hashed_record[0][0] == persisted_rows[0][0][0]["id"]

    # Ensure we flagged the other rows as needing to be deleted
    assert len(rows_to_purge) == number_records - 1
    for r in persisted_rows[1:]:
        assert r[0][0]["id"] in rows_to_purge


async def test_determine_rows_to_hash_and_discard_multiple_values(
    member_versioned_test_client, external_record
):
    # Given
    number_records = 5
    address = factory.AddressFactory.create()

    # Create records one at at time, rather than in bulk
    # Our original query doesn't handle duplicate records in a batch with addresses well
    persisted_rows_hash_1 = []
    persisted_rows_hash_2 = []
    hash_1_record_to_save, hash_2_record_to_save = None, None

    for i in range(number_records):
        row_hash_1 = await member_versioned_test_client.bulk_persist_external_records(
            external_records=[
                ExternalRecordAndAddress(
                    external_record=external_record, record_address=address
                )
            ]
        )
        persisted_rows_hash_1.append(row_hash_1)
        if not hash_1_record_to_save:
            hash_1_record_to_save = row_hash_1[0][0]

        # Create rows that will have a different hash value
        row_hash_2 = await member_versioned_test_client.bulk_persist_external_records(
            external_records=[
                ExternalRecordAndAddress(
                    external_record=external_record, record_address=None
                )
            ]
        )
        persisted_rows_hash_2.append(row_hash_2)
        if not hash_2_record_to_save:
            hash_2_record_to_save = row_hash_2[0][0]

    # We need to set the first record to be the most recently created one - this should be the one we don't want to delete when we remove non-hashed rows
    await member_versioned_test_client.set_created_at(
        id=hash_1_record_to_save["id"],
        created_at=hash_1_record_to_save["created_at"] - datetime.timedelta(days=10),
    )
    await member_versioned_test_client.set_created_at(
        id=hash_2_record_to_save["id"],
        created_at=hash_2_record_to_save["created_at"] - datetime.timedelta(days=10),
    )

    records_to_hash = await member_versioned_test_client.get_values_to_hash_for_org(
        organization_id=external_record["organization_id"]
    )

    # When
    hashed_record, rows_to_purge = determine_rows_to_hash_and_discard(records_to_hash)

    # Then
    # Ensure we saved the most recent row to hash
    assert len(hashed_record) == 2
    for r in hashed_record:
        assert r[0] in [hash_1_record_to_save["id"], hash_2_record_to_save["id"]]

    # Ensure we flagged the other rows as needing to be deleted
    assert len(rows_to_purge) == 2 * (number_records - 1)
    assert hash_1_record_to_save["id"] not in rows_to_purge
    assert hash_2_record_to_save["id"] not in rows_to_purge


async def test_remove_dupes_and_rehash_for_organization(
    test_config,
    member_versioned_test_client,
    configuration_test_client,
    external_record,
):
    # Given
    # region set up rows to hash/remove
    number_records = 5
    address = factory.AddressFactory.create()

    # Set up a record for this org that has already been hashed - we don't want to overwrite it
    temp_external_record = external_record.copy()
    temp_external_record["first_name"] = temp_external_record["first_name"][::-1]
    (
        temp_external_record["hash_value"],
        temp_external_record["hash_version"],
    ) = generate_hash_for_external_record(temp_external_record, address)

    await member_versioned_test_client.bulk_persist_external_records_hash(
        external_records=[
            ExternalRecordAndAddress(
                external_record=temp_external_record, record_address=address
            )
        ]
    )

    # These are the records we would like to attempt to hash
    hash_0_record_to_save, hash_1_record_to_save = None, None

    # Create records one at a time, rather than in bulk
    # Our original query doesn't handle duplicate records in a batch with addresses well
    for i in range(number_records):
        row_hash_1 = await member_versioned_test_client.bulk_persist_external_records(
            external_records=[
                ExternalRecordAndAddress(
                    external_record=external_record, record_address=address
                )
            ]
        )
        if not hash_0_record_to_save:
            hash_0_record_to_save = row_hash_1[0][0]
            await member_versioned_test_client.set_created_at(
                id=hash_0_record_to_save["id"],
                created_at=hash_0_record_to_save["created_at"]
                - datetime.timedelta(days=10),
            )

        # Create rows that will have a different hash value - the lack of address should generate different results
        row_hash_2 = await member_versioned_test_client.bulk_persist_external_records(
            external_records=[
                ExternalRecordAndAddress(
                    external_record=external_record, record_address=None
                )
            ]
        )
        if not hash_1_record_to_save:
            hash_1_record_to_save = row_hash_2[0][0]
            # We need to set the first record to be the most recently created one - this should be the one we don't want to delete when we remove non-hashed rows
            await member_versioned_test_client.set_created_at(
                id=hash_1_record_to_save["id"],
                created_at=hash_1_record_to_save["created_at"]
                - datetime.timedelta(days=10),
            )

    # endregion set up rows to hash/remove

    # region set up rows to NOT hash/remove
    # Create rows living under a different organization- we should not update them
    org_to_ignore = await configuration_test_client.persist(
        model=data_models.ConfigurationFactory.create()
    )

    # Use a unique hash version number, so we can easily tell that it wasn't updated
    record_with_hash = factory.ExternalRecordFactoryWithHash.create(
        organization_id=org_to_ignore.organization_id, hash_version=-1
    )
    external_record_and_address_with_hash = (
        factory.ExternalRecordAndAddressFactoryWithHash(
            external_record=record_with_hash
        )
    )
    record_with_no_hash = factory.ExternalRecordFactory.create(
        organization_id=org_to_ignore.organization_id
    )
    external_record_and_address_with_no_hash = factory.ExternalRecordAndAddressFactory(
        external_record=record_with_no_hash
    )

    records_to_ignore = [
        external_record_and_address_with_hash,
        external_record_and_address_with_no_hash,
    ]
    saved_records_to_ignore = (
        await member_versioned_test_client.bulk_persist_external_records_hash(
            external_records=records_to_ignore
        )
    )

    # endregion set up rows to NOT hash/remove

    # When
    # Update the values we need to hash
    records_removed, records_updated = await remove_dupes_and_rehash_for_organization(
        organization_id=test_config.organization_id,
        members_versioned=member_versioned_test_client,
    )

    # Then
    # Ensure we didn't touch records not in our org
    for r in saved_records_to_ignore[0]:
        org_to_ignore_results = await member_versioned_test_client.get(r["id"])
        assert org_to_ignore_results.hash_value == r["hash_value"]
        assert org_to_ignore_results.hash_version == r["hash_version"]

    # We persisted 2 * number of records originally-all should have been removed except the ones we intended to hash
    assert records_removed == (2 * number_records) - 2
    # assert records_updated == 2 # Removed while we turn off hashing and only have purging
    org_to_update_results = await member_versioned_test_client.get_for_org(
        organization_id=test_config.organization_id
    )
    assert len(org_to_update_results) == 3

    # Temporarily turned off while we focus on purge
    # Ensure the records we wanted to hash, did indeed hash
    # hash_0_record_result = await member_versioned_test_client.get(
    #     hash_0_record_to_save["id"]
    # )
    # hash_1_record_result = await member_versioned_test_client.get(
    #     hash_1_record_to_save["id"]
    # )
    # assert hash_0_record_result.hash_value is not None
    # assert hash_1_record_result.hash_value is not None
