from __future__ import annotations

import copy
import dataclasses
import datetime
from typing import List, Tuple

import aiosql
import asyncpg
import pytest
from tests.factories import data_models as factory
from tests.functional.conftest import NUMBER_TEST_OBJECTS

from app.tasks.purge_duplicate_optum_records import determine_rows_to_hash_and_discard
from app.utils.utils import generate_hash_for_external_record
from db.clients import (
    client,
    configuration_client,
    file_client,
    member_verification_client,
    member_versioned_client,
    verification_client,
)
from db.model import (
    Configuration,
    DateRange,
    ExternalRecordAndAddress,
    MemberVersioned,
    Verification,
)

pytestmark = pytest.mark.asyncio


async def _create_red_and_blue_members(
    num_red: int,
    num_blue: int,
    test_file,
    member_versioned_test_client,
) -> Tuple[List[int], List[int]]:
    # Create red and blue members for our test file
    await member_versioned_test_client.bulk_persist(
        models=factory.MemberVersionedFactory.create_batch(
            num_red,
            organization_id=test_file.organization_id,
            file_id=test_file.id,
            custom_attributes={"color": "red"},
        ),
    )
    await member_versioned_test_client.bulk_persist(
        models=factory.MemberVersionedFactory.create_batch(
            num_blue,
            organization_id=test_file.organization_id,
            file_id=test_file.id,
            custom_attributes={"color": "blue"},
        ),
    )
    red_ids = []
    blue_ids = []
    all_members = await member_versioned_test_client.all()
    for member in all_members:
        member_color = member.custom_attributes.get("color", None)
        if member_color == "red":
            red_ids.append(member.id)
        elif member_color == "blue":
            blue_ids.append(member.id)
    return red_ids, blue_ids


class TestMemberVersionedClient:
    # region fetch tests

    @staticmethod
    async def test_all(
        multiple_test_members_versioned: member_versioned_client.MembersVersioned,
        member_versioned_test_client,
    ):
        # Given
        # We have created 100 members -> one for each of our multiple files. Ensure we have grabbed all of them
        expected_total = NUMBER_TEST_OBJECTS * NUMBER_TEST_OBJECTS

        # When
        all_members = await member_versioned_test_client.all()

        # Then
        # Ensure we have grabbed all members for all files
        assert len(all_members) == expected_total

    @staticmethod
    async def test_get(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        # When
        returned_member = await member_versioned_test_client.get(
            test_member_versioned.id
        )

        # Then
        assert returned_member == test_member_versioned

    @staticmethod
    async def test_get_for_org(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        assert await member_versioned_test_client.get_for_org(
            test_member_versioned.organization_id
        ) == [test_member_versioned]

    @staticmethod
    async def test_get_count_for_org(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        assert (
            await member_versioned_test_client.get_count_for_org(
                test_member_versioned.organization_id
            )
            == 1
        )

    @staticmethod
    async def test_get_counts_for_orgs(
        test_file: file_client.Files, member_versioned_test_client
    ):
        # Given
        # Bulk create members for our test file
        await member_versioned_test_client.bulk_persist(
            models=factory.MemberVersionedFactory.create_batch(
                NUMBER_TEST_OBJECTS,
                organization_id=test_file.organization_id,
                file_id=test_file.id,
            ),
        )

        # When
        member_count = await member_versioned_test_client.get_count_for_org(
            test_file.organization_id
        )

        # Then
        assert member_count == NUMBER_TEST_OBJECTS

    @staticmethod
    @pytest.mark.parametrize(
        argnames="num_red,num_blue",
        argvalues=[
            (10, 0),
            (0, 10),
            (5, 5),
        ],
        ids=["all-red", "all-blue", "half-half"],
    )
    async def test_get_count_for_sub_population_criteria(
        num_red,
        num_blue,
        test_file,
        member_versioned_test_client,
    ):
        # Given
        await _create_red_and_blue_members(
            num_red=num_red,
            num_blue=num_blue,
            test_file=test_file,
            member_versioned_test_client=member_versioned_test_client,
        )

        # When
        red_member_count = await member_versioned_test_client.get_count_for_sub_population_criteria(
            criteria=f"organization_id = {test_file.organization_id} AND custom_attributes->>'color' = 'red' AND effective_range @> CURRENT_DATE"
        )
        blue_member_count = await member_versioned_test_client.get_count_for_sub_population_criteria(
            criteria=f"organization_id = {test_file.organization_id} AND custom_attributes->>'color' = 'blue' AND effective_range @> CURRENT_DATE"
        )

        # Then
        assert red_member_count == num_red
        assert blue_member_count == num_blue

    @staticmethod
    @pytest.mark.parametrize(
        argnames="num_red,num_blue",
        argvalues=[
            (10, 0),
            (0, 10),
            (5, 5),
        ],
        ids=["all-red", "all-blue", "half-half"],
    )
    async def test_get_ids_for_sub_population_criteria(
        num_red,
        num_blue,
        test_file,
        member_versioned_test_client,
    ):
        # Given
        expected_red_ids, expected_blue_ids = await _create_red_and_blue_members(
            num_red=num_red,
            num_blue=num_blue,
            test_file=test_file,
            member_versioned_test_client=member_versioned_test_client,
        )

        # When
        red_member_ids = await member_versioned_test_client.get_ids_for_sub_population_criteria(
            criteria=f"organization_id = {test_file.organization_id} AND custom_attributes->>'color' = 'red' AND effective_range @> CURRENT_DATE"
        )
        blue_member_ids = await member_versioned_test_client.get_ids_for_sub_population_criteria(
            criteria=f"organization_id = {test_file.organization_id} AND custom_attributes->>'color' = 'blue' AND effective_range @> CURRENT_DATE"
        )

        # Then
        assert set(red_member_ids) == set(expected_red_ids)
        assert set(blue_member_ids) == set(expected_blue_ids)

    @staticmethod
    async def test_get_for_file(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        assert await member_versioned_test_client.get_for_file(
            test_member_versioned.file_id
        ) == [test_member_versioned]

    @staticmethod
    async def test_get_for_files(
        multiple_test_file: file_client.File, member_versioned_test_client
    ):
        # Given
        file_member_map = {}
        for f in multiple_test_file:
            member = await member_versioned_test_client.persist(
                model=factory.MemberVersionedFactory.create(
                    organization_id=f.organization_id, file_id=f.id
                )
            )
            file_member_map[f.id] = member

        # When
        returned_members = await member_versioned_test_client.get_for_files(
            *file_member_map.keys()
        )
        returned_member_map = {m.file_id: m for m in returned_members}

        # Then
        assert len(returned_members) == len(multiple_test_file)
        assert file_member_map == returned_member_map

    @staticmethod
    async def test_get_count_for_file(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        assert (
            await member_versioned_test_client.get_count_for_file(
                test_member_versioned.file_id
            )
            == 1
        )

    @staticmethod
    @pytest.mark.parametrize(
        argnames="member_email",
        argvalues=["foo@foobar.com", "fOO@fooBar.com", "foo@foobar.com   "],
        ids=["compliant_email", "email_ignore_case", "email_ignore_whitespace"],
    )
    async def test_get_by_dob_and_email(
        test_file: file_client.File, member_versioned_test_client, member_email
    ):
        # Given
        test_member = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_file.organization_id,
                file_id=test_file.id,
                email=member_email,
            )
        )

        # When
        returned_member = await member_versioned_test_client.get_by_dob_and_email(
            date_of_birth=test_member.date_of_birth, email=test_member.email
        )

        # Then
        assert [test_member] == returned_member

    @staticmethod
    async def test_get_by_dob_and_email_multiple_records_kafka(
        test_config: configuration_client.Configuration,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
    ):
        # Given
        email = "foo@foobar.com"
        test_member_old = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                file_id=None,
                email=email,
            )
        )

        test_member_new = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                file_id=None,
                email=email,
                date_of_birth=test_member_old.date_of_birth,
                unique_corp_id=test_member_old.unique_corp_id,
                dependent_id=test_member_old.dependent_id,
            )
        )

        # Manually set our updated_at timestamp, so we can check to ensure we return the most recent record
        await member_versioned_test_client.set_updated_at(
            id=test_member_new.id,
            updated_at=test_member_old.updated_at + datetime.timedelta(days=10),
        )
        test_member_new = await member_versioned_test_client.get(test_member_new.id)

        # When
        returned_member = await member_versioned_test_client.get_by_dob_and_email(
            date_of_birth=test_member_old.date_of_birth, email=email
        )

        # Then
        assert [test_member_new] == returned_member

    @staticmethod
    async def test_get_by_dob_and_email_multiple_records_file(
        test_config: configuration_client.Configuration,
        file_test_client: file_client.Files,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
    ):
        """Test that the sort on file_id returns the most recent record for file-based records"""
        # Given
        email = "foo@foobar.com"
        old_file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id, name="primary/clean.csv"
            )
        )
        new_file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id, name="primary/clean.csv"
            )
        )
        test_member_old = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                file_id=old_file.id,
                email=email,
            )
        )
        test_member_new = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                file_id=new_file.id,
                email=email,
                date_of_birth=test_member_old.date_of_birth,
                unique_corp_id=test_member_old.unique_corp_id,
                dependent_id=test_member_old.dependent_id,
            )
        )
        # Manually set our updated_at timestamp for the new_file, make sure it is more recent than old_file
        # since file expiration is our last step
        await member_versioned_test_client.set_updated_at(
            id=test_member_old.id,
            updated_at=test_member_new.updated_at + datetime.timedelta(days=10),
        )
        test_member_new = await member_versioned_test_client.get(test_member_new.id)

        # When
        returned_member = await member_versioned_test_client.get_by_dob_and_email(
            date_of_birth=test_member_old.date_of_birth, email=email
        )

        # Then
        assert [test_member_new] == returned_member

    # region secondary verification
    @staticmethod
    @pytest.mark.parametrize(
        argnames="work_state,first_name,last_name",
        argvalues=[
            ("NY", "Alan", "Turing"),
            (None, "Alan", "Turing"),
            ("", "Alan", "Turing"),
            ("NY", "aLaN", "TURING"),
            ("NY", "  Alan  ", " Turing     "),
        ],
        ids=[
            "standard_verification",
            "no_work_state",
            "empty_work_state",
            "ignore_case",
            "ignore_whitespace",
        ],
    )
    async def test_get_by_secondary_verification(
        test_file: file_client.File,
        member_versioned_test_client,
        work_state,
        first_name,
        last_name,
    ):
        # Given
        test_member = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_file.organization_id,
                file_id=test_file.id,
                work_state="NY",
                first_name="Alan",
                last_name="Turing",
            )
        )
        # When
        verified = await member_versioned_test_client.get_by_secondary_verification(
            date_of_birth=test_member.date_of_birth,
            first_name=first_name,
            last_name=last_name,
            work_state=work_state,
        )
        # Then
        assert verified == [test_member]

    @staticmethod
    async def test_get_by_secondary_verification_multiple_records_same_org_identity_kafka(
        test_config: configuration_client.Configuration,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
    ):
        """Test when we have multiple members with same org identity (org_id, unique_corp_id, dependent_id)"""
        # Given
        work_state = "NY"
        first_name = "Alan"
        last_name = "Turing"
        unique_corp_id = "1"
        dependent_id = "1"

        test_member = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                unique_corp_id=unique_corp_id,
                dependent_id=dependent_id,
                file_id=None,
                work_state=work_state,
                first_name=first_name,
                last_name=last_name,
            )
        )

        test_member_new = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                unique_corp_id=unique_corp_id,
                dependent_id=dependent_id,
                file_id=None,
                work_state=work_state,
                first_name=first_name,
                last_name=last_name,
                date_of_birth=test_member.date_of_birth,
            )
        )

        # Manually set our updated_at timestamp, so we can check to ensure we return the most recent record
        await member_versioned_test_client.set_updated_at(
            id=test_member.id, updated_at=datetime.datetime(year=2000, month=12, day=10)
        )
        await member_versioned_test_client.set_updated_at(
            id=test_member_new.id,
            updated_at=datetime.datetime(year=2000, month=12, day=11),
        )
        test_member_new = await member_versioned_test_client.get(test_member_new.id)

        # When
        verified = await member_versioned_test_client.get_by_secondary_verification(
            date_of_birth=test_member.date_of_birth,
            first_name=first_name,
            last_name=last_name,
            work_state=work_state,
        )
        # Then
        assert verified == [test_member_new]

    @staticmethod
    async def test_get_by_secondary_verification_multiple_records_same_org_identity_file(
        test_config: configuration_client.Configuration,
        file_test_client: file_client.Files,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
    ):
        """Test when we have multiple members with same org identity (org_id, unique_corp_id, dependent_id)"""
        # Given
        work_state = "NY"
        first_name = "Alan"
        last_name = "Turing"
        unique_corp_id = "1"
        dependent_id = "1"

        old_file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id, name="primary/clean.csv"
            )
        )
        new_file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id, name="primary/clean.csv"
            )
        )

        test_member_old = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                unique_corp_id=unique_corp_id,
                dependent_id=dependent_id,
                file_id=old_file.id,
                work_state=work_state,
                first_name=first_name,
                last_name=last_name,
            )
        )

        test_member_new = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                unique_corp_id=unique_corp_id,
                dependent_id=dependent_id,
                file_id=new_file.id,
                work_state=work_state,
                first_name=first_name,
                last_name=last_name,
                date_of_birth=test_member_old.date_of_birth,
            )
        )

        # Manually set our updated_at timestamp, older record having a more recent updated_at because of expiration
        await member_versioned_test_client.set_updated_at(
            id=test_member_new.id,
            updated_at=datetime.datetime(year=2000, month=12, day=10),
        )
        await member_versioned_test_client.set_updated_at(
            id=test_member_old.id,
            updated_at=datetime.datetime(year=2000, month=12, day=11),
        )
        test_member_new = await member_versioned_test_client.get(test_member_new.id)

        # When
        verified = await member_versioned_test_client.get_by_secondary_verification(
            date_of_birth=test_member_new.date_of_birth,
            first_name=first_name,
            last_name=last_name,
            work_state=work_state,
        )
        # Then
        assert verified == [test_member_new]

    @staticmethod
    async def test_get_by_secondary_verification_multiple_records_different_org_identity(
        test_file: file_client.File,
        member_versioned_test_client,
    ):
        """Test when we have multiple members with different org identity (org_id, unique_corp_id, dependent_id)"""
        # Given
        first_name = "Alan"
        last_name = "Turing"
        date_of_birth = datetime.date(year=2000, month=5, day=2)
        work_state = "NY"

        test_member_a = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_file.organization_id,
                unique_corp_id="1",
                dependent_id="1",
                file_id=test_file.id,
                work_state=work_state,
                first_name=first_name,
                last_name=last_name,
                date_of_birth=date_of_birth,
            )
        )

        test_member_b = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_file.organization_id,
                unique_corp_id="2",
                dependent_id="2",
                file_id=test_file.id,
                work_state=work_state,
                first_name=first_name,
                last_name=last_name,
                date_of_birth=date_of_birth,
            )
        )

        # When
        matches = await member_versioned_test_client.get_by_secondary_verification(
            date_of_birth=date_of_birth,
            first_name=first_name,
            last_name=last_name,
            work_state=work_state,
        )

        # Then
        assert {m.id for m in matches} == {test_member_a.id, test_member_b.id}

    @staticmethod
    async def test_get_by_secondary_verification_multiple_records_different_org_identity_with_duplicates_kafka(
        test_config: configuration_client.Configuration,
        member_versioned_test_client,
    ):
        """
        Test when we have multiple members with different org identity
        with new and old records per identity (org_id, unique_corp_id, dependent_id)
        """
        # Given
        first_name = "Alan"
        last_name = "Turing"
        date_of_birth = datetime.date(year=2000, month=5, day=2)
        work_state = "NY"

        test_member_a = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                unique_corp_id="1",
                dependent_id="1",
                file_id=None,
                work_state=work_state,
                first_name=first_name,
                last_name=last_name,
                date_of_birth=date_of_birth,
            )
        )

        test_member_a_new = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                unique_corp_id="1",
                dependent_id="1",
                file_id=None,
                work_state=work_state,
                first_name=first_name,
                last_name=last_name,
                date_of_birth=date_of_birth,
            )
        )

        await member_versioned_test_client.set_updated_at(
            id=test_member_a.id,
            updated_at=datetime.datetime(year=2000, month=12, day=10),
        )
        await member_versioned_test_client.set_updated_at(
            id=test_member_a_new.id,
            updated_at=datetime.datetime(year=2000, month=12, day=11),
        )

        test_member_b = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                unique_corp_id="2",
                dependent_id="2",
                file_id=None,
                work_state=work_state,
                first_name=first_name,
                last_name=last_name,
                date_of_birth=date_of_birth,
            )
        )

        test_member_b_new = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                unique_corp_id="2",
                dependent_id="2",
                file_id=None,
                work_state=work_state,
                first_name=first_name,
                last_name=last_name,
                date_of_birth=date_of_birth,
            )
        )

        await member_versioned_test_client.set_updated_at(
            id=test_member_b.id,
            updated_at=datetime.datetime(year=2000, month=12, day=10),
        )
        await member_versioned_test_client.set_updated_at(
            id=test_member_b_new.id,
            updated_at=datetime.datetime(year=2000, month=12, day=11),
        )

        # When
        matches = await member_versioned_test_client.get_by_secondary_verification(
            date_of_birth=date_of_birth,
            first_name=first_name,
            last_name=last_name,
            work_state=work_state,
        )

        # Then
        assert {m.id for m in matches} == {test_member_a_new.id, test_member_b_new.id}

    @staticmethod
    async def test_get_by_secondary_verification_multiple_records_different_org_identity_with_duplicates_file(
        test_config: configuration_client.Configuration,
        file_test_client: file_client.Files,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
    ):
        """
        Test when we have multiple members with different org identity
        with new and old records per identity (org_id, unique_corp_id, dependent_id)
        """
        # Given
        first_name = "Alan"
        last_name = "Turing"
        date_of_birth = datetime.date(year=2000, month=5, day=2)
        work_state = "NY"

        old_file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id, name="primary/clean.csv"
            )
        )
        new_file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id, name="primary/clean.csv"
            )
        )

        test_member_a_old = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                unique_corp_id="1",
                dependent_id="1",
                file_id=old_file.id,
                work_state=work_state,
                first_name=first_name,
                last_name=last_name,
                date_of_birth=date_of_birth,
            )
        )

        test_member_a_new = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                unique_corp_id="1",
                dependent_id="1",
                file_id=new_file.id,
                work_state=work_state,
                first_name=first_name,
                last_name=last_name,
                date_of_birth=date_of_birth,
            )
        )

        await member_versioned_test_client.set_updated_at(
            id=test_member_a_new.id,
            updated_at=datetime.datetime(year=2000, month=12, day=10),
        )
        await member_versioned_test_client.set_updated_at(
            id=test_member_a_old.id,
            updated_at=datetime.datetime(year=2000, month=12, day=11),
        )

        test_member_b_old = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                unique_corp_id="2",
                dependent_id="2",
                file_id=old_file.id,
                work_state=work_state,
                first_name=first_name,
                last_name=last_name,
                date_of_birth=date_of_birth,
            )
        )

        test_member_b_new = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                unique_corp_id="2",
                dependent_id="2",
                file_id=new_file.id,
                work_state=work_state,
                first_name=first_name,
                last_name=last_name,
                date_of_birth=date_of_birth,
            )
        )

        await member_versioned_test_client.set_updated_at(
            id=test_member_b_new.id,
            updated_at=datetime.datetime(year=2000, month=12, day=10),
        )
        await member_versioned_test_client.set_updated_at(
            id=test_member_b_old.id,
            updated_at=datetime.datetime(year=2000, month=12, day=11),
        )

        # When
        matches = await member_versioned_test_client.get_by_secondary_verification(
            date_of_birth=date_of_birth,
            first_name=first_name,
            last_name=last_name,
            work_state=work_state,
        )

        # Then
        assert {m.id for m in matches} == {test_member_a_new.id, test_member_b_new.id}

    @staticmethod
    async def test_get_by_secondary_verification_multiple_records_different_org_identity_with_duplicates_file_and_kafka(
        test_config: configuration_client.Configuration,
        file_test_client: file_client.Files,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
    ):
        """
        Test when we have multiple members with different org identity, and different sources (file, kafka)
        with new and old records per identity (org_id, unique_corp_id, dependent_id)
        """
        # Given
        first_name = "Alan"
        last_name = "Turing"
        date_of_birth = datetime.date(year=2000, month=5, day=2)
        work_state = "NY"

        test_member_a_old_kafka = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                unique_corp_id="1",
                dependent_id="1",
                file_id=None,
                work_state=work_state,
                first_name=first_name,
                last_name=last_name,
                date_of_birth=date_of_birth,
            )
        )

        test_member_a_new_kafka = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                unique_corp_id="1",
                dependent_id="1",
                file_id=None,
                work_state=work_state,
                first_name=first_name,
                last_name=last_name,
                date_of_birth=date_of_birth,
            )
        )

        await member_versioned_test_client.set_updated_at(
            id=test_member_a_old_kafka.id,
            updated_at=datetime.datetime(year=2000, month=12, day=10),
        )
        await member_versioned_test_client.set_updated_at(
            id=test_member_a_new_kafka.id,
            updated_at=datetime.datetime(year=2000, month=12, day=11),
        )

        old_file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id, name="primary/clean.csv"
            )
        )
        new_file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id, name="primary/clean.csv"
            )
        )

        test_member_b_old_file = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                unique_corp_id="2",
                dependent_id="2",
                file_id=old_file.id,
                work_state=work_state,
                first_name=first_name,
                last_name=last_name,
                date_of_birth=date_of_birth,
            )
        )

        test_member_b_new_file = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                unique_corp_id="2",
                dependent_id="2",
                file_id=new_file.id,
                work_state=work_state,
                first_name=first_name,
                last_name=last_name,
                date_of_birth=date_of_birth,
            )
        )

        await member_versioned_test_client.set_updated_at(
            id=test_member_b_new_file.id,
            updated_at=datetime.datetime(year=2000, month=12, day=10),
        )
        await member_versioned_test_client.set_updated_at(
            id=test_member_b_old_file.id,
            updated_at=datetime.datetime(year=2000, month=12, day=11),
        )

        # When
        matches = await member_versioned_test_client.get_by_secondary_verification(
            date_of_birth=date_of_birth,
            first_name=first_name,
            last_name=last_name,
            work_state=work_state,
        )

        # Then
        assert {m.id for m in matches} == {
            test_member_a_new_kafka.id,
            test_member_b_new_file.id,
        }

    @staticmethod
    async def test_get_by_secondary_verification_multiple_records_different_org_identity_with_duplicates_multi_org(
        member_versioned_test_client, configuration_test_client
    ):
        """
        Test when we have multiple members with different org identity
        with new and old records per identity across different orgs (org_id, unique_corp_id, dependent_id)
        """
        # Given
        first_name = "Alan"
        last_name = "Turing"
        date_of_birth = datetime.date(year=2000, month=5, day=2)
        work_state = "NY"

        org_a = await configuration_test_client.persist(
            model=factory.ConfigurationFactory.create()
        )
        org_b = await configuration_test_client.persist(
            model=factory.ConfigurationFactory.create()
        )

        test_member_org_a = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=org_a.organization_id,
                unique_corp_id="1",
                dependent_id="1",
                file_id=None,
                work_state=work_state,
                first_name=first_name,
                last_name=last_name,
                date_of_birth=date_of_birth,
            )
        )

        test_member_org_a_new = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=org_a.organization_id,
                unique_corp_id="1",
                dependent_id="1",
                file_id=None,
                work_state=work_state,
                first_name=first_name,
                last_name=last_name,
                date_of_birth=date_of_birth,
            )
        )

        await member_versioned_test_client.set_updated_at(
            id=test_member_org_a.id,
            updated_at=datetime.datetime(year=2000, month=12, day=10),
        )
        await member_versioned_test_client.set_updated_at(
            id=test_member_org_a_new.id,
            updated_at=datetime.datetime(year=2000, month=12, day=11),
        )

        test_member_org_b = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=org_b.organization_id,
                unique_corp_id="2",
                dependent_id="2",
                file_id=None,
                work_state=work_state,
                first_name=first_name,
                last_name=last_name,
                date_of_birth=date_of_birth,
            )
        )

        test_member_org_b_new = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=org_b.organization_id,
                unique_corp_id="2",
                dependent_id="2",
                file_id=None,
                work_state=work_state,
                first_name=first_name,
                last_name=last_name,
                date_of_birth=date_of_birth,
            )
        )

        await member_versioned_test_client.set_updated_at(
            id=test_member_org_b.id,
            updated_at=datetime.datetime(year=2000, month=12, day=10),
        )
        await member_versioned_test_client.set_updated_at(
            id=test_member_org_b_new.id,
            updated_at=datetime.datetime(year=2000, month=12, day=11),
        )

        # When
        matches = await member_versioned_test_client.get_by_secondary_verification(
            date_of_birth=date_of_birth,
            first_name=first_name,
            last_name=last_name,
            work_state=work_state,
        )

        # Then
        assert {m.id for m in matches} == {
            test_member_org_a_new.id,
            test_member_org_b_new.id,
        }

    # endregion secondary verification

    # region tertiary verification
    @staticmethod
    async def test_get_by_tertiary_verification(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        # When
        verified = await member_versioned_test_client.get_by_tertiary_verification(
            date_of_birth=test_member_versioned.date_of_birth,
            unique_corp_id=test_member_versioned.unique_corp_id,
        )
        # Then
        assert verified == [test_member_versioned]

    @staticmethod
    async def test_get_by_email_and_name(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        # When
        verified = await member_versioned_test_client.get_by_email_and_name(
            email=test_member_versioned.email,
            first_name=test_member_versioned.first_name,
            last_name=test_member_versioned.last_name,
        )
        # Then
        assert verified == [test_member_versioned]

    @staticmethod
    async def test_get_by_tertiary_verification_multiple_external_ids(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
        configuration_test_client,
        faker,
    ):
        # Given
        # Multiple external IDs for the same organization.
        await configuration_test_client.add_external_id(
            organization_id=test_member_versioned.organization_id,
            source=faker.domain_word(),
            external_id=faker.swift11(),
        )
        await configuration_test_client.add_external_id(
            organization_id=test_member_versioned.organization_id,
            source=faker.domain_word(),
            external_id=faker.swift11(),
        )
        # When
        verified = await member_versioned_test_client.get_by_tertiary_verification(
            date_of_birth=test_member_versioned.date_of_birth,
            unique_corp_id=test_member_versioned.unique_corp_id,
        )
        # Then
        # We still resolve to a single member record.
        assert verified == [test_member_versioned]

    @staticmethod
    async def test_get_by_tertiary_verification_multiple_records(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        # Given
        test_member_versioned_new = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_member_versioned.organization_id,
                file_id=test_member_versioned.file_id,
                work_state=test_member_versioned.work_state,
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                date_of_birth=test_member_versioned.date_of_birth,
                unique_corp_id=test_member_versioned.unique_corp_id,
            )
        )

        # Manually set our updated at timestamp, so we can check to ensure we return the most recent record
        await member_versioned_test_client.set_updated_at(
            id=test_member_versioned_new.id,
            updated_at=test_member_versioned.created_at + datetime.timedelta(days=10),
        )
        test_member_new = await member_versioned_test_client.get(
            test_member_versioned_new.id
        )

        # When
        verified = await member_versioned_test_client.get_by_tertiary_verification(
            date_of_birth=test_member_versioned.date_of_birth,
            unique_corp_id=test_member_versioned.unique_corp_id,
        )
        # Then
        assert verified == [test_member_new]

    @staticmethod
    async def test_get_by_tertiary_verification_multiple_records_file(
        test_config: configuration_client.Configuration,
        file_test_client: file_client.Files,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
    ):
        # Given
        old_file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id, name="primary/clean.csv"
            )
        )
        new_file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id, name="primary/clean.csv"
            )
        )
        test_member_old = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                file_id=old_file.id,
            )
        )
        test_member_new = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                file_id=new_file.id,
                work_state=test_member_old.work_state,
                first_name=test_member_old.first_name,
                last_name=test_member_old.last_name,
                date_of_birth=test_member_old.date_of_birth,
                unique_corp_id=test_member_old.unique_corp_id,
            )
        )

        # Manually set our updated at timestamp, so we can check to ensure we return the most recent record
        await member_versioned_test_client.set_updated_at(
            id=test_member_old.id,
            updated_at=test_member_new.updated_at + datetime.timedelta(days=10),
        )
        test_member_new = await member_versioned_test_client.get(test_member_new.id)

        # When
        verified = await member_versioned_test_client.get_by_tertiary_verification(
            date_of_birth=test_member_new.date_of_birth,
            unique_corp_id=test_member_new.unique_corp_id,
        )
        # Then
        assert verified == [test_member_new]

    # endregion tertiary verification

    # region any verification
    @staticmethod
    async def test_get_by_any_verification(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        # When
        verified_all = await member_versioned_test_client.get_by_any_verification(
            date_of_birth=test_member_versioned.date_of_birth,
            first_name=test_member_versioned.first_name,
            last_name=test_member_versioned.last_name,
            work_state=test_member_versioned.work_state,
            email=test_member_versioned.email,
        )
        verified_email = await member_versioned_test_client.get_by_any_verification(
            date_of_birth=test_member_versioned.date_of_birth,
            email=test_member_versioned.email,
        )
        verified_name_state = (
            await member_versioned_test_client.get_by_any_verification(
                date_of_birth=test_member_versioned.date_of_birth,
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                work_state=test_member_versioned.work_state,
            )
        )
        # Then
        assert (
            verified_all
            == verified_email
            == verified_name_state
            == test_member_versioned
        )

    @staticmethod
    async def test_get_by_any_verification_multiple_records_file(
        test_config: configuration_client.Configuration,
        file_test_client: file_client.Files,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
    ):
        # Given
        old_file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id, name="primary/clean.csv"
            )
        )
        new_file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id, name="primary/clean.csv"
            )
        )
        test_member_old = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                file_id=old_file.id,
            )
        )
        test_member_new = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_member_old.organization_id,
                file_id=new_file.id,
                work_state=test_member_old.work_state,
                first_name=test_member_old.first_name,
                last_name=test_member_old.last_name,
                date_of_birth=test_member_old.date_of_birth,
                unique_corp_id=test_member_old.unique_corp_id,
                email=test_member_old.email,
            )
        )

        # Manually set our updated at timestamp, so we can check to ensure we return the most recent record
        await member_versioned_test_client.set_updated_at(
            id=test_member_old.id,
            updated_at=test_member_new.updated_at + datetime.timedelta(days=10),
        )
        test_member_versioned_new = await member_versioned_test_client.get(
            test_member_new.id
        )

        # When
        verified_all = await member_versioned_test_client.get_by_any_verification(
            date_of_birth=test_member_old.date_of_birth,
            first_name=test_member_old.first_name,
            last_name=test_member_old.last_name,
            work_state=test_member_old.work_state,
            email=test_member_old.email,
        )
        verified_email = await member_versioned_test_client.get_by_any_verification(
            date_of_birth=test_member_old.date_of_birth,
            email=test_member_old.email,
        )
        verified_name_state = (
            await member_versioned_test_client.get_by_any_verification(
                date_of_birth=test_member_old.date_of_birth,
                first_name=test_member_old.first_name,
                last_name=test_member_old.last_name,
                work_state=test_member_old.work_state,
            )
        )
        # Then
        assert (
            verified_all
            == verified_email
            == verified_name_state
            == test_member_versioned_new
        )

    @staticmethod
    async def test_get_by_any_verification_multiple_records_kafka(
        test_config: configuration_client.Configuration,
        file_test_client: file_client.Files,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
    ):
        # Given
        test_member_old = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                file_id=None,
            )
        )
        test_member_new = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_member_old.organization_id,
                file_id=None,
                work_state=test_member_old.work_state,
                first_name=test_member_old.first_name,
                last_name=test_member_old.last_name,
                date_of_birth=test_member_old.date_of_birth,
                unique_corp_id=test_member_old.unique_corp_id,
                email=test_member_old.email,
            )
        )

        # Manually set our updated at timestamp, so we can check to ensure we return the most recent record
        await member_versioned_test_client.set_updated_at(
            id=test_member_new.id,
            updated_at=test_member_old.updated_at + datetime.timedelta(days=10),
        )
        test_member_versioned_new = await member_versioned_test_client.get(
            test_member_new.id
        )

        # When
        verified_all = await member_versioned_test_client.get_by_any_verification(
            date_of_birth=test_member_old.date_of_birth,
            first_name=test_member_old.first_name,
            last_name=test_member_old.last_name,
            work_state=test_member_old.work_state,
            email=test_member_old.email,
        )
        verified_email = await member_versioned_test_client.get_by_any_verification(
            date_of_birth=test_member_old.date_of_birth,
            email=test_member_old.email,
        )
        verified_name_state = (
            await member_versioned_test_client.get_by_any_verification(
                date_of_birth=test_member_old.date_of_birth,
                first_name=test_member_old.first_name,
                last_name=test_member_old.last_name,
                work_state=test_member_old.work_state,
            )
        )
        # Then
        assert (
            verified_all
            == verified_email
            == verified_name_state
            == test_member_versioned_new
        )

    @staticmethod
    async def test_get_by_any_verification_multiple_records(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        # Given
        test_member_versioned_new = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_member_versioned.organization_id,
                file_id=test_member_versioned.file_id,
                work_state=test_member_versioned.work_state,
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                date_of_birth=test_member_versioned.date_of_birth,
                unique_corp_id=test_member_versioned.unique_corp_id,
                email=test_member_versioned.email,
            )
        )

        # Manually set our updated at timestamp, so we can check to ensure we return the most recent record
        await member_versioned_test_client.set_updated_at(
            id=test_member_versioned_new.id,
            updated_at=test_member_versioned.updated_at + datetime.timedelta(days=10),
        )
        test_member_versioned_new = await member_versioned_test_client.get(
            test_member_versioned_new.id
        )

        # When
        verified_all = await member_versioned_test_client.get_by_any_verification(
            date_of_birth=test_member_versioned.date_of_birth,
            first_name=test_member_versioned.first_name,
            last_name=test_member_versioned.last_name,
            work_state=test_member_versioned.work_state,
            email=test_member_versioned.email,
        )
        verified_email = await member_versioned_test_client.get_by_any_verification(
            date_of_birth=test_member_versioned.date_of_birth,
            email=test_member_versioned.email,
        )
        verified_name_state = (
            await member_versioned_test_client.get_by_any_verification(
                date_of_birth=test_member_versioned.date_of_birth,
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                work_state=test_member_versioned.work_state,
            )
        )
        # Then
        assert (
            verified_all
            == verified_email
            == verified_name_state
            == test_member_versioned_new
        )

    # endregion any verification

    # region overeligiblity

    @staticmethod
    @pytest.mark.parametrize(
        argnames="first_name,last_name",
        argvalues=[
            ("Alan", "Turing"),
            ("aLaN", "TURING"),
            ("  Alan  ", " Turing     "),
        ],
        ids=[
            "standard_verification",
            "ignore_case",
            "ignore_whitespace",
        ],
    )
    async def test_get_by_overeligiblity(
        test_file: file_client.File,
        member_versioned_test_client,
        test_member_versioned,
        first_name,
        last_name,
    ):
        # Given
        test_member = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_file.organization_id,
                file_id=test_file.id,
                first_name="Alan",
                last_name="Turing",
                date_of_birth=test_member_versioned.date_of_birth,
            )
        )
        # When
        verified = await member_versioned_test_client.get_by_overeligibility(
            date_of_birth=test_member.date_of_birth,
            first_name=first_name,
            last_name=last_name,
        )
        # Then
        assert verified == [test_member]

    @staticmethod
    async def test_get_by_overeligiblity_multiple_records_different_org_same_info(
        test_config: configuration_client.Configuration,
        configuration_test_client,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        test_member_versioned,
    ):
        """Test when we have multiple members with same identity (fn, ln, dob)"""
        # Given
        test_config_2 = await configuration_test_client.persist(
            model=factory.ConfigurationFactory.create()
        )
        test_member_2 = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config_2.organization_id,
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                date_of_birth=test_member_versioned.date_of_birth,
            )
        )

        # When
        verified = await member_versioned_test_client.get_by_overeligibility(
            date_of_birth=test_member_2.date_of_birth,
            first_name=test_member_2.first_name,
            last_name=test_member_2.last_name,
        )
        # Then
        assert len(verified) == 2
        assert test_member_2 in verified
        assert test_member_versioned in verified

    @staticmethod
    async def test_get_by_overeligiblity_multiple_records_same_org(
        test_config: configuration_client.Configuration,
        file_test_client: file_client.Files,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
    ):
        """Test when we have multiple members with same identifying information in an org- return the most recent one"""
        # Given
        first_name = "Alan"
        last_name = "Turing"
        unique_corp_id = "1"
        dependent_id = "1"

        old_file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id, name="primary/clean.csv"
            )
        )
        new_file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id, name="primary/clean.csv"
            )
        )

        test_member_old = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                unique_corp_id=unique_corp_id,
                dependent_id=dependent_id,
                file_id=old_file.id,
                first_name=first_name,
                last_name=last_name,
            )
        )

        test_member_new = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                unique_corp_id=unique_corp_id,
                dependent_id=dependent_id,
                file_id=new_file.id,
                first_name=first_name,
                last_name=last_name,
                date_of_birth=test_member_old.date_of_birth,
            )
        )

        # Manually set our updated_at timestamp, older record having a more recent updated_at because of expiration
        await member_versioned_test_client.set_updated_at(
            id=test_member_new.id,
            updated_at=datetime.datetime(year=2000, month=12, day=10),
        )
        await member_versioned_test_client.set_updated_at(
            id=test_member_old.id,
            updated_at=datetime.datetime(year=2000, month=12, day=11),
        )
        test_member_new = await member_versioned_test_client.get(test_member_new.id)

        # When
        verified = await member_versioned_test_client.get_by_overeligibility(
            date_of_birth=test_member_new.date_of_birth,
            first_name=first_name,
            last_name=last_name,
        )
        # Then
        assert verified == [test_member_new]

    @staticmethod
    async def test_get_by_overeligiblity_multiple_orgs_older_record_same_org(
        test_config: configuration_client.Configuration,
        configuration_test_client,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        test_member_versioned,
    ):
        """Test when we have multiple members with same identity (fn, ln, dob) - more than one result per org"""
        # Given

        # Set up the record that will live in the same org
        test_config_2 = await configuration_test_client.persist(
            model=factory.ConfigurationFactory.create()
        )
        _ = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config_2.organization_id,
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                date_of_birth=test_member_versioned.date_of_birth,
            )
        )

        test_config_1_new_member = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                unique_corp_id=test_member_versioned.unique_corp_id,
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                date_of_birth=test_member_versioned.date_of_birth,
            )
        )

        # Manually set our updated_at timestamp, older record having a more recent updated_at because of expiration
        await member_versioned_test_client.set_updated_at(
            id=test_config_1_new_member.id,
            updated_at=datetime.datetime(year=2000, month=12, day=10),
        )
        await member_versioned_test_client.set_updated_at(
            id=test_member_versioned.id,
            updated_at=datetime.datetime(year=2000, month=12, day=9),
        )

        # When
        verified = await member_versioned_test_client.get_by_overeligibility(
            date_of_birth=test_config_1_new_member.date_of_birth,
            first_name=test_member_versioned.first_name,
            last_name=test_member_versioned.last_name,
        )
        # Then

        assert len(verified) == 2
        assert test_member_versioned not in verified

    # endregion overeligibility
    @staticmethod
    @pytest.mark.parametrize(
        argnames="range,filtered",
        argvalues=[
            # not_ready_no_upper_range_filtered
            (
                asyncpg.Range(
                    lower=datetime.datetime.today() + datetime.timedelta(days=3)
                ),
                True,
            ),
            (  # not_ready_upper_range_filtered
                asyncpg.Range(
                    lower=datetime.date.today() + datetime.timedelta(days=3),
                    upper=datetime.date.today() + datetime.timedelta(days=100),
                ),
                True,
            ),
            # expired_no_lower_range_filtered
            (
                asyncpg.Range(upper=datetime.date.today() - datetime.timedelta(days=3)),
                True,
            ),
            (  # expired_range_filtered
                asyncpg.Range(
                    lower=datetime.date.today() - datetime.timedelta(days=100),
                    upper=datetime.date.today() - datetime.timedelta(days=3),
                ),
                True,
            ),
            # current_not_filtered
            (
                asyncpg.Range(
                    lower_inc=datetime.date(1970, 1, 1),
                    upper_inc=datetime.date(1970, 1, 1),
                ),
                False,
            ),
            (  # current_expires_today_not_filtered
                asyncpg.Range(
                    lower=datetime.date.today() - datetime.timedelta(days=100),
                    upper_inc=datetime.date.today(),
                ),
                False,
            ),
            (  # current_expires_100_days_not_filtered
                asyncpg.Range(
                    lower=datetime.date.today() - datetime.timedelta(days=100),
                    upper_inc=datetime.date.today() + datetime.timedelta(days=100),
                ),
                False,
            ),
        ],
        ids=[
            "not_ready_no_upper_range_filtere",
            "not_ready_upper_range_filtered",
            "expired_no_lower_range_filtered",
            "expired_range_filtered",
            "current_not_filtered",
            "current_expires_today_not_filtered",
            "current_expires_100_days_not_filtered",
        ],
    )
    async def test_effective_range_filtering(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
        range,
        filtered,
    ):
        # Given
        previous_match = await member_versioned_test_client.get_by_any_verification(
            test_member_versioned.date_of_birth,
            first_name=test_member_versioned.first_name,
            last_name=test_member_versioned.last_name,
            work_state=test_member_versioned.work_state,
            email=test_member_versioned.email,
        )
        expected_match = (
            None
            if filtered
            else dataclasses.replace(test_member_versioned, effective_range=range)
        )
        await member_versioned_test_client.set_effective_range(
            id=test_member_versioned.id, range=range
        )
        # When
        current_match = await member_versioned_test_client.get_by_any_verification(
            test_member_versioned.date_of_birth,
            first_name=test_member_versioned.first_name,
            last_name=test_member_versioned.last_name,
            work_state=test_member_versioned.work_state,
            email=test_member_versioned.email,
        )
        # Then
        assert previous_match == test_member_versioned
        assert current_match == expected_match

    @staticmethod
    async def test_get_by_client_specific_verification(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        # When
        verified = (
            await member_versioned_test_client.get_by_client_specific_verification(
                organization_id=test_member_versioned.organization_id,
                unique_corp_id=test_member_versioned.unique_corp_id,
                date_of_birth=test_member_versioned.date_of_birth,
            )
        )
        # Then
        assert verified == test_member_versioned

    @staticmethod
    async def test_get_by_client_specific_verification_multiple_records(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        # Given
        test_member_versioned_new = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                file_id=test_member_versioned.file_id,
                organization_id=test_member_versioned.organization_id,
                date_of_birth=test_member_versioned.date_of_birth,
                unique_corp_id=test_member_versioned.unique_corp_id,
            )
        )

        # Manually set our updated at timestamp, so we can check to ensure we return the most recent record
        await member_versioned_test_client.set_updated_at(
            id=test_member_versioned_new.id,
            updated_at=test_member_versioned.updated_at + datetime.timedelta(days=10),
        )
        test_member_versioned_new = await member_versioned_test_client.get(
            test_member_versioned_new.id
        )

        # When
        verified = (
            await member_versioned_test_client.get_by_client_specific_verification(
                organization_id=test_member_versioned.organization_id,
                unique_corp_id=test_member_versioned.unique_corp_id,
                date_of_birth=test_member_versioned.date_of_birth,
            )
        )
        # Then
        assert verified == test_member_versioned_new

    @staticmethod
    async def test_get_by_org_identity(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        # Given
        identity = test_member_versioned.identity()
        # When
        match = await member_versioned_test_client.get_by_org_identity(identity)
        # Then
        assert match == test_member_versioned

    @staticmethod
    async def test_get_members_for_unique_corp_id_with_one_record(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        # Given
        unique_corp_id = test_member_versioned.unique_corp_id
        # When
        match = await member_versioned_test_client.get_members_for_unique_corp_id(
            unique_corp_id=unique_corp_id
        )

        # Then
        assert match[0]["unique_corp_id"] == test_member_versioned.unique_corp_id

    @staticmethod
    async def test_get_members_for_unique_corp_id_with_multiple_records(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        # Given
        count = 10
        unique_corp_id = test_member_versioned.unique_corp_id
        file_id = test_member_versioned.file_id
        organization_id = test_member_versioned.organization_id
        # define one unique_corp_id and create count number of member records with identical unique_corp_id
        await member_versioned_test_client.bulk_persist(
            models=factory.MemberVersionedFactory.create_batch(
                count,
                organization_id=organization_id,
                file_id=file_id,
                unique_corp_id=unique_corp_id,
            )
        )

        # When
        match = await member_versioned_test_client.get_members_for_unique_corp_id(
            unique_corp_id=unique_corp_id
        )
        # Then
        assert len(match) == count + 1

    # Query a modified_unique_corp_id will return no records
    @staticmethod
    async def test_get_members_for_unique_corp_id_with_no_record(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        # Given
        count = 10
        unique_corp_id = test_member_versioned.unique_corp_id
        file_id = test_member_versioned.file_id
        organization_id = test_member_versioned.organization_id
        await member_versioned_test_client.bulk_persist(
            models=factory.MemberVersionedFactory.create_batch(
                count,
                organization_id=organization_id,
                file_id=file_id,
                unique_corp_id=unique_corp_id,
            )
        )
        # modify the original unique_corp_id to ensure no records in the table
        modified_unique_corp_id = unique_corp_id + "not_exist"

        # When
        match = await member_versioned_test_client.get_members_for_unique_corp_id(
            unique_corp_id=modified_unique_corp_id
        )
        # Then
        assert len(match) == 0

    @staticmethod
    async def test_get_all_by_name_and_date_of_birth(
        test_member_versioned,
        member_versioned_test_client,
    ):
        # When
        matches = await member_versioned_test_client.get_all_by_name_and_date_of_birth(
            date_of_birth=test_member_versioned.date_of_birth,
            first_name=test_member_versioned.first_name,
            last_name=test_member_versioned.last_name,
        )
        # Then
        assert len(matches) == 1

    @staticmethod
    async def test_get_by_date_of_birth_and_unique_corp_id(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        # Given
        date_of_birth = test_member_versioned.date_of_birth
        unique_corp_id = test_member_versioned.unique_corp_id

        # When
        match = (
            await member_versioned_test_client.get_by_date_of_birth_and_unique_corp_id(
                date_of_birth=date_of_birth, unique_corp_id=unique_corp_id
            )
        )
        # Then
        assert match.date_of_birth == test_member_versioned.date_of_birth
        assert match.unique_corp_id == test_member_versioned.unique_corp_id

    @staticmethod
    async def test_get_by_name_and_unique_corp_id(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        # Given
        first_name = test_member_versioned.first_name
        last_name = test_member_versioned.last_name
        unique_corp_id = test_member_versioned.unique_corp_id

        # When
        match = await member_versioned_test_client.get_by_name_and_unique_corp_id(
            first_name=first_name, last_name=last_name, unique_corp_id=unique_corp_id
        )
        # Then
        assert match.first_name == test_member_versioned.first_name
        assert match.last_name == test_member_versioned.last_name
        assert match.unique_corp_id == test_member_versioned.unique_corp_id

    @staticmethod
    async def test_get_unique_corp_id_for_member(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        # Given
        member_id = test_member_versioned.id

        # When
        match = await member_versioned_test_client.get_unique_corp_id_for_member(
            member_id=member_id
        )

        # Then
        assert match == test_member_versioned.unique_corp_id  # Add your assertion here

    @staticmethod
    async def test_get_unique_corp_id_for_member_no_record(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        # Given
        member_id = test_member_versioned.id - 1

        # When
        match = await member_versioned_test_client.get_unique_corp_id_for_member(
            member_id=member_id
        )

        # Then
        assert match != test_member_versioned.unique_corp_id  # Add your assertion here

    @staticmethod
    async def test_get_other_user_ids_in_family_no_dependents(
        member_verification_test_client: member_verification_client.MemberVerifications,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        test_member_versioned: member_versioned_client.MemberVersioned,
        test_verification: verification_client.Verification,
    ):
        # Given
        await member_verification_test_client.persist(
            model=factory.MemberVerificationFactory.create(
                member_id=test_member_versioned.id, verification_id=test_verification.id
            )
        )
        # When
        user_ids: List[
            int
        ] = await member_versioned_test_client.get_other_user_ids_in_family(
            user_id=test_verification.user_id
        )

        # Then
        assert not user_ids

    @staticmethod
    async def test_get_other_user_ids_in_family_dependent_exists(
        member_verification_test_client: member_verification_client.MemberVerifications,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        verification_test_client: verification_client.Verifications,
        test_member_versioned: member_versioned_client.MemberVersioned,
        test_verification: verification_client.Verification,
    ):
        # Given
        # 1. Primary
        await member_verification_test_client.persist(
            model=factory.MemberVerificationFactory.create(
                member_id=test_member_versioned.id, verification_id=test_verification.id
            )
        )

        # 2. Dependent
        dependent_member = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_member_versioned.organization_id,
                # Same unique_corp_id
                unique_corp_id=test_member_versioned.unique_corp_id,
            )
        )
        dependent_verification = await verification_test_client.persist(
            model=factory.VerificationFactory.create(
                organization_id=test_member_versioned.organization_id
            )
        )
        await member_verification_test_client.persist(
            model=factory.MemberVerificationFactory.create(
                member_id=dependent_member.id, verification_id=dependent_verification.id
            )
        )
        # When
        user_ids: List[
            int
        ] = await member_versioned_test_client.get_other_user_ids_in_family(
            user_id=test_verification.user_id
        )

        # Then
        assert user_ids[0] == dependent_verification.user_id

    @staticmethod
    async def test_get_other_user_ids_in_family_dependent_exists_shared_record(
        member_verification_test_client: member_verification_client.MemberVerifications,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        verification_test_client: verification_client.Verifications,
        test_member_versioned: member_versioned_client.MemberVersioned,
        test_verification: verification_client.Verification,
    ):
        # Given
        # 1. Primary
        await member_verification_test_client.persist(
            model=factory.MemberVerificationFactory.create(
                member_id=test_member_versioned.id, verification_id=test_verification.id
            )
        )

        # 2. Dependent
        dependent_verification = await verification_test_client.persist(
            model=factory.VerificationFactory.create(
                organization_id=test_member_versioned.organization_id
            )
        )
        await member_verification_test_client.persist(
            model=factory.MemberVerificationFactory.create(
                member_id=test_member_versioned.id,
                verification_id=dependent_verification.id,
            )
        )
        # When
        user_ids: List[
            int
        ] = await member_versioned_test_client.get_other_user_ids_in_family(
            user_id=test_verification.user_id
        )

        # Then
        assert user_ids[0] == dependent_verification.user_id

    @staticmethod
    @pytest.mark.parametrize(
        argnames=("deactivated_at", "expected"),
        argvalues=[
            (datetime.datetime.today() - datetime.timedelta(days=7), False),
            (datetime.datetime.today() + datetime.timedelta(days=7), True),
            (None, True),
        ],
        ids=[
            "verification-is-deactivated-in-past",
            "verification-is-deactivated-in-future",
            "verification-is-valid",
        ],
    )
    async def test_get_other_user_ids_in_family_response_verification_validity(
        member_verification_test_client: member_verification_client.MemberVerifications,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        verification_test_client: verification_client.Verifications,
        test_config: configuration_client.Configuration,
        deactivated_at: datetime.datetime,
        expected: bool,
    ):
        # Given
        # 1. Primary
        primary_member = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
            )
        )
        primary_verification = await verification_test_client.persist(
            model=factory.VerificationFactory.create(
                organization_id=test_config.organization_id,
            )
        )
        await member_verification_test_client.persist(
            model=factory.MemberVerificationFactory.create(
                member_id=primary_member.id, verification_id=primary_verification.id
            )
        )

        # 2. Dependent
        dependent_member = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                # Same unique_corp_id
                unique_corp_id=primary_member.unique_corp_id,
            )
        )
        dependent_verification = await verification_test_client.persist(
            model=factory.VerificationFactory.create(
                organization_id=test_config.organization_id,
                deactivated_at=deactivated_at,
            )
        )
        await member_verification_test_client.persist(
            model=factory.MemberVerificationFactory.create(
                member_id=dependent_member.id, verification_id=dependent_verification.id
            )
        )
        # When
        user_ids: List[
            int
        ] = await member_versioned_test_client.get_other_user_ids_in_family(
            user_id=primary_verification.user_id
        )

        # Then
        assert (dependent_verification.user_id in user_ids) is expected

    @staticmethod
    async def test_get_by_org_identity_multiple_records(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        # Given
        identity = test_member_versioned.identity()

        test_member_versioned_new = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                file_id=test_member_versioned.file_id,
                organization_id=test_member_versioned.organization_id,
                date_of_birth=test_member_versioned.date_of_birth,
                unique_corp_id=test_member_versioned.unique_corp_id,
            )
        )

        # Manually set our updated at timestamp, so we can check to ensure we return the most recent record
        await member_versioned_test_client.set_updated_at(
            id=test_member_versioned_new.id,
            updated_at=test_member_versioned.updated_at + datetime.timedelta(days=10),
        )
        test_member_versioned_new = await member_versioned_test_client.get(
            test_member_versioned_new.id
        )

        # When
        match = await member_versioned_test_client.get_by_org_identity(identity)
        # Then
        assert match == test_member_versioned_new

    @staticmethod
    async def test_get_by_org_email(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
        faker,
    ):
        # Given
        dependent = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_member_versioned.organization_id,
                unique_corp_id=test_member_versioned.unique_corp_id,
                dependent_id=faker.swift11(),
                email=test_member_versioned.email,
                file_id=None,
            )
        )
        # When
        matches = await member_versioned_test_client.get_by_org_email(
            organization_id=test_member_versioned.organization_id,
            email=test_member_versioned.email,
        )
        # Then
        assert dependent in matches
        assert test_member_versioned in matches

    @staticmethod
    async def test_get_member_difference(
        test_file: file_client.File, member_versioned_test_client
    ):
        # Given
        await member_versioned_test_client.bulk_persist(
            models=factory.MemberVersionedFactory.create_batch(
                10, organization_id=test_file.organization_id, file_id=test_file.id
            )
        )
        existing: List[client.Member] = await member_versioned_test_client.all()
        # When
        # New batch.
        new = [
            *factory.MemberVersionedFactory.create_batch(
                10, organization_id=test_file.organization_id, file_id=test_file.id
            ),
            *existing[1:],
        ]
        # This member wasn't provided.
        missing = existing[0:1]
        # Then
        difference = await member_versioned_test_client.get_difference_by_org_corp_id(
            organization_id=test_file.organization_id,
            corp_ids=[n.unique_corp_id for n in new],
        )
        assert difference == missing

    @staticmethod
    async def test_get_by_name_and_date_of_birth(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        # Given
        # test_member_versioned fixture

        # When
        matching_records = (
            await member_versioned_test_client.get_by_name_and_date_of_birth(
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                date_of_birth=test_member_versioned.date_of_birth,
            )
        )

        # Then
        # Should find test_member_versioned
        assert len(matching_records) == 1
        assert {record.id for record in matching_records} == {test_member_versioned.id}

    @staticmethod
    async def test_get_by_name_and_date_of_birth_no_match(
        member_versioned_test_client,
    ):
        # Given
        stranger_danger = factory.MemberVersionedFactory.create()

        # When
        matching_records = (
            await member_versioned_test_client.get_by_name_and_date_of_birth(
                first_name=stranger_danger.first_name,
                last_name=stranger_danger.last_name,
                date_of_birth=stranger_danger.date_of_birth,
            )
        )

        # Then
        assert len(matching_records) == 0

    @staticmethod
    async def test_get_by_name_and_date_of_birth_multiple_records_same_org(
        configuration_test_client,
        file_test_client,
        member_versioned_test_client,
    ):
        # Given
        # Two records for the same name and dob in the same org
        org_1 = await configuration_test_client.persist(
            model=factory.ConfigurationFactory.create()
        )
        file_1_1 = await file_test_client.persist(
            model=factory.FileFactory.create(organization_id=org_1.organization_id)
        )
        record_1_1 = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=file_1_1.organization_id, file_id=file_1_1.id
            )
        )
        file_1_2 = await file_test_client.persist(
            model=factory.FileFactory.create(organization_id=org_1.organization_id)
        )
        record_1_2 = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=file_1_2.organization_id,
                file_id=file_1_2.id,
                first_name=record_1_1.first_name,
                last_name=record_1_1.last_name,
                date_of_birth=record_1_1.date_of_birth,
                updated_at=(record_1_1.updated_at + datetime.timedelta(days=1)),
            )
        )

        # When
        matching_records = (
            await member_versioned_test_client.get_by_name_and_date_of_birth(
                first_name=record_1_1.first_name,
                last_name=record_1_1.last_name,
                date_of_birth=record_1_1.date_of_birth,
            )
        )

        # Then
        # Should return the most recent record only
        assert len(matching_records) == 1
        matching_record_ids = {record.id for record in matching_records}
        expected_records_ids = {record_1_2.id}
        assert matching_record_ids == expected_records_ids

    @staticmethod
    async def test_get_by_name_and_date_of_birth_same_records_multiple_orgs(
        configuration_test_client,
        file_test_client,
        member_versioned_test_client,
    ):
        # Given
        # Two records for the same name and dob in different orgs
        org_1 = await configuration_test_client.persist(
            model=factory.ConfigurationFactory.create()
        )
        file_1 = await file_test_client.persist(
            model=factory.FileFactory.create(organization_id=org_1.organization_id)
        )
        record_1 = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=file_1.organization_id, file_id=file_1.id
            )
        )
        org_2 = await configuration_test_client.persist(
            model=factory.ConfigurationFactory.create()
        )
        file_2 = await file_test_client.persist(
            model=factory.FileFactory.create(organization_id=org_2.organization_id)
        )
        record_2 = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=file_2.organization_id,
                file_id=file_2.id,
                first_name=record_1.first_name,
                last_name=record_1.last_name,
                date_of_birth=record_1.date_of_birth,
            )
        )

        # When
        matching_records = (
            await member_versioned_test_client.get_by_name_and_date_of_birth(
                first_name=record_1.first_name,
                last_name=record_1.last_name,
                date_of_birth=record_1.date_of_birth,
            )
        )

        # Then
        # Should return both records
        assert len(matching_records) == 2
        matching_record_ids = {record.id for record in matching_records}
        expected_records_ids = {record_1.id, record_2.id}
        assert matching_record_ids == expected_records_ids

    @staticmethod
    async def test_get_by_name_and_date_of_birth_multiple_records_multiple_orgs(
        configuration_test_client,
        file_test_client,
        member_versioned_test_client,
    ):
        # Given
        # Four records for the same name and dob, 2 in each of 2 different orgs
        org_1 = await configuration_test_client.persist(
            model=factory.ConfigurationFactory.create()
        )
        file_1_1 = await file_test_client.persist(
            model=factory.FileFactory.create(organization_id=org_1.organization_id)
        )
        record_1_1 = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=file_1_1.organization_id, file_id=file_1_1.id
            )
        )
        file_1_2 = await file_test_client.persist(
            model=factory.FileFactory.create(organization_id=org_1.organization_id)
        )
        record_1_2 = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=file_1_2.organization_id,
                file_id=file_1_2.id,
                first_name=record_1_1.first_name,
                last_name=record_1_1.last_name,
                date_of_birth=record_1_1.date_of_birth,
                updated_at=(record_1_1.updated_at + datetime.timedelta(days=1)),
            )
        )
        org_2 = await configuration_test_client.persist(
            model=factory.ConfigurationFactory.create()
        )
        file_2_1 = await file_test_client.persist(
            model=factory.FileFactory.create(organization_id=org_2.organization_id)
        )
        record_2_1 = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=file_2_1.organization_id, file_id=file_2_1.id
            )
        )
        file_2_2 = await file_test_client.persist(
            model=factory.FileFactory.create(organization_id=org_2.organization_id)
        )
        record_2_2 = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=file_2_2.organization_id,
                file_id=file_2_2.id,
                updated_at=(record_2_1.updated_at + datetime.timedelta(days=1)),
                first_name=record_1_1.first_name,
                last_name=record_1_1.last_name,
                date_of_birth=record_1_1.date_of_birth,
            )
        )

        # When
        matching_records = (
            await member_versioned_test_client.get_by_name_and_date_of_birth(
                first_name=record_1_1.first_name,
                last_name=record_1_1.last_name,
                date_of_birth=record_1_1.date_of_birth,
            )
        )

        # Then
        # Should return the most recent record from each org
        assert len(matching_records) == 2
        matching_record_ids = {record.id for record in matching_records}
        expected_records_ids = {record_1_2.id, record_2_2.id}
        assert matching_record_ids == expected_records_ids

    @staticmethod
    async def test_get_wallet_enablement(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        # Given
        current_date = datetime.date.today()
        test_member_versioned.record.update(
            wallet_enabled=True,
            employee_start_date=current_date,
            employee_eligibility_date=current_date,
        )
        expected = (
            test_member_versioned.record["wallet_enabled"],
            test_member_versioned.record["employee_start_date"],
            test_member_versioned.record["employee_eligibility_date"],
        )
        member = await member_versioned_test_client.persist(model=test_member_versioned)
        # When
        enablement = await member_versioned_test_client.get_wallet_enablement(
            member_id=member.id
        )
        # Then
        assert enablement
        assert (
            enablement.enabled,
            enablement.start_date,
            enablement.eligibility_date,
        ) == expected

    @staticmethod
    async def test_get_wallet_enablement_by_identity(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        # Given
        current_date = datetime.date.today()
        test_member_versioned.record.update(
            wallet_enabled=True,
            employee_start_date=current_date,
            employee_eligibility_date=current_date,
        )
        expected = (
            test_member_versioned.record["wallet_enabled"],
            test_member_versioned.record["employee_start_date"],
            test_member_versioned.record["employee_eligibility_date"],
        )
        # Manually update our updated_at date to have us select the record with the wallet information updated- we don't upsert records, but instead insert new ones
        member = await member_versioned_test_client.persist(model=test_member_versioned)
        await member_versioned_test_client.set_updated_at(
            id=member.id,
            updated_at=test_member_versioned.updated_at + datetime.timedelta(days=10),
        )
        # When
        enablement = (
            await member_versioned_test_client.get_wallet_enablement_by_identity(
                identity=member.identity()
            )
        )
        # Then
        assert enablement
        assert (
            enablement.enabled,
            enablement.start_date,
            enablement.eligibility_date,
        ) == expected

    @staticmethod
    async def test_get_wallet_enablement_by_identity_multiple_records(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        # Given
        current_date = datetime.date.today()
        test_member_versioned.record.update(
            wallet_enabled=True,
            employee_start_date=current_date,
            employee_eligibility_date=current_date,
        )

        member = await member_versioned_test_client.persist(model=test_member_versioned)

        test_member_versioned_new = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                file_id=test_member_versioned.file_id,
                organization_id=test_member_versioned.organization_id,
                unique_corp_id=test_member_versioned.unique_corp_id,
                dependent_id=test_member_versioned.dependent_id,
            )
        )

        # Manually set our updated at timestamp, so we can check to ensure we return the most recent record
        await member_versioned_test_client.set_updated_at(
            id=test_member_versioned_new.id,
            updated_at=test_member_versioned.updated_at + datetime.timedelta(days=10),
        )
        test_member_versioned_new = await member_versioned_test_client.get(
            test_member_versioned_new.id
        )

        # When
        enablement = (
            await member_versioned_test_client.get_wallet_enablement_by_identity(
                identity=member.identity()
            )
        )
        # Then
        assert enablement.member_id == test_member_versioned_new.id

    @staticmethod
    async def test_get_wallet_enablement_by_identity_multiple_records_returns_created_at_of_first_record(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
        member_test_client,
    ):
        # Given
        current_date = datetime.date.today()
        test_member_versioned.record.update(
            wallet_enabled=True,
            employee_start_date=current_date,
            employee_eligibility_date=current_date,
        )
        # This represents the first record we receive for an org-identity
        initial_member = await member_test_client.persist(
            model=factory.MemberFactory.create(
                file_id=test_member_versioned.file_id,
                organization_id=test_member_versioned.organization_id,
                unique_corp_id=test_member_versioned.unique_corp_id,
                dependent_id=test_member_versioned.dependent_id,
            )
        )
        # Save a copy to member_versioned
        await member_versioned_test_client.persist(model=test_member_versioned)

        # Manually set our updated at timestamp, so we can check to ensure we return the most recent record
        await member_versioned_test_client.set_updated_at(
            id=initial_member.id,
            updated_at=test_member_versioned.updated_at + datetime.timedelta(days=10),
        )
        initial_member = await member_test_client.get(pk=initial_member.id)

        # When
        enablement = (
            await member_versioned_test_client.get_wallet_enablement_by_identity(
                identity=test_member_versioned.identity()
            )
        )
        # Then
        assert enablement.created_at == initial_member.created_at

    @staticmethod
    async def test_get_wallet_enablement_multiple_records_returns_created_at_of_first_record(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_test_client,
        member_versioned_test_client,
    ):
        # Given
        current_date = datetime.date.today()
        test_member_versioned.record.update(
            wallet_enabled=True,
            employee_start_date=current_date,
            employee_eligibility_date=current_date,
        )

        initial_member = await member_test_client.persist(
            model=factory.MemberFactory.create(
                file_id=test_member_versioned.file_id,
                organization_id=test_member_versioned.organization_id,
                unique_corp_id=test_member_versioned.unique_corp_id,
                dependent_id=test_member_versioned.dependent_id,
            )
        )

        updated_member_versioned = await member_versioned_test_client.persist(
            model=test_member_versioned
        )

        # Manually set our updated at timestamp, so we can check to ensure we return the most recent record
        await member_test_client.set_created_at(
            id=initial_member.id,
            created_at=initial_member.created_at + datetime.timedelta(days=10),
        )
        initial_member = await member_test_client.get(initial_member.id)

        # When
        enablement = await member_versioned_test_client.get_wallet_enablement(
            member_id=updated_member_versioned.id
        )
        # Then
        assert enablement.created_at == initial_member.created_at

    @staticmethod
    @pytest.mark.parametrize(
        argnames="wallet_eligibility_start_date,employee_start_date",
        argvalues=[
            (
                datetime.date.fromisoformat("2000-01-01"),
                datetime.date.fromisoformat("2000-02-01"),
            ),
            (None, datetime.date.fromisoformat("2000-02-01")),
            (datetime.date.fromisoformat("2000-01-01"), None),
            (None, None),
        ],
    )
    async def test_get_wallet_enablement_start_date_logic(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
        wallet_eligibility_start_date,
        employee_start_date,
    ):
        # Given
        test_member_versioned.record.update(
            wallet_enabled=True,
        )

        if wallet_eligibility_start_date:
            test_member_versioned.record.update(
                wallet_eligibility_start_date=wallet_eligibility_start_date,
            )

        if employee_start_date:
            test_member_versioned.record.update(
                employee_start_date=employee_start_date,
            )

        coalesce_order = [wallet_eligibility_start_date, employee_start_date]

        expected_enablement_start_date = next(
            (date for date in coalesce_order if date is not None), None
        )

        member = await member_versioned_test_client.persist(model=test_member_versioned)
        await member_versioned_test_client.set_updated_at(
            id=member.id,
            updated_at=test_member_versioned.updated_at + datetime.timedelta(days=10),
        )
        # When
        enablement = (
            await member_versioned_test_client.get_wallet_enablement_by_identity(
                identity=member.identity()
            )
        )

        # Then
        assert enablement and enablement.start_date == expected_enablement_start_date

    @staticmethod
    async def test_load_child_queries(
        member_versioned_test_client: member_versioned_client.MemberVersioneds,
        test_config: Configuration,
    ):
        # Given a custom query
        name: str = "custom"
        sql: str = """
        -- name: custom_query
        SELECT * FROM eligibility.member_versioned;
        """

        # When
        custom_queries = aiosql.from_str(sql, "asyncpg")
        member_versioned_test_client.load_child_queries(
            name=name, queries=custom_queries
        )

        await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id, file_id=None
            )
        )

        async with member_versioned_test_client.client.connector.connection() as c:
            fetched = (
                await member_versioned_test_client.client.queries.custom.custom_query(c)
            )

        # Then
        assert len(fetched) == 1

    @staticmethod
    async def test_get_values_to_hash_for_org(
        test_config,
        member_versioned_test_client,
        verification_test_client,
        member_verification_test_client,
    ):
        # Given
        expected_result_count = 10
        unhashed_input: List[
            member_versioned_test_client.Member
        ] = factory.MemberVersionedFactory.create_batch(
            size=expected_result_count,
            organization_id=test_config.organization_id,
            file_id=None,
        )
        unhashed_records = await member_versioned_test_client.bulk_persist(
            models=unhashed_input, coerce=True
        )

        unhashed_record_ids = [r.id for r in unhashed_records[1:]]

        unhashed_address = factory.AddressFactory.create(
            member_id=unhashed_records[1].id
        )
        await member_versioned_test_client.set_address_for_member(
            address=unhashed_address
        )

        # Create one non-hashed value that has verification attached - we don't want it
        test_verification = await verification_test_client.persist(
            model=factory.VerificationFactory.create(
                organization_id=test_config.organization_id,
            )
        )
        await member_verification_test_client.persist(
            model=factory.MemberVerificationFactory.create(
                member_id=unhashed_records[0].id, verification_id=test_verification.id
            )
        )

        hashed_input: List[
            member_versioned_test_client.Member
        ] = factory.MemberVersionedFactoryWithHash.create_batch(
            size=5, organization_id=test_config.organization_id, file_id=None
        )

        await member_versioned_test_client.bulk_persist(
            models=hashed_input, coerce=True
        )

        # When
        records_to_hash = await member_versioned_test_client.get_values_to_hash_for_org(
            organization_id=test_config.organization_id
        )

        # Then
        assert len(records_to_hash) == expected_result_count - 1

        for r in records_to_hash:
            assert r["id"] in unhashed_record_ids

            # Ensure we returned the address we set for a given record
            if r["id"] == unhashed_records[1].id:
                assert r["address_id"] is not None
            else:
                assert r["address_id"] is None

    # endregion

    # region mutate tests
    @staticmethod
    async def test_persist(test_file: file_client.File, member_versioned_test_client):
        # Given
        member = factory.MemberVersionedFactory.create(
            file_id=test_file.id,
            organization_id=test_file.organization_id,
        )

        # When
        created_model = await member_versioned_test_client.persist(model=member)

        # Then
        assert (
            member.date_of_birth,
            member.first_name,
            member.last_name,
            member.email,
        ) == (
            created_model.date_of_birth,
            created_model.first_name,
            created_model.last_name,
            created_model.email,
        )

    @staticmethod
    async def test_bulk_persist_and_update(
        test_file: file_client.File,
        member_versioned_test_client,
    ):
        # Given
        inputs: List[
            member_versioned_test_client.Member
        ] = factory.MemberVersionedFactory.create_batch(
            10, organization_id=test_file.organization_id, file_id=test_file.id
        )
        raw = await member_versioned_test_client.bulk_persist(
            models=inputs, coerce=False
        )
        created = await member_versioned_test_client.get_for_file(test_file.id)
        # When
        # The client added some zeroes on the next file (╯°□°)╯︵ ┻━┻
        for m in inputs:
            m.unique_corp_id = "0" + m.unique_corp_id
        await member_versioned_test_client.bulk_persist(models=inputs)
        outputs = await member_versioned_test_client.all()
        # Then
        # Oh hey! it's okay (•_•) ( •_•)>⌐■-■ (⌐■_■)
        assert {(m.first_name, m.last_name) for m in outputs} == {
            (m.first_name, m.last_name) for m in created
        }
        assert len(outputs) == len(created) * 2
        assert all(isinstance(r["record"], dict) for r in raw)

    @staticmethod
    async def test_delete(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        # Given
        member_id = test_member_versioned.id

        # When
        await member_versioned_test_client.delete(member_id)

        # Then
        returned_member = await member_versioned_test_client.get(member_id)
        assert returned_member is None  # noqa

    @staticmethod
    async def test_bulk_delete(
        multiple_test_members: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        # Given
        member_ids = [m.id for m in multiple_test_members]

        # When
        await member_versioned_test_client.bulk_delete(*member_ids)

        # Then
        returned_members = await member_versioned_test_client.all()
        assert returned_members == []

    @staticmethod
    async def test_delete_all_for_org(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
        configuration_test_client,
    ):
        # Given
        other_org: configuration_client.Configuration = (
            await configuration_test_client.persist(
                model=factory.ConfigurationFactory.create()
            )
        )
        size = 10
        await member_versioned_test_client.bulk_persist(
            models=factory.MemberVersionedFactory.create_batch(
                size,
                organization_id=other_org.organization_id,
                file_id=None,
            )
        )
        # When
        other_members_count = await member_versioned_test_client.get_count_for_org(
            other_org.organization_id
        )
        await member_versioned_test_client.delete_all_for_org(other_org.organization_id)
        # Then
        assert other_members_count == size
        assert (
            await member_versioned_test_client.get_count_for_org(
                other_org.organization_id
            )
        ) == 0

    @staticmethod
    async def test_set_effective_range(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        # Given
        new_range = asyncpg.Range(lower=datetime.date(1999, 1, 1))
        # When
        set_range = await member_versioned_test_client.set_effective_range(
            test_member_versioned.id, new_range
        )
        # Then
        assert set_range == new_range

    @staticmethod
    async def test_bulk_set_effective_range(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        # Given
        await member_versioned_test_client.bulk_persist(
            models=factory.MemberVersionedFactory.create_batch(
                9, organization_id=test_member_versioned.organization_id, file_id=None
            )
        )
        records = await member_versioned_test_client.all()
        # using sorted to guarantee we are comparing the same id's
        # since the records are not always returned in same order
        new_ranges = sorted(
            [
                (m.id, asyncpg.Range(lower=datetime.date(1999, 1, 1 + i)))
                for i, m in enumerate(records)
            ]
        )
        # When
        await member_versioned_test_client.bulk_set_effective_range(new_ranges)
        set_ranges = sorted(
            [
                (m.id, m.effective_range)
                for m in await member_versioned_test_client.all()
            ]
        )
        # Then
        assert set_ranges == new_ranges

    @staticmethod
    async def test_set_address_for_member(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        # Given
        expected = factory.AddressFactory.create(
            member_id=test_member_versioned.id, country_code="None", address_type="263"
        )
        # When
        await member_versioned_test_client.set_address_for_member(address=expected)
        # Then
        actual = await member_versioned_test_client.get_address_by_member_id(
            member_id=test_member_versioned.id
        )

        assert (
            expected["member_id"],
            expected["address_1"],
            expected["address_2"],
            expected["city"],
            expected["state"],
            expected["postal_code"],
            expected["postal_code_suffix"],
            expected["country_code"],
            expected["address_type"],
        ) == (
            actual.member_id,
            actual.address_1,
            actual.address_2,
            actual.city,
            actual.state,
            actual.postal_code,
            actual.postal_code_suffix,
            actual.country_code,
            actual.address_type,
        )

    @staticmethod
    @pytest.mark.parametrize(
        argnames="set_to_value",
        argvalues=[
            "True",
            "False",
            "",
        ],
    )
    async def test_set_do_not_contact(
        test_member_versioned_do_not_contact: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
        set_to_value,
    ):
        # When
        set_do_not_contact = await member_versioned_test_client.set_do_not_contact(
            id=test_member_versioned_do_not_contact.id, do_not_contact=set_to_value
        )
        # Then
        assert set_do_not_contact == set_to_value

    @staticmethod
    @pytest.mark.parametrize(
        argnames="set_to_value",
        argvalues=[
            "True",
            "False",
            "",
        ],
    )
    async def test_set_dependent_id_for_member(
        test_member_versioned_dependent_id: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
        set_to_value,
    ):
        # When
        set_dependent_id = (
            await member_versioned_test_client.set_dependent_id_for_member(
                id=test_member_versioned_dependent_id.id, dependent_id=set_to_value
            )
        )
        # Then
        assert set_dependent_id == set_to_value

    @staticmethod
    @pytest.mark.parametrize(
        argnames="set_to_value",
        argvalues=[
            "True",
            "False",
            "",
        ],
    )
    async def test_bulk_set_do_not_contact(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
        set_to_value,
    ):
        # Given
        await member_versioned_test_client.bulk_persist(
            models=factory.MemberVersionedFactory.create_batch(
                9, organization_id=test_member_versioned.organization_id, file_id=None
            )
        )
        records = await member_versioned_test_client.all()
        # using sorted to guarantee we are comparing the same id's
        # since the records are not always returned in same order
        new_records = sorted([(m.id, set_to_value) for i, m in enumerate(records)])
        # When
        await member_versioned_test_client.bulk_set_do_not_contact(new_records)
        set_records = sorted(
            [(m.id, m.do_not_contact) for m in await member_versioned_test_client.all()]
        )
        # Then
        assert new_records == set_records

    @staticmethod
    async def test_bulk_persist_external_records(
        member_versioned_test_client, configuration_test_client, test_config
    ):
        # Given
        # Set up our config
        external_id = factory.ExternalIDFactory.create(
            organization_id=test_config.organization_id
        )
        await configuration_test_client.add_external_id(**external_id)

        # Mock out the list of record and address dicts that we generate from our pubsub messages
        records = []
        for i in range(10):
            record = factory.ExternalRecordFactory.create(
                external_id=external_id["external_id"],
                source=external_id["source"],
                organization_id=external_id["organization_id"],
                record={"external_id": external_id["external_id"]},
            )
            address = factory.AddressFactory.create()
            records.append({"external_record": record, "record_address": address})

        # Get some records with overlapping unique corp ids.
        extra_records = []
        for er in records[0:2]:
            record = {
                **copy.deepcopy(er["external_record"]),
                "unique_corp_id": f"0{er['external_record']['unique_corp_id']}",
            }
            extra_records.append(
                {
                    "external_record": record,
                    "record_address": factory.AddressFactory.create(),
                }
            )

        # When
        (
            persisted_members,
            persisted_addresses,
        ) = await member_versioned_test_client.bulk_persist_external_records(
            records + extra_records
        )
        member_ids = {m["id"] for m in persisted_members}
        address_member_ids = {a["member_id"] for a in persisted_addresses}
        # Then
        # All records are persisted.
        assert len(persisted_members) == len(records) + len(extra_records)

        # They're all associated to the correct organization.
        assert all(
            p["organization_id"] == test_config.organization_id
            for p in persisted_members
        )
        # All of their `record` objects are correctly encoded/decoded
        assert all(isinstance(p["record"], dict) for p in persisted_members)
        # All addresses were persisted.
        assert member_ids == address_member_ids

    @staticmethod
    async def test_bulk_persist_external_records_same_record_twice(
        member_versioned_test_client, configuration_test_client, test_config
    ):
        # Given
        # Set up our config
        external_id = factory.ExternalIDFactory.create(
            organization_id=test_config.organization_id
        )
        await configuration_test_client.add_external_id(**external_id)

        # Mock out the list of record and address dicts that we generate from our pubsub messages
        record = factory.ExternalRecordFactory.create(
            external_id=external_id["external_id"],
            source=external_id["source"],
            organization_id=external_id["organization_id"],
            record={"external_id": external_id["external_id"]},
        )
        address = factory.AddressFactory.create()

        records = [{"external_record": record, "record_address": address}]
        records.append(copy.deepcopy(records[0]))

        (
            persisted_members,
            persisted_addresses,
        ) = await member_versioned_test_client.bulk_persist_external_records(records)

        # Then
        assert (len(persisted_addresses), len(persisted_members)) == (2, 2) and {
            a["member_id"] for a in persisted_addresses
        } == {m["id"] for m in persisted_members}

    @staticmethod
    async def test_bulk_persist_external_records_with_hash_same_record_twice_in_same_operation(
        member_versioned_test_client, configuration_test_client, test_config
    ):
        # Given
        # Set up our config
        external_id = factory.ExternalIDFactory.create(
            organization_id=test_config.organization_id
        )
        await configuration_test_client.add_external_id(**external_id)

        # Mock out the list of record and address dicts that we generate from our pubsub messages
        record = factory.ExternalRecordFactoryWithHash.create(
            external_id=external_id["external_id"],
            source=external_id["source"],
            organization_id=external_id["organization_id"],
            record={"external_id": external_id["external_id"]},
        )
        address = factory.AddressFactory.create()

        records = [{"external_record": record, "record_address": address}]
        records.append(copy.deepcopy(records[0]))

        # Try to save duplicated records in the same insert statement- we should de-dupe them before insert (to prevent SQL errors)
        (
            persisted_members,
            persisted_addresses,
        ) = await member_versioned_test_client.bulk_persist_external_records_hash(
            records
        )

        # Then
        assert (len(persisted_addresses), len(persisted_members)) == (1, 1)
        assert {a["member_id"] for a in persisted_addresses} == {
            m["id"] for m in persisted_members
        }

    @staticmethod
    async def test_bulk_persist_external_records_duplicates(
        test_config: configuration_client.Configuration,
        member_versioned_test_client,
        configuration_test_client,
    ):
        # Given
        external_id = factory.ExternalIDFactory.create(
            organization_id=test_config.organization_id
        )
        await configuration_test_client.add_external_id(**external_id)
        first = factory.ExternalRecordFactory.create(
            external_id=external_id["external_id"],
            source=external_id["source"],
            record={"external_id": external_id["external_id"]},
            organization_id=external_id["organization_id"],
        )
        second = copy.deepcopy(first)
        second.update(
            received_ts=first["received_ts"] + 1,
            email="new@email.net",
        )
        third = copy.deepcopy(second)
        third.update(
            received_ts=second["received_ts"] + 1,
            email="another.new@email.net",
        )
        # When
        persist_record_input = [{"external_record": r} for r in [first, third, second]]
        (
            persisted_records,
            persisted_addresses,
        ) = await member_versioned_test_client.bulk_persist_external_records(
            external_records=persist_record_input
        )
        # Then
        assert len(persisted_records) == 3

    @staticmethod
    async def test_bulk_persist_external_records_hash_duplicates(
        test_config: configuration_client.Configuration,
        member_versioned_test_client,
        configuration_test_client,
    ):
        # Given
        record = factory.ExternalRecordFactoryWithHash.create(
            organization_id=test_config.organization_id
        )
        external_record_and_address = factory.ExternalRecordAndAddressFactoryWithHash(
            external_record=record
        )
        # When

        (
            persisted_record,
            persisted_addresses,
        ) = await member_versioned_test_client.bulk_persist_external_records_hash(
            external_records=[external_record_and_address]
        )

        # Run our persist again
        (
            persisted_record_2,
            persisted_address_2,
        ) = await member_versioned_test_client.bulk_persist_external_records_hash(
            external_records=[external_record_and_address]
        )

        records = await member_versioned_test_client.all()
        addresses = await member_versioned_test_client.get_count_of_member_address()

        # Then
        assert len(persisted_record) == len(persisted_record_2) == 1
        assert len(persisted_addresses) == 1
        # Duplicated address is not persisted
        assert len(persisted_address_2) == 0
        assert len(records) == 1
        assert addresses == 1

    @staticmethod
    async def test_bulk_persist_external_records_hash_some_null_hash_values(
        test_config: configuration_client.Configuration,
        member_versioned_test_client,
        configuration_test_client,
    ):
        # Given
        record_with_hash = factory.ExternalRecordFactoryWithHash.create(
            organization_id=test_config.organization_id
        )
        external_record_and_address_with_hash = (
            factory.ExternalRecordAndAddressFactoryWithHash(
                external_record=record_with_hash
            )
        )
        record_with_no_hash = factory.ExternalRecordFactory.create(
            organization_id=test_config.organization_id
        )
        external_record_and_address_with_no_hash = (
            factory.ExternalRecordAndAddressFactory(external_record=record_with_no_hash)
        )

        # When
        records = [
            external_record_and_address_with_hash,
            external_record_and_address_with_no_hash,
        ]
        (
            persisted_record,
            persisted_addresses,
        ) = await member_versioned_test_client.bulk_persist_external_records_hash(
            external_records=records
        )

        # Run our persist again
        (
            persisted_record_2,
            persisted_address_2,
        ) = await member_versioned_test_client.bulk_persist_external_records_hash(
            external_records=records
        )

        all_records = await member_versioned_test_client.all()
        all_addresses = await member_versioned_test_client.get_count_of_member_address()

        # Then
        assert len(persisted_record) == len(persisted_record_2) == 2
        assert len(persisted_addresses) == 2
        # Duplicated address is not persisted
        assert len(persisted_address_2) == 1
        assert len(all_records) == all_addresses == 3

        # Ensure we inserted hashed records only once, but other records multiple times
        hash_records = []
        non_hash = []
        for r in all_records:
            if r.hash_value is not None:
                hash_records.append(r)
            else:
                non_hash.append(r)
        assert len(hash_records) == 1
        assert len(non_hash) == 2

    @staticmethod
    async def test_bulk_persist_external_records_address_removed(
        member_versioned_test_client, configuration_test_client, test_config
    ):
        # Given
        # Set up our config
        external_id = factory.ExternalIDFactory.create(
            organization_id=test_config.organization_id
        )
        await configuration_test_client.add_external_id(**external_id)

        # Mock out the list of record and address dicts that we generate from our pubsub messages
        # Create 10 records with addresses
        records = []
        for i in range(10):
            record = factory.ExternalRecordFactory.create(
                external_id=external_id["external_id"],
                source=external_id["source"],
                record={"external_id": external_id["external_id"]},
                organization_id=external_id["organization_id"],
            )
            address = factory.AddressFactory.create()
            records.append({"external_record": record, "record_address": address})

        # Add a record that does not have any address associated with it
        record_without_address = factory.ExternalRecordFactory.create(
            external_id=external_id["external_id"],
            source=external_id["source"],
            record={"external_id": external_id["external_id"]},
            organization_id=external_id["organization_id"],
        )
        records.append(
            {"external_record": record_without_address, "record_address": None}
        )

        # When
        (
            persisted_members,
            persisted_addresses,
        ) = await member_versioned_test_client.bulk_persist_external_records(records)

        # Then
        # Confirm we have saved all our addresses (remember one record doesn't have an address)
        assert len(persisted_addresses) == len(records) - 1

        # When
        # Run our update again-update all our members
        # This time however, do not provide addresses for half the users-want to ensure we remove these addresses
        # We also want to ensure that we handle our record without an address ever associated
        for r in records[0:5]:
            r["record_address"] = None
        (
            persisted_members,
            persisted_addresses,
        ) = await member_versioned_test_client.bulk_persist_external_records(records)

        # Then
        member_with_addresses = member_without_addresses = 0
        for m in persisted_members:
            saved_address = await member_versioned_test_client.get_address_by_member_id(
                member_id=m["id"]
            )
            if saved_address:
                member_with_addresses += 1
            else:
                member_without_addresses += 1

        assert len(persisted_addresses) == 5
        assert member_with_addresses == 5
        # Represents 5 records we updated to not have addresses, as well as our original record with no address
        assert member_without_addresses == 6

    @staticmethod
    async def test_bulk_persist_external_records_hash_no_address(
        member_versioned_test_client, configuration_test_client, test_config
    ):
        # Given
        # Set up our config
        external_id = factory.ExternalIDFactory.create(
            organization_id=test_config.organization_id
        )
        await configuration_test_client.add_external_id(**external_id)

        # Mock out the list of record and address dicts that we generate from our pubsub messages
        # Create 10 records with addresses
        record = factory.ExternalRecordFactoryWithHash.create(
            external_id=external_id["external_id"],
            source=external_id["source"],
            record={"external_id": external_id["external_id"]},
            organization_id=external_id["organization_id"],
        )
        records = [{"external_record": record, "record_address": None}]

        # When
        (
            persisted_members,
            persisted_addresses,
        ) = await member_versioned_test_client.bulk_persist_external_records_hash(
            records
        )

        # Then
        assert len(persisted_addresses) == 0

    @staticmethod
    async def test_bulk_persist_external_records_hash_update_member(
        member_versioned_test_client, configuration_test_client, test_config
    ):
        # Given
        # Set up our config
        external_id = factory.ExternalIDFactory.create(
            organization_id=test_config.organization_id
        )
        await configuration_test_client.add_external_id(**external_id)

        # Mock out the list of record and address dicts that we generate from our pubsub messages
        record = factory.ExternalRecordFactoryWithHash.create(
            external_id=external_id["external_id"],
            source=external_id["source"],
            record={"external_id": external_id["external_id"]},
            organization_id=external_id["organization_id"],
        )
        address = factory.AddressFactory.create()
        records = [{"external_record": record, "record_address": address}]

        # When
        (
            persisted_members,
            persisted_addresses,
        ) = await member_versioned_test_client.bulk_persist_external_records_hash(
            records
        )

        # Then
        assert len(persisted_addresses) == len(persisted_members)

        # When
        # Update the member record
        for r in records:
            r["external_record"]["hash_value"] = r["external_record"]["hash_value"][
                ::-1
            ]
        (
            persisted_members_2,
            persisted_addresses_2,
        ) = await member_versioned_test_client.bulk_persist_external_records_hash(
            records
        )

        # Then
        assert persisted_members != persisted_members_2
        assert persisted_addresses != persisted_addresses_2

        members = await member_versioned_test_client.all()
        assert len(members) == 2

        addresses = await member_versioned_test_client.get_count_of_member_address()
        assert addresses == 2

    # endregion
    @staticmethod
    async def test_get_count_of_member_address(
        member_versioned_test_client, configuration_test_client, test_config
    ):
        # Given
        # Set up our config
        external_id = factory.ExternalIDFactory.create(
            organization_id=test_config.organization_id
        )
        await configuration_test_client.add_external_id(**external_id)

        # Mock out the list of record and address dicts that we generate from our pubsub messages
        records = []
        for i in range(10):
            record = factory.ExternalRecordFactory.create(
                external_id=external_id["external_id"],
                source=external_id["source"],
                organization_id=external_id["organization_id"],
                record={"external_id": external_id["external_id"]},
            )
            address = factory.AddressFactory.create()
            records.append({"external_record": record, "record_address": address})

        await member_versioned_test_client.bulk_persist_external_records(records)

        # When
        count: int = await member_versioned_test_client.get_count_of_member_address()

        # Then
        assert count == 10

    @staticmethod
    async def test_get_members_for_pre_verification_by_org(
        member_versioned_test_client, test_config: Configuration
    ):
        """Test that cursor works for iterating through results"""
        # Given
        members: List[
            MemberVersioned
        ] = await member_versioned_test_client.bulk_persist(
            models=factory.MemberVersionedFactory.create_batch(
                size=100, organization_id=test_config.organization_id
            )
        )

        fetched = []

        # When
        async with member_versioned_test_client.get_members_for_pre_verification_by_organization_cursor(
            organization_id=test_config.organization_id
        ) as cursor:
            while batch := await cursor.fetch(10):
                for record in batch:
                    fetched.append(record)

        assert len(fetched) == len(members)

    @staticmethod
    async def test_get_members_for_pre_verification_by_org_some_pre_verified(
        member_versioned_test_client, test_config: Configuration
    ):
        """Test that query only returns records that are not pre-verified"""
        # Given
        num_not_pre_verified = 54
        await member_versioned_test_client.bulk_persist(
            models=factory.MemberVersionedFactory.create_batch(
                size=100, organization_id=test_config.organization_id, pre_verified=True
            )
            + factory.MemberVersionedFactory.create_batch(
                size=num_not_pre_verified,
                organization_id=test_config.organization_id,
                pre_verified=False,
            )
        )

        fetched = []

        # When
        async with member_versioned_test_client.get_members_for_pre_verification_by_organization_cursor(
            organization_id=test_config.organization_id
        ) as cursor:
            while batch := await cursor.fetch(10):
                for record in batch:
                    fetched.append(record)

        assert len(fetched) == num_not_pre_verified

    @staticmethod
    async def test_get_members_for_pre_verification_by_org_some_expired(
        member_versioned_test_client, test_config: Configuration
    ):
        """Test that query only returns records that are not expired"""
        # Given
        num_not_expired = 54
        await member_versioned_test_client.bulk_persist(
            models=factory.MemberVersionedFactory.create_batch(
                size=100,
                organization_id=test_config.organization_id,
                effective_range=DateRange(
                    lower=datetime.date(year=2012, month=1, day=1),
                    upper=datetime.date(year=2022, month=1, day=1),
                ),
            )
            + factory.MemberVersionedFactory.create_batch(
                size=num_not_expired, organization_id=test_config.organization_id
            )
        )

        fetched = []

        # When
        async with member_versioned_test_client.get_members_for_pre_verification_by_organization_cursor(
            organization_id=test_config.organization_id
        ) as cursor:
            while batch := await cursor.fetch(10):
                for record in batch:
                    fetched.append(record)

        assert len(fetched) == num_not_expired

    @staticmethod
    async def test_get_members_for_pre_verification_by_org_some_valid_tomorrow(
        member_versioned_test_client, test_config: Configuration
    ):
        """Test that query only returns records that are not expired or valid tomorrow"""
        # Given
        num_valid_tomorrow = 54
        await member_versioned_test_client.bulk_persist(
            models=factory.MemberVersionedFactory.create_batch(
                size=100,
                organization_id=test_config.organization_id,
                effective_range=DateRange(
                    lower=datetime.date(year=2012, month=1, day=1),
                    upper=datetime.date(year=2022, month=1, day=1),
                ),
            )
            + factory.MemberVersionedFactory.create_batch(
                size=num_valid_tomorrow,
                organization_id=test_config.organization_id,
                effective_range=DateRange(
                    lower=datetime.date.today() + datetime.timedelta(days=1),
                    upper=None,
                ),
            )
        )

        fetched = []

        # When
        async with member_versioned_test_client.get_members_for_pre_verification_by_organization_cursor(
            organization_id=test_config.organization_id
        ) as cursor:
            while batch := await cursor.fetch(10):
                for record in batch:
                    fetched.append(record)

        assert len(fetched) == num_valid_tomorrow

    @staticmethod
    async def test_get_members_for_pre_verification_by_org_not_verified_only(
        member_versioned_test_client,
        member_verification_test_client,
        test_verification: Verification,
        test_config: Configuration,
    ):
        """Test that query only returns records that are not verified already"""
        # Given
        num_total_members = 100
        num_verified = 10
        members: List[
            MemberVersioned
        ] = await member_versioned_test_client.bulk_persist(
            models=factory.MemberVersionedFactory.create_batch(
                size=num_total_members, organization_id=test_config.organization_id
            )
        )

        await member_verification_test_client.bulk_persist(
            models=[
                factory.MemberVerificationFactory(
                    member_id=member.id, verification_id=test_verification.id
                )
                for member in members[:num_verified]
            ]
        )

        fetched = []

        # When
        async with member_versioned_test_client.get_members_for_pre_verification_by_organization_cursor(
            organization_id=test_config.organization_id
        ) as cursor:
            while batch := await cursor.fetch(10):
                for record in batch:
                    fetched.append(record)

        # Then
        assert len(fetched) == num_total_members - num_verified

    @staticmethod
    @pytest.mark.parametrize(
        argnames=("original_value", "set_to_value"),
        argvalues=[(True, True), (True, False), (False, True), (False, False)],
        ids=["true-to-true", "true-to-false", "false-to-true", "false-to-false"],
    )
    async def test_set_pre_verified(
        member_versioned_test_client,
        test_config,
        original_value: bool,
        set_to_value: bool,
    ):
        # Given
        expected_member: MemberVersioned = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_config.organization_id, pre_verified=original_value
            )
        )
        # When
        await member_versioned_test_client.set_pre_verified(
            id=expected_member.id, pre_verified=set_to_value
        )
        # Then
        updated_member: MemberVersioned = await member_versioned_test_client.get(
            pk=expected_member.id
        )

        assert updated_member.pre_verified == set_to_value

    @staticmethod
    async def test_purge_duplicate_non_hash_optum(
        test_config,
        member_versioned_test_client,
        verification_test_client,
        member_verification_test_client,
        external_record,
    ):

        # Given
        number_records = 5
        address = factory.AddressFactory.create()

        # Create records one at a time, rather than in bulk
        # Our original query doesn't handle duplicate records in a batch with addresses well
        hash_1_record_to_save, hash_2_record_to_save = None, None

        for i in range(number_records):
            row_hash_1 = (
                await member_versioned_test_client.bulk_persist_external_records(
                    external_records=[
                        ExternalRecordAndAddress(
                            external_record=external_record, record_address=address
                        )
                    ]
                )
            )
            if not hash_1_record_to_save:
                hash_1_record_to_save = row_hash_1[0][0]

            # Create rows that will have a different hash value
            row_hash_2 = (
                await member_versioned_test_client.bulk_persist_external_records(
                    external_records=[
                        ExternalRecordAndAddress(
                            external_record=external_record, record_address=None
                        )
                    ]
                )
            )
            if not hash_2_record_to_save:
                hash_2_record_to_save = row_hash_2[0][0]

        # We need to set the first record to be the most recently created one - this should be the one we don't want to delete when we remove non-hashed rows
        await member_versioned_test_client.set_created_at(
            id=hash_1_record_to_save["id"],
            created_at=hash_1_record_to_save["created_at"]
            - datetime.timedelta(days=10),
        )
        await member_versioned_test_client.set_created_at(
            id=hash_2_record_to_save["id"],
            created_at=hash_2_record_to_save["created_at"]
            - datetime.timedelta(days=10),
        )

        records_to_hash = await member_versioned_test_client.get_values_to_hash_for_org(
            organization_id=external_record["organization_id"]
        )

        hashed_record, ids_to_delete = determine_rows_to_hash_and_discard(
            records_to_hash
        )

        # Then
        # Ensure we removed the correct records and put them in our historical table
        await member_versioned_test_client.purge_duplicate_non_hash_optum(
            member_ids=ids_to_delete
        )

        # Ensure we have two records remaining - we will later hash these
        remaining_member_records = await member_versioned_test_client.all()
        assert len(remaining_member_records) == 2

        remaining_address_records = (
            await member_versioned_test_client.get_count_of_member_address()
        )
        assert remaining_address_records == 1

    @staticmethod
    async def test_update_hash_values_for_optum(
        test_config,
        member_versioned_test_client,
        external_record,
    ):

        # Given
        number_records = 5
        address = factory.AddressFactory.create()

        # Create records one at a time, rather than in bulk
        # Our original query doesn't handle duplicate records in a batch with addresses well
        hash_1_record_to_save, hash_2_record_to_save = None, None

        for i in range(number_records):
            row_hash_1 = (
                await member_versioned_test_client.bulk_persist_external_records(
                    external_records=[
                        ExternalRecordAndAddress(
                            external_record=external_record, record_address=address
                        )
                    ]
                )
            )
            if not hash_1_record_to_save:
                hash_1_record_to_save = row_hash_1[0][0]

            # Create rows that will have a different hash value
            row_hash_2 = (
                await member_versioned_test_client.bulk_persist_external_records(
                    external_records=[
                        ExternalRecordAndAddress(
                            external_record=external_record, record_address=None
                        )
                    ]
                )
            )
            if not hash_2_record_to_save:
                hash_2_record_to_save = row_hash_2[0][0]

        # We need to set the first record to be the most recently created one - this should be the one we don't want to delete when we remove non-hashed rows
        await member_versioned_test_client.set_created_at(
            id=hash_1_record_to_save["id"],
            created_at=hash_1_record_to_save["created_at"]
            - datetime.timedelta(days=10),
        )
        await member_versioned_test_client.set_created_at(
            id=hash_2_record_to_save["id"],
            created_at=hash_2_record_to_save["created_at"]
            - datetime.timedelta(days=10),
        )

        records_to_hash = await member_versioned_test_client.get_values_to_hash_for_org(
            organization_id=external_record["organization_id"]
        )
        id_and_hash_to_update, ids_to_delete = determine_rows_to_hash_and_discard(
            records_to_hash
        )

        # When
        # Update the values we need to hash
        (
            updated_count,
            removed_duplicate_count,
        ) = await member_versioned_test_client.update_hash_values_for_optum(
            records=id_and_hash_to_update,
            organization_id=external_record["organization_id"],
        )

        # Then
        all_records = await member_versioned_test_client.all()
        unhashed_count, hashed_count = 0, 0

        for r in all_records:
            if r.hash_value is None:
                unhashed_count += 1
            else:
                # Ensure we only hashed the records anticipated
                assert r.id in [i[0] for i in id_and_hash_to_update]
                hashed_count += 1

        # Ensure we did not update records that should have not been hashed
        assert unhashed_count == (number_records * 2) - 2
        assert hashed_count == 2
        assert updated_count == 2

    @staticmethod
    async def test_update_hash_values_for_optum_conflict_with_existing_hash(
        test_config,
        member_versioned_test_client,
        external_record,
    ):

        # Given
        number_records = 5
        address = factory.AddressFactory.create()

        # Set up a record that has already been hashed- we don't want to overwrite it
        temp_external_record = external_record.copy()
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
        hash_record_0_to_save, hash_1_record_to_save = None, None

        # Create records one at a time, rather than in bulk
        # Our original query doesn't handle duplicate records in a batch with addresses well
        for i in range(number_records):
            row_hash_1 = (
                await member_versioned_test_client.bulk_persist_external_records(
                    external_records=[
                        ExternalRecordAndAddress(
                            external_record=external_record, record_address=address
                        )
                    ]
                )
            )
            if not hash_record_0_to_save:
                hash_record_0_to_save = row_hash_1[0][0]
                await member_versioned_test_client.set_created_at(
                    id=hash_record_0_to_save["id"],
                    created_at=hash_record_0_to_save["created_at"]
                    - datetime.timedelta(days=10),
                )

            # Create rows that will have a different hash value- the lack of address should generate different results
            row_hash_2 = (
                await member_versioned_test_client.bulk_persist_external_records(
                    external_records=[
                        ExternalRecordAndAddress(
                            external_record=external_record, record_address=None
                        )
                    ]
                )
            )
            if not hash_1_record_to_save:
                hash_1_record_to_save = row_hash_2[0][0]
                # We need to set the first record to be the most recently created one - this should be the one we don't want to delete when we remove non-hashed rows
                await member_versioned_test_client.set_created_at(
                    id=hash_1_record_to_save["id"],
                    created_at=hash_1_record_to_save["created_at"]
                    - datetime.timedelta(days=10),
                )

        records_to_hash = await member_versioned_test_client.get_values_to_hash_for_org(
            organization_id=external_record["organization_id"]
        )
        id_and_hash_to_update, ids_to_delete = determine_rows_to_hash_and_discard(
            records_to_hash
        )

        # When
        # Update the values we need to hash
        (
            updated_count,
            removed_duplicate_count,
        ) = await member_versioned_test_client.update_hash_values_for_optum(
            records=id_and_hash_to_update,
            organization_id=external_record["organization_id"],
        )

        # Then
        all_records = await member_versioned_test_client.all()

        unhashed_count, hashed_count = 0, 0
        for r in all_records:
            if r.hash_value is None:
                unhashed_count += 1
            else:
                hashed_count += 1
        assert unhashed_count == (number_records * 2) - 2
        assert hashed_count == 2
        assert updated_count == 1
        assert removed_duplicate_count == 1

        # Ensure we hashed the correct rows
        # Hash 0 should have had a conflict with an existing temp row- we should have removed it
        hash_0_result = await member_versioned_test_client.get(
            hash_record_0_to_save["id"]
        )
        assert hash_0_result is None

        hash_1_result = await member_versioned_test_client.get(
            hash_1_record_to_save["id"]
        )
        assert hash_1_result.hash_value is not None
        assert hash_1_result.hash_version is not None

    @staticmethod
    async def test_get_by_member_2(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
        member_2_test_client,
    ):
        test_member_2 = factory.Member2Factory.create(
            id=1001,
            organization_id=test_member_versioned.organization_id,
            first_name=test_member_versioned.first_name,
            last_name=test_member_versioned.last_name,
            email=test_member_versioned.email,
            date_of_birth=test_member_versioned.date_of_birth,
            work_state=test_member_versioned.work_state,
            unique_corp_id=test_member_versioned.unique_corp_id,
        )
        # When
        returned_member = await member_versioned_test_client.get_by_member_2(
            member_2=test_member_2
        )

        # Then
        assert returned_member == test_member_versioned

        test_member_2_no_match = factory.Member2Factory.create(
            id=1002,
            organization_id=test_member_versioned.organization_id,
            first_name=test_member_versioned.first_name,
            last_name=test_member_versioned.last_name,
            email=test_member_versioned.email,
            date_of_birth=test_member_versioned.date_of_birth,
            work_state=test_member_versioned.work_state + "_no_match",
            unique_corp_id=test_member_versioned.unique_corp_id,
        )
        # When
        returned_member = await member_versioned_test_client.get_by_member_2(
            member_2=test_member_2_no_match
        )

        # Then
        assert returned_member is None

    @staticmethod
    async def test_get_by_dob_name_and_work_state(
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client,
    ):
        # When match
        found = await member_versioned_test_client.get_by_dob_name_and_work_state(
            date_of_birth=test_member_versioned.date_of_birth,
            first_name=test_member_versioned.first_name,
            last_name=test_member_versioned.last_name,
            work_state=test_member_versioned.work_state,
        )

        # Then
        assert len(found) == 1
        assert {record.id for record in found} == {test_member_versioned.id}

        # When not match
        not_found_first_name = (
            await member_versioned_test_client.get_by_dob_name_and_work_state(
                date_of_birth=test_member_versioned.date_of_birth,
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                work_state="Non-Exist-State",
            )
        )
        assert len(not_found_first_name) == 0
