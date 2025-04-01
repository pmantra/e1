from __future__ import annotations

import copy
import dataclasses
import datetime
from typing import List

import aiosql
import asyncpg
import pytest
from tests.factories import data_models as factory
from tests.functional.conftest import NUMBER_TEST_OBJECTS

from db.clients import (
    client,
    configuration_client,
    file_client,
    member_client,
    member_versioned_client,
)
from db.model import Configuration, File, Member

pytestmark = pytest.mark.asyncio


class TestMemberClient:

    # region fetch tests

    @staticmethod
    async def test_all(
        multiple_test_members: member_client.Members, member_test_client
    ):
        # Given
        # We have created 100 members -> one for each of our multiple files. Ensure we have grabbed all of them
        expected_total = NUMBER_TEST_OBJECTS * NUMBER_TEST_OBJECTS

        # When
        all_members = await member_test_client.all()

        # Then
        # Ensure we have grabbed all members for all files
        assert len(all_members) == expected_total

    @staticmethod
    async def test_get(test_member: member_client.Members, member_test_client):
        # When
        returned_member = await member_test_client.get(test_member.id)

        # Then
        assert returned_member == test_member

    @staticmethod
    async def test_get_for_org(test_member: member_client.Members, member_test_client):
        assert await member_test_client.get_for_org(test_member.organization_id) == [
            test_member
        ]

    @staticmethod
    async def test_get_count_for_org(
        test_member: member_client.Members, member_test_client
    ):
        assert (
            await member_test_client.get_count_for_org(test_member.organization_id) == 1
        )

    @staticmethod
    async def test_get_counts_for_orgs(
        test_file: file_client.Files, member_test_client
    ):
        # Given
        # Bulk create members for our test file
        await member_test_client.bulk_persist(
            models=factory.MemberFactory.create_batch(
                NUMBER_TEST_OBJECTS,
                organization_id=test_file.organization_id,
                file_id=test_file.id,
            ),
        )

        # When
        member_count = await member_test_client.get_count_for_org(
            test_file.organization_id
        )

        # Then
        assert member_count == NUMBER_TEST_OBJECTS

    @staticmethod
    async def test_get_for_file(test_member: member_client.Member, member_test_client):
        assert await member_test_client.get_for_file(test_member.file_id) == [
            test_member
        ]

    @staticmethod
    async def test_get_for_files(
        multiple_test_file: file_client.File, member_test_client
    ):
        # Given
        file_member_map = {}
        for f in multiple_test_file:
            member = await member_test_client.persist(
                model=factory.MemberFactory.create(
                    organization_id=f.organization_id, file_id=f.id
                )
            )
            file_member_map[f.id] = member

        # When
        returned_members = await member_test_client.get_for_files(
            *file_member_map.keys()
        )
        returned_member_map = {m.file_id: m for m in returned_members}

        # Then
        assert len(returned_members) == len(multiple_test_file)
        assert file_member_map == returned_member_map

    @staticmethod
    async def test_get_count_for_file(
        test_member: member_client.Member, member_test_client
    ):
        assert await member_test_client.get_count_for_file(test_member.file_id) == 1

    @staticmethod
    @pytest.mark.parametrize(
        argnames="member_email",
        argvalues=["foo@foobar.com", "fOO@fooBar.com", "foo@foobar.com   "],
        ids=["compliant_email", "email_ignore_case", "email_ignore_whitespace"],
    )
    async def test_get_by_dob_and_email(
        test_file: file_client.File, member_test_client, member_email
    ):
        # Given
        test_member = await member_test_client.persist(
            model=factory.MemberFactory.create(
                organization_id=test_file.organization_id,
                file_id=test_file.id,
                email=member_email,
            )
        )

        # When
        returned_member = await member_test_client.get_by_dob_and_email(
            date_of_birth=test_member.date_of_birth, email=test_member.email
        )

        # Then
        assert test_member == returned_member

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
        member_test_client,
        work_state,
        first_name,
        last_name,
    ):

        # Given
        test_member = await member_test_client.persist(
            model=factory.MemberFactory.create(
                organization_id=test_file.organization_id,
                file_id=test_file.id,
                work_state="NY",
                first_name="Alan",
                last_name="Turing",
            )
        )
        # When
        verified = await member_test_client.get_by_secondary_verification(
            date_of_birth=test_member.date_of_birth,
            first_name=first_name,
            last_name=last_name,
            work_state=work_state,
        )
        # Then
        assert verified == [test_member]

    @staticmethod
    async def test_get_by_tertiary_verification(
        test_member: member_client.Member, member_test_client
    ):
        # When
        verified = await member_test_client.get_by_tertiary_verification(
            date_of_birth=test_member.date_of_birth,
            unique_corp_id=test_member.unique_corp_id,
        )
        # Then
        assert verified == [test_member]

    @staticmethod
    async def test_get_by_email_and_name(
        member_test_client,
        test_file,
    ):
        # Given
        test_member = await member_test_client.persist(
            model=factory.MemberFactory.create(
                organization_id=test_file.organization_id,
                file_id=test_file.id,
                email="foo@bar.com",
                first_name="foo",
                last_name="bar",
            )
        )

        # When
        verified = await member_test_client.get_by_email_and_name(
            email=test_member.email,
            first_name=test_member.first_name,
            last_name=test_member.last_name,
        )
        # Then
        assert verified == [test_member]

    # FIXME [Optum]
    @staticmethod
    async def test_get_by_tertiary_verification_optum_hack(
        test_member: member_client.Member,
        member_test_client,
        configuration_test_client,
        faker,
    ):
        # Given
        # Mark this member as part of an optum healthplan.
        await configuration_test_client.add_external_id(
            organization_id=test_member.organization_id,
            source="optum",
            external_id=faker.swift11(),
        )
        test_member.unique_corp_id = "123456789"
        test_member = await member_test_client.persist(model=test_member)
        # Create another potential match, but from a different organization.
        other = factory.MemberFactory.create(
            date_of_birth=test_member.date_of_birth,
            file_id=None,
        )
        other_config = factory.ConfigurationFactory.create(
            organization_id=other.organization_id
        )
        await configuration_test_client.persist(model=other_config)
        await member_test_client.persist(model=other)
        # When
        verified = await member_test_client.get_by_tertiary_verification(
            date_of_birth=test_member.date_of_birth,
            unique_corp_id="ABC" + test_member.unique_corp_id,
        )
        # Then
        # We should only have matched to the optum client's member.
        assert verified == [test_member]

    @staticmethod
    async def test_get_by_tertiary_verification_multiple_external_ids(
        test_member: member_client.Member,
        member_test_client,
        configuration_test_client,
        faker,
    ):
        # Given
        # Multiple external IDs for the same organization.
        await configuration_test_client.add_external_id(
            organization_id=test_member.organization_id,
            source=faker.domain_word(),
            external_id=faker.swift11(),
        )
        await configuration_test_client.add_external_id(
            organization_id=test_member.organization_id,
            source=faker.domain_word(),
            external_id=faker.swift11(),
        )
        # When
        verified = await member_test_client.get_by_tertiary_verification(
            date_of_birth=test_member.date_of_birth,
            unique_corp_id=test_member.unique_corp_id,
        )
        # Then
        # We still resolve to a single member record.
        assert verified == [test_member]

    @staticmethod
    async def test_get_by_any_verification(
        test_member: member_client.Member, member_test_client
    ):
        # When
        verified_all = await member_test_client.get_by_any_verification(
            dob=test_member.date_of_birth,
            first_name=test_member.first_name,
            last_name=test_member.last_name,
            work_state=test_member.work_state,
            email=test_member.email,
        )
        verified_email = await member_test_client.get_by_any_verification(
            dob=test_member.date_of_birth,
            email=test_member.email,
        )
        verified_name_state = await member_test_client.get_by_any_verification(
            dob=test_member.date_of_birth,
            first_name=test_member.first_name,
            last_name=test_member.last_name,
            work_state=test_member.work_state,
        )
        # Then
        assert verified_all == verified_email == verified_name_state == test_member

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
        test_member: member_client.Member, member_test_client, range, filtered
    ):
        # Given
        previous_match = await member_test_client.get_by_any_verification(
            test_member.date_of_birth,
            first_name=test_member.first_name,
            last_name=test_member.last_name,
            work_state=test_member.work_state,
            email=test_member.email,
        )
        expected_match = (
            None
            if filtered
            else dataclasses.replace(test_member, effective_range=range)
        )
        await member_test_client.set_effective_range(id=test_member.id, range=range)
        # When
        current_match = await member_test_client.get_by_any_verification(
            test_member.date_of_birth,
            first_name=test_member.first_name,
            last_name=test_member.last_name,
            work_state=test_member.work_state,
            email=test_member.email,
        )
        # Then
        assert previous_match == test_member
        assert current_match == expected_match

    @staticmethod
    async def test_get_by_client_specific_verification(
        test_member: member_client.Member, member_test_client
    ):
        # When
        verified = await member_test_client.get_by_client_specific_verification(
            organization_id=test_member.organization_id,
            unique_corp_id=test_member.unique_corp_id,
            date_of_birth=test_member.date_of_birth,
        )
        # Then
        assert verified == test_member

    @staticmethod
    async def test_get_by_org_identity(
        test_member: member_client.Member, member_test_client
    ):
        # Given
        identity = test_member.identity()
        # When
        match = await member_test_client.get_by_org_identity(identity)
        # Then
        assert match == test_member

    @staticmethod
    async def test_get_by_org_email(
        test_member: member_client.Member, member_test_client, faker
    ):
        # Given
        dependent = await member_test_client.persist(
            model=factory.MemberFactory.create(
                organization_id=test_member.organization_id,
                unique_corp_id=test_member.unique_corp_id,
                dependent_id=faker.swift11(),
                email=test_member.email,
                file_id=None,
            )
        )
        # When
        matches = await member_test_client.get_by_org_email(
            organization_id=test_member.organization_id,
            email=test_member.email,
        )
        # Then
        assert matches == [test_member, dependent]

    @staticmethod
    async def test_get_member_difference(
        test_file: file_client.File, member_test_client
    ):
        # Given
        await member_test_client.bulk_persist(
            models=factory.MemberFactory.create_batch(
                10, organization_id=test_file.organization_id, file_id=test_file.id
            )
        )
        existing: List[client.Member] = await member_test_client.all()
        # When
        # New batch.
        new = [
            *factory.MemberFactory.create_batch(
                10, organization_id=test_file.organization_id, file_id=test_file.id
            ),
            *existing[1:],
        ]
        # This member wasn't provided.
        missing = existing[0:1]
        # Then
        difference = await member_test_client.get_difference_by_org_corp_id(
            organization_id=test_file.organization_id,
            corp_ids=[n.unique_corp_id for n in new],
        )
        assert difference == missing

    @staticmethod
    async def test_get_by_name_and_date_of_birth(
        test_member: member_client.Member, member_test_client
    ):
        # When
        matching_record = await member_test_client.get_by_name_and_date_of_birth(
            first_name=test_member.first_name,
            last_name=test_member.last_name,
            date_of_birth=test_member.date_of_birth,
        )
        # Then
        assert matching_record == [test_member]

    @staticmethod
    async def test_get_wallet_enablement(
        test_member: member_client.Member, member_test_client
    ):

        # Given
        current_date = datetime.date.today()
        test_member.record.update(
            wallet_enabled=True,
            employee_start_date=current_date,
            employee_eligibility_date=current_date,
        )
        expected = (
            test_member.record["wallet_enabled"],
            test_member.record["employee_start_date"],
            test_member.record["employee_eligibility_date"],
        )
        member = await member_test_client.persist(model=test_member)
        # When
        enablement = await member_test_client.get_wallet_enablement(member_id=member.id)
        # Then
        assert enablement
        assert (
            enablement.enabled,
            enablement.start_date,
            enablement.eligibility_date,
        ) == expected

    @staticmethod
    async def test_get_wallet_enablement_by_identity(
        test_member: member_client.Member, member_test_client
    ):
        # Given
        current_date = datetime.date.today()
        test_member.record.update(
            wallet_enabled=True,
            employee_start_date=current_date,
            employee_eligibility_date=current_date,
        )
        expected = (
            test_member.record["wallet_enabled"],
            test_member.record["employee_start_date"],
            test_member.record["employee_eligibility_date"],
        )
        member = await member_test_client.persist(model=test_member)
        # When
        enablement = await member_test_client.get_wallet_enablement_by_identity(
            identity=member.identity()
        )
        # Then
        assert enablement
        assert (
            enablement.enabled,
            enablement.start_date,
            enablement.eligibility_date,
        ) == expected

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
        test_member: member_client.Member,
        member_test_client,
        wallet_eligibility_start_date,
        employee_start_date,
    ):
        # Given
        test_member.record.update(
            wallet_enabled=True,
        )

        if wallet_eligibility_start_date:
            test_member.record.update(
                wallet_eligibility_start_date=wallet_eligibility_start_date,
            )

        if employee_start_date:
            test_member.record.update(
                employee_start_date=employee_start_date,
            )

        coalesce_order = [wallet_eligibility_start_date, employee_start_date]

        expected_enablement_start_date = next(
            (date for date in coalesce_order if date is not None), None
        )

        member = await member_test_client.persist(model=test_member)
        # When
        enablement = await member_test_client.get_wallet_enablement_by_identity(
            identity=member.identity()
        )

        # Then
        assert enablement and enablement.start_date == expected_enablement_start_date

    @staticmethod
    async def test_get_kafka_record_count_for_org(
        member_test_client: member_client.Members,
        test_config: Configuration,
        test_file: File,
    ):
        # Given
        expected_kafka_member_count = 100
        file_member_count = 12
        kafka_members: List[Member] = factory.MemberFactory.create_batch(
            size=expected_kafka_member_count,
            organization_id=test_config.organization_id,
            file_id=None,
        )
        file_members: List[Member] = factory.MemberFactory.create_batch(
            size=file_member_count,
            organization_id=test_config.organization_id,
            file_id=test_file.id,
        )
        await member_test_client.bulk_persist(models=kafka_members + file_members)

        # When
        retrieved_kafka_member_count = (
            await member_test_client.get_kafka_record_count_for_org(
                organization_id=test_config.organization_id
            )
        )

        # Then
        assert retrieved_kafka_member_count == expected_kafka_member_count

    @staticmethod
    async def test_get_file_record_count_for_org(
        member_test_client: member_client.Members,
        test_config: Configuration,
        test_file: File,
    ):
        # Given
        expected_file_member_count = 100
        kafka_member_count = 12
        kafka_members: List[Member] = factory.MemberFactory.create_batch(
            size=kafka_member_count,
            organization_id=test_config.organization_id,
            file_id=None,
        )
        file_members: List[Member] = factory.MemberFactory.create_batch(
            size=expected_file_member_count,
            organization_id=test_config.organization_id,
            file_id=test_file.id,
        )
        await member_test_client.bulk_persist(models=kafka_members + file_members)

        # When
        retrieved_file_member_count = (
            await member_test_client.get_file_record_count_for_org(
                organization_id=test_config.organization_id
            )
        )

        # Then
        assert retrieved_file_member_count == expected_file_member_count

    @staticmethod
    async def test_load_child_queries(
        member_test_client: member_client.Members, test_config: Configuration
    ):
        # Given a custom query
        name: str = "custom"
        sql: str = """
        -- name: custom_query
        SELECT * FROM eligibility.member;
        """

        # When
        custom_queries = aiosql.from_str(sql, "asyncpg")
        member_test_client.load_child_queries(name=name, queries=custom_queries)

        await member_test_client.persist(
            model=factory.MemberFactory.create(
                organization_id=test_config.organization_id, file_id=None
            )
        )

        async with member_test_client.client.connector.connection() as c:
            fetched = await member_test_client.client.queries.custom.custom_query(c)

        # Then
        assert len(fetched) == 1

    # endregion

    # region mutate tests
    @staticmethod
    async def test_persist(test_file: file_client.File, member_test_client):

        # Given
        member = factory.MemberFactory.create(
            file_id=test_file.id,
            organization_id=test_file.organization_id,
        )

        # When
        created_model = await member_test_client.persist(model=member)

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
    async def test_bulk_persist_and_upsert(
        test_file: file_client.File,
        member_test_client,
    ):
        # Given
        inputs: List[member_client.Member] = factory.MemberFactory.create_batch(
            10, organization_id=test_file.organization_id, file_id=test_file.id
        )
        raw = await member_test_client.bulk_persist(models=inputs, coerce=False)
        created = await member_test_client.get_for_file(test_file.id)
        # When
        # The client added some zeroes on the next file (╯°□°)╯︵ ┻━┻
        for m in inputs:
            m.unique_corp_id = "0" + m.unique_corp_id
        await member_test_client.bulk_persist(models=inputs)
        outputs = await member_test_client.all()
        # Then
        # Oh hey! it's okay (•_•) ( •_•)>⌐■-■ (⌐■_■)
        assert {(m.first_name, m.last_name) for m in outputs} == {
            (m.first_name, m.last_name) for m in created
        }
        assert len(outputs) == len(created)
        assert all(isinstance(r["record"], dict) for r in raw)

    @staticmethod
    async def test_delete(test_member: member_client.Member, member_test_client):
        # Given
        member_id = test_member.id

        # When
        await member_test_client.delete(member_id)

        # Then
        returned_member = await member_test_client.get(member_id)
        assert returned_member is None  # noqa

    @staticmethod
    async def test_bulk_delete(
        multiple_test_members: member_client.Member, member_test_client
    ):

        # Given
        member_ids = [m.id for m in multiple_test_members]

        # When
        await member_test_client.bulk_delete(*member_ids)

        # Then
        returned_members = await member_test_client.all()
        assert returned_members == []

    @staticmethod
    async def test_delete_all_for_org(
        test_member: member_client.Member, member_test_client, configuration_test_client
    ):
        # Given
        other_org: configuration_client.Configuration = (
            await configuration_test_client.persist(
                model=factory.ConfigurationFactory.create()
            )
        )
        size = 10
        await member_test_client.bulk_persist(
            models=factory.MemberFactory.create_batch(
                size,
                organization_id=other_org.organization_id,
                file_id=None,
            )
        )
        # When
        other_members_count = await member_test_client.get_count_for_org(
            other_org.organization_id
        )
        await member_test_client.delete_all_for_org(other_org.organization_id)
        # Then
        assert other_members_count == size
        assert (
            await member_test_client.get_count_for_org(other_org.organization_id)
        ) == 0

    @staticmethod
    async def test_set_effective_range(
        test_member: member_client.Member, member_test_client
    ):
        # Given
        new_range = asyncpg.Range(lower=datetime.date(1999, 1, 1))
        # When
        set_range = await member_test_client.set_effective_range(
            test_member.id, new_range
        )
        # Then
        assert set_range == new_range

    @staticmethod
    async def test_bulk_set_effective_range(
        test_member: member_client.Member, member_test_client
    ):
        # Given
        await member_test_client.bulk_persist(
            models=factory.MemberFactory.create_batch(
                9, organization_id=test_member.organization_id, file_id=None
            )
        )
        records = await member_test_client.all()
        # using sorted to guarantee we are comparing the same id's
        # since the records are not always returned in same order
        new_ranges = sorted(
            [
                (m.id, asyncpg.Range(lower=datetime.date(1999, 1, 1 + i)))
                for i, m in enumerate(records)
            ]
        )
        # When
        await member_test_client.bulk_set_effective_range(new_ranges)
        set_ranges = sorted(
            [(m.id, m.effective_range) for m in await member_test_client.all()]
        )
        # Then
        assert set_ranges == new_ranges

    @staticmethod
    async def test_set_address_for_member(
        test_member: member_client.Member, member_test_client
    ):
        # Given
        expected = factory.AddressFactory.create(
            member_id=test_member.id, country_code="None", address_type="263"
        )
        # When
        await member_test_client.set_address_for_member(address=expected)
        # Then
        actual = await member_test_client.get_address_by_member_id(
            member_id=test_member.id
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
    async def test_update_address_for_member(
        test_member: member_client.Member, member_test_client
    ):
        # Given
        previous = factory.AddressFactory.create(
            member_id=test_member.id, country_code="None", address_type="263"
        )
        expected = factory.AddressFactory.create(
            member_id=test_member.id, country_code="None", address_type="263"
        )
        # When
        await member_test_client.set_address_for_member(address=previous)
        await member_test_client.set_address_for_member(address=expected)
        # Then
        actual = await member_test_client.get_address_by_member_id(
            member_id=test_member.id
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
        test_member_do_not_contact: member_client.Member,
        member_test_client,
        set_to_value,
    ):
        # When
        set_do_not_contact = await member_test_client.set_do_not_contact(
            id=test_member_do_not_contact.id, do_not_contact=set_to_value
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
    async def test_bulk_set_do_not_contact(
        test_member: member_client.Member, member_test_client, set_to_value
    ):
        # Given
        await member_test_client.bulk_persist(
            models=factory.MemberFactory.create_batch(
                9, organization_id=test_member.organization_id, file_id=None
            )
        )
        records = await member_test_client.all()
        # using sorted to guarantee we are comparing the same id's
        # since the records are not always returned in same order
        new_records = sorted([(m.id, set_to_value) for i, m in enumerate(records)])
        # When
        await member_test_client.bulk_set_do_not_contact(new_records)
        set_records = sorted(
            [(m.id, m.do_not_contact) for m in await member_test_client.all()]
        )
        # Then
        assert new_records == set_records

    @staticmethod
    async def test_bulk_persist_external_records(
        member_test_client, configuration_test_client, test_config
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
                **er["external_record"],
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
        ) = await member_test_client.bulk_persist_external_records(
            records + extra_records
        )
        member_ids = {m["id"] for m in persisted_members}
        address_member_ids = {a["member_id"] for a in persisted_addresses}
        # Then
        # All non-overlapping records are persisted.
        assert len(persisted_members) == len(records)

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
    async def test_bulk_persist_external_records_update_address(
        member_test_client, configuration_test_client, test_config
    ):
        # Given
        # Set up our config
        external_id = factory.ExternalIDFactory.create(
            organization_id=test_config.organization_id
        )
        await configuration_test_client.add_external_id(**external_id)

        # Mock out the record and address dicts that we generate from our pubsub messages

        record = factory.ExternalRecordFactory.create(
            external_id=external_id["external_id"],
            source=external_id["source"],
            record={"external_id": external_id["external_id"]},
            organization_id=external_id["organization_id"],
        )
        address = factory.AddressFactory.create()
        (
            original_member,
            original_persisted_address,
        ) = await member_test_client.bulk_persist_external_records(
            [{"external_record": record, "record_address": address}]
        )

        member_id = original_member[0]["id"]

        # When

        # Pass in the same member, but just an updated address
        updated_address = factory.AddressFactory.create()

        (
            updated_member,
            updated_persisted_address,
        ) = await member_test_client.bulk_persist_external_records(
            [{"external_record": record, "record_address": updated_address}]
        )

        # Then
        # Assert we saved the right member
        assert original_member[0]["id"] == updated_member[0]["id"]

        # Ensure we updated our new address
        assert updated_persisted_address[0]["member_id"] == member_id
        saved_address = await member_test_client.get_address_by_member_id(
            member_id=member_id
        )
        assert saved_address.address_1 == updated_persisted_address[0]["address_1"]

    @staticmethod
    async def test_bulk_persist_external_records_duplicates(
        test_config: configuration_client.Configuration,
        member_test_client,
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
        ) = await member_test_client.bulk_persist_external_records(
            external_records=persist_record_input
        )
        # Then
        assert len(persisted_records) == 1
        assert persisted_records[0]["email"] == third["email"]

    @staticmethod
    async def test_bulk_persist_external_records_address_removed(
        member_test_client, configuration_test_client, test_config
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

        # Add an additional record that does not have any address associated with it
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
        ) = await member_test_client.bulk_persist_external_records(records)

        # Then
        # Confirm we have saved all our addresses (remember one record doesn't have an address)
        assert len(persisted_addresses) == len(records) - 1

        # When
        # Run our update again- update all our members
        # This time however, do not provide addresses for half the users- want to ensure we remove these addresses
        # We also want to ensure that we handle our record without an address ever associated
        for r in records[0:5]:
            r["record_address"] = None
        (
            persisted_members,
            persisted_addresses,
        ) = await member_test_client.bulk_persist_external_records(records)

        # Then
        member_with_addresses = member_without_addresses = 0
        for m in persisted_members:
            saved_address = await member_test_client.get_address_by_member_id(
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

    # endregion
    @staticmethod
    async def test_get_id_range_for_member(
        member_test_client: member_client.Members,
        configuration_test_client: configuration_client.Configurations,
        test_config: Configuration,
    ):
        # Given
        batch_size = 1_000
        await member_test_client.bulk_persist(
            models=factory.MemberFactory.create_batch(
                size=1_000, organization_id=test_config.organization_id, file_id=None
            )
        )

        # When
        min_id, max_id = await member_test_client.get_id_range_for_member()

        assert (max_id - min_id) == (batch_size - 1)

    @staticmethod
    async def test_get_id_range_for_member_address(
        member_test_client: member_client.Members,
        configuration_test_client: configuration_client.Configurations,
        test_config: Configuration,
    ):
        # Given
        batch_size = 1_000
        external_id = factory.ExternalIDFactory.create(
            organization_id=test_config.organization_id
        )
        await configuration_test_client.add_external_id(**external_id)

        # Mock out the list of record and address dicts that we generate from our pubsub messages
        records = []
        for i in range(batch_size):
            record = factory.ExternalRecordFactory.create(
                external_id=external_id["external_id"],
                source=external_id["source"],
                organization_id=external_id["organization_id"],
                record={"external_id": external_id["external_id"]},
            )
            address = factory.AddressFactory.create()
            records.append({"external_record": record, "record_address": address})

        await member_test_client.bulk_persist_external_records(records)

        # When
        min_id, max_id = await member_test_client.get_id_range_for_member_address()

        assert (max_id - min_id) == (batch_size - 1)

    @staticmethod
    async def test_migrate_member(
        member_test_client: member_client.Members,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        configuration_test_client: configuration_client.Configurations,
        test_config: Configuration,
    ):
        # Given
        batch_size = 10_000
        await member_test_client.bulk_persist(
            models=factory.MemberFactory.create_batch(
                size=batch_size,
                organization_id=test_config.organization_id,
                file_id=None,
            )
        )

        # When
        await member_test_client.migrate_member()

        # Then
        migrated = await member_versioned_test_client.get_count_for_org(
            organization_id=test_config.organization_id
        )

        assert migrated == batch_size

    @staticmethod
    async def test_migrate_member_address(
        member_test_client: member_client.Members,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        configuration_test_client: configuration_client.Configurations,
        test_config: Configuration,
    ):
        # Given
        batch_size = 1_000
        external_id = factory.ExternalIDFactory.create(
            organization_id=test_config.organization_id
        )
        await configuration_test_client.add_external_id(**external_id)

        # Mock out the list of record and address dicts that we generate from our pubsub messages
        records = []
        for i in range(batch_size):
            record = factory.ExternalRecordFactory.create(
                external_id=external_id["external_id"],
                source=external_id["source"],
                organization_id=external_id["organization_id"],
                record={"external_id": external_id["external_id"]},
            )
            address = factory.AddressFactory.create()
            records.append({"external_record": record, "record_address": address})

        await member_test_client.bulk_persist_external_records(records)

        # When
        await member_test_client.migrate_member()
        await member_test_client.migrate_member_address()

        # Then
        count_migrated = (
            await member_versioned_test_client.get_count_of_member_address()
        )

        assert count_migrated == batch_size
