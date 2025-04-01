from __future__ import annotations

import datetime
from typing import List

import pytest
from tests.factories.data_models import (
    ConfigurationFactory,
    Member2Factory,
    MemberVersionedFactory,
    Verification2Factory,
)

from db import model

pytestmark = pytest.mark.asyncio


class TestMember2Client:

    # region fetch tests

    @staticmethod
    @pytest.mark.parametrize(
        argnames="member_id,member_email",
        argvalues=[
            (1001, "foo@foobar.com"),
            (1002, "fOO@fooBar.com"),
            (1003, "foo@foobar.com   "),
        ],
        ids=["compliant_email", "email_ignore_case", "email_ignore_whitespace"],
    )
    async def test_get_by_dob_and_email(
        member_2_test_client, test_config, member_id, member_email
    ):
        # Given
        test_member = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=member_id,
                organization_id=test_config.organization_id,
                email=member_email,
            )
        )

        # When
        returned_member = await member_2_test_client.get_by_dob_and_email(
            date_of_birth=test_member.date_of_birth, email=test_member.email
        )

        # Then
        assert [test_member] == returned_member

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
        test_config,
        member_2_test_client,
        work_state,
        first_name,
        last_name,
    ):
        # Given
        test_member = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=10001,
                organization_id=test_config.organization_id,
                work_state="NY",
                first_name="Alan",
                last_name="Turing",
            )
        )
        # When
        verified = await member_2_test_client.get_by_secondary_verification(
            date_of_birth=test_member.date_of_birth,
            first_name=first_name,
            last_name=last_name,
            work_state=work_state,
        )
        # Then
        assert verified == [test_member]

    @staticmethod
    async def test_get_by_tertiary_verification(
        test_config,
        member_2_test_client,
    ):
        # Given
        test_member = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=10001,
                organization_id=test_config.organization_id,
                work_state="NY",
                first_name="Alan",
                last_name="Turing",
            )
        )

        # When
        verified = await member_2_test_client.get_by_tertiary_verification(
            date_of_birth=test_member.date_of_birth,
            unique_corp_id=test_member.unique_corp_id,
        )
        # Then
        assert verified == [test_member]

    @staticmethod
    async def test_get_by_org_identity(member_2_test_client, test_config):
        # Given
        identify = model.OrgIdentity(
            organization_id=test_config.organization_id,
            unique_corp_id="mock_unique_corp_id",
            dependent_id="mock_dependent_id",
        )
        member_created = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=10001,
                organization_id=identify.organization_id,
                unique_corp_id=identify.unique_corp_id,
                dependent_id=identify.dependent_id,
            )
        )

        # When
        member_read = await member_2_test_client.get_by_org_identity(identity=identify)

        # Then
        assert member_read == member_created

    @staticmethod
    async def test_get_member_by_id(member_2_test_client, test_config):
        # Given
        member_created = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=10001,
                organization_id=test_config.organization_id,
            )
        )

        # When
        member_read = await member_2_test_client.get(pk=10001)
        not_exist_member = await member_2_test_client.get(pk=10009)

        # Then
        assert member_read == member_created
        assert not_exist_member is None

    @staticmethod
    async def test_get_wallet_enablement_by_identity(member_2_test_client, test_config):
        # Given
        identify = model.OrgIdentity(
            organization_id=test_config.organization_id,
            unique_corp_id="mock_unique_corp_id",
            dependent_id="mock_dependent_id",
        )
        member_created = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=10001,
                organization_id=identify.organization_id,
                unique_corp_id=identify.unique_corp_id,
                dependent_id=identify.dependent_id,
                record={
                    "insurance_plan": "mock_insurance_plan",
                    "wallet_enabled": True,
                },
            )
        )

        # When
        wallet_enablement = (
            await member_2_test_client.get_wallet_enablement_by_identity(
                identity=identify
            )
        )

        # Then
        assert wallet_enablement.member_id == member_created.id
        assert wallet_enablement.enabled == member_created.record["wallet_enabled"]
        assert (
            wallet_enablement.insurance_plan == member_created.record["insurance_plan"]
        )

    @staticmethod
    async def test_get_wallet_enablement(member_2_test_client, test_config):
        # Given
        member_id = 10001
        member_created = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=member_id,
                organization_id=test_config.organization_id,
                unique_corp_id="mock_unique_corp_id",
                dependent_id="mock_dependent_id",
                record={
                    "insurance_plan": "mock_insurance_plan",
                    "wallet_enabled": True,
                },
            )
        )

        # When
        wallet_enablement = await member_2_test_client.get_wallet_enablement(
            member_id=member_id,
        )

        # Then
        assert wallet_enablement.member_id == member_created.id
        assert wallet_enablement.enabled == member_created.record["wallet_enabled"]
        assert (
            wallet_enablement.insurance_plan == member_created.record["insurance_plan"]
        )

    @staticmethod
    async def test_get_other_user_ids_in_family(
        member_2_test_client, test_config, verification_2_test_client
    ):
        user_id = 1
        member_id = 1001
        dependent_id = 2
        dependent_member_id = 1002
        unique_corp_id = "Z000"

        test_member = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=member_id,
                organization_id=test_config.organization_id,
                unique_corp_id=unique_corp_id,
            )
        )

        test_verification = await verification_2_test_client.persist(
            model=Verification2Factory.create(
                organization_id=test_config.organization_id,
                unique_corp_id=unique_corp_id,
                member_id=test_member.id,
                user_id=user_id,
            )
        )

        user_ids: List[int] = await member_2_test_client.get_other_user_ids_in_family(
            user_id=test_verification.user_id
        )

        # Then
        assert not user_ids

        test_member_dependent = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=dependent_member_id,
                organization_id=test_config.organization_id,
                unique_corp_id=unique_corp_id,
            )
        )
        _ = await verification_2_test_client.persist(
            model=Verification2Factory.create(
                organization_id=test_config.organization_id,
                unique_corp_id=unique_corp_id,
                member_id=test_member_dependent.id,
                user_id=dependent_id,
            )
        )
        user_ids_dependent: List[
            int
        ] = await member_2_test_client.get_other_user_ids_in_family(
            user_id=test_verification.user_id
        )

        # Then
        assert dependent_id in user_ids_dependent

    @staticmethod
    async def test_get_by_email_and_name(member_2_test_client, test_config):
        member_id = 1001
        test_member_2 = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=member_id,
                organization_id=test_config.organization_id,
            )
        )
        # When
        verified = await member_2_test_client.get_by_email_and_name(
            email=test_member_2.email,
            first_name=test_member_2.first_name,
            last_name=test_member_2.last_name,
        )
        # Then
        assert verified == [test_member_2]

    @staticmethod
    async def test_get_by_client_specific_verification(
        member_2_test_client, test_config
    ):
        member_id = 1001
        test_member_2 = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=member_id,
                organization_id=test_config.organization_id,
            )
        )
        # When
        verified = await member_2_test_client.get_by_client_specific_verification(
            organization_id=test_member_2.organization_id,
            unique_corp_id=test_member_2.unique_corp_id,
            date_of_birth=test_member_2.date_of_birth,
        )
        # Then
        assert verified == test_member_2

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
        member_2_test_client,
        test_config,
        first_name,
        last_name,
    ):
        # Given
        test_member = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=1001,
                organization_id=test_config.organization_id,
                first_name="Alan",
                last_name="Turing",
            )
        )
        # When
        verified = await member_2_test_client.get_by_overeligibility(
            date_of_birth=test_member.date_of_birth,
            first_name=first_name,
            last_name=last_name,
        )
        # Then
        assert verified == [test_member]

    @staticmethod
    async def test_get_by_overeligiblity_multiple_records_different_org_same_info(
        test_config,
        configuration_test_client,
        member_2_test_client,
        test_member_versioned,
    ):
        """Test when we have multiple members with same identity (fn, ln, dob)"""
        # Given
        test_config_2 = await configuration_test_client.persist(
            model=ConfigurationFactory.create()
        )
        test_member = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=1001,
                organization_id=test_config.organization_id,
                first_name="Alan",
                last_name="Turing",
            )
        )
        test_member_2 = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=1002,
                organization_id=test_config_2.organization_id,
                first_name=test_member.first_name,
                last_name=test_member.last_name,
                date_of_birth=test_member.date_of_birth,
            )
        )

        # When
        verified = await member_2_test_client.get_by_overeligibility(
            date_of_birth=test_member_2.date_of_birth,
            first_name=test_member_2.first_name,
            last_name=test_member_2.last_name,
        )
        # Then
        assert len(verified) == 2
        assert test_member_2 in verified
        assert test_member in verified

    @staticmethod
    async def test_get_by_overeligiblity_multiple_records_same_org(
        test_config,
        member_2_test_client,
    ):
        """Test when we have multiple members with same identifying information in an org- return the most recent one"""
        # Given
        first_name = "Alan"
        last_name = "Turing"
        unique_corp_id = "1"
        dependent_id = "1"

        test_member_old = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=1001,
                organization_id=test_config.organization_id,
                unique_corp_id=unique_corp_id,
                dependent_id=dependent_id,
                first_name=first_name,
                last_name=last_name,
            )
        )

        test_member_new = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=1002,
                organization_id=test_config.organization_id,
                unique_corp_id=unique_corp_id,
                dependent_id=dependent_id,
                first_name=first_name,
                last_name=last_name,
                date_of_birth=test_member_old.date_of_birth,
            )
        )

        await member_2_test_client.set_updated_at(
            id=test_member_new.id,
            updated_at=datetime.datetime(year=2000, month=12, day=12),
        )
        await member_2_test_client.set_updated_at(
            id=test_member_old.id,
            updated_at=datetime.datetime(year=2000, month=12, day=11),
        )
        test_member_new = await member_2_test_client.get(test_member_new.id)

        # When
        verified = await member_2_test_client.get_by_overeligibility(
            date_of_birth=test_member_new.date_of_birth,
            first_name=first_name,
            last_name=last_name,
        )
        # Then
        assert verified == [test_member_new]

    @staticmethod
    async def test_get_by_overeligiblity_multiple_orgs_older_record_same_org(
        test_config,
        configuration_test_client,
        member_2_test_client,
    ):
        """Test when we have multiple members with same identity (fn, ln, dob) - more than one result per org"""
        # Given

        # Set up the record that will live in the same org
        test_config_2 = await configuration_test_client.persist(
            model=ConfigurationFactory.create()
        )
        test_member = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=1001,
                organization_id=test_config.organization_id,
                first_name="Alan",
                last_name="Turing",
            )
        )

        _ = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=1002,
                organization_id=test_config_2.organization_id,
                first_name=test_member.first_name,
                last_name=test_member.last_name,
                date_of_birth=test_member.date_of_birth,
            )
        )

        test_config_1_new_member = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=1003,
                organization_id=test_config.organization_id,
                unique_corp_id=test_member.unique_corp_id,
                first_name=test_member.first_name,
                last_name=test_member.last_name,
                date_of_birth=test_member.date_of_birth,
            )
        )

        # Manually set our updated_at timestamp, older record having a more recent updated_at because of expiration
        await member_2_test_client.set_updated_at(
            id=test_config_1_new_member.id,
            updated_at=datetime.datetime(year=2000, month=12, day=10),
        )
        await member_2_test_client.set_updated_at(
            id=test_member.id,
            updated_at=datetime.datetime(year=2000, month=12, day=9),
        )

        # When
        verified = await member_2_test_client.get_by_overeligibility(
            date_of_birth=test_config_1_new_member.date_of_birth,
            first_name=test_member.first_name,
            last_name=test_member.last_name,
        )
        # Then

        assert len(verified) == 2
        assert test_member not in verified

    @staticmethod
    async def test_get_by_member_versioned(
        test_member_versioned,
        test_config,
        configuration_test_client,
        member_2_test_client,
        member_versioned_test_client,
    ):

        test_member_2 = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=1001,
                organization_id=test_member_versioned.organization_id,
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                email=test_member_versioned.email,
                date_of_birth=test_member_versioned.date_of_birth,
                work_state=test_member_versioned.work_state,
                unique_corp_id=test_member_versioned.unique_corp_id,
            )
        )
        # When
        returned_member = await member_2_test_client.get_by_member_versioned(
            member_versioned=test_member_versioned
        )

        # Then
        assert returned_member == test_member_2

        test_member_versioned.work_state = (
            test_member_versioned.work_state + "_no_match"
        )

        # When
        returned_member = await member_2_test_client.get_by_member_versioned(
            member_versioned=test_member_versioned
        )

        # Then
        assert returned_member is None

        test_member_versioned_no_state = await member_versioned_test_client.persist(
            model=MemberVersionedFactory.create(
                organization_id=test_member_versioned.organization_id,
                work_state=None,
            )
        )
        test_member_2_no_state = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=1002,
                organization_id=test_member_versioned_no_state.organization_id,
                first_name=test_member_versioned_no_state.first_name,
                last_name=test_member_versioned_no_state.last_name,
                email=test_member_versioned_no_state.email,
                date_of_birth=test_member_versioned_no_state.date_of_birth,
                work_state=None,
                unique_corp_id=test_member_versioned_no_state.unique_corp_id,
            )
        )
        returned_member = await member_2_test_client.get_by_member_versioned(
            member_versioned=test_member_versioned_no_state
        )
        assert returned_member == test_member_2_no_state

    @staticmethod
    async def test_get_all_by_name_and_date_of_birth(
        test_config,
        member_2_test_client,
    ):
        member_id = 1001
        test_member_2 = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=member_id,
                organization_id=test_config.organization_id,
                unique_corp_id="unique_corp_id_1",
            )
        )
        test_member_2_other = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=member_id + 1,
                first_name=test_member_2.first_name,
                last_name=test_member_2.last_name,
                date_of_birth=test_member_2.date_of_birth,
                organization_id=test_config.organization_id,
                unique_corp_id="unique_corp_id_2",
            )
        )
        # When
        results = await member_2_test_client.get_all_by_name_and_date_of_birth(
            date_of_birth=test_member_2.date_of_birth,
            first_name=test_member_2.first_name,
            last_name=test_member_2.last_name,
        )
        # Then
        assert len(results) == 2
        assert test_member_2 in results
        assert test_member_2_other in results

    @staticmethod
    async def test_get_by_dob_name_and_work_state(
        test_config,
        member_2_test_client,
    ):
        member_id = 1001
        test_member_2 = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=member_id,
                organization_id=test_config.organization_id,
                unique_corp_id="unique_corp_id_1",
                work_state="FL",
            )
        )
        test_member_2_other = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=member_id + 1,
                first_name=test_member_2.first_name,
                last_name=test_member_2.last_name,
                date_of_birth=test_member_2.date_of_birth,
                work_state=test_member_2.work_state,
                organization_id=test_config.organization_id,
                unique_corp_id="unique_corp_id_2",
            )
        )
        # When
        results = await member_2_test_client.get_by_dob_name_and_work_state(
            date_of_birth=test_member_2.date_of_birth,
            work_state=test_member_2.work_state,
            first_name=test_member_2.first_name,
            last_name=test_member_2.last_name,
        )
        # Then
        assert len(results) == 2
        assert test_member_2 in results
        assert test_member_2_other in results

    @staticmethod
    async def test_get_by_name_and_unique_corp_id(
        test_config,
        member_2_test_client,
    ):
        member_id = 1001
        test_member_2 = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=member_id,
                organization_id=test_config.organization_id,
                unique_corp_id="unique_corp_id_1",
            )
        )
        _ = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=member_id + 1,
                first_name=test_member_2.first_name,
                last_name=test_member_2.last_name,
                organization_id=test_config.organization_id,
                unique_corp_id="unique_corp_id_2",
            )
        )
        # When
        result = await member_2_test_client.get_by_name_and_unique_corp_id(
            unique_corp_id=test_member_2.unique_corp_id,
            first_name=test_member_2.first_name,
            last_name=test_member_2.last_name,
        )
        # Then
        assert result == test_member_2

    @staticmethod
    async def test_get_by_date_of_birth_and_unique_corp_id(
        test_config,
        member_2_test_client,
    ):
        member_id = 1001
        test_member_2 = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=member_id,
                organization_id=test_config.organization_id,
                unique_corp_id="unique_corp_id_1",
            )
        )
        _ = await member_2_test_client.persist(
            model=Member2Factory.create(
                id=member_id + 1,
                date_of_birth=test_member_2.date_of_birth,
                organization_id=test_config.organization_id,
                unique_corp_id="unique_corp_id_2",
            )
        )
        # When
        result = await member_2_test_client.get_by_date_of_birth_and_unique_corp_id(
            unique_corp_id=test_member_2.unique_corp_id,
            date_of_birth=test_member_2.date_of_birth,
        )
        # Then
        assert result == test_member_2
