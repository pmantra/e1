import datetime
from datetime import timedelta
from typing import List

import pytest
from tests.factories import data_models as factory
from verification import repository

from db import model as db_model
from db.clients import configuration_client, member_versioned_client

pytestmark = pytest.mark.asyncio


class TestPersistMembers:
    @staticmethod
    async def test_persist_members_and_address_records(
        member_versioned_repo: repository.MemberVersionedRepository,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        configuration_test_client: configuration_client.Configurations,
    ):
        # Given
        org: db_model.Configuration = await configuration_test_client.persist(
            model=factory.ConfigurationFactory.create()
        )
        record: db_model.ExternalRecord = factory.ExternalRecordFactory.create(
            organization_id=org.organization_id
        )
        external_record_and_address: db_model.ExternalRecordAndAddress = (
            factory.ExternalRecordAndAddressFactory(external_record=record)
        )

        # When
        await member_versioned_repo.persist_members_and_address_records(
            records=[external_record_and_address]
        )

        # Then
        members: List[
            db_model.MemberVersioned
        ] = await member_versioned_test_client.get_for_org(
            organization_id=org.organization_id
        )
        member: db_model.MemberVersioned = members[0]
        assert (
            member.organization_id,
            member.unique_corp_id,
            member.dependent_id,
            member.date_of_birth,
            member.email,
            member.first_name,
            member.last_name,
        ) == (
            record["organization_id"],
            record["unique_corp_id"],
            record["dependent_id"],
            record["date_of_birth"],
            record["email"],
            record["first_name"],
            record["last_name"],
        )


# region fetch
class TestGetMembersByOrg:
    @staticmethod
    async def test_get_member_versioned_by_organization_id(
        member_versioned_repo: repository.MemberVersionedRepository,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        test_member_versioned: member_versioned_client.MemberVersioned,
    ):
        # Given
        org_id = test_member_versioned.organization_id

        # When
        returned_member_record = await member_versioned_repo.get_members_for_org(
            organization_id=org_id
        )

        # Then
        assert returned_member_record == [test_member_versioned]


class TestGetMembersByFile:
    @staticmethod
    async def test_get_member_versioned_by_file_id(
        member_versioned_repo: repository.MemberVersionedRepository,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        test_member_versioned: member_versioned_client.MemberVersioned,
    ):
        # Given
        file_id = test_member_versioned.file_id

        # When
        returned_member_record = await member_versioned_repo.get_members_for_file(
            file_id=file_id
        )

        # Then
        assert returned_member_record == [test_member_versioned]


class TestGetMembersByPrimaryVerification:
    @staticmethod
    async def test_get_member_by_dob_and_email(
        member_versioned_repo: repository.MemberVersionedRepository,
        test_member_versioned: member_versioned_client.MemberVersioned,
    ):

        # Given
        # When
        returned_member_record = await member_versioned_repo.get_by_dob_and_email(
            date_of_birth=test_member_versioned.date_of_birth,
            email=test_member_versioned.email,
        )

        # Then
        assert returned_member_record == [test_member_versioned]

    @staticmethod
    async def test_get_member_by_dob_and_email_no_match(
        member_versioned_repo: repository.MemberVersionedRepository,
        test_member_versioned: member_versioned_client.MemberVersioned,
    ):
        # Given
        # When
        returned_member_record = await member_versioned_repo.get_by_dob_and_email(
            date_of_birth=test_member_versioned.date_of_birth + timedelta(days=10),
            email=test_member_versioned.email,
        )

        # Then
        assert returned_member_record == []

    @staticmethod
    async def test_get_member_by_dob_and_email_multiple_match(
        member_versioned_repo: repository.MemberVersionedRepository,
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
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
            updated_at=test_member_versioned.updated_at + timedelta(days=10),
        )
        test_member_versioned_new = await member_versioned_test_client.get(
            test_member_versioned_new.id
        )
        # When
        returned_member_record = await member_versioned_repo.get_by_dob_and_email(
            date_of_birth=test_member_versioned.date_of_birth,
            email=test_member_versioned.email,
        )

        # Then
        assert len(returned_member_record) == 1
        assert returned_member_record[0].id == test_member_versioned_new.id

    @staticmethod
    async def test_get_member_by_dob_and_email_latest_is_expired(
        member_versioned_repo: repository.MemberVersionedRepository,
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
    ):
        """Test the scenario where we have a valid record and also an updated expired record for a member"""
        # Given
        test_member_versioned_expired = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_member_versioned.organization_id,
                file_id=test_member_versioned.file_id,
                work_state=test_member_versioned.work_state,
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                date_of_birth=test_member_versioned.date_of_birth,
                unique_corp_id=test_member_versioned.unique_corp_id,
                email=test_member_versioned.email,
                # Latest member record for this person which is expired
                effective_range=factory.DateRangeFactory.create(
                    upper=datetime.date.today() - datetime.timedelta(days=10)
                ),
            )
        )

        # Manually set our updated at timestamp, so we can check to ensure we return the most recent record
        await member_versioned_test_client.set_updated_at(
            id=test_member_versioned_expired.id,
            updated_at=test_member_versioned.updated_at + timedelta(days=10),
        )

        # When
        returned_member_record = await member_versioned_repo.get_by_dob_and_email(
            date_of_birth=test_member_versioned.date_of_birth,
            email=test_member_versioned.email,
        )

        # Then
        assert not returned_member_record

    @staticmethod
    async def test_get_member_by_dob_and_email_fetches_successfully_before_expiration(
        member_versioned_repo: repository.MemberVersionedRepository,
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
    ):
        """Test the scenario where we have a valid record and also an updated expired record for a member"""
        # Given
        pre_expiration_member_record = await member_versioned_repo.get_by_dob_and_email(
            date_of_birth=test_member_versioned.date_of_birth,
            email=test_member_versioned.email,
        )

        test_member_versioned_expired = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_member_versioned.organization_id,
                file_id=test_member_versioned.file_id,
                work_state=test_member_versioned.work_state,
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                date_of_birth=test_member_versioned.date_of_birth,
                unique_corp_id=test_member_versioned.unique_corp_id,
                email=test_member_versioned.email,
                # Latest member record for this person which is expired
                effective_range=factory.DateRangeFactory.create(
                    upper=datetime.date.today() - datetime.timedelta(days=10)
                ),
            )
        )

        # Manually set our updated at timestamp, so we can check to ensure we return the most recent record
        await member_versioned_test_client.set_updated_at(
            id=test_member_versioned_expired.id,
            updated_at=test_member_versioned.updated_at + timedelta(days=10),
        )

        # When
        returned_member_record = await member_versioned_repo.get_by_dob_and_email(
            date_of_birth=test_member_versioned.date_of_birth,
            email=test_member_versioned.email,
        )

        # Then
        assert pre_expiration_member_record and not returned_member_record


class TestGetMembersBySecondaryVerification:
    @staticmethod
    async def test_get_member_by_secondary_verification(
        member_versioned_repo: repository.MemberVersionedRepository,
        test_member_versioned: member_versioned_client.MemberVersioned,
    ):
        # Given
        # When
        returned_member_record = (
            await member_versioned_repo.get_by_secondary_verification(
                date_of_birth=test_member_versioned.date_of_birth,
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                work_state=test_member_versioned.work_state,
            )
        )

        # Then
        assert returned_member_record == [test_member_versioned]

    @staticmethod
    async def test_get_member_by_secondary_verification_no_match(
        member_versioned_repo: repository.MemberVersionedRepository,
        test_member_versioned: member_versioned_client.MemberVersioned,
    ):
        # Given
        # When
        returned_member_record = (
            await member_versioned_repo.get_by_secondary_verification(
                date_of_birth=test_member_versioned.date_of_birth + timedelta(days=10),
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                work_state=test_member_versioned.work_state,
            )
        )

        # Then
        assert returned_member_record == []

    @staticmethod
    async def test_get_member_by_secondary_verification_multiple_match(
        member_versioned_repo: repository.MemberVersionedRepository,
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
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
            updated_at=test_member_versioned.updated_at + timedelta(days=10),
        )
        test_member_versioned_new = await member_versioned_test_client.get(
            test_member_versioned_new.id
        )
        # When
        returned_member_record = (
            await member_versioned_repo.get_by_secondary_verification(
                date_of_birth=test_member_versioned.date_of_birth,
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                work_state=test_member_versioned.work_state,
            )
        )

        # Then
        assert returned_member_record[0].id == test_member_versioned_new.id

    @staticmethod
    async def test_get_member_by_secondary_verification_latest_is_expired(
        member_versioned_repo: repository.MemberVersionedRepository,
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
    ):
        """Test the scenario where we have a valid record and also an updated expired record for a member"""
        # Given
        test_member_versioned_expired = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_member_versioned.organization_id,
                file_id=test_member_versioned.file_id,
                work_state=test_member_versioned.work_state,
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                date_of_birth=test_member_versioned.date_of_birth,
                unique_corp_id=test_member_versioned.unique_corp_id,
                email=test_member_versioned.email,
                # Latest member record for this person which is expired
                effective_range=factory.DateRangeFactory.create(
                    upper=datetime.date.today() - datetime.timedelta(days=10)
                ),
            )
        )

        # Manually set our updated at timestamp, so we can check to ensure we return the most recent record
        await member_versioned_test_client.set_updated_at(
            id=test_member_versioned_expired.id,
            updated_at=test_member_versioned.updated_at + timedelta(days=10),
        )

        # When
        returned_member_record = (
            await member_versioned_repo.get_by_secondary_verification(
                date_of_birth=test_member_versioned.date_of_birth,
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                work_state=test_member_versioned.work_state,
            )
        )

        # Then
        assert not returned_member_record

    @staticmethod
    async def test_get_member_by_secondary_verification_fetches_successfully_before_expiration(
        member_versioned_repo: repository.MemberVersionedRepository,
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
    ):
        """Test the scenario where we have a valid record and also an updated expired record for a member"""
        # Given
        pre_expiration_member_record = (
            await member_versioned_repo.get_by_secondary_verification(
                date_of_birth=test_member_versioned.date_of_birth,
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                work_state=test_member_versioned.work_state,
            )
        )

        test_member_versioned_expired = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_member_versioned.organization_id,
                file_id=test_member_versioned.file_id,
                work_state=test_member_versioned.work_state,
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                date_of_birth=test_member_versioned.date_of_birth,
                unique_corp_id=test_member_versioned.unique_corp_id,
                email=test_member_versioned.email,
                # Latest member record for this person which is expired
                effective_range=factory.DateRangeFactory.create(
                    upper=datetime.date.today() - datetime.timedelta(days=10)
                ),
            )
        )

        # Manually set our updated at timestamp, so we can check to ensure we return the most recent record
        await member_versioned_test_client.set_updated_at(
            id=test_member_versioned_expired.id,
            updated_at=test_member_versioned.updated_at + timedelta(days=10),
        )

        # When
        returned_member_record = (
            await member_versioned_repo.get_by_secondary_verification(
                date_of_birth=test_member_versioned.date_of_birth,
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                work_state=test_member_versioned.work_state,
            )
        )

        # Then
        assert pre_expiration_member_record and not returned_member_record


class TestGetMembersByTertiaryVerification:
    @staticmethod
    async def test_get_member_by_tertiary_verification(
        member_versioned_repo: repository.MemberVersionedRepository,
        test_member_versioned: member_versioned_client.MemberVersioned,
    ):
        # Given
        # When
        returned_member_record = (
            await member_versioned_repo.get_by_tertiary_verification(
                date_of_birth=test_member_versioned.date_of_birth,
                unique_corp_id=test_member_versioned.unique_corp_id,
            )
        )

        # Then
        assert returned_member_record == [test_member_versioned]

    @staticmethod
    async def test_get_member_by_tertiary_verification_no_match(
        member_versioned_repo: repository.MemberVersionedRepository,
        test_member_versioned: member_versioned_client.MemberVersioned,
    ):
        # Given
        # When
        returned_member_record = (
            await member_versioned_repo.get_by_tertiary_verification(
                date_of_birth=test_member_versioned.date_of_birth + timedelta(days=10),
                unique_corp_id=test_member_versioned.unique_corp_id,
            )
        )

        # Then
        assert returned_member_record == []

    @staticmethod
    async def test_get_member_by_tertiary_verification_multiple_match(
        member_versioned_repo: repository.MemberVersionedRepository,
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
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
            updated_at=test_member_versioned.updated_at + timedelta(days=10),
        )
        test_member_versioned_new = await member_versioned_test_client.get(
            test_member_versioned_new.id
        )
        # When
        returned_member_record = (
            await member_versioned_repo.get_by_tertiary_verification(
                date_of_birth=test_member_versioned.date_of_birth,
                unique_corp_id=test_member_versioned.unique_corp_id,
            )
        )

        # Then
        assert returned_member_record[0].id == test_member_versioned_new.id

    @staticmethod
    async def test_get_member_by_tertiary_verification_latest_is_expired(
        member_versioned_repo: repository.MemberVersionedRepository,
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
    ):
        # Given
        test_member_versioned_expired = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_member_versioned.organization_id,
                file_id=test_member_versioned.file_id,
                work_state=test_member_versioned.work_state,
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                date_of_birth=test_member_versioned.date_of_birth,
                unique_corp_id=test_member_versioned.unique_corp_id,
                email=test_member_versioned.email,
                # Latest member record for this person which is expired
                effective_range=factory.DateRangeFactory.create(
                    upper=datetime.date.today() - datetime.timedelta(days=10)
                ),
            )
        )

        # Manually set our updated at timestamp, so we can check to ensure we return the most recent record
        await member_versioned_test_client.set_updated_at(
            id=test_member_versioned_expired.id,
            updated_at=test_member_versioned.updated_at + timedelta(days=10),
        )

        # When
        returned_member_record = (
            await member_versioned_repo.get_by_tertiary_verification(
                date_of_birth=test_member_versioned.date_of_birth,
                unique_corp_id=test_member_versioned.unique_corp_id,
            )
        )

        # Then
        assert not returned_member_record

    @staticmethod
    async def test_get_member_by_tertiary_verification_fetches_successfully_before_expiration(
        member_versioned_repo: repository.MemberVersionedRepository,
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
    ):
        # Given
        pre_expiration_member_record = (
            await member_versioned_repo.get_by_tertiary_verification(
                date_of_birth=test_member_versioned.date_of_birth,
                unique_corp_id=test_member_versioned.unique_corp_id,
            )
        )

        test_member_versioned_expired = await member_versioned_test_client.persist(
            model=factory.MemberVersionedFactory.create(
                organization_id=test_member_versioned.organization_id,
                file_id=test_member_versioned.file_id,
                work_state=test_member_versioned.work_state,
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                date_of_birth=test_member_versioned.date_of_birth,
                unique_corp_id=test_member_versioned.unique_corp_id,
                email=test_member_versioned.email,
                # Latest member record for this person which is expired
                effective_range=factory.DateRangeFactory.create(
                    upper=datetime.date.today() - datetime.timedelta(days=10)
                ),
            )
        )

        # Manually set our updated at timestamp, so we can check to ensure we return the most recent record
        await member_versioned_test_client.set_updated_at(
            id=test_member_versioned_expired.id,
            updated_at=test_member_versioned.updated_at + timedelta(days=10),
        )

        # When
        returned_member_record = (
            await member_versioned_repo.get_by_tertiary_verification(
                date_of_birth=test_member_versioned.date_of_birth,
                unique_corp_id=test_member_versioned.unique_corp_id,
            )
        )

        # Then
        assert pre_expiration_member_record and not returned_member_record


class TestGetMembersByAnyVerification:
    @staticmethod
    @pytest.mark.parametrize(
        argnames="birth_date_delta, match_exists",
        argvalues=[(0, True), (10, False)],
        ids=["match", "no_match_different_birthdate"],
    )
    async def test_get_member_by_any_verification_primary(
        member_versioned_repo: repository.MemberVersionedRepository,
        test_member_versioned: member_versioned_client.MemberVersioned,
        birth_date_delta,
        match_exists,
    ):
        # Given
        # When
        returned_member_record = await member_versioned_repo.get_by_any_verification(
            date_of_birth=test_member_versioned.date_of_birth
            + timedelta(days=birth_date_delta),
            email=test_member_versioned.email,
            first_name=None,
            last_name=None,
            work_state=None,
        )

        # Then
        if match_exists:
            assert returned_member_record == test_member_versioned
        else:
            assert returned_member_record is None

    @staticmethod
    @pytest.mark.parametrize(
        argnames="first_name_override, match_exists",
        argvalues=[(None, True), ("Foobar", False)],
        ids=["match_exists", "no_match_different_name"],
    )
    async def test_get_member_by_any_verification_secondary(
        member_versioned_repo: repository.MemberVersionedRepository,
        test_member_versioned: member_versioned_client.MemberVersioned,
        first_name_override: str,
        match_exists: bool,
    ):
        # Given
        # When
        first_name = (
            first_name_override
            if first_name_override
            else test_member_versioned.first_name
        )
        returned_member_record = await member_versioned_repo.get_by_any_verification(
            date_of_birth=test_member_versioned.date_of_birth,
            first_name=first_name,
            last_name=test_member_versioned.last_name,
            work_state=test_member_versioned.work_state,
            email=None,
        )

        # Then
        if match_exists:
            assert returned_member_record == test_member_versioned
        else:
            assert returned_member_record is None

    @staticmethod
    async def test_get_member_by_any_verification_multiple_match(
        member_versioned_repo: repository.MemberVersionedRepository,
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
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
            updated_at=test_member_versioned.updated_at + timedelta(days=10),
        )
        test_member_versioned_new = await member_versioned_test_client.get(
            test_member_versioned_new.id
        )
        # When
        returned_member_record = await member_versioned_repo.get_by_any_verification(
            date_of_birth=test_member_versioned.date_of_birth,
            first_name=test_member_versioned.first_name,
            last_name=test_member_versioned.last_name,
            work_state=test_member_versioned.work_state,
            email=test_member_versioned.email,
        )

        # Then
        assert returned_member_record.id == test_member_versioned_new.id

    @staticmethod
    async def test_get_member_by_any_verification_latest_is_expired(
        member_versioned_repo: repository.MemberVersionedRepository,
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
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
                # Latest member record for this person which is expired
                effective_range=factory.DateRangeFactory.create(
                    upper=datetime.date.today() - datetime.timedelta(days=10)
                ),
            )
        )

        # Manually set our created at timestamp, so we can check to ensure we return the most recent record
        await member_versioned_test_client.set_updated_at(
            id=test_member_versioned_new.id,
            updated_at=test_member_versioned.updated_at + timedelta(days=10),
        )

        # When
        returned_member_record = await member_versioned_repo.get_by_any_verification(
            date_of_birth=test_member_versioned.date_of_birth,
            first_name=test_member_versioned.first_name,
            last_name=test_member_versioned.last_name,
            work_state=test_member_versioned.work_state,
            email=test_member_versioned.email,
        )

        # Then
        assert not returned_member_record

    @staticmethod
    async def test_get_member_by_any_verification_fetches_successfully_before_expiration(
        member_versioned_repo: repository.MemberVersionedRepository,
        test_member_versioned: member_versioned_client.MemberVersioned,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
    ):
        # Given
        pre_expiration_member_record = (
            await member_versioned_repo.get_by_any_verification(
                date_of_birth=test_member_versioned.date_of_birth,
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                work_state=test_member_versioned.work_state,
                email=test_member_versioned.email,
            )
        )

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
                # Latest member record for this person which is expired
                effective_range=factory.DateRangeFactory.create(
                    upper=datetime.date.today() - datetime.timedelta(days=10)
                ),
            )
        )

        # Manually set our created at timestamp, so we can check to ensure we return the most recent record
        await member_versioned_test_client.set_updated_at(
            id=test_member_versioned_new.id,
            updated_at=test_member_versioned.updated_at + timedelta(days=10),
        )

        # When
        returned_member_record = await member_versioned_repo.get_by_any_verification(
            date_of_birth=test_member_versioned.date_of_birth,
            first_name=test_member_versioned.first_name,
            last_name=test_member_versioned.last_name,
            work_state=test_member_versioned.work_state,
            email=test_member_versioned.email,
        )

        # Then
        assert pre_expiration_member_record and not returned_member_record


# endregion
