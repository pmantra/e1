import datetime
import random
import uuid
from unittest import mock

import pytest
from tests.factories import data_models as factory
from verification import repository

from db.clients import (
    member_verification_client,
    verification_2_client,
    verification_attempt_client,
    verification_client,
)
from db.model import VerificationTypes

pytestmark = pytest.mark.asyncio


class TestPersistVerification:
    @staticmethod
    async def test_persist_verification(
        verification_repo: repository.VerificationRepository,
        verification_test_client: verification_client.Verifications,
        test_config,
        test_member_versioned,
    ):
        # Given
        test_verification = factory.VerificationFactory.create(
            organization_id=test_config.organization_id,
            verified_at=datetime.datetime.now(tz=datetime.timezone.utc),
        )

        # When
        created_verification = await verification_repo.create_verification(
            user_id=test_verification.user_id,
            organization_id=test_verification.organization_id,
            verification_type=test_verification.verification_type,
            unique_corp_id=test_verification.unique_corp_id,
            first_name=test_verification.first_name,
            last_name=test_verification.last_name,
            date_of_birth=test_verification.date_of_birth,
            additional_fields=test_verification.additional_fields,
            verified_at=test_verification.verified_at,
            verification_session=test_verification.verification_session,
        )

        # Then
        assert created_verification.user_id == test_verification.user_id
        assert (
            created_verification.verification_type
            == test_verification.verification_type
        )
        assert created_verification.first_name == test_verification.first_name
        assert created_verification.last_name == test_verification.last_name
        assert created_verification.date_of_birth == test_verification.date_of_birth
        assert (
            created_verification.additional_fields
            == test_verification.additional_fields
        )
        assert created_verification.verified_at == test_verification.verified_at
        assert created_verification.verification_session == uuid.UUID(
            test_verification.verification_session
        )

    @staticmethod
    async def test_persist_verification_null_dob_additional_fields(
        verification_repo: repository.VerificationRepository,
        verification_test_client: verification_client.Verifications,
        test_config,
        test_member_versioned,
    ):
        # Given
        additional_fields = {"is_employee": True}
        test_verification = factory.VerificationFactory.create(
            organization_id=test_config.organization_id,
            date_of_birth=None,
            additional_fields=additional_fields,
            verified_at=None,
        )

        # When
        created_verification = await verification_repo.create_verification(
            user_id=test_verification.user_id,
            organization_id=test_verification.organization_id,
            verification_type=test_verification.verification_type,
            unique_corp_id=test_verification.unique_corp_id,
            first_name=test_verification.first_name,
            last_name=test_verification.last_name,
            date_of_birth=test_verification.date_of_birth,
            additional_fields=test_verification.additional_fields,
        )

        # Then
        assert created_verification.additional_fields == additional_fields
        assert created_verification.date_of_birth is None
        # Ensure we used a default value
        assert created_verification.verified_at


class TestPersistVerificationAttempt:
    @staticmethod
    async def test_persist_verification_attempt_unsuccessful(
        verification_repo: repository.VerificationRepository,
        verification_test_client: verification_client.Verifications,
        verification_attempt_test_client: verification_attempt_client.VerificationAttempts,
        test_config,
        test_member_versioned,
    ):
        # Given
        test_verification_attempt = factory.VerificationAttemptFactory.create(
            organization_id=test_config.organization_id, verification_id=None
        )

        # When
        created_verification_attempt = (
            await verification_repo.create_verification_attempt(
                verification_type=test_verification_attempt.verification_type,
                organization_id=test_verification_attempt.organization_id,
                date_of_birth=test_verification_attempt.date_of_birth,
                additional_fields=test_verification_attempt.additional_fields,
            )
        )

        # Then
        assert (
            created_verification_attempt.verification_type
            == test_verification_attempt.verification_type
        )
        assert (
            created_verification_attempt.date_of_birth
            == test_verification_attempt.date_of_birth
        )
        assert created_verification_attempt.successful_verification is False
        assert created_verification_attempt.verification_id is None
        assert (
            created_verification_attempt.additional_fields
            == test_verification_attempt.additional_fields
        )

    @staticmethod
    async def test_persist_verification_attempt_successful_verification(
        verification_repo: repository.VerificationRepository,
        verification_test_client: verification_client.Verifications,
        verification_attempt_test_client: verification_attempt_client.VerificationAttempts,
        test_config,
        test_member_versioned,
    ):
        # Given
        test_verification_attempt = factory.VerificationAttemptFactory.create(
            organization_id=test_config.organization_id
        )

        # When
        created_verification_attempt = (
            await verification_repo.create_verification_attempt(
                verification_type=test_verification_attempt.verification_type,
                organization_id=test_verification_attempt.organization_id,
                date_of_birth=test_verification_attempt.date_of_birth,
                verification_id=test_verification_attempt.verification_id,
                verified_at=test_verification_attempt.verified_at,
            )
        )

        # Then
        assert (
            created_verification_attempt.verification_type
            == test_verification_attempt.verification_type
        )
        assert (
            created_verification_attempt.date_of_birth
            == test_verification_attempt.date_of_birth
        )
        assert (
            created_verification_attempt.verified_at.date()
            == test_verification_attempt.verified_at
        )
        assert created_verification_attempt.successful_verification is True
        assert (
            created_verification_attempt.verification_id
            == test_verification_attempt.verification_id
        )


class TestCreateMemberVerification:
    @staticmethod
    async def test_create_member_verification_successful_verification(
        verification_repo: repository.VerificationRepository,
        verification_test_client: verification_client.Verifications,
        test_member_versioned,
        test_verification,
        test_verification_attempt,
    ):
        # Given
        # When
        created_member_verification_record = (
            await verification_repo.create_member_verification(
                member_id=test_member_versioned.id,
                verification_id=test_verification.id,
                verification_attempt_id=test_verification_attempt.id,
            )
        )
        # Then
        assert (
            created_member_verification_record.verification_id == test_verification.id
        )
        assert created_member_verification_record.member_id == test_member_versioned.id
        assert (
            created_member_verification_record.verification_attempt_id
            == test_verification_attempt.id
        )

    @staticmethod
    async def test_create_member_verification_failed_verification(
        verification_repo: repository.VerificationRepository,
        verification_test_client: verification_client.Verifications,
        test_member_versioned,
        test_verification_attempt,
    ):
        # Given
        # When
        created_member_verification_record = (
            await verification_repo.create_member_verification(
                member_id=test_member_versioned.id,
                verification_attempt_id=test_verification_attempt.id,
            )
        )
        # Then
        assert created_member_verification_record.verification_id is None
        assert created_member_verification_record.member_id == test_member_versioned.id
        assert (
            created_member_verification_record.verification_attempt_id
            == test_verification_attempt.id
        )


class TestGetVerificationMemberId:
    @staticmethod
    async def test_get_verification_for_member_id(
        verification_repo: repository.VerificationRepository,
        test_verification,
        test_member_verification,
    ):
        # When
        returned_verification = await verification_repo.get_verification_for_member(
            member_id=test_member_verification.member_id
        )

        # Then
        assert test_verification == returned_verification

    @staticmethod
    async def test_get_verification_for_member_id_no_member_verification(
        verification_repo: repository.VerificationRepository, test_member_versioned
    ):
        # When
        returned_verification = await verification_repo.get_verification_for_member(
            member_id=test_member_versioned.id
        )

        # Then
        assert returned_verification is None

    @staticmethod
    async def test_get_all_verification_for_member_id(
        verification_repo: repository.VerificationRepository,
        member_verification_test_client: member_verification_client.MemberVerifications,
        test_member_versioned,
        multiple_test_verifications,
    ):
        # When
        member_verifications = []
        for v in multiple_test_verifications:
            member_verifications.append(
                await member_verification_test_client.persist(
                    model=factory.MemberVerificationFactory.create(
                        member_id=test_member_versioned.id, verification_id=v.id
                    )
                )
            )

        returned_verification = (
            await verification_repo.get_all_verifications_for_member(
                member_id=test_member_versioned.id
            )
        )

        # Then
        assert len(member_verifications) == len(returned_verification)


class TestGetVerificationAttempts:
    @staticmethod
    async def test_get_verification_attempts_for_member(
        verification_repo: repository.VerificationRepository,
        member_verification_test_client: member_verification_client.MemberVerifications,
        test_member_versioned,
        multiple_test_verifications,
    ):
        # Given
        failed_attempt_count = 5
        successful_verification_attempts = []
        failed_verification_attempts = []
        for v in multiple_test_verifications:
            attempt = await verification_repo.create_verification_attempt(
                verification_type=v.verification_type,
                organization_id=v.organization_id,
                date_of_birth=v.date_of_birth,
                verification_id=v.id,
            )
            successful_verification_attempts.append(attempt)
            await verification_repo.create_member_verification(
                member_id=test_member_versioned.id,
                verification_attempt_id=attempt.id,
                verification_id=v.id,
            )

        for v in range(failed_attempt_count):
            attempt = await verification_repo.create_verification_attempt(
                verification_type=random.choice(list(VerificationTypes)).value,
                date_of_birth=test_member_versioned.date_of_birth,
                organization_id=test_member_versioned.organization_id,
                verification_id=None,
            )

            failed_verification_attempts.append(attempt)
            await verification_repo.create_member_verification(
                member_id=test_member_versioned.id, verification_attempt_id=attempt.id
            )

        # When
        verification_attempts = (
            await verification_repo.get_verification_attempts_for_member(
                member_id=test_member_versioned.id
            )
        )

        # Then
        assert len(verification_attempts["failed"]) == failed_attempt_count
        assert len(verification_attempts["successful"]) == len(
            successful_verification_attempts
        )

    @staticmethod
    async def test_get_verification_attempts_for_member_no_results(
        verification_repo: repository.VerificationRepository,
        member_verification_test_client: member_verification_client.MemberVerifications,
        test_member_versioned,
        multiple_test_verifications,
    ):
        # Given
        failed_attempt_count = 5
        successful_verification_attempts = []
        failed_verification_attempts = []
        for v in multiple_test_verifications:
            attempt = await verification_repo.create_verification_attempt(
                verification_type=v.verification_type,
                organization_id=v.organization_id,
                date_of_birth=v.date_of_birth,
                verification_id=v.id,
            )
            successful_verification_attempts.append(attempt)
            await verification_repo.create_member_verification(
                member_id=test_member_versioned.id,
                verification_attempt_id=attempt.id,
                verification_id=v.id,
            )

        for v in range(failed_attempt_count):
            attempt = await verification_repo.create_verification_attempt(
                verification_type=random.choice(list(VerificationTypes)).value,
                date_of_birth=test_member_versioned.date_of_birth,
                organization_id=test_member_versioned.organization_id,
                verification_id=None,
            )

            failed_verification_attempts.append(attempt)
            await verification_repo.create_member_verification(
                member_id=test_member_versioned.id, verification_attempt_id=attempt.id
            )

        # When
        verification_attempts = (
            await verification_repo.get_verification_attempts_for_member(
                member_id=test_member_versioned.id
            )
        )

        # Then
        assert len(verification_attempts["failed"]) == failed_attempt_count
        assert len(verification_attempts["successful"]) == len(
            successful_verification_attempts
        )


class TestGetUserForEligibilityMemberID:
    @staticmethod
    async def test_get_user_ids_for_eligibility_member_id(
        verification_repo: repository.VerificationRepository,
        member_verification_test_client: member_verification_client.MemberVerifications,
        test_member_versioned,
        multiple_test_verifications,
    ):
        # Given
        expected_user_ids = []
        for v in multiple_test_verifications:

            expected_user_ids.append(v.user_id)
            attempt = await verification_repo.create_verification_attempt(
                verification_type=v.verification_type,
                organization_id=v.organization_id,
                date_of_birth=v.date_of_birth,
                verification_id=v.id,
            )

            await verification_repo.create_member_verification(
                member_id=test_member_versioned.id,
                verification_attempt_id=attempt.id,
                verification_id=v.id,
            )

        # When
        returned_user_ids = (
            await verification_repo.get_user_ids_for_eligibility_member_id(
                member_id=test_member_versioned.id
            )
        )

        # Then
        assert expected_user_ids.sort() == returned_user_ids.sort()


class TestGetEligibilityVerificationRecordForUser:
    @staticmethod
    async def test_get_eligibility_verification_record_for_user(
        verification_repo: repository.VerificationRepository,
        verification_test_client: verification_client.Verifications,
        test_eligibility_verification_record,
    ):
        # When
        returned_verification = (
            await verification_repo.get_eligibility_verification_record_for_user(
                user_id=test_eligibility_verification_record.user_id
            )
        )

        # Then
        assert test_eligibility_verification_record == returned_verification


class TestGetAllEligibilityVerificationRecordsForUser:
    @staticmethod
    async def test_get_all_eligibility_verification_records_for_user(
        verification_repo: repository.VerificationRepository,
        verification_test_client: verification_client.Verifications,
        test_eligibility_verification_record,
    ):
        # When
        returned_verifications = (
            await verification_repo.get_all_eligibility_verification_records_for_user(
                user_id=test_eligibility_verification_record.user_id
            )
        )

        # Then
        assert test_eligibility_verification_record == returned_verifications[0]


class TestGetEligibleMemberForUser:
    @staticmethod
    async def test_get_eligibility_member_id_for_user_and_org(
        verification_repo: repository.VerificationRepository,
        member_verification_test_client: member_verification_client.MemberVerifications,
        multiple_test_members_versioned,
        multiple_test_verifications,
    ):
        # Given
        for i in range(len(multiple_test_verifications) - 1):
            v = multiple_test_verifications[i]
            m = multiple_test_members_versioned[i]

            attempt = await verification_repo.create_verification_attempt(
                verification_type=v.verification_type,
                organization_id=v.organization_id,
                date_of_birth=v.date_of_birth,
                verification_id=v.id,
            )

            await verification_repo.create_member_verification(
                member_id=m.id,
                verification_attempt_id=attempt.id,
                verification_id=v.id,
            )

        user_id = multiple_test_verifications[0].user_id
        org_id = multiple_test_verifications[0].organization_id
        expected_member_id = multiple_test_members_versioned[0].id

        # When
        returned_member_id = (
            await verification_repo.get_eligibility_member_id_for_user_and_org(
                user_id=user_id, organization_id=org_id
            )
        )

        # Then
        assert returned_member_id == expected_member_id


class TestGetVerificationKeyForUser:
    @staticmethod
    async def test_get_verification_key_for_user(
        verification_repo: repository.VerificationRepository,
        verification_test_client: verification_client.Verifications,
        test_eligibility_verification_record,
    ):
        # When
        returned_verifications = await verification_repo.get_verification_key_for_user(
            user_id=test_eligibility_verification_record.user_id
        )

        # Then
        assert (
            test_eligibility_verification_record.verification_id
            == returned_verifications.verification_1_id
        )


class TestDeactivateVerificationForUser:
    @staticmethod
    async def test_deactivate_verification_for_user(
        verification_repo: repository.VerificationRepository,
        verification_test_client: verification_client.Verifications,
        test_verification,
    ):
        returned_verifications = (
            await verification_repo.deactivate_verification_for_user(
                user_id=test_verification.user_id,
                verification_id=test_verification.id,
            )
        )
        assert test_verification.id == returned_verifications.id
        # Get the start of the current day (UTC)
        start_of_today = datetime.datetime.utcnow().replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=datetime.timezone.utc
        )
        assert returned_verifications.deactivated_at == start_of_today

    @staticmethod
    async def test_deactivate_verification_for_user_v2(
        verification_repo: repository.VerificationRepository,
        verification_test_client: verification_client.Verifications,
        verification_2_test_client: verification_2_client.Verification2Client,
        test_verification,
    ):
        with mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_write",
            return_value=True,
        ):
            verification_2 = factory.Verification2Factory.create(
                id=test_verification.verification_2_id,
                organization_id=test_verification.organization_id,
                user_id=test_verification.user_id,
            )
            await verification_2_test_client.persist(model=verification_2)

            returned_verifications = (
                await verification_repo.deactivate_verification_for_user(
                    user_id=test_verification.user_id,
                    verification_id=test_verification.id,
                )
            )
            assert test_verification.id == returned_verifications.id

    @staticmethod
    async def test_deactivate_verification_for_user_v2_error(
        verification_repo: repository.VerificationRepository,
        verification_test_client: verification_client.Verifications,
        verification_2_test_client: verification_2_client.Verification2Client,
        test_verification,
    ):
        with mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_write",
            return_value=True,
        ):
            with pytest.raises(ValueError):
                test_verification_error = await verification_test_client.persist(
                    model=factory.VerificationFactory.create(
                        organization_id=test_verification.organization_id,
                        verification_session=None,
                        user_id=test_verification.user_id,
                        verification_2_id=None,
                    )
                )
                await verification_repo.deactivate_verification_for_user(
                    user_id=test_verification_error.user_id,
                    verification_id=test_verification_error.id,
                )
