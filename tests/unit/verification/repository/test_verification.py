import datetime
from unittest.mock import ANY, patch

import pytest
from tests.factories import data_models
from verification.repository import VerificationRepository

from db import model as db_model

pytestmark = pytest.mark.asyncio


class TestData:
    user_id = 100
    organization_id = 101
    verification_record = data_models.EligibilityVerificationForUserFactory.create(
        user_id=user_id,
        organization_id=organization_id,
        is_v2=False,
    )
    verification_2_record = data_models.EligibilityVerificationForUserFactory.create(
        user_id=user_id,
        organization_id=organization_id,
        eligibility_member_version=1000,
        is_v2=True,
    )
    verification_record.verification_1_id = verification_record.verification_id
    verification_record.verification_2_id = verification_2_record.verification_id
    verification_2_record.verification_1_id = verification_record.verification_id
    verification_2_record.verification_2_id = verification_2_record.verification_id


# region get_eligibility_verification_record_for_user


@pytest.mark.parametrize(
    argnames="found_verification_1, found_verification_2, e9y2_enabled, expected",
    argvalues=[
        (TestData.verification_record, None, False, TestData.verification_record),
        (TestData.verification_record, None, True, TestData.verification_record),
        (
            TestData.verification_record,
            TestData.verification_2_record,
            False,
            TestData.verification_record,
        ),
        (
            TestData.verification_record,
            TestData.verification_2_record,
            True,
            TestData.verification_record,
        ),
        (None, TestData.verification_2_record, False, None),
        (None, TestData.verification_2_record, True, None),
        (None, None, True, None),
        (None, None, False, None),
    ],
    ids=[
        "verification_1 found, verification_2 not found, e9y2 not enabled, should return verification_1",
        "verification_1 found, verification_2 not found, e9y2 enabled, should return verification_1 with v2 enabled",
        "verification_1 found, verification_2 found, e9y2 not enabled, should return verification_1",
        "verification_1 found, verification_2 found, e9y2 enabled, should return verification_1 with v2 enabled",
        "verification_1 not found, verification_2 found, e9y2 not enabled, should return None",
        "verification_1 not found, verification_2 found, e9y2 enabled, should return None",
        "verification_1 not found, verification_2 not found, e9y2 enabled, should return None",
        "verification_1 not found, verification_2 not found, e9y2 not enabled, should return None",
    ],
)
async def test_get_verification_record(
    found_verification_1, found_verification_2, e9y2_enabled, expected
):

    with patch(
        "db.clients.verification_client.Verifications.get_eligibility_verification_record_for_user",
        return_value=found_verification_1,
    ), patch(
        "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
        return_value=e9y2_enabled,
    ):
        repo = VerificationRepository()
        result = await repo.get_eligibility_verification_record_for_user(
            user_id=TestData.user_id
        )
        if expected is not None:
            expected.is_v2 = e9y2_enabled
        assert result == expected


# endregion


# region create_verification_2
async def test_create_verification_2():
    # given
    verification_2: db_model.Verification2 = data_models.Verification2Factory.create(
        user_id=TestData.user_id,
        organization_id=102,
        member_id=1234,
        member_version=1001,
    )
    with patch(
        "db.clients.verification_2_client.Verification2Client.persist",
    ) as verification_2_persist, patch(
        "verification.repository.verification.logger.info"
    ) as logger_info:
        repo = VerificationRepository()

        # when
        _ = await repo.create_verification_2(
            user_id=verification_2.user_id,
            verification_type=verification_2.verification_type,
            organization_id=verification_2.organization_id,
            unique_corp_id=verification_2.unique_corp_id,
            dependent_id=verification_2.dependent_id,
            first_name=verification_2.first_name,
            last_name=verification_2.last_name,
            email=verification_2.email,
            work_state=verification_2.work_state,
            date_of_birth=verification_2.date_of_birth,
            additional_fields=verification_2.additional_fields,
            verification_session=verification_2.verification_session,
            eligibility_member_id=verification_2.member_id,
            eligibility_member_version=verification_2.member_version,
        )

        # then
        verification_2_persist.assert_called_once_with(
            user_id=verification_2.user_id,
            organization_id=verification_2.organization_id,
            verification_type=verification_2.verification_type,
            date_of_birth=verification_2.date_of_birth,
            first_name=verification_2.first_name,
            last_name=verification_2.last_name,
            email=verification_2.email,
            unique_corp_id=verification_2.unique_corp_id,
            dependent_id=verification_2.dependent_id,
            work_state=verification_2.work_state,
            deactivated_at=verification_2.deactivated_at,
            verified_at=ANY,
            additional_fields=verification_2.additional_fields,
            verification_session=verification_2.verification_session,
            member_id=verification_2.member_id,
            member_version=verification_2.member_version,
            connection=None,
        )
        logger_info.assert_called_once_with(
            "Verfication2 created successfully",
            verification_id=ANY,
            user_id=verification_2.user_id,
            organization_id=verification_2.organization_id,
            verification_type=verification_2.verification_type,
            verified_at=ANY,
            erification_session=verification_2.verification_session,
            member_id=verification_2.member_id,
            member_version=verification_2.member_version,
        )

    # endregion


# region create_verification_dual_write
async def test_create_verification_dual_write():
    # given
    verification_2: db_model.Verification2 = data_models.Verification2Factory.create(
        user_id=TestData.user_id,
        organization_id=102,
        member_id=1234,
        member_version=1001,
    )
    verification_1: db_model.Verification = data_models.VerificationFactory.create(
        id=1,
        user_id=TestData.user_id,
        organization_id=102,
        verification_2_id=1001,
    )
    verification_attempt: db_model.VerificationAttempt = (
        data_models.VerificationAttemptFactory.create(
            id=1,
        )
    )
    with patch(
        "db.clients.verification_2_client.Verification2Client.persist",
        return_value=verification_2,
    ) as verification_2_persist, patch(
        "db.clients.verification_client.Verifications.persist",
        return_value=verification_1,
    ) as verification_persist, patch(
        "db.clients.verification_attempt_client.VerificationAttempts.persist",
        return_value=verification_attempt,
    ) as verification_attempt_persist, patch(
        "db.clients.member_verification_client.MemberVerifications.persist",
    ) as member_verification_persist:
        repo = VerificationRepository()
        verified_at = datetime.datetime.now()

        # when
        _ = await repo.create_verification_dual_write(
            user_id=verification_2.user_id,
            verification_type=verification_2.verification_type,
            organization_id=verification_2.organization_id,
            unique_corp_id=verification_2.unique_corp_id,
            dependent_id=verification_2.dependent_id,
            first_name=verification_2.first_name,
            last_name=verification_2.last_name,
            email=verification_2.email,
            work_state=verification_2.work_state,
            date_of_birth=verification_2.date_of_birth,
            additional_fields=verification_2.additional_fields,
            verification_session=verification_2.verification_session,
            verified_at=verified_at,
            deactivated_at=None,
            eligibility_member_1_id=1,
            eligibility_member_2_id=1234,
            eligibility_member_2_version=1001,
        )

        # then
        verification_2_persist.assert_called_once_with(
            user_id=verification_2.user_id,
            organization_id=verification_2.organization_id,
            verification_type=verification_2.verification_type,
            date_of_birth=verification_2.date_of_birth,
            first_name=verification_2.first_name,
            last_name=verification_2.last_name,
            email=verification_2.email,
            unique_corp_id=verification_2.unique_corp_id,
            dependent_id=verification_2.dependent_id,
            work_state=verification_2.work_state,
            deactivated_at=verification_2.deactivated_at,
            verified_at=verified_at,
            additional_fields=verification_2.additional_fields,
            verification_session=verification_2.verification_session,
            member_id=verification_2.member_id,
            member_version=verification_2.member_version,
            connection=ANY,
        )

        verification_persist.assert_called_once_with(
            user_id=verification_2.user_id,
            organization_id=verification_2.organization_id,
            verification_type=verification_2.verification_type,
            date_of_birth=verification_2.date_of_birth,
            first_name=verification_2.first_name,
            last_name=verification_2.last_name,
            email=verification_2.email,
            unique_corp_id=verification_2.unique_corp_id,
            dependent_id=verification_2.dependent_id,
            work_state=verification_2.work_state,
            deactivated_at=verification_2.deactivated_at,
            verified_at=verified_at,
            additional_fields=verification_2.additional_fields,
            verification_session=verification_2.verification_session,
            verification_2_id=ANY,
            connection=ANY,
        )

        verification_attempt_persist.assert_called_once_with(
            verification_type=verification_2.verification_type,
            organization_id=verification_2.organization_id,
            unique_corp_id=verification_2.unique_corp_id,
            dependent_id=verification_2.dependent_id,
            first_name=verification_2.first_name,
            last_name=verification_2.last_name,
            email=verification_2.email,
            work_state=verification_2.work_state,
            policy_used="",
            successful_verification=True,
            date_of_birth=verification_2.date_of_birth,
            verification_id=ANY,
            verified_at=verified_at,
            additional_fields=verification_2.additional_fields,
            user_id=verification_2.user_id,
            connection=ANY,
        )

        member_verification_persist.assert_called_once()


# endregion

# region create_multiple_verification_dual_write
async def test_create_multiple_verification_dual_write():
    # given
    verification_2: db_model.Verification2 = data_models.Verification2Factory.create(
        user_id=TestData.user_id,
        organization_id=102,
        member_id=1234,
        member_version=1001,
    )
    verification_1: db_model.Verification = data_models.VerificationFactory.create(
        id=1,
        user_id=TestData.user_id,
        organization_id=102,
        verification_2_id=1001,
    )
    verification_attempt: db_model.VerificationAttempt = (
        data_models.VerificationAttemptFactory.create(
            id=1,
        )
    )
    verification_data_list = [
        db_model.VerificationData(
            eligibility_member_id=1001,
            organization_id=102,
            unique_corp_id="test_corp",
            dependent_id="",
            email="test@gmail.com",
            work_state="NY",
            additional_fields={},
            verification_attempt_id=1,
            verification_id=1,
            member_1_id=1,
            member_2_id=1234,
            member_2_version=1001,
        )
    ]

    with patch(
        "db.clients.verification_2_client.Verification2Client.bulk_persist",
        return_value=[verification_2],
    ) as verification_2_persist, patch(
        "db.clients.verification_client.Verifications.bulk_persist",
        return_value=[verification_1],
    ) as verification_persist, patch(
        "db.clients.verification_attempt_client.VerificationAttempts.bulk_persist",
        return_value=[verification_attempt],
    ) as verification_attempt_persist, patch(
        "db.clients.member_verification_client.MemberVerifications.bulk_persist",
    ) as member_verification_persist:
        repo = VerificationRepository()
        verified_at = datetime.datetime.now()

        # when
        _ = await repo.create_multiple_verification_dual_write(
            user_id=verification_2.user_id,
            verification_type=verification_2.verification_type,
            verification_data_list=verification_data_list,
            first_name=verification_2.first_name,
            last_name=verification_2.last_name,
            date_of_birth=verification_2.date_of_birth,
            verified_at=verified_at,
            verification_session=verification_2.verification_session,
        )

        # then
        verification_2_persist.assert_called_once()

        verification_persist.assert_called_once()

        verification_attempt_persist.assert_called_once()

        member_verification_persist.assert_called_once()

    verification_data_list_no_member = [
        db_model.VerificationData(
            eligibility_member_id=1001,
            organization_id=102,
            unique_corp_id="test_corp",
            dependent_id="",
            email="test@gmail.com",
            work_state="NY",
            additional_fields={},
            verification_attempt_id=1,
            verification_id=1,
            member_1_id=None,
            member_2_id=None,
            member_2_version=None,
        )
    ]

    with patch(
        "db.clients.verification_2_client.Verification2Client.bulk_persist",
        return_value=[verification_2],
    ) as verification_2_persist, patch(
        "db.clients.verification_client.Verifications.bulk_persist",
        return_value=[verification_1],
    ) as verification_persist, patch(
        "db.clients.verification_attempt_client.VerificationAttempts.bulk_persist",
        return_value=[verification_attempt],
    ) as verification_attempt_persist, patch(
        "db.clients.member_verification_client.MemberVerifications.bulk_persist",
    ) as member_verification_persist:
        repo = VerificationRepository()
        verified_at = datetime.datetime.now()

        # when
        _ = await repo.create_multiple_verification_dual_write(
            user_id=verification_2.user_id,
            verification_type=verification_2.verification_type,
            verification_data_list=verification_data_list_no_member,
            first_name=verification_2.first_name,
            last_name=verification_2.last_name,
            date_of_birth=verification_2.date_of_birth,
            verified_at=verified_at,
            verification_session=verification_2.verification_session,
        )

        # then
        verification_2_persist.assert_called_once()

        verification_persist.assert_called_once()

        verification_attempt_persist.assert_called_once()

        member_verification_persist.assert_not_called()


# endregion
