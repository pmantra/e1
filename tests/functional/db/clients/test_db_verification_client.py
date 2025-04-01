from __future__ import annotations

import datetime
from typing import List

import pytest
from tests.factories import data_models as factory
from tests.functional.conftest import NUMBER_TEST_OBJECTS

from db import model as db_model
from db.clients import (
    configuration_client,
    file_client,
    member_verification_client,
    member_versioned_client,
    verification_attempt_client,
    verification_client,
)

pytestmark = pytest.mark.asyncio


def _get_member_verification_records_for_user(
    multiple_test_members, verifications_for_user
):
    member_ids = [member.id for member in multiple_test_members]
    verification_ids = [verification.id for verification in verifications_for_user]

    records = [
        factory.MemberVerificationFactory.create(
            member_id=member_id,
            verification_id=verification_id,
            verification_attempt_id=None,
        )
        for i, (member_id, verification_id) in enumerate(
            zip(member_ids, verification_ids)
        )
    ]
    return records


class TestVerificationClient:
    # region persist tests
    @staticmethod
    async def test_persist_verified_at(
        test_config: configuration_client.Configuration, verification_test_client
    ):
        # Given
        verification: db_model.Verification = factory.VerificationFactory.create(
            organization_id=test_config.organization_id,
            user_id=1,
            verified_at=datetime.datetime.now(tz=datetime.timezone.utc),
        )

        # When
        persisted: db_model.Verification = await verification_test_client.persist(
            model=verification
        )
        fetched: db_model.Verification = await verification_test_client.get(
            persisted.id
        )
        # Then
        assert verification.verified_at == fetched.verified_at

    # region fetch tests
    @staticmethod
    async def test_all(
        multiple_test_verifications: verification_client.Verifications,
        verification_test_client,
    ):
        # Given
        # We have created 100 verifications -> one for each of our multiple configs. Ensure we have grabbed all of them
        expected_total = NUMBER_TEST_OBJECTS

        # When
        all_verifications = await verification_test_client.all()

        # Then
        # Ensure we have grabbed all verifications
        assert len(all_verifications) == expected_total

    @staticmethod
    async def test_get(
        test_verification: verification_client.Verifications, verification_test_client
    ):
        # When
        returned_verification = await verification_test_client.get(test_verification.id)

        # Then
        assert returned_verification == test_verification

    # region get memberId for userID

    @staticmethod
    async def test_get_member_id_for_user_id_with_multiple_verifications(
        test_file,
        multiple_test_members_versioned,
        verification_test_client,
        member_verification_test_client,
    ):
        # Given
        user_id = 1234

        # Create multiple verifications for a single user
        multiple_verifications_table = []
        for _ in range(3):
            multiple_verifications_table.append(
                await verification_test_client.persist(
                    model=factory.VerificationFactory.create(
                        user_id=user_id,
                        organization_id=test_file.organization_id,
                    )
                )
            )

        # Create member_verification tie for each member_versioned/verification record
        member_verifications = []
        for verification, member_versioned in zip(
            multiple_verifications_table, multiple_test_members_versioned
        ):
            member_verifications.append(
                await member_verification_test_client.persist(
                    model=factory.MemberVerificationFactory.create(
                        member_id=member_versioned.id,
                        verification_id=verification.id,
                    )
                )
            )

        # When
        returned_member = await verification_test_client.get_member_id_for_user_id(
            user_id=user_id
        )

        # Then
        # Ensure we have returned the most recently created member_verification
        assert returned_member == member_verifications[-1].member_id

    @staticmethod
    async def test_get_member_id_for_user_id_with_multiple_member_verification(
        test_file,
        test_verification,
        multiple_test_members_versioned,
        verification_test_client,
        member_verification_test_client,
    ):
        # Given
        user_id = test_verification.user_id

        # Create multiple member_verification records for this user
        max_member_verification_id = -1
        max_member_id = -1
        for member in multiple_test_members_versioned:
            mv = await member_verification_test_client.persist(
                model=factory.MemberVerificationFactory.create(
                    member_id=member.id,
                    verification_id=test_verification.id,
                )
            )
            # Make a note of the most recent member_verification record for our user- we want to return that one
            if mv.id > max_member_verification_id:
                max_member_id = member.id
                max_member_verification_id = mv.id

        # When
        returned_member = await verification_test_client.get_member_id_for_user_id(
            user_id=user_id
        )

        # Then
        assert returned_member == max_member_id

    # endregion  get memberId for userID

    @staticmethod
    async def test_get_member_id_for_user_id(
        test_verification: verification_client.Verifications,
        multiple_test_member_verification_multiple_members: member_verification_client.MemberVerification,
        verification_test_client,
    ):
        # Given
        user_id = test_verification.user_id
        verification_id = test_verification.id

        expected_member_id = None
        for member_verification in multiple_test_member_verification_multiple_members:
            if member_verification.verification_id == verification_id:
                expected_member_id = member_verification.member_id
        # When
        returned_member_id = await verification_test_client.get_member_id_for_user_id(
            user_id=user_id
        )

        # Then
        assert expected_member_id == returned_member_id

    @staticmethod
    async def test_get_member_id_for_user_and_org(
        test_verification: verification_client.Verifications,
        multiple_test_member_verification_multiple_members: member_verification_client.MemberVerification,
        verification_test_client,
    ):
        # Given
        user_id = test_verification.user_id
        organization_id = test_verification.organization_id
        verification_id = test_verification.id

        expected_member_id = None
        for member_verification in multiple_test_member_verification_multiple_members:
            if member_verification.verification_id == verification_id:
                expected_member_id = member_verification.member_id
        # When
        returned_member_id = (
            await verification_test_client.get_member_id_for_user_and_org(
                user_id=user_id, organization_id=organization_id
            )
        )

        # Then
        assert expected_member_id == returned_member_id

    @staticmethod
    async def test_get_member_id_for_user_and_org_with_overeligibility(
        test_file,
        same_user_multiple_test_verifications,
        multiple_test_members_versioned,
        same_user_multiple_test_member_verification,
        verification_test_client,
        member_verification_test_client,
    ):
        # Given
        user_id = same_user_multiple_test_verifications[0].user_id
        organization_id = same_user_multiple_test_verifications[0].organization_id
        verification_id = same_user_multiple_test_verifications[0].id

        expected_member_id = next(
            (
                mv.member_id
                for mv in same_user_multiple_test_member_verification
                if mv.verification_id == verification_id
            ),
            None,
        )

        # When
        returned_member = await verification_test_client.get_member_id_for_user_and_org(
            user_id=user_id, organization_id=organization_id
        )

        # Then
        assert returned_member == expected_member_id

    @staticmethod
    async def test_get_for_ids(
        multiple_test_verifications: verification_client.Verifications,
        verification_test_client,
    ):
        # Given
        expected_total = NUMBER_TEST_OBJECTS
        verification_ids = [v.id for v in multiple_test_verifications]

        # When
        all_verifications = await verification_test_client.get_for_ids(verification_ids)

        # Then
        # Ensure we have grabbed all verifications
        assert len(all_verifications) == expected_total

    @staticmethod
    async def test_get_for_member_id(
        test_verification: verification_client.Verifications,
        test_member_versioned: member_versioned_client.MembersVersioned,
        test_verification_attempt: verification_attempt_client.VerificationAttempts,
        verification_test_client,
        member_verification_test_client,
    ):
        # Given
        await member_verification_test_client.persist(
            member_id=test_member_versioned.id,
            verification_id=test_verification.id,
            verification_attempt_id=test_verification_attempt.id,
        )

        # When
        verification_for_member = await verification_test_client.get_for_member_id(
            member_id=test_member_versioned.id
        )

        # Then
        assert verification_for_member == test_verification

    @staticmethod
    async def test_get_for_member_id_inactive_in_future(
        test_config: configuration_client.Configuration,
        test_member_versioned: member_versioned_client.MembersVersioned,
        test_verification_attempt: verification_attempt_client.VerificationAttempts,
        verification_test_client,
        member_verification_test_client,
    ):
        # Given
        future_inactive_verification = await verification_test_client.persist(
            model=factory.VerificationFactory.create(
                organization_id=test_config.organization_id,
                deactivated_at=datetime.date.today() + datetime.timedelta(weeks=1),
            )
        )

        await member_verification_test_client.persist(
            member_id=test_member_versioned.id,
            verification_id=future_inactive_verification.id,
            verification_attempt_id=test_verification_attempt.id,
        )

        # When
        verification_for_member = await verification_test_client.get_for_member_id(
            member_id=test_member_versioned.id
        )

        # Then
        assert verification_for_member == future_inactive_verification

    @staticmethod
    async def test_get_for_member_id_active_verifications_only(
        test_config: configuration_client.Configuration,
        test_member_versioned: member_versioned_client.MembersVersioned,
        test_verification_attempt: verification_attempt_client.VerificationAttempts,
        verification_test_client,
        member_verification_test_client,
    ):
        # Given
        inactive_verification = await verification_test_client.persist(
            model=factory.VerificationFactory.create(
                organization_id=test_config.organization_id,
                deactivated_at=datetime.date(2000, 1, 1),
            )
        )

        await member_verification_test_client.persist(
            member_id=test_member_versioned.id,
            verification_id=inactive_verification.id,
            verification_attempt_id=test_verification_attempt.id,
        )

        # When
        verification_for_member = await verification_test_client.get_for_member_id(
            member_id=test_member_versioned.id
        )

        # Then
        assert not verification_for_member

    @staticmethod
    async def test_get_all_for_member_id(
        multiple_test_verifications: verification_client.Verifications,
        test_member_versioned: member_versioned_client.MembersVersioned,
        multiple_test_verification_attempts: verification_attempt_client.VerificationAttempts,
        verification_test_client,
        member_verification_test_client,
    ):
        # Given
        for i in range(len(multiple_test_verifications)):
            await member_verification_test_client.persist(
                member_id=test_member_versioned.id,
                verification_id=multiple_test_verifications[i].id,
                verification_attempt_id=multiple_test_verification_attempts[i].id,
            )

        # When
        verifications_for_member = await verification_test_client.get_all_for_member_id(
            member_id=test_member_versioned.id
        )

        # Then
        for v in multiple_test_verifications:
            assert v in verifications_for_member

    @staticmethod
    async def test_get_user_ids_for_member_id(
        multiple_test_verifications: verification_client.Verifications,
        test_member_versioned: member_versioned_client.MembersVersioned,
        multiple_test_verification_attempts: verification_attempt_client.VerificationAttempts,
        verification_test_client,
        member_verification_test_client,
    ):
        # Given
        user_ids = set()
        for i in range(len(multiple_test_verifications)):
            await member_verification_test_client.persist(
                member_id=test_member_versioned.id,
                verification_id=multiple_test_verifications[i].id,
                verification_attempt_id=multiple_test_verification_attempts[i].id,
            )
            user_ids.add(multiple_test_verifications[i].user_id)

        # When
        user_ids_for_member = (
            await verification_test_client.get_user_ids_for_eligibility_member_id(
                member_id=test_member_versioned.id
            )
        )

        # Then
        for u in user_ids:
            assert u in user_ids_for_member

    @staticmethod
    async def test_get_for_org(
        test_verification: verification_client.Verifications, verification_test_client
    ):
        assert await verification_test_client.get_for_org(
            test_verification.organization_id
        ) == [test_verification]

    @staticmethod
    async def test_get_count_for_org(
        test_verification: verification_client.Verifications, verification_test_client
    ):
        assert (
            await verification_test_client.get_count_for_org(
                test_verification.organization_id
            )
            == 1
        )

    @staticmethod
    async def test_get_counts_for_orgs(
        test_file: file_client.Files, verification_test_client
    ):
        # Given
        # Bulk create members for our test file
        await verification_test_client.bulk_persist(
            models=factory.VerificationFactory.create_batch(
                NUMBER_TEST_OBJECTS,
                organization_id=test_file.organization_id,
            ),
        )

        # When
        verification_count = await verification_test_client.get_counts_for_orgs(
            test_file.organization_id
        )

        # Then
        assert verification_count[0]["count"] == NUMBER_TEST_OBJECTS

    @staticmethod
    async def test_delete(
        test_verification: verification_client.Verifications, verification_test_client
    ):
        # Given
        verification_id = test_verification.id

        # When
        await verification_test_client.delete(verification_id)

        # Then
        returned_verification = await verification_test_client.get(verification_id)
        assert returned_verification is None  # noqa

    @staticmethod
    async def test_bulk_delete(
        multiple_test_verifications: verification_client.Verifications,
        verification_test_client,
    ):
        # Given
        verification_ids = [v.id for v in multiple_test_verifications]

        # When
        await verification_test_client.bulk_delete(*verification_ids)

        # Then
        returned_members = await verification_test_client.all()
        assert returned_members == []

    @staticmethod
    async def test_delete_all_for_org(
        test_verification: verification_client.Verifications,
        verification_test_client,
        configuration_test_client,
    ):
        # Given
        other_org: configuration_client.Configuration = (
            await configuration_test_client.persist(
                model=factory.ConfigurationFactory.create()
            )
        )
        size = 10
        await verification_test_client.bulk_persist(
            models=factory.VerificationFactory.create_batch(
                size,
                organization_id=other_org.organization_id,
            )
        )
        # When
        other_verifications_count = await verification_test_client.get_count_for_org(
            other_org.organization_id
        )
        await verification_test_client.delete_all_for_org(other_org.organization_id)
        # Then
        assert other_verifications_count == size
        assert (
            await verification_test_client.get_count_for_org(other_org.organization_id)
        ) == 0

    @staticmethod
    async def test_get_eligibility_verification_record_for_user(
        test_eligibility_verification_record: db_model.EligibilityVerificationForUser,
        verification_test_client,
    ):
        # When
        user_id = test_eligibility_verification_record.user_id
        returned_value = (
            await verification_test_client.get_eligibility_verification_record_for_user(
                user_id=user_id
            )
        )
        # Then
        assert returned_value == test_eligibility_verification_record

    @staticmethod
    async def test_get_eligibility_verification_record_for_user_v2(
        test_config,
        test_member_versioned,
        test_member_2,
        verification_test_client,
        member_verification_test_client,
        verification_2_test_client,
    ):
        # When
        user_id = 999

        test_verification_2 = await verification_2_test_client.persist(
            model=factory.Verification2Factory.create(
                organization_id=test_config.organization_id,
                member_id=test_member_2.id,
                member_version=test_member_2.version,
                user_id=user_id,
            )
        )
        test_verification_1 = await verification_test_client.persist(
            model=factory.VerificationFactory.create(
                organization_id=test_config.organization_id,
                user_id=user_id,
                verification_2_id=test_verification_2.id,
            )
        )
        await member_verification_test_client.persist(
            member_id=test_member_versioned.id,
            verification_id=test_verification_1.id,
            verification_attempt_id=None,
        )
        returned_value = (
            await verification_test_client.get_eligibility_verification_record_for_user(
                user_id=user_id
            )
        )
        # Then
        assert returned_value.eligibility_member_2_id == test_member_2.id
        assert returned_value.eligibility_member_2_version == test_member_2.version

        returned_values = await verification_test_client.get_all_eligibility_verification_record_for_user(
            user_id=user_id
        )
        assert len(returned_values) == 1
        returned_verification = returned_values[0]
        assert returned_verification.eligibility_member_2_id == test_member_2.id
        assert (
            returned_verification.eligibility_member_2_version == test_member_2.version
        )

    @staticmethod
    async def test_get_all_eligibility_verification_records_for_user(
        multiple_test_verifications_for_user: [],
        test_eligibility_verification_record: db_model.EligibilityVerificationForUser,
        verification_test_client,
    ):
        # When
        user_id = 999
        verifications = multiple_test_verifications_for_user
        returned_value = await verification_test_client.get_all_eligibility_verification_record_for_user(
            user_id=user_id
        )
        # Then
        assert len(returned_value) == len(verifications)
        assert [verification.id for verification in verifications] == [
            ev.verification_id for ev in returned_value
        ]

    @staticmethod
    async def test_get_eligibility_verification_record_for_user_null_verification_values(
        test_member_versioned,
        test_config,
        verification_test_client,
        member_verification_test_client,
    ):
        # Given
        # Set the values that may be null for a verification record - when we return our final record,
        # we should use the e9y record values
        test_verification_record = await verification_test_client.persist(
            model=factory.VerificationFactory.create(
                organization_id=test_config.organization_id,
                first_name="",
                last_name="",
                unique_corp_id="",
                dependent_id="",
                work_state="",
                email="",
            )
        )
        await member_verification_test_client.persist(
            member_id=test_member_versioned.id,
            verification_id=test_verification_record.id,
            verification_attempt_id=None,
        )

        # When
        return_value = (
            await verification_test_client.get_eligibility_verification_record_for_user(
                user_id=test_verification_record.user_id
            )
        )

        # Then
        assert return_value.first_name == test_member_versioned.first_name
        assert return_value.last_name == test_member_versioned.last_name
        assert return_value.unique_corp_id == test_member_versioned.unique_corp_id
        assert return_value.dependent_id == test_member_versioned.dependent_id
        assert return_value.work_state == test_member_versioned.work_state
        assert return_value.email == test_member_versioned.email

    @staticmethod
    async def test_get_all_eligibility_verification_record_for_user_null_verification_values(
        test_config,
        verification_test_client,
        member_verification_test_client,
        multiple_test_config,
        multiple_test_members_versioned_from_test_config,
    ):
        # Given
        # Set the values that may be null for all verification records - when we return our final records,
        # we should use the e9y record values
        user_id = 999
        test_verification_records = await verification_test_client.bulk_persist(
            models=[
                factory.VerificationFactory.create(
                    user_id=user_id,
                    organization_id=c.organization_id,
                    first_name="",
                    last_name="",
                    unique_corp_id="",
                    dependent_id="",
                    work_state="",
                    email="",
                )
                for c in multiple_test_config
            ]
        )

        records = _get_member_verification_records_for_user(
            multiple_test_members=multiple_test_members_versioned_from_test_config,
            verifications_for_user=test_verification_records,
        )
        await member_verification_test_client.bulk_persist(models=records)

        # When
        return_value = await verification_test_client.get_all_eligibility_verification_record_for_user(
            user_id=user_id
        )

        # Then
        attributes_to_check = [
            "first_name",
            "last_name",
            "unique_corp_id",
            "dependent_id",
            "work_state",
            "email",
        ]

        for attr in attributes_to_check:
            expected_values = [
                getattr(m, attr)
                for m in multiple_test_members_versioned_from_test_config
            ]
            actual_values = [getattr(v, attr) for v in return_value]
            assert all(
                exp == act for exp, act in zip(expected_values, actual_values)
            ), f"Attribute '{attr}' mismatch found"

    @staticmethod
    async def test_get_eligibility_verification_record_for_user_no_e9y_record(
        test_config,
        verification_test_client,
        test_verification,
    ):
        # Test case for when we have verification against something like client-specific where there would not be an e9y record
        # Given
        # When
        return_value = (
            await verification_test_client.get_eligibility_verification_record_for_user(
                user_id=test_verification.user_id
            )
        )

        # Then
        assert return_value.first_name == test_verification.first_name
        assert return_value.last_name == test_verification.last_name
        assert return_value.unique_corp_id == test_verification.unique_corp_id
        assert not return_value.dependent_id
        assert not return_value.work_state
        assert not return_value.email
        assert not return_value.eligibility_member_id

    @staticmethod
    async def test_get_all_eligibility_verification_record_for_user_no_e9y_records(
        test_config,
        verification_test_client,
        multiple_test_verifications_for_user,
    ):
        # Test case for when we have verification against something like client-specific where there would not be an e9y record
        # Given
        user_id = 999
        # When
        return_value = await verification_test_client.get_all_eligibility_verification_record_for_user(
            user_id=user_id
        )

        # Then
        attributes_to_check = [
            "first_name",
            "last_name",
            "unique_corp_id",
            "dependent_id",
            "work_state",
            "email",
        ]

        for attr in attributes_to_check:
            expected_values = [
                getattr(m, attr) for m in multiple_test_verifications_for_user
            ]
            actual_values = [getattr(v, attr) for v in return_value]
            assert all(
                exp == act for exp, act in zip(expected_values, actual_values)
            ), f"Attribute '{attr}' mismatch found"

    @staticmethod
    async def test_get_eligibility_verification_record_latest_verified(
        test_config, verification_test_client
    ):
        """When a user has multiple verificaiton records, test that we return the latest verified record"""
        # Given
        user_id = 1
        await verification_test_client.persist(
            model=factory.VerificationFactory.create(
                user_id=user_id,
                organization_id=test_config.organization_id,
                verified_at=datetime.datetime(year=2020, month=10, day=12),
            )
        )
        second_verification = await verification_test_client.persist(
            model=factory.VerificationFactory.create(
                user_id=user_id,
                organization_id=test_config.organization_id,
                verified_at=datetime.datetime(year=2020, month=10, day=14),
            )
        )
        # When
        verification = (
            await verification_test_client.get_eligibility_verification_record_for_user(
                user_id=user_id
            )
        )

        # Then
        assert verification.verification_id == second_verification.id

    @staticmethod
    async def test_get_all_eligibility_verification_record_latest_verified(
        multiple_test_config,
        verification_test_client,
    ):
        """When a user has multiple verification records across multiple orgs, test that we return the latest verified record for an org"""
        # Given
        user_id = 999
        verification_records = await verification_test_client.bulk_persist(
            models=[
                factory.VerificationFactory.create(
                    user_id=user_id,
                    organization_id=c.organization_id,
                    verified_at=datetime.datetime(year=2020, month=10, day=12),
                )
                for c in multiple_test_config
            ]
        )
        updated_verification_records = await verification_test_client.bulk_persist(
            models=[
                factory.VerificationFactory.create(
                    user_id=user_id,
                    organization_id=record.organization_id,
                    verified_at=datetime.datetime(year=2020, month=10, day=14),
                )
                for record in verification_records
            ]
        )

        # When
        verifications = await verification_test_client.get_all_eligibility_verification_record_for_user(
            user_id=user_id
        )

        # Then
        assert [v.verification_id for v in verifications] == [
            v.id for v in updated_verification_records
        ]

    @staticmethod
    async def test_get_all_eligibility_verification_record_for_user_no_verification_records(
        test_config,
        verification_test_client,
        member_verification_test_client,
        multiple_test_config,
        multiple_test_members_versioned_from_test_config,
    ):
        # Given
        user_id = 999

        # When
        return_value = await verification_test_client.get_all_eligibility_verification_record_for_user(
            user_id=user_id
        )

        # Then
        assert return_value is None

    @staticmethod
    @pytest.mark.parametrize(
        argnames=("deactivated_at", "is_expected"),
        argvalues=[
            (None, lambda x: x is not None),
            (datetime.datetime.now() - datetime.timedelta(days=1), lambda x: x is None),
            (
                datetime.datetime.now() + datetime.timedelta(days=1),
                lambda x: x is not None,
            ),
        ],
        ids=[
            "deactivated-at-not-set",
            "deactivated-at-in-past",
            "deactivated-at-in-future",
        ],
    )
    async def test_get_eligibility_verification_record_deactivated_at_set(
        test_config, verification_test_client, deactivated_at, is_expected
    ):
        """Test that records with a deactivated_at set are not returned"""
        # Given
        user_id = 1
        await verification_test_client.persist(
            model=factory.VerificationFactory.create(
                user_id=user_id,
                organization_id=test_config.organization_id,
                verified_at=datetime.datetime(year=2020, month=10, day=12),
                # A record that was deactivated yesterday
                deactivated_at=deactivated_at,
            )
        )

        # When
        verification = (
            await verification_test_client.get_eligibility_verification_record_for_user(
                user_id=user_id
            )
        )

        # Then
        assert is_expected(verification)

    @staticmethod
    async def test_deactivate_verification_record_for_user(
        test_config, verification_test_client
    ):
        # Given
        user_id = 1
        verification = await verification_test_client.persist(
            model=factory.VerificationFactory.create(
                user_id=user_id,
                organization_id=test_config.organization_id,
                verified_at=datetime.datetime(year=2020, month=10, day=12),
            )
        )

        # When
        await verification_test_client.deactivate_verification_record_for_user(
            verification_id=verification.id,
            user_id=verification.user_id,
        )

        deactivated_verification = await verification_test_client.get(verification.id)

        # Then
        assert deactivated_verification.deactivated_at is not None

    @staticmethod
    async def test_get_verification_key_for_verification_2_id(
        test_member_versioned,
        test_config,
        verification_test_client,
        member_verification_test_client,
        test_eligibility_verification_record,
    ):
        # when
        res_found = await verification_test_client.get_verification_key_for_verification_2_id(
            verification_2_id=test_eligibility_verification_record.verification_2_id,
        )
        res_not_found = (
            await verification_test_client.get_verification_key_for_verification_2_id(
                verification_2_id=test_eligibility_verification_record.verification_2_id
                + 1,
            )
        )
        # then
        assert (
            res_found.verification_1_id
            == test_eligibility_verification_record.verification_id
        )
        assert res_not_found is None

    @staticmethod
    async def test_get_e9y_data_for_member_track_backfill(
        test_file,
        verification_test_client,
        member_verification_test_client,
        member_versioned_test_client,
    ):
        # when
        user_id = 1122
        expired_range = db_model.DateRange(
            lower=datetime.date(year=2022, month=1, day=1),
            upper_inc=datetime.date(year=2023, month=1, day=1),
        )
        member_11 = factory.MemberVersionedFactory.create(
            organization_id=test_file.organization_id,
            file_id=test_file.id,
            effective_range=expired_range,
        )
        member_12 = factory.MemberVersionedFactory.create(
            organization_id=test_file.organization_id,
            file_id=test_file.id,
        )

        member_21 = factory.MemberVersionedFactory.create(
            organization_id=test_file.organization_id,
            file_id=test_file.id,
            effective_range=expired_range,
        )
        member_22 = factory.MemberVersionedFactory.create(
            organization_id=test_file.organization_id,
            file_id=test_file.id,
        )
        (
            member_11,
            member_12,
            member_21,
            member_22,
        ) = await member_versioned_test_client.bulk_persist(
            models=[member_11, member_12, member_21, member_22]
        )
        verification_1 = factory.VerificationFactory.create(
            id=1,
            user_id=user_id,
            organization_id=test_file.organization_id,
            deactivated_at=datetime.datetime(2022, 1, 1, 0, 0),
        )

        verification_2 = factory.VerificationFactory.create(
            id=2,
            user_id=user_id,
            organization_id=test_file.organization_id,
            deactivated_at=None,
        )

        verification_1, verification_2 = await verification_test_client.bulk_persist(
            models=[verification_1, verification_2]
        )
        member_verification_11 = factory.MemberVerificationFactory.create(
            verification_id=verification_1.id,
            member_id=member_11.id,
        )
        member_verification_12 = factory.MemberVerificationFactory.create(
            verification_id=verification_1.id,
            member_id=member_12.id,
        )
        member_verification_21 = factory.MemberVerificationFactory.create(
            verification_id=verification_2.id,
            member_id=member_21.id,
        )
        member_verification_22 = factory.MemberVerificationFactory.create(
            verification_id=verification_2.id,
            member_id=member_22.id,
        )

        await member_verification_test_client.bulk_persist(
            models=[
                member_verification_11,
                member_verification_12,
                member_verification_21,
                member_verification_22,
            ]
        )

        # Then
        res: List[
            db_model.BackfillMemberTrackEligibilityData
        ] = await verification_test_client.get_e9y_data_for_member_track_backfill(
            user_id=user_id
        )

        # then

        assert len(res) == 4
        assert set([r.verification_id for r in res]) == set(
            [verification_1.id, verification_2.id]
        )
        assert set([r.member_id for r in res]) == set(
            [member_11.id, member_12.id, member_21.id, member_22.id]
        )
