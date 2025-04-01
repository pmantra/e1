from __future__ import annotations

import pytest
from tests.factories import data_models as factory
from tests.functional.conftest import NUMBER_TEST_OBJECTS

from db.clients import (
    configuration_client,
    file_client,
    verification_attempt_client,
    verification_client,
)

pytestmark = pytest.mark.asyncio


class TestVerificationAttemptClient:

    # region fetch tests

    @staticmethod
    async def test_all(
        multiple_test_verification_attempts: verification_attempt_client.VerificationAttempts,
        verification_attempt_test_client,
    ):
        # Given
        # We have created 100 verifications -> one for each of our multiple configs. Ensure we have grabbed all of them
        expected_total = NUMBER_TEST_OBJECTS

        # When
        all_verifications_attempts = await verification_attempt_test_client.all()

        # Then
        # Ensure we have grabbed all verifications
        assert len(all_verifications_attempts) == expected_total

    @staticmethod
    async def test_get(
        test_verification_attempt: verification_attempt_client.VerificationAttempts,
        verification_attempt_test_client,
    ):
        # When
        returned_verification_attempt = await verification_attempt_test_client.get(
            test_verification_attempt.id
        )

        # Then
        assert returned_verification_attempt == test_verification_attempt

    @staticmethod
    async def test_get_for_org(
        test_verification_attempt: verification_attempt_client.VerificationAttempts,
        verification_attempt_test_client,
    ):
        assert await verification_attempt_test_client.get_for_org(
            test_verification_attempt.organization_id
        ) == [test_verification_attempt]

    @staticmethod
    async def test_get_count_for_org(
        test_verification_attempt: verification_attempt_client.VerificationAttempts,
        verification_attempt_test_client,
    ):
        assert (
            await verification_attempt_test_client.get_count_for_org(
                test_verification_attempt.organization_id
            )
            == 1
        )

    @staticmethod
    async def test_get_counts_for_orgs(
        test_file: file_client.Files, verification_attempt_test_client
    ):
        # Given
        # Bulk create members for our test file
        await verification_attempt_test_client.bulk_persist(
            models=factory.VerificationAttemptFactory.create_batch(
                NUMBER_TEST_OBJECTS,
                organization_id=test_file.organization_id,
            ),
        )

        # When
        verification_count = await verification_attempt_test_client.get_counts_for_orgs(
            test_file.organization_id
        )

        # Then
        assert verification_count[0]["count"] == NUMBER_TEST_OBJECTS

    @staticmethod
    async def test_get_for_ids(
        multiple_test_verification_attempts: verification_attempt_client.VerificationAttempts,
        verification_attempt_test_client,
    ):
        # Given
        expected_total = NUMBER_TEST_OBJECTS
        verification_attempt_ids = [v.id for v in multiple_test_verification_attempts]

        # When
        all_verifications = await verification_attempt_test_client.get_for_ids(
            verification_attempt_ids
        )

        # Then
        # Ensure we have grabbed all verifications
        assert len(all_verifications) == expected_total

    @staticmethod
    async def test_delete(
        test_verification: verification_client.Verifications,
        verification_attempt_test_client,
    ):
        # Given
        verification_id = test_verification.id

        # When
        await verification_attempt_test_client.delete(verification_id)

        # Then
        returned_verification = await verification_attempt_test_client.get(
            verification_id
        )
        assert returned_verification is None  # noqa

    @staticmethod
    async def test_bulk_delete(
        multiple_test_verifications: verification_client.Verifications,
        verification_attempt_test_client,
    ):

        # Given
        verification_ids = [v.id for v in multiple_test_verifications]

        # When
        await verification_attempt_test_client.bulk_delete(*verification_ids)

        # Then
        returned_members = await verification_attempt_test_client.all()
        assert returned_members == []

    @staticmethod
    async def test_delete_all_for_org(
        test_verification_attempt: verification_attempt_client.VerificationAttempts,
        verification_attempt_test_client,
        configuration_test_client,
    ):
        # Given
        other_org: configuration_client.Configuration = (
            await configuration_test_client.persist(
                model=factory.ConfigurationFactory.create()
            )
        )
        size = 10
        await verification_attempt_test_client.bulk_persist(
            models=factory.VerificationAttemptFactory.create_batch(
                size,
                organization_id=other_org.organization_id,
            )
        )
        # When
        other_verifications_count = (
            await verification_attempt_test_client.get_count_for_org(
                other_org.organization_id
            )
        )
        await verification_attempt_test_client.delete_all_for_org(
            other_org.organization_id
        )
        # Then
        assert other_verifications_count == size
        assert (
            await verification_attempt_test_client.get_count_for_org(
                other_org.organization_id
            )
        ) == 0
