from __future__ import annotations

import pytest
from tests.functional.conftest import NUMBER_TEST_OBJECTS

from db.clients import member_verification_client

pytestmark = pytest.mark.asyncio


class TestMemberVerificationClient:

    # region fetch tests

    @staticmethod
    async def test_all(
        multiple_test_member_verification: member_verification_client.MemberVerification,
        member_verification_test_client,
    ):
        # Given
        expected_total = NUMBER_TEST_OBJECTS + 1

        # When
        all_verifications = await member_verification_test_client.all()

        # Then
        # Ensure we have grabbed all verifications
        assert len(all_verifications) == expected_total

    @staticmethod
    async def test_get(
        test_member_verification: member_verification_client.MemberVerification,
        member_verification_test_client,
    ):
        # When
        returned_member_verification = await member_verification_test_client.get(
            test_member_verification.id
        )

        # Then
        assert returned_member_verification == test_member_verification

    @staticmethod
    async def test_get_for_member_id(
        multiple_test_member_verification_multiple_members: member_verification_client.MemberVerification,
        member_verification_test_client,
    ):
        member_id = multiple_test_member_verification_multiple_members[0].member_id
        returned_result = await member_verification_test_client.get_for_member_id(
            member_id=member_id
        )
        assert returned_result == multiple_test_member_verification_multiple_members[0]

    @staticmethod
    async def test_get_all_for_member_id(
        multiple_test_member_verification_multiple_members: member_verification_client.MemberVerification,
        member_verification_test_client,
    ):
        member_id = multiple_test_member_verification_multiple_members[0].member_id
        returned_result = await member_verification_test_client.get_all_for_member_id(
            member_id=member_id
        )
        assert returned_result == [
            multiple_test_member_verification_multiple_members[0]
        ]

    @staticmethod
    async def test_get_for_verification_id(
        multiple_test_member_verification_multiple_members: member_verification_client.MemberVerification,
        member_verification_test_client,
    ):
        verification_id = multiple_test_member_verification_multiple_members[
            0
        ].verification_id
        returned_result = await member_verification_test_client.get_for_verification_id(
            verification_id=verification_id
        )
        assert returned_result == [
            multiple_test_member_verification_multiple_members[0]
        ]

    @staticmethod
    async def test_get_for_verification_attempt_id(
        test_member_verification_attempt: member_verification_client.MemberVerification,
        member_verification_test_client,
    ):
        verification_attempt_id = (
            test_member_verification_attempt.verification_attempt_id
        )
        returned_result = (
            await member_verification_test_client.get_for_verification_attempt_id(
                verification_attempt_id=verification_attempt_id
            )
        )
        assert returned_result == [test_member_verification_attempt]

    @staticmethod
    async def test_delete(
        test_member_verification: member_verification_client.MemberVerification,
        member_verification_test_client,
    ):
        # Given
        member_verification_id = test_member_verification.id

        # When
        await member_verification_test_client.delete(member_verification_id)

        # Then
        returned_verification = await member_verification_test_client.get(
            member_verification_id
        )
        assert returned_verification is None  # noqa
