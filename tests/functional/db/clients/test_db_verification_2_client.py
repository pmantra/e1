from __future__ import annotations

import pytest
from tests.factories import data_models

pytestmark = pytest.mark.asyncio


class TestData:
    member_id = 10000
    user_id_with_member = 9001
    user_id_no_member = 9002
    user_id_with_multiple_verifications = 9003


@pytest.fixture
async def test_member_2(test_config, member_2_test_client):
    return await member_2_test_client.persist(
        model=data_models.Member2Factory.create(
            id=TestData.member_id,
            organization_id=test_config.organization_id,
        )
    )


@pytest.fixture
async def test_verification_2_with_member_2(
    test_config,
    verification_2_test_client,
    test_member_2,
):
    return await verification_2_test_client.persist(
        model=data_models.Verification2Factory.create(
            organization_id=test_config.organization_id,
            member_id=test_member_2.id,
            member_version=test_member_2.version,
            user_id=TestData.user_id_with_member,
        )
    )


@pytest.fixture
async def test_verification_2_no_member_2(
    test_config,
    verification_2_test_client,
):
    return await verification_2_test_client.persist(
        model=data_models.Verification2Factory.create(
            organization_id=test_config.organization_id,
            user_id=TestData.user_id_no_member,
        )
    )


class TestVerification2Client:

    # region get_for_member_id
    @staticmethod
    async def test_get_for_member_id(
        test_member_2,
        test_verification_2_with_member_2,
        verification_2_test_client,
    ):
        # when
        res_found = await verification_2_test_client.get_for_member_id(
            member_id=TestData.member_id
        )
        res_not_found = await verification_2_test_client.get_for_member_id(
            member_id=TestData.member_id + 1
        )

        # then
        assert res_found == test_verification_2_with_member_2
        assert res_not_found is None

    # engregion

    # region deactivate_verification_record_for_user
    @staticmethod
    async def test_deactivate_verification_record_for_user(
        test_member_2,
        test_verification_2_with_member_2,
        verification_2_test_client,
    ):
        # when
        res_found = (
            await verification_2_test_client.deactivate_verification_record_for_user(
                user_id=TestData.user_id_with_member,
                verification_id=test_verification_2_with_member_2.id,
            )
        )
        res_not_found = (
            await verification_2_test_client.deactivate_verification_record_for_user(
                user_id=TestData.user_id_with_member + 1,
                verification_id=test_verification_2_with_member_2.id,
            )
        )
        # then
        assert res_found.deactivated_at is not None
        assert res_not_found is None

    # engregion

    # region get_verification_key_for_id
    @staticmethod
    async def test_get_verification_key_for_id(
        test_member_2,
        test_verification_2_with_member_2,
        verification_2_test_client,
    ):
        # when
        res_found = await verification_2_test_client.get_verification_key_for_id(
            id=test_verification_2_with_member_2.id
        )
        res_not_found = await verification_2_test_client.get_verification_key_for_id(
            id=test_verification_2_with_member_2.id + 1
        )
        # then
        assert res_found.member_2_id == test_verification_2_with_member_2.member_id
        assert res_found.member_id is None
        assert res_found.is_v2
        assert res_found.verification_2_id == test_verification_2_with_member_2.id
        assert res_not_found is None

    # engregion

    # region get_verification_key_for_user_and_org
    @staticmethod
    async def test_get_verification_key_for_user_and_org(
        test_member_2,
        test_verification_2_with_member_2,
        verification_2_test_client,
    ):
        # when
        res_found = (
            await verification_2_test_client.get_verification_key_for_user_and_org(
                user_id=TestData.user_id_with_member,
                organization_id=test_member_2.organization_id,
            )
        )
        res_not_found = (
            await verification_2_test_client.get_verification_key_for_user_and_org(
                user_id=TestData.user_id_with_member + 1,
                organization_id=test_member_2.organization_id,
            )
        )
        # then
        assert res_found.member_2_id == test_verification_2_with_member_2.member_id
        assert res_not_found is None

    # engregion

    # region persist
    @staticmethod
    async def test_persist_with_member_associated(
        test_member_2,
        verification_2_test_client,
    ):
        # given
        verification_2 = data_models.Verification2Factory.create(
            organization_id=test_member_2.organization_id,
            member_id=test_member_2.id,
            member_version=test_member_2.version,
            user_id=TestData.user_id_with_member,
        )
        # when
        created = await verification_2_test_client.persist(model=verification_2)
        # Then
        assert created.user_id == verification_2.user_id
        assert created.organization_id == verification_2.organization_id
        assert created.unique_corp_id == verification_2.unique_corp_id
        assert created.dependent_id == verification_2.dependent_id
        assert created.first_name == verification_2.first_name
        assert created.last_name == verification_2.last_name
        assert created.email == verification_2.email
        assert created.date_of_birth == verification_2.date_of_birth
        assert created.work_state == verification_2.work_state
        assert created.verification_type == verification_2.verification_type
        assert created.deactivated_at == verification_2.deactivated_at
        assert created.verified_at.date() == verification_2.verified_at.date()
        assert created.additional_fields == verification_2.additional_fields
        assert created.member_id == verification_2.member_id
        assert created.member_version == verification_2.member_version

    @staticmethod
    async def test_persist_with_no_member(
        test_config,
        verification_2_test_client,
    ):
        # given
        verification_2 = data_models.Verification2Factory.create(
            organization_id=test_config.organization_id,
            user_id=TestData.user_id_no_member,
            member_version=None,
        )
        # when
        created = await verification_2_test_client.persist(model=verification_2)
        # Then
        assert created.user_id == verification_2.user_id
        assert created.organization_id == verification_2.organization_id
        assert created.unique_corp_id == verification_2.unique_corp_id
        assert created.dependent_id == verification_2.dependent_id
        assert created.first_name == verification_2.first_name
        assert created.last_name == verification_2.last_name
        assert created.email == verification_2.email
        assert created.date_of_birth == verification_2.date_of_birth
        assert created.work_state == verification_2.work_state
        assert created.verification_type == verification_2.verification_type
        assert created.deactivated_at == verification_2.deactivated_at
        assert created.verified_at.date() == verification_2.verified_at.date()
        assert created.additional_fields == verification_2.additional_fields
        assert created.member_id is None
        assert created.member_version is None

    # endregion
