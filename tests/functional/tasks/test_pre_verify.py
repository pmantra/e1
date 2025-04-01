import datetime
from typing import List, Tuple

import pytest
from tests.factories import data_models as factories

from app.tasks import pre_verify
from db import model
from db.clients import (
    configuration_client,
    member_verification_client,
    member_versioned_client,
    verification_client,
)

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def verified_member(
    test_config: model.Configuration,
    configuration_test_client: configuration_client.Configurations,
    member_versioned_test_client: member_versioned_client.MembersVersioned,
    verification_test_client: verification_client.Verifications,
    member_verification_test_client: member_verification_client.MemberVerifications,
) -> Tuple[model.Configuration, model.MemberVersioned, model.Verification]:
    member: model.MemberVersioned = await member_versioned_test_client.persist(
        model=factories.MemberVersionedFactory.create(
            organization_id=test_config.organization_id,
            effective_range=model.DateRange(
                lower=datetime.date(year=2000, day=21, month=5), upper=None
            ),
            pre_verified=False,
        )
    )
    verification: model.Verification = await verification_test_client.persist(
        model=factories.VerificationFactory.create(
            organization_id=test_config.organization_id,
            verification_type=model.VerificationTypes.PRIMARY,
        ),
    )
    await member_verification_test_client.persist(
        model=factories.MemberVerificationFactory.create(
            member_id=member.id, verification_id=verification.id
        )
    )
    return test_config, member, verification


class TestPreVerifyOrg:
    @staticmethod
    async def test_pre_verify_org(
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        verification_test_client: verification_client.Verifications,
        member_verification_test_client: member_verification_client.MemberVerifications,
        verified_member: Tuple[
            model.Configuration, model.MemberVersioned, model.Verification
        ],
    ):
        """Test that we pre-verify records in the org"""
        # given
        config, member, verification = verified_member
        # an updated version of the member record
        updated_member: model.MemberVersioned = (
            await member_versioned_test_client.persist(model=member)
        )

        # When
        await pre_verify.pre_verify_org(
            organization_id=config.organization_id,
            members_versioned=member_versioned_test_client,
            verifications=verification_test_client,
        )

        # Then - make sure that a new member_verification record was created
        member_verifications: List[
            model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=verification.id
        )

        assert {(mv.member_id, mv.verification_id) for mv in member_verifications} == {
            (member.id, verification.id),
            (updated_member.id, verification.id),
        }

    @staticmethod
    async def test_pre_verify_org_wrong_org(
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        verification_test_client: verification_client.Verifications,
        member_verification_test_client: member_verification_client.MemberVerifications,
        verified_member: Tuple[
            model.Configuration, model.MemberVersioned, model.Verification
        ],
    ):
        """Test that we are only pre-verifying for the org that we want"""
        # given
        config, member, verification = verified_member
        # an updated version of the member record
        await member_versioned_test_client.persist(model=member)

        # When
        await pre_verify.pre_verify_org(
            organization_id=config.organization_id + 1,
            members_versioned=member_versioned_test_client,
            verifications=verification_test_client,
        )

        # Then - make sure that no additional member_verification records were created
        member_verifications: List[
            model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=verification.id
        )

        assert {(mv.member_id, mv.verification_id) for mv in member_verifications} == {
            (member.id, verification.id)
        }

    @staticmethod
    async def test_pre_verify_org_records_are_set_to_pre_verified_true(
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        verification_test_client: verification_client.Verifications,
        member_verification_test_client: member_verification_client.MemberVerifications,
        verified_member: Tuple[
            model.Configuration, model.MemberVersioned, model.Verification
        ],
    ):
        """Test that after we pre-verify records, they are set to pre_verified=True"""
        # given
        config, member, verification = verified_member
        # an updated version of the member record
        updated_member: model.MemberVersioned = (
            await member_versioned_test_client.persist(model=member)
        )

        # When
        await pre_verify.pre_verify_org(
            organization_id=config.organization_id,
            members_versioned=member_versioned_test_client,
            verifications=verification_test_client,
        )

        # Then
        member: model.MemberVersioned = await member_versioned_test_client.get(
            updated_member.id
        )

        assert member.pre_verified
