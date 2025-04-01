import dataclasses
import datetime
from typing import Any, List, Tuple

import pytest
from tests.factories import data_models as factories

from db import model
from db.clients import (
    configuration_client,
    file_client,
    member_verification_client,
    member_versioned_client,
    verification_client,
)

pytestmark = pytest.mark.asyncio


@dataclasses.dataclass
class VerificationValues:
    first_name: str = None
    last_name: str = None
    date_of_birth: datetime.date = None
    email: str = None
    unique_corp_id: str = None
    work_state: str = None


PRIMARY_VERIFICATION = VerificationValues(
    email="test_email", date_of_birth=datetime.date(year=1993, month=5, day=12)
)
ALTERNATE_VERIFICATION = VerificationValues(
    first_name="Kendall",
    last_name="Roy",
    date_of_birth=datetime.date(year=1993, month=5, day=12),
    work_state="NY",
    unique_corp_id="ISNDNASD90909",
)
GENDER_CODE = "M"
DEPENDENT_ID = "13324ASDFDA"
EMPLOYER_ASSIGNED_ID = "00000AASDFDA"
RECORD = {"loves_cheese": False}


@pytest.fixture
async def member_verification_primary(
    test_file: model.File,
    test_config: model.Configuration,
    configuration_test_client: configuration_client.Configurations,
    member_versioned_test_client: member_versioned_client.MembersVersioned,
    verification_test_client: verification_client.Verifications,
    member_verification_test_client: member_verification_client.MemberVerifications,
) -> Tuple[model.MemberVersioned, model.Verification]:
    """
    This fixture sets up data for the following scenario

    primary registered
    verification type: primary
    verification params: (email, date_of_birth)
    """
    member: model.MemberVersioned = await member_versioned_test_client.persist(
        model=factories.MemberVersionedFactory.create(
            organization_id=test_config.organization_id,
            file_id=test_file.id,
            # verification fields
            email=PRIMARY_VERIFICATION.email,
            date_of_birth=PRIMARY_VERIFICATION.date_of_birth,
            # other fields
            gender_code=GENDER_CODE,
            dependent_id=DEPENDENT_ID,
            record=RECORD,
            employer_assigned_id=EMPLOYER_ASSIGNED_ID,
        )
    )
    verification: model.Verification = await verification_test_client.persist(
        model=factories.VerificationFactory.create(
            organization_id=test_config.organization_id,
            email=PRIMARY_VERIFICATION.email,
            date_of_birth=PRIMARY_VERIFICATION.date_of_birth,
            verification_type=model.VerificationTypes.PRIMARY,
        ),
    )
    await member_verification_test_client.persist(
        model=factories.MemberVerificationFactory.create(
            member_id=member.id, verification_id=verification.id
        )
    )
    return member, verification


@pytest.fixture
async def multiple_member_verification_expired_and_valid(
    test_config: model.Configuration,
    configuration_test_client: configuration_client.Configurations,
    member_versioned_test_client: member_versioned_client.MembersVersioned,
    verification_test_client: verification_client.Verifications,
    member_verification_test_client: member_verification_client.MemberVerifications,
) -> Tuple[model.MemberVersioned, model.Verification, model.Verification]:
    """
    This fixture sets up data for the following scenario

    primary registered but with 2 verifications against same org, 1 expired, 1 not expired
    verification type: primary
    verification params: (email, date_of_birth)
    """
    member: model.MemberVersioned = await member_versioned_test_client.persist(
        model=factories.MemberVersionedFactory.create(
            organization_id=test_config.organization_id,
            # verification fields
            email=PRIMARY_VERIFICATION.email,
            date_of_birth=PRIMARY_VERIFICATION.date_of_birth,
            # other fields
            gender_code=GENDER_CODE,
            dependent_id=DEPENDENT_ID,
            record=RECORD,
            employer_assigned_id=EMPLOYER_ASSIGNED_ID,
        )
    )
    expired_verification: model.Verification = await verification_test_client.persist(
        model=factories.VerificationFactory.create(
            organization_id=test_config.organization_id,
            email=PRIMARY_VERIFICATION.email,
            date_of_birth=PRIMARY_VERIFICATION.date_of_birth,
            verification_type=model.VerificationTypes.PRIMARY,
            deactivated_at=datetime.datetime.now() - datetime.timedelta(days=10),
        ),
    )
    valid_verification: model.Verification = await verification_test_client.persist(
        model=factories.VerificationFactory.create(
            organization_id=test_config.organization_id,
            email=PRIMARY_VERIFICATION.email,
            date_of_birth=PRIMARY_VERIFICATION.date_of_birth,
            verification_type=model.VerificationTypes.PRIMARY,
            deactivated_at=None,
        ),
    )
    await member_verification_test_client.persist(
        model=factories.MemberVerificationFactory.create(
            member_id=member.id, verification_id=expired_verification.id
        )
    )
    await member_verification_test_client.persist(
        model=factories.MemberVerificationFactory.create(
            member_id=member.id, verification_id=valid_verification.id
        )
    )
    return member, expired_verification, valid_verification


@pytest.fixture
async def member_verification_primary_expired(
    test_config: model.Configuration,
    configuration_test_client: configuration_client.Configurations,
    member_versioned_test_client: member_versioned_client.MembersVersioned,
    verification_test_client: verification_client.Verifications,
    member_verification_test_client: member_verification_client.MemberVerifications,
) -> Tuple[model.MemberVersioned, model.Verification]:
    """
    This fixture sets up data for the following scenario

    primary registered but verification is expired
    verification type: primary
    verification params: (email, date_of_birth)
    """
    member: model.MemberVersioned = await member_versioned_test_client.persist(
        model=factories.MemberVersionedFactory.create(
            organization_id=test_config.organization_id,
            # verification fields
            email=PRIMARY_VERIFICATION.email,
            date_of_birth=PRIMARY_VERIFICATION.date_of_birth,
            # other fields
            gender_code=GENDER_CODE,
            dependent_id=DEPENDENT_ID,
            record=RECORD,
            employer_assigned_id=EMPLOYER_ASSIGNED_ID,
        )
    )
    verification: model.Verification = await verification_test_client.persist(
        model=factories.VerificationFactory.create(
            organization_id=test_config.organization_id,
            email=PRIMARY_VERIFICATION.email,
            date_of_birth=PRIMARY_VERIFICATION.date_of_birth,
            verification_type=model.VerificationTypes.PRIMARY,
            deactivated_at=datetime.datetime.now() - datetime.timedelta(days=10),
        ),
    )
    await member_verification_test_client.persist(
        model=factories.MemberVerificationFactory.create(
            member_id=member.id, verification_id=verification.id
        )
    )
    return member, verification


@pytest.fixture
async def member_and_dependent_verification_primary(
    test_config: model.Configuration,
    configuration_test_client: configuration_client.Configurations,
    member_versioned_test_client: member_versioned_client.MembersVersioned,
    verification_test_client: verification_client.Verifications,
    member_verification_test_client: member_verification_client.MemberVerifications,
) -> Tuple[model.MemberVersioned, model.Verification, model.Verification]:
    """
    This fixture sets up data for the following scenario

    primary and dependent registered
    verification type: primary
    verification params: (email, date_of_birth)
    """
    member: model.MemberVersioned = await member_versioned_test_client.persist(
        model=factories.MemberVersionedFactory.create(
            organization_id=test_config.organization_id,
            # verification fields
            email=PRIMARY_VERIFICATION.email,
            date_of_birth=PRIMARY_VERIFICATION.date_of_birth,
            # other fields
            gender_code=GENDER_CODE,
            dependent_id=DEPENDENT_ID,
            record=RECORD,
            employer_assigned_id=EMPLOYER_ASSIGNED_ID,
        )
    )
    verification_primary: model.Verification = await verification_test_client.persist(
        model=factories.VerificationFactory.create(
            user_id=1,
            organization_id=test_config.organization_id,
            # verification fields
            email=PRIMARY_VERIFICATION.email,
            date_of_birth=PRIMARY_VERIFICATION.date_of_birth,
            # other fields
            verification_type=model.VerificationTypes.PRIMARY,
        ),
    )
    verification_dependent: model.Verification = await verification_test_client.persist(
        model=factories.VerificationFactory.create(
            user_id=2,
            organization_id=test_config.organization_id,
            # verification fields
            email=PRIMARY_VERIFICATION.email,
            date_of_birth=PRIMARY_VERIFICATION.date_of_birth,
            # other fields
            verification_type=model.VerificationTypes.PRIMARY,
        ),
    )
    await member_verification_test_client.persist(
        model=factories.MemberVerificationFactory.create(
            member_id=member.id, verification_id=verification_primary.id
        )
    )
    await member_verification_test_client.persist(
        model=factories.MemberVerificationFactory.create(
            member_id=member.id, verification_id=verification_dependent.id
        )
    )
    return member, verification_primary, verification_dependent


@pytest.fixture
async def member_verification_alternate(
    test_config: model.Configuration,
    configuration_test_client: configuration_client.Configurations,
    member_versioned_test_client: member_versioned_client.MembersVersioned,
    verification_test_client: verification_client.Verifications,
    member_verification_test_client: member_verification_client.MemberVerifications,
) -> Tuple[model.MemberVersioned, model.Verification]:
    """
    This fixture sets up data for the following scenario

    primary registered
    verification type: alternate
    verification params: (first_name, last_name, date_of_birth, work_state, unique_corp_id)
    """
    member: model.MemberVersioned = await member_versioned_test_client.persist(
        model=factories.MemberVersionedFactory.create(
            organization_id=test_config.organization_id,
            # verification fields
            first_name=ALTERNATE_VERIFICATION.first_name,
            last_name=ALTERNATE_VERIFICATION.last_name,
            date_of_birth=ALTERNATE_VERIFICATION.date_of_birth,
            work_state=ALTERNATE_VERIFICATION.work_state,
            unique_corp_id=ALTERNATE_VERIFICATION.unique_corp_id,
            # other fields
            gender_code=GENDER_CODE,
            dependent_id=DEPENDENT_ID,
            record=RECORD,
            employer_assigned_id=EMPLOYER_ASSIGNED_ID,
        )
    )
    verification: model.Verification = await verification_test_client.persist(
        model=factories.VerificationFactory.create(
            organization_id=test_config.organization_id,
            # verification fields
            first_name=ALTERNATE_VERIFICATION.first_name,
            last_name=ALTERNATE_VERIFICATION.last_name,
            date_of_birth=ALTERNATE_VERIFICATION.date_of_birth,
            work_state=ALTERNATE_VERIFICATION.work_state,
            unique_corp_id=ALTERNATE_VERIFICATION.unique_corp_id,
            # other fields
            verification_type=model.VerificationTypes.ALTERNATE,
        ),
    )
    await member_verification_test_client.persist(
        model=factories.MemberVerificationFactory.create(
            member_id=member.id, verification_id=verification.id
        )
    )
    return member, verification


@pytest.fixture
async def member_and_dependent_verification_alternate(
    test_config: model.Configuration,
    configuration_test_client: configuration_client.Configurations,
    member_versioned_test_client: member_versioned_client.MembersVersioned,
    verification_test_client: verification_client.Verifications,
    member_verification_test_client: member_verification_client.MemberVerifications,
) -> Tuple[model.MemberVersioned, model.Verification, model.Verification]:
    """
    This fixture sets up data for the following scenario

    primary and dependent have registered
    verification type: alternate
    verification params: (first_name, last_name, date_of_birth, work_state, unique_corp_id)
    """
    member: model.MemberVersioned = await member_versioned_test_client.persist(
        model=factories.MemberVersionedFactory.create(
            organization_id=test_config.organization_id,
            # verification fields
            first_name=ALTERNATE_VERIFICATION.first_name,
            last_name=ALTERNATE_VERIFICATION.last_name,
            date_of_birth=ALTERNATE_VERIFICATION.date_of_birth,
            work_state=ALTERNATE_VERIFICATION.work_state,
            unique_corp_id=ALTERNATE_VERIFICATION.unique_corp_id,
            # other fields
            gender_code=GENDER_CODE,
            dependent_id=DEPENDENT_ID,
            record=RECORD,
            employer_assigned_id=EMPLOYER_ASSIGNED_ID,
        )
    )
    verification_primary: model.Verification = await verification_test_client.persist(
        model=factories.VerificationFactory.create(
            user_id=1,
            organization_id=test_config.organization_id,
            # verification fields
            first_name=ALTERNATE_VERIFICATION.first_name,
            last_name=ALTERNATE_VERIFICATION.last_name,
            date_of_birth=ALTERNATE_VERIFICATION.date_of_birth,
            work_state=ALTERNATE_VERIFICATION.work_state,
            unique_corp_id=ALTERNATE_VERIFICATION.unique_corp_id,
            # other fields
            verification_type=model.VerificationTypes.ALTERNATE,
        ),
    )
    verification_dependent: model.Verification = await verification_test_client.persist(
        model=factories.VerificationFactory.create(
            user_id=2,
            organization_id=test_config.organization_id,
            # verification fields
            first_name=ALTERNATE_VERIFICATION.first_name,
            last_name=ALTERNATE_VERIFICATION.last_name,
            date_of_birth=ALTERNATE_VERIFICATION.date_of_birth,
            work_state=ALTERNATE_VERIFICATION.work_state,
            unique_corp_id=ALTERNATE_VERIFICATION.unique_corp_id,
            # other fields
            verification_type=model.VerificationTypes.ALTERNATE,
        ),
    )
    await member_verification_test_client.persist(
        model=factories.MemberVerificationFactory.create(
            member_id=member.id, verification_id=verification_primary.id
        )
    )
    await member_verification_test_client.persist(
        model=factories.MemberVerificationFactory.create(
            member_id=member.id, verification_id=verification_dependent.id
        )
    )
    return member, verification_primary, verification_dependent


class TestBatchPreVerificationCommon:
    @staticmethod
    async def test_pre_verification_insert_no_verification_exists(
        test_config: model.Configuration,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        member_verification_test_client: member_verification_client.MemberVerifications,
        verification_test_client: verification_client.Verifications,
    ):
        """
        Test that we are not pre-verifying any records that don't have an existing
        member_versioned, verification, member_verification match
        """
        # Given no existing member and no verification
        # When
        member = await member_versioned_test_client.persist(
            model=factories.MemberVersionedFactory.create(
                organization_id=test_config.organization_id
            )
        )

        await verification_test_client.batch_pre_verify_records_by_org(
            organization_id=member.organization_id, batch_size=1000
        )

        # Then
        member_verifications: List[
            model.MemberVerification
        ] = await member_verification_test_client.all()
        assert not member_verifications

    @staticmethod
    async def test_pre_verification_expired_verification(
        member_verification_primary_expired: Tuple[
            model.MemberVersioned, model.Verification
        ],
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        member_verification_test_client: member_verification_client.MemberVerifications,
        verification_test_client: verification_client.Verifications,
    ):
        """
        Test that we are not pre-verifying records against expired verifications
        """
        # Given
        original_member, verification = member_verification_primary_expired
        # When - Persist that same member again since we received an update
        updated_member = await member_versioned_test_client.persist(
            model=original_member
        )

        await verification_test_client.batch_pre_verify_records_by_org(
            organization_id=updated_member.organization_id, batch_size=1000
        )

        # Then
        member_verifications: List[
            model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=verification.id
        )
        assert {
            original_member.id,
        } == {mv.member_id for mv in member_verifications}

    @staticmethod
    async def test_pre_verification_expired_and_valid_verification(
        multiple_member_verification_expired_and_valid: Tuple[
            model.MemberVersioned, model.Verification, model.Verification
        ],
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        member_verification_test_client: member_verification_client.MemberVerifications,
        verification_test_client: verification_client.Verifications,
    ):
        """
        Test that we are not pre-verifying records against expired verifications,
        but verifying against the valid verification
        """
        # Given
        (
            original_member,
            expired_verification,
            valid_verification,
        ) = multiple_member_verification_expired_and_valid
        # When - Persist that same member again since we received an update
        updated_member: model.MemberVersioned = (
            await member_versioned_test_client.persist(model=original_member)
        )

        await verification_test_client.batch_pre_verify_records_by_org(
            organization_id=updated_member.organization_id, batch_size=1000
        )

        # Then
        expired_member_verifications: List[
            model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=expired_verification.id
        )
        valid_member_verifications: List[
            model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=valid_verification.id
        )
        assert {
            (original_member.id, expired_verification.id),
            (original_member.id, valid_verification.id),
            (updated_member.id, valid_verification.id),
        } == {
            (mv.member_id, mv.verification_id)
            for mv in expired_member_verifications + valid_member_verifications
        }

    @staticmethod
    async def test_pre_verification_twice(
        member_verification_primary: Tuple[model.MemberVersioned, model.Verification],
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        member_verification_test_client: member_verification_client.MemberVerifications,
        verification_test_client: verification_client.Verifications,
    ):
        """
        Test that we are pre-verifying an update of a record that was verified with primary
        verification (email, date_of_birth), and then a subsequent new record is also pre-verified
        """
        # Given
        original_member, verification = member_verification_primary
        # When - Persist that same member again since we received an update
        updated_member: model.MemberVersioned = (
            await member_versioned_test_client.persist(model=original_member)
        )

        await verification_test_client.batch_pre_verify_records_by_org(
            organization_id=updated_member.organization_id, batch_size=1000
        )

        updated_again_member: model.MemberVersioned = (
            await member_versioned_test_client.persist(model=original_member)
        )

        await verification_test_client.batch_pre_verify_records_by_org(
            organization_id=updated_member.organization_id, batch_size=1000
        )

        # Then
        member_verifications: List[
            model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=verification.id
        )
        assert {updated_member.id, original_member.id, updated_again_member.id} == {
            mv.member_id for mv in member_verifications
        }

    @staticmethod
    async def test_pre_verification_filter_by_file(
        member_verification_primary: Tuple[model.MemberVersioned, model.Verification],
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        member_verification_test_client: member_verification_client.MemberVerifications,
        verification_test_client: verification_client.Verifications,
        file_test_client: file_client.Files,
    ):
        """
        Test that when a file_id is passed into pre-verification, it works as intended
        """
        # Given
        original_member, verification = member_verification_primary
        # When - Persist that same member again since we received an update
        new_file = await file_test_client.persist(
            model=factories.FileFactory.create(
                organization_id=original_member.organization_id
            )
        )
        original_member.file_id = new_file.id
        updated_member: model.MemberVersioned = (
            await member_versioned_test_client.persist(model=original_member)
        )

        await verification_test_client.batch_pre_verify_records_by_org(
            organization_id=updated_member.organization_id,
            file_id=new_file.id,
            batch_size=1000,
        )

        # Then
        member_verifications: List[
            model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=verification.id
        )
        assert {updated_member.id, original_member.id} == {
            mv.member_id for mv in member_verifications
        }

    @staticmethod
    async def test_pre_verification_filter_by_file_wrong_file(
        member_verification_primary: Tuple[model.MemberVersioned, model.Verification],
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        member_verification_test_client: member_verification_client.MemberVerifications,
        verification_test_client: verification_client.Verifications,
        file_test_client: file_client.Files,
    ):
        """
        Test that when a wrong file_id is passed into pre-verification, it doesn't works as intended
        """
        # Given
        original_member, verification = member_verification_primary
        # When - Persist that same member again since we received an update
        new_file = await file_test_client.persist(
            model=factories.FileFactory.create(
                organization_id=original_member.organization_id
            )
        )
        original_member.file_id = new_file.id
        updated_member: model.MemberVersioned = (
            await member_versioned_test_client.persist(model=original_member)
        )

        await verification_test_client.batch_pre_verify_records_by_org(
            organization_id=updated_member.organization_id,
            file_id=new_file.id + 1,
            batch_size=1000,
        )

        # Then
        member_verifications: List[
            model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=verification.id
        )
        assert updated_member.id not in {mv.member_id for mv in member_verifications}

    @staticmethod
    async def test_pre_verification_batch_returns_number_pre_verified(
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        member_verification_test_client: member_verification_client.MemberVerifications,
        verification_test_client: verification_client.Verifications,
        test_config: configuration_client.Configuration,
    ):
        """
        Test that pre-verification works on batch of members
        """
        # Given
        n = 43
        original_members: List[
            model.MemberVersioned
        ] = await member_versioned_test_client.bulk_persist(
            models=factories.MemberVersionedFactory.create_batch(
                size=n, organization_id=test_config.organization_id
            )
        )
        verifications: List[
            model.Verification
        ] = await verification_test_client.bulk_persist(
            models=factories.VerificationFactory.create_batch(
                size=n,
                organization_id=test_config.organization_id,
                verification_type=model.VerificationTypes.PRIMARY,
            )
        )
        await member_verification_test_client.bulk_persist(
            models=[
                factories.MemberVerificationFactory.create(
                    member_id=m.id, verification_id=v.id
                )
                for m, v in zip(original_members, verifications)
            ]
        )

        # When
        await member_versioned_test_client.bulk_persist(models=original_members)

        num_pre_verified: int = (
            await verification_test_client.batch_pre_verify_records_by_org(
                organization_id=test_config.organization_id, batch_size=1000
            )
        )

        # Then
        assert num_pre_verified == n

    @staticmethod
    async def test_pre_verification_batch_returns_number_pre_verified_does_not_exceed_batch_size(
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        member_verification_test_client: member_verification_client.MemberVerifications,
        verification_test_client: verification_client.Verifications,
        test_config: configuration_client.Configuration,
    ):
        """
        Test that pre-verification does not exceed batch size
        """
        # Given
        n = 200
        batch_size = 43
        original_members: List[
            model.MemberVersioned
        ] = await member_versioned_test_client.bulk_persist(
            models=factories.MemberVersionedFactory.create_batch(
                size=n, organization_id=test_config.organization_id
            )
        )
        verifications: List[
            model.Verification
        ] = await verification_test_client.bulk_persist(
            models=factories.VerificationFactory.create_batch(
                size=n,
                organization_id=test_config.organization_id,
                verification_type=model.VerificationTypes.PRIMARY,
            )
        )
        await member_verification_test_client.bulk_persist(
            models=[
                factories.MemberVerificationFactory.create(
                    member_id=m.id, verification_id=v.id
                )
                for m, v in zip(original_members, verifications)
            ]
        )

        # When
        await member_versioned_test_client.bulk_persist(models=original_members)

        num_pre_verified: int = (
            await verification_test_client.batch_pre_verify_records_by_org(
                organization_id=test_config.organization_id, batch_size=batch_size
            )
        )

        # Then
        assert num_pre_verified == batch_size

    @staticmethod
    async def test_pre_verification_batch_with_dependents(
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        member_verification_test_client: member_verification_client.MemberVerifications,
        verification_test_client: verification_client.Verifications,
        test_config: configuration_client.Configuration,
    ):
        """
        Test that pre-verification works on batch of members with some dependents
        """
        # Given
        n = 100
        original_members: List[
            model.MemberVersioned
        ] = await member_versioned_test_client.bulk_persist(
            models=factories.MemberVersionedFactory.create_batch(
                size=n, organization_id=test_config.organization_id
            )
        )
        primary_verifications: List[
            model.Verification
        ] = await verification_test_client.bulk_persist(
            models=factories.VerificationFactory.create_batch(
                size=n,
                organization_id=test_config.organization_id,
                verification_type=model.VerificationTypes.PRIMARY,
            )
        )
        dependent_verifications: List[
            model.Verification
        ] = await verification_test_client.bulk_persist(
            models=factories.VerificationFactory.create_batch(
                size=n,
                organization_id=test_config.organization_id,
                verification_type=model.VerificationTypes.PRIMARY,
            )
        )
        await member_verification_test_client.bulk_persist(
            models=[
                factories.MemberVerificationFactory.create(
                    member_id=m.id, verification_id=v.id
                )
                for m, v in zip(original_members, primary_verifications)
            ]
        )
        await member_verification_test_client.bulk_persist(
            models=[
                factories.MemberVerificationFactory.create(
                    member_id=m.id, verification_id=v.id
                )
                for m, v in zip(original_members, dependent_verifications)
            ]
        )

        # When
        updated_members: List[
            model.MemberVersioned
        ] = await member_versioned_test_client.bulk_persist(models=original_members)

        await verification_test_client.batch_pre_verify_records_by_org(
            organization_id=test_config.organization_id, batch_size=1000
        )

        # Then
        member_verifications: List[
            model.MemberVerification
        ] = await member_verification_test_client.all()

        assert {m.id for m in (original_members + updated_members)} == {
            mv.member_id for mv in member_verifications
        }


class TestBatchPreVerificationPrimary:
    @staticmethod
    async def test_pre_verification_insert_primary_verification_exists(
        member_verification_primary: Tuple[model.MemberVersioned, model.Verification],
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        member_verification_test_client: member_verification_client.MemberVerifications,
        verification_test_client: verification_client.Verifications,
    ):
        """
        Test that we are pre-verifying an update of a record that was verified with primary
        verification (email, date_of_birth)
        """
        # Given
        original_member, verification = member_verification_primary
        # When - Persist that same member again since we received an update
        updated_member: model.MemberVersioned = (
            await member_versioned_test_client.persist(model=original_member)
        )
        await verification_test_client.batch_pre_verify_records_by_org(
            organization_id=updated_member.organization_id, batch_size=1000
        )
        # Then
        member_verifications: List[
            model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=verification.id
        )
        assert len(member_verifications) == 2 and {
            updated_member.id,
            original_member.id,
        } == {mv.member_id for mv in member_verifications}

    @staticmethod
    @pytest.mark.parametrize(
        argnames="field,updated_value",
        argvalues=[
            ("email", "something" + PRIMARY_VERIFICATION.email),
            (
                "date_of_birth",
                PRIMARY_VERIFICATION.date_of_birth + datetime.timedelta(days=23),
            ),
        ],
        ids=["email-changed", "date_of_birth-changed"],
    )
    async def test_pre_verification_insert_primary_verification_param_changed(
        member_verification_primary: Tuple[model.MemberVersioned, model.Verification],
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        member_verification_test_client: member_verification_client.MemberVerifications,
        verification_test_client: verification_client.Verifications,
        field: str,
        updated_value: Any,
    ):
        """
        Test that we are NOT pre-verifying an update of a record that was verified with primary
        verification (email, date_of_birth) because one of the fields has changed
        """
        # Given
        original_member, verification = member_verification_primary
        changed_member = dataclasses.replace(original_member)

        # Update the fields on the new member
        setattr(changed_member, field, updated_value)

        # When - Persist that same member again since we received an update
        updated_member = await member_versioned_test_client.persist(
            model=changed_member
        )
        await verification_test_client.batch_pre_verify_records_by_org(
            organization_id=updated_member.organization_id, batch_size=1000
        )

        # Then
        member_verifications: List[
            model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=verification.id
        )
        assert len(member_verifications) == 1 and {original_member.id} == {
            mv.member_id for mv in member_verifications
        }

    @staticmethod
    @pytest.mark.parametrize(
        argnames="field,updated_value",
        argvalues=[
            ("dependent_id", DEPENDENT_ID + "KFDS"),
            ("employer_assigned_id", EMPLOYER_ASSIGNED_ID + "FD9F"),
            ("record", {}),
            ("gender_code", "F" if GENDER_CODE == "M" else "M"),
        ],
        ids=[
            "dependent_id-changed",
            "employer_assigned_id-changed",
            "record-changed",
            "gender_code-changed",
        ],
    )
    async def test_pre_verification_insert_primary_verification_non_verification_param_changed(
        member_verification_primary: Tuple[model.MemberVersioned, model.Verification],
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        member_verification_test_client: member_verification_client.MemberVerifications,
        verification_test_client: verification_client.Verifications,
        field: str,
        updated_value: Any,
    ):
        """
        Test that we are pre-verifying an update of a record that was verified with primary
        verification (email, date_of_birth) because other fields (dependent_id, employer_assigned_id,
        record, gender_code) has changed.

        (first_name, last_name, date_of_birth, email, unique_corp_id, work_state) has remained the same.
        """
        # Given
        original_member, verification = member_verification_primary

        # Update the fields on the new member copy
        changed_member = dataclasses.replace(original_member)
        setattr(changed_member, field, updated_value)

        # When - Persist that same member again since we received an update
        updated_member: model.MemberVersioned = (
            await member_versioned_test_client.persist(model=changed_member)
        )
        await verification_test_client.batch_pre_verify_records_by_org(
            organization_id=updated_member.organization_id, batch_size=1000
        )
        # Then
        member_verifications: List[
            model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=verification.id
        )

        assert len(member_verifications) == 2 and {
            original_member.id,
            updated_member.id,
        } == {mv.member_id for mv in member_verifications}

    @staticmethod
    async def test_pre_verification_insert_primary_verification_exists_member_and_dependent(
        member_and_dependent_verification_primary: Tuple[
            model.MemberVersioned, model.Verification, model.Verification
        ],
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        member_verification_test_client: member_verification_client.MemberVerifications,
        verification_test_client: verification_client.Verifications,
    ):
        """
        Test that we are pre-verifying an update of a record that was verified with primary
        verification (email, date_of_birth) - make sure that we pre-verify both the primary and dependent
        """
        # Given
        (
            original_member,
            primary_verification,
            dependent_verification,
        ) = member_and_dependent_verification_primary
        # When - Persist that same member again since we received an update
        updated_member: model.MemberVersioned = (
            await member_versioned_test_client.persist(model=original_member)
        )
        await verification_test_client.batch_pre_verify_records_by_org(
            organization_id=updated_member.organization_id, batch_size=1000
        )

        # Then
        primary_mvs: List[
            model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=primary_verification.id
        )
        dependent_mvs: List[
            model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=dependent_verification.id
        )

        assert {
            (
                original_member.id,
                primary_verification.id,
            ),  # original member_verification for primary
            (
                original_member.id,
                dependent_verification.id,
            ),  # original member_verification for dependent
            (
                updated_member.id,
                primary_verification.id,
            ),  # new member_verification for primary
            (
                updated_member.id,
                dependent_verification.id,
            ),  # new member_verification for dependent
        } == {(mv.member_id, mv.verification_id) for mv in primary_mvs + dependent_mvs}

    @staticmethod
    @pytest.mark.parametrize(
        argnames="field,updated_value",
        argvalues=[
            ("email", "something" + PRIMARY_VERIFICATION.email),
            (
                "date_of_birth",
                PRIMARY_VERIFICATION.date_of_birth + datetime.timedelta(days=23),
            ),
        ],
        ids=["email-changed", "date_of_birth-changed"],
    )
    async def test_pre_verification_insert_primary_verification_param_changed_member_and_dependent(
        member_and_dependent_verification_primary: Tuple[
            model.MemberVersioned, model.Verification, model.Verification
        ],
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        member_verification_test_client: member_verification_client.MemberVerifications,
        verification_test_client: verification_client.Verifications,
        field: str,
        updated_value: Any,
    ):
        """
        Test that we are NOT pre-verifying an update of a record that was verified with primary
        verification (email, date_of_birth) because one of the fields has changed
        """
        # Given
        (
            original_member,
            primary_verification,
            dependent_verification,
        ) = member_and_dependent_verification_primary

        changed_member = dataclasses.replace(original_member)

        # Update the fields on the new member
        setattr(changed_member, field, updated_value)

        # When - Persist that same member again since we received an update
        updated_member = await member_versioned_test_client.persist(
            model=changed_member
        )
        await verification_test_client.batch_pre_verify_records_by_org(
            organization_id=updated_member.organization_id, batch_size=1000
        )
        # Then
        primary_mvs: List[
            model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=primary_verification.id
        )
        dependent_mvs: List[
            model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=dependent_verification.id
        )

        assert {
            (
                original_member.id,
                primary_verification.id,
            ),  # original member_verification for primary
            (
                original_member.id,
                dependent_verification.id,
            ),  # original member_verification for dependent
        } == {(mv.member_id, mv.verification_id) for mv in primary_mvs + dependent_mvs}

    @staticmethod
    @pytest.mark.parametrize(
        argnames="field,updated_value",
        argvalues=[
            ("dependent_id", DEPENDENT_ID + "KFDS"),
            ("employer_assigned_id", EMPLOYER_ASSIGNED_ID + "FD9F"),
            ("record", {}),
            ("gender_code", "F" if GENDER_CODE == "M" else "M"),
        ],
        ids=[
            "dependent_id-changed",
            "employer_assigned_id-changed",
            "record-changed",
            "gender_code-changed",
        ],
    )
    async def test_pre_verification_insert_primary_verification_non_verification_param_changed_member_and_dependent(
        member_and_dependent_verification_primary: Tuple[
            model.MemberVersioned, model.Verification, model.Verification
        ],
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        member_verification_test_client: member_verification_client.MemberVerifications,
        verification_test_client: verification_client.Verifications,
        field: str,
        updated_value: Any,
    ):
        """
        Test that we are pre-verifying an update of a record that was verified with primary
        verification (email, date_of_birth) because other fields (dependent_id, employer_assigned_id,
        record, gender_code) has changed.

        (first_name, last_name, date_of_birth, email, unique_corp_id, work_state) has remained the same.

        Both the primary and dependent verification should be pre-verified
        """
        # Given
        (
            original_member,
            primary_verification,
            dependent_verification,
        ) = member_and_dependent_verification_primary

        changed_member = dataclasses.replace(original_member)

        # Update the fields on the new member
        setattr(changed_member, field, updated_value)

        # When - Persist that same member again since we received an update
        updated_member: model.MemberVersioned = (
            await member_versioned_test_client.persist(model=changed_member)
        )

        await verification_test_client.batch_pre_verify_records_by_org(
            organization_id=updated_member.organization_id, batch_size=1000
        )
        # Then
        primary_mvs: List[
            model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=primary_verification.id
        )
        dependent_mvs: List[
            model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=dependent_verification.id
        )

        assert {
            (
                original_member.id,
                primary_verification.id,
            ),  # original member_verification for primary
            (
                original_member.id,
                dependent_verification.id,
            ),  # original member_verification for dependent
            (
                updated_member.id,
                primary_verification.id,
            ),  # new member_verification for primary
            (
                updated_member.id,
                dependent_verification.id,
            ),  # new member_verification for dependent
        } == {(mv.member_id, mv.verification_id) for mv in primary_mvs + dependent_mvs}

    @staticmethod
    @pytest.mark.parametrize(
        argnames="verification_type",
        argvalues=[
            model.VerificationTypes.CLIENT_SPECIFIC,
            model.VerificationTypes.MANUAL,
            model.VerificationTypes.FILELESS,
            model.VerificationTypes.PRE_VERIFY,
        ],
        ids=[
            "does-not-preverify-client-specific",
            "does-not-preverify-manual",
            "does-not-preverify-fileless",
            "does-not-preverify-pre-verify",
        ],
    )
    async def test_pre_verification_does_not_pre_verify_non_compatible_verification_types(
        test_config: model.Configuration,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        member_verification_test_client: member_verification_client.MemberVerifications,
        verification_test_client: verification_client.Verifications,
        verification_type: model.VerificationTypes,
    ):
        """Make sure we are only pre-verifying PRIMARY, ALTERNATE, MULTISTEP verification types"""
        # Given
        verification: model.Verification = await verification_test_client.persist(
            model=factories.VerificationFactory.create(
                organization_id=test_config.organization_id,
                email=PRIMARY_VERIFICATION.email,
                date_of_birth=PRIMARY_VERIFICATION.date_of_birth,
                verification_type=verification_type,
            ),
        )
        member: model.MemberVersioned = await member_versioned_test_client.persist(
            model=factories.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                # verification fields
                email=PRIMARY_VERIFICATION.email,
                date_of_birth=PRIMARY_VERIFICATION.date_of_birth,
                # other fields
                gender_code=GENDER_CODE,
                dependent_id=DEPENDENT_ID,
                record=RECORD,
                employer_assigned_id=EMPLOYER_ASSIGNED_ID,
            )
        )
        await member_verification_test_client.persist(
            model=factories.MemberVerificationFactory.create(
                member_id=member.id, verification_id=verification.id
            )
        )
        # When
        updated_member = await member_versioned_test_client.persist(
            model=factories.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                # verification fields
                email=PRIMARY_VERIFICATION.email,
                date_of_birth=PRIMARY_VERIFICATION.date_of_birth,
                # other fields
                gender_code=GENDER_CODE,
                dependent_id=DEPENDENT_ID,
                record=RECORD,
                employer_assigned_id=EMPLOYER_ASSIGNED_ID,
            )
        )
        await verification_test_client.batch_pre_verify_records_by_org(
            organization_id=updated_member.organization_id, batch_size=1000
        )
        # Then
        member_verifications: List[
            model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=verification.id
        )

        assert {(member.id, verification.id)} == {
            (mv.member_id, mv.verification_id) for mv in member_verifications
        }


class TestBatchPreVerificationAlternate:
    @staticmethod
    async def test_pre_verification_insert_alternate_verification_exists(
        member_verification_primary: Tuple[model.MemberVersioned, model.Verification],
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        member_verification_test_client: member_verification_client.MemberVerifications,
        verification_test_client: verification_client.Verifications,
    ):
        """
        Test that we are pre-verifying an update of a record that was verified with alternate
        verification (first_name, last_name, date_of_birth, work_state, unique_corp_id)
        """
        # Given
        original_member, verification = member_verification_primary
        # When - Persist that same member again since we received an update
        updated_member: model.MemberVersioned = (
            await member_versioned_test_client.persist(model=original_member)
        )
        await verification_test_client.batch_pre_verify_records_by_org(
            organization_id=updated_member.organization_id, batch_size=1000
        )

        # Then
        member_verifications: List[
            model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=verification.id
        )
        assert len(member_verifications) == 2 and {
            updated_member.id,
            original_member.id,
        } == {mv.member_id for mv in member_verifications}

    @staticmethod
    @pytest.mark.parametrize(
        argnames="field,updated_value",
        argvalues=[
            ("first_name", ALTERNATE_VERIFICATION.first_name + "l"),
            ("last_name", ALTERNATE_VERIFICATION.last_name + "y"),
            (
                "date_of_birth",
                PRIMARY_VERIFICATION.date_of_birth + datetime.timedelta(days=23),
            ),
            ("work_state", ALTERNATE_VERIFICATION.work_state + "C"),
            ("unique_corp_id", ALTERNATE_VERIFICATION.unique_corp_id + "0"),
        ],
        ids=[
            "first_name-changed",
            "last_name-changed",
            "date_of_birth-changed",
            "work_state-changed",
            "unique_corp_id-changed",
        ],
    )
    async def test_pre_verification_insert_alternate_verification_param_changed(
        member_verification_primary: Tuple[model.MemberVersioned, model.Verification],
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        member_verification_test_client: member_verification_client.MemberVerifications,
        verification_test_client: verification_client.Verifications,
        field: str,
        updated_value: Any,
    ):
        """
        Test that we are NOT pre-verifying an update of a record that was verified with alternate
        verification (first_name, last_name, date_of_birth, work_state, unique_corp_id)
        because one of the fields has changed
        """
        # Given
        original_member, verification = member_verification_primary
        changed_member = dataclasses.replace(original_member)

        # Update the fields on the new member
        setattr(changed_member, field, updated_value)

        # When - Persist that same member again since we received an update
        updated_member = await member_versioned_test_client.persist(
            model=changed_member
        )

        await verification_test_client.batch_pre_verify_records_by_org(
            organization_id=updated_member.organization_id, batch_size=1000
        )

        # Then
        member_verifications: List[
            model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=verification.id
        )
        assert len(member_verifications) == 1 and {original_member.id} == {
            mv.member_id for mv in member_verifications
        }

    @staticmethod
    @pytest.mark.parametrize(
        argnames="field,updated_value",
        argvalues=[
            ("dependent_id", DEPENDENT_ID + "KFDS"),
            ("employer_assigned_id", EMPLOYER_ASSIGNED_ID + "FD9F"),
            ("record", {}),
            ("gender_code", "F" if GENDER_CODE == "M" else "M"),
        ],
        ids=[
            "dependent_id-changed",
            "employer_assigned_id-changed",
            "record-changed",
            "gender_code-changed",
        ],
    )
    async def test_pre_verification_insert_alternate_verification_non_verification_param_changed(
        member_verification_primary: Tuple[model.MemberVersioned, model.Verification],
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        member_verification_test_client: member_verification_client.MemberVerifications,
        verification_test_client: verification_client.Verifications,
        field: str,
        updated_value: Any,
    ):
        """
        Test that we are pre-verifying an update of a record that was verified with alternate
        verification (first_name, last_name, date_of_birth, work_state, unique_corp_id) because other fields (dependent_id, employer_assigned_id,
        record, gender_code) has changed.

        (first_name, last_name, date_of_birth, email, unique_corp_id, work_state) has remained the same.
        """
        # Given
        original_member, verification = member_verification_primary

        # Update the fields on the new member copy
        changed_member = dataclasses.replace(original_member)
        setattr(changed_member, field, updated_value)

        # When - Persist that same member again since we received an update
        updated_member: model.MemberVersioned = (
            await member_versioned_test_client.persist(model=changed_member)
        )

        await verification_test_client.batch_pre_verify_records_by_org(
            organization_id=updated_member.organization_id, batch_size=1000
        )

        # Then
        member_verifications: List[
            model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=verification.id
        )

        assert len(member_verifications) == 2 and {
            original_member.id,
            updated_member.id,
        } == {mv.member_id for mv in member_verifications}

    @staticmethod
    async def test_pre_verification_insert_alternate_verification_exists_member_and_dependent(
        member_and_dependent_verification_primary: Tuple[
            model.MemberVersioned, model.Verification, model.Verification
        ],
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        member_verification_test_client: member_verification_client.MemberVerifications,
        verification_test_client: verification_client.Verifications,
    ):
        """
        Test that we are pre-verifying an update of a record that was verified with alternate
        verification (first_name, last_name, date_of_birth, work_state, unique_corp_id)

        Make sure that we pre-verify both the primary and dependent
        """
        # Given
        (
            original_member,
            primary_verification,
            dependent_verification,
        ) = member_and_dependent_verification_primary
        # When - Persist that same member again since we received an update
        updated_member: model.MemberVersioned = (
            await member_versioned_test_client.persist(model=original_member)
        )

        await verification_test_client.batch_pre_verify_records_by_org(
            organization_id=updated_member.organization_id, batch_size=1000
        )

        # Then
        primary_mvs: List[
            model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=primary_verification.id
        )
        dependent_mvs: List[
            model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=dependent_verification.id
        )

        assert {
            (
                original_member.id,
                primary_verification.id,
            ),  # original member_verification for primary
            (
                original_member.id,
                dependent_verification.id,
            ),  # original member_verification for dependent
            (
                updated_member.id,
                primary_verification.id,
            ),  # new member_verification for primary
            (
                updated_member.id,
                dependent_verification.id,
            ),  # new member_verification for dependent
        } == {(mv.member_id, mv.verification_id) for mv in primary_mvs + dependent_mvs}

    @staticmethod
    @pytest.mark.parametrize(
        argnames="field,updated_value",
        argvalues=[
            ("first_name", ALTERNATE_VERIFICATION.first_name + "l"),
            ("last_name", ALTERNATE_VERIFICATION.last_name + "y"),
            (
                "date_of_birth",
                PRIMARY_VERIFICATION.date_of_birth + datetime.timedelta(days=23),
            ),
            ("work_state", ALTERNATE_VERIFICATION.work_state + "C"),
            ("unique_corp_id", ALTERNATE_VERIFICATION.unique_corp_id + "0"),
        ],
        ids=[
            "first_name-changed",
            "last_name-changed",
            "date_of_birth-changed",
            "work_state-changed",
            "unique_corp_id-changed",
        ],
    )
    async def test_pre_verification_insert_alternate_verification_param_changed_member_and_dependent(
        member_and_dependent_verification_primary: Tuple[
            model.MemberVersioned, model.Verification, model.Verification
        ],
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        member_verification_test_client: member_verification_client.MemberVerifications,
        verification_test_client: verification_client.Verifications,
        field: str,
        updated_value: Any,
    ):
        """
        Test that we are NOT pre-verifying an update of a record that was verified with alternate
        verification (first_name, last_name, date_of_birth, work_state, unique_corp_id)
        because one of the fields has changed
        """
        # Given
        (
            original_member,
            primary_verification,
            dependent_verification,
        ) = member_and_dependent_verification_primary

        changed_member = dataclasses.replace(original_member)

        # Update the fields on the new member
        setattr(changed_member, field, updated_value)

        # When - Persist that same member again since we received an update
        updated_member = await member_versioned_test_client.persist(
            model=changed_member
        )
        await verification_test_client.batch_pre_verify_records_by_org(
            organization_id=updated_member.organization_id, batch_size=1000
        )

        # Then
        primary_mvs: List[
            model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=primary_verification.id
        )
        dependent_mvs: List[
            model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=dependent_verification.id
        )

        assert {
            (
                original_member.id,
                primary_verification.id,
            ),  # original member_verification for primary
            (
                original_member.id,
                dependent_verification.id,
            ),  # original member_verification for dependent
        } == {(mv.member_id, mv.verification_id) for mv in primary_mvs + dependent_mvs}

    @staticmethod
    @pytest.mark.parametrize(
        argnames="field,updated_value",
        argvalues=[
            ("dependent_id", DEPENDENT_ID + "KFDS"),
            ("employer_assigned_id", EMPLOYER_ASSIGNED_ID + "FD9F"),
            ("record", {}),
            ("gender_code", "F" if GENDER_CODE == "M" else "M"),
        ],
        ids=[
            "dependent_id-changed",
            "employer_assigned_id-changed",
            "record-changed",
            "gender_code-changed",
        ],
    )
    async def test_pre_verification_insert_alternate_verification_non_verification_param_changed_member_and_dependent(
        member_and_dependent_verification_primary: Tuple[
            model.MemberVersioned, model.Verification, model.Verification
        ],
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        member_verification_test_client: member_verification_client.MemberVerifications,
        verification_test_client: verification_client.Verifications,
        field: str,
        updated_value: Any,
    ):
        """
        Test that we are pre-verifying an update of a record that was verified with alternate
        verification (first_name, last_name, date_of_birth, work_state, unique_corp_id) because other fields (dependent_id, employer_assigned_id,
        record, gender_code) has changed.

        (first_name, last_name, date_of_birth, email, unique_corp_id, work_state) has remained the same.

        Both the primary and dependent verification should be pre-verified
        """
        # Given
        (
            original_member,
            primary_verification,
            dependent_verification,
        ) = member_and_dependent_verification_primary

        changed_member = dataclasses.replace(original_member)

        # Update the fields on the new member
        setattr(changed_member, field, updated_value)

        # When - Persist that same member again since we received an update
        updated_member: model.MemberVersioned = (
            await member_versioned_test_client.persist(model=changed_member)
        )
        await verification_test_client.batch_pre_verify_records_by_org(
            organization_id=updated_member.organization_id, batch_size=1000
        )

        # Then
        primary_mvs: List[
            model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=primary_verification.id
        )
        dependent_mvs: List[
            model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=dependent_verification.id
        )

        assert {
            (
                original_member.id,
                primary_verification.id,
            ),  # original member_verification for primary
            (
                original_member.id,
                dependent_verification.id,
            ),  # original member_verification for dependent
            (
                updated_member.id,
                primary_verification.id,
            ),  # new member_verification for primary
            (
                updated_member.id,
                dependent_verification.id,
            ),  # new member_verification for dependent
        } == {(mv.member_id, mv.verification_id) for mv in primary_mvs + dependent_mvs}
