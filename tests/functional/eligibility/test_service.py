import datetime
from typing import Dict, List, Optional
from unittest import mock

import pytest
from maven import feature_flags
from tests.factories import data_models
from verification import repository

from app.eligibility import errors, service
from app.eligibility.errors import (
    CreateVerificationError,
    GetMatchError,
    OverEligibilityError,
    RecordAlreadyClaimedError,
)
from app.eligibility.populations import model as pop_model
from app.utils import feature_flag
from db import model as db_model
from db.clients import (
    member_2_client,
    member_verification_client,
    member_versioned_client,
    population_client,
    sub_population_client,
    verification_2_client,
    verification_client,
)

pytestmark = pytest.mark.asyncio


@pytest.fixture
def verification_repo(
    verification_test_client,
    verification_attempt_test_client,
    member_verification_test_client,
    verification_2_test_client,
):
    return repository.VerificationRepository(
        verification_client=verification_test_client,
        verification_attempt_client=verification_attempt_test_client,
        member_verification_client=member_verification_test_client,
        verification_2_client=verification_2_test_client,
    )


@pytest.fixture
def member_versioned_repo(member_versioned_test_client):
    return repository.MemberVersionedRepository(
        member_versioned_client=member_versioned_test_client
    )


@pytest.fixture(scope="module")
def svc() -> service.EligibilityService:
    return service.EligibilityService()


@pytest.fixture
async def verified_member_zz_full_2(
    svc: service.EligibilityService,
    test_config: db_model.Configuration,
    # replacement clients & repo for the service
    population_test_client: population_client.Populations,
    sub_population_test_client: sub_population_client.SubPopulations,
    verification_repo: repository.VerificationRepository,
    member_versioned_test_client: member_versioned_client.MembersVersioned,
    # extra clients needed to write necessary data for test
    verification_test_client: verification_client.Verifications,
    member_verification_test_client: member_verification_client.MemberVerifications,
    member_2_test_client: member_2_client.Member2Client,
) -> (db_model.MemberVersioned, db_model.Verification):
    # Given
    svc.populations = population_test_client
    svc.sub_populations = sub_population_test_client
    svc.verifications = verification_repo
    svc.members_versioned = member_versioned_test_client
    svc.members_2 = member_2_test_client

    test_member_versioned = await member_versioned_test_client.persist(
        model=data_models.MemberVersionedFactory.create(
            organization_id=test_config.organization_id,
            work_state="ZZ",
            custom_attributes='{"employment_status": "Full", "group_number": "2"}',
        )
    )
    test_verification = await verification_test_client.persist(
        model=data_models.VerificationFactory.create(
            organization_id=test_config.organization_id,
            verified_at=datetime.date.today(),
            updated_at=datetime.date.today(),
        )
    )
    test_member_2 = await member_2_test_client.persist(
        model=data_models.Member2Factory.create(
            id=test_member_versioned.id,
            organization_id=test_config.organization_id,
            work_state="ZZ",
            custom_attributes='{"employment_status": "Full", "group_number": "2"}',
        )
    )
    await member_verification_test_client.persist(
        model=data_models.MemberVerificationFactory.create(
            member_id=test_member_versioned.id, verification_id=test_verification.id
        )
    )

    return test_member_versioned, test_verification, test_member_2


@pytest.fixture
async def verified_member_zz_full_3(
    svc: service.EligibilityService,
    test_config: db_model.Configuration,
    multiple_test_config: db_model.Configuration,
    # replacement clients & repo for the service
    population_test_client: population_client.Populations,
    sub_population_test_client: sub_population_client.SubPopulations,
    verification_repo: repository.VerificationRepository,
    member_versioned_test_client: member_versioned_client.MembersVersioned,
    # extra clients needed to write necessary data for test
    verification_test_client: verification_client.Verifications,
    member_verification_test_client: member_verification_client.MemberVerifications,
    member_2_test_client: member_2_client.Member2Client,
) -> (db_model.MemberVersioned, db_model.Verification):
    # Given
    svc.populations = population_test_client
    svc.sub_populations = sub_population_test_client
    svc.verifications = verification_repo
    svc.members_versioned = member_versioned_test_client
    svc.members_2 = member_2_test_client

    week_ago = datetime.datetime.now() - datetime.timedelta(days=7)
    # should map to fs_05
    test_member_versioned_1 = await member_versioned_test_client.persist(
        model=data_models.MemberVersionedFactory.create(
            organization_id=test_config.organization_id,
            work_state="ZZ",
            custom_attributes='{"employment_status": "Full", "group_number": "2"}',
        )
    )
    test_verification_1 = await verification_test_client.persist(
        model=data_models.VerificationFactory.create(
            organization_id=test_config.organization_id,
            verified_at=datetime.date.today(),
            updated_at=week_ago,
        )
    )
    await member_verification_test_client.persist(
        model=data_models.MemberVerificationFactory.create(
            member_id=test_member_versioned_1.id, verification_id=test_verification_1.id
        )
    )

    test_member_versioned_2 = await member_versioned_test_client.persist(
        model=data_models.MemberVersionedFactory.create(
            organization_id=multiple_test_config[2].organization_id,
            work_state="NY",
            custom_attributes="{}",
        )
    )
    test_verification_2 = await verification_test_client.persist(
        model=data_models.VerificationFactory.create(
            user_id=test_verification_1.user_id,
            organization_id=multiple_test_config[2].organization_id,
            verified_at=week_ago,
            updated_at=datetime.date.today(),
        )
    )
    test_member_2 = await member_2_test_client.persist(
        model=data_models.Member2Factory.create(
            id=test_member_versioned_1.id,
            organization_id=test_config.organization_id,
            work_state="ZZ",
            custom_attributes='{"employment_status": "Full", "group_number": "2"}',
        )
    )
    await member_verification_test_client.persist(
        model=data_models.MemberVerificationFactory.create(
            member_id=test_member_versioned_2.id, verification_id=test_verification_2.id
        )
    )

    return [test_member_versioned_1, test_member_versioned_2, test_member_2], [
        test_verification_1,
        test_verification_2,
    ]


@pytest.fixture
def sub_pop_zz_full_2_track_info(
    mapped_sub_populations: List[pop_model.SubPopulation],
) -> (int, List[int]):
    sub_pop_id = None
    for sub_pop in mapped_sub_populations:
        if sub_pop.feature_set_name == "fs_05":
            sub_pop_id = sub_pop.id
    assert sub_pop_id is not None
    sub_pop_feature_list = [1202, 1204, 1206, 1208, 1210]
    return sub_pop_id, sub_pop_feature_list


@pytest.fixture
def mock_verification_data():
    def create_mock_verification_data(
        eligibility_member_id: Optional[int] = None,
        organization_id: Optional[int] = None,
        unique_corp_id: Optional[str] = None,
        dependent_id: Optional[str] = None,
        email: Optional[str] = None,
        work_state: Optional[str] = None,
        additional_fields: Optional[Dict[str, str]] = None,
        verification_session: Optional[str] = None,
        member_1_id: Optional[int] = None,
        member_2_id: Optional[int] = None,
        member_2_version: Optional[int] = None,
    ):
        verification_params = {}

        # Conditionally add parameters if they are provided
        if "eligibility_member_id" in locals():
            verification_params["eligibility_member_id"] = eligibility_member_id
        if "organization_id" in locals() and organization_id is not None:
            verification_params["organization_id"] = organization_id
        if unique_corp_id is not None:
            verification_params["unique_corp_id"] = unique_corp_id
        if "dependent_id" in locals():
            verification_params["dependent_id"] = dependent_id
        if email is not None:
            verification_params["email"] = email
        if work_state is not None:
            verification_params["work_state"] = work_state
        if additional_fields is not None:
            verification_params["additional_fields"] = additional_fields
        if member_1_id is not None:
            verification_params["member_1_id"] = member_1_id
        if member_2_id is not None:
            verification_params["member_2_id"] = member_2_id
        if member_2_version is not None:
            verification_params["member_2_version"] = member_2_version

        return data_models.VerificationDataFactory.create(
            verification_id=None,
            verification_attempt_id=None,
            **verification_params,
        )

    return create_mock_verification_data


class TestCreateVerificationForUsers:
    @staticmethod
    async def test_create_verification_for_user(
        svc, test_member_versioned, verification_repo
    ):
        # Given
        user_id = 12345
        additional_fields = {"employee": True, "mavenID": "1234foo"}
        svc.verifications = verification_repo
        verified_at = datetime.datetime.now(tz=datetime.timezone.utc)

        # When
        generated_verification_record = await svc.create_verification_for_user(
            user_id=user_id,
            organization_id=test_member_versioned.organization_id,
            verification_type="PRIMARY",
            date_of_birth=test_member_versioned.date_of_birth,
            unique_corp_id=test_member_versioned.unique_corp_id,
            eligibility_member_id=test_member_versioned.id,
            verified_at=verified_at.isoformat(),
            additional_fields=additional_fields,
        )

        # Then
        assert generated_verification_record.user_id == user_id
        assert (
            generated_verification_record.eligibility_member_id
            == test_member_versioned.id
        )
        assert (
            generated_verification_record.first_name == test_member_versioned.first_name
        )
        assert (
            generated_verification_record.last_name == test_member_versioned.last_name
        )
        assert (
            generated_verification_record.unique_corp_id
            == test_member_versioned.unique_corp_id
        )
        assert (
            generated_verification_record.date_of_birth
            == test_member_versioned.date_of_birth
        )
        assert generated_verification_record.additional_fields == additional_fields
        assert generated_verification_record.verified_at == verified_at

    @staticmethod
    async def test_create_verification_for_user_no_e9y_member_record(svc, test_config):
        # Given
        user_id = 12345
        date_of_birth = datetime.date(2020, 1, 1)
        unique_corp_id = "maven_foo"

        # When
        generated_verification_record = await svc.create_verification_for_user(
            user_id=user_id,
            organization_id=test_config.organization_id,
            verification_type="PRIMARY",
            date_of_birth=date_of_birth,
            unique_corp_id=unique_corp_id,
        )

        # Then
        assert generated_verification_record.user_id == user_id
        assert generated_verification_record.eligibility_member_id is None
        assert generated_verification_record.unique_corp_id == unique_corp_id

    @staticmethod
    async def test_create_verification_for_user_missing_e9y_member_record(
        svc, test_config
    ):
        # Given
        user_id = 12345
        date_of_birth = datetime.date(2020, 1, 1)
        unique_corp_id = "maven_foo"
        eligibility_member_id = 12345

        # When
        generated_verification_record = await svc.create_verification_for_user(
            user_id=user_id,
            organization_id=test_config.organization_id,
            verification_type="PRIMARY",
            date_of_birth=date_of_birth,
            unique_corp_id=unique_corp_id,
            eligibility_member_id=eligibility_member_id,
        )

        # Then
        assert (
            generated_verification_record
            and generated_verification_record.eligibility_member_id is None
        )

    @staticmethod
    async def test_create_verification_for_user_null_dates(svc, test_config):
        # Given
        user_id = 12345
        unique_corp_id = "maven_foo"

        # When
        generated_verification_record = await svc.create_verification_for_user(
            user_id=user_id,
            organization_id=test_config.organization_id,
            verification_type="PRIMARY",
            date_of_birth=None,
            unique_corp_id=unique_corp_id,
            verified_at=None,
        )

        # Then
        assert generated_verification_record.user_id == user_id
        assert generated_verification_record.eligibility_member_id is None
        assert generated_verification_record.unique_corp_id == unique_corp_id
        assert generated_verification_record.date_of_birth is None
        # We should use a default value if one is not provided
        assert generated_verification_record.verified_at
        assert generated_verification_record.additional_fields == {}

    @staticmethod
    async def test_create_verification_for_user_verification_creation_error(svc):
        # Given
        # When/Then
        with pytest.raises(CreateVerificationError):
            await svc.create_verification_for_user(
                user_id=12345,
                organization_id=12345,  # pass in a fake orgID to cause error
                verification_type="PRIMARY",
                date_of_birth=datetime.date(2020, 1, 1),
                unique_corp_id="maven_foo",
            )

    @staticmethod
    async def test_create_verification_for_user_verification_attempt_creation_error(
        svc, test_config
    ):
        # Given
        # When/Then
        with mock.patch(
            "verification.repository.verification.VerificationRepository.create_verification_attempt",
            side_effect=ValueError,
        ):
            with pytest.raises(CreateVerificationError):
                await svc.create_verification_for_user(
                    user_id=12345,
                    organization_id=test_config.organization_id,
                    verification_type="PRIMARY",
                    date_of_birth=datetime.date(2020, 1, 1),
                    unique_corp_id="maven_foo",
                )

    @staticmethod
    async def test_create_verification_for_user_missing_eligibility_member(
        svc, test_config, test_member_versioned
    ):
        # Given
        # When/Then
        with mock.patch(
            "verification.repository.verification.VerificationRepository.create_member_verification",
            side_effect=ValueError,
        ):
            verification = await svc.create_verification_for_user(
                user_id=12345,
                organization_id=test_config.organization_id,
                verification_type="PRIMARY",
                date_of_birth=datetime.date(2020, 1, 1),
                unique_corp_id="maven_foo",
                eligibility_member_id=test_member_versioned.id,
            )

        assert verification

    @staticmethod
    async def test_create_verification_for_user_e9y_record_already_used(
        svc, test_config, test_member_verification, test_member_versioned
    ):
        # Given
        # When/Then
        with mock.patch(
            "app.eligibility.service.EligibilityService.verify_eligibility_record_usable",
            return_value=False,
        ):
            with pytest.raises(RecordAlreadyClaimedError):
                await svc.create_verification_for_user(
                    user_id=12345,
                    organization_id=test_config.organization_id,
                    verification_type="PRIMARY",
                    date_of_birth=datetime.date(2020, 1, 1),
                    unique_corp_id="maven_foo",
                    eligibility_member_id=test_member_versioned.id,
                )

    @staticmethod
    async def test_create_verification_for_user_write_disabled(svc, test_config):
        # Given
        user_id = 12345
        unique_corp_id = "maven_foo"

        with mock.patch(
            "app.utils.feature_flag.is_write_disabled",
            return_value=True,
        ), pytest.raises(
            errors.CreateVerificationError,
            match="Creation is disabled due to feature flag",
        ):
            await svc.create_verification_for_user(
                user_id=user_id,
                organization_id=test_config.organization_id,
                verification_type="PRIMARY",
                date_of_birth=None,
                unique_corp_id=unique_corp_id,
                verified_at=None,
            )


@pytest.fixture
def ff_test_data():
    with feature_flags.test_data() as td:
        yield td


class TestCreateMultipleVerificationsV1:
    @staticmethod
    async def test_create_multiple_verifications_v1(
        svc,
        multiple_test_members_versioned_from_test_config,
        verification_repo,
        mock_verification_data,
    ):
        # Given
        user_id = 12345
        first_name = "Don"
        last_name = "Joe"
        date_of_birth = datetime.date(1990, 1, 1)

        svc.verifications = verification_repo

        # Create a list of VerificationData objects
        verified_at = datetime.datetime.now(tz=datetime.timezone.utc)

        # Create a list of VerificationData objects
        verification_data_list = [
            mock_verification_data(
                eligibility_member_id=t.id,
                organization_id=t.organization_id,
                unique_corp_id=t.unique_corp_id,
                dependent_id=t.dependent_id,
                email=t.email,
                work_state=t.work_state,
                additional_fields={},
            )
            for t in multiple_test_members_versioned_from_test_config
        ]

        # When
        await svc.create_multiple_verifications_v1(
            user_id=user_id,
            verification_data_1_not_already_claimed=verification_data_list,
            verification_type="PRIMARY",
            date_of_birth=date_of_birth,
            first_name=first_name,
            last_name=last_name,
            verified_at=verified_at,
        )

        verifications_created = await svc.get_all_verifications_for_user(
            user_id=user_id
        )

        # Then
        assert len(verification_data_list) == len(verifications_created)
        for verification_data, verification_created in zip(
            verification_data_list, verifications_created
        ):
            assert (
                verification_data.verification_id
                == verification_created.verification_id
            )
            assert (
                verification_data.organization_id
                == verification_created.organization_id
            )

    @staticmethod
    async def test_create_multiple_verifications_v1_nop_when_empty_data(svc):
        # Given
        user_id = 12345
        first_name = "Don"
        last_name = "Joe"
        date_of_birth = datetime.date(1990, 1, 1)
        verified_at = datetime.datetime.now(tz=datetime.timezone.utc)
        # svc.verifications = verification_repo

        # When
        with mock.patch(
            "verification.repository.verification.VerificationRepository.create_multiple_verifications",
        ) as mock_create_verifications, mock.patch(
            "verification.repository.verification.VerificationRepository.create_multiple_verification_attempts",
        ) as mock_create_verification_attempts, mock.patch(
            "verification.repository.verification.VerificationRepository.create_multiple_member_verifications",
        ) as mock_create_member_verifications:
            await svc.create_multiple_verifications_v1(
                user_id=user_id,
                verification_data_1_not_already_claimed=[],
                verification_type="PRIMARY",
                date_of_birth=date_of_birth,
                first_name=first_name,
                last_name=last_name,
                verified_at=verified_at,
            )

            mock_create_verifications.assert_not_called()
            mock_create_verification_attempts.assert_not_called()
            mock_create_member_verifications.assert_not_called()

    @staticmethod
    async def test_create_multiple_verifications_write_disabled(svc):
        # Given
        user_id = 12345
        first_name = "Don"
        last_name = "Joe"
        date_of_birth = datetime.date(1990, 1, 1)
        verified_at = datetime.datetime.now(tz=datetime.timezone.utc)

        with mock.patch(
            "app.utils.feature_flag.is_write_disabled",
            return_value=True,
        ), pytest.raises(
            errors.CreateVerificationError,
            match="Creation is disabled due to feature flag",
        ):
            await svc.create_multiple_verifications_v1(
                user_id=user_id,
                verification_data_1_not_already_claimed=[],
                verification_type="PRIMARY",
                date_of_birth=date_of_birth,
                first_name=first_name,
                last_name=last_name,
                verified_at=verified_at,
            )


class TestPrepareAndCreateMultipleVerificationsV2:
    @staticmethod
    async def test_prepare_and_create_multiple_verifications_v2(
        svc,
        test_config,
        configuration_test_client,
        member_2_test_client,
        member_versioned_test_client,
        verification_repo,
        mock_verification_data,
    ):
        user_id = 12345
        first_name = "Don"
        last_name = "Joe"
        date_of_birth = datetime.date(1990, 1, 1)
        email = "email@test.net"
        work_state = "FL"
        unique_corp_id = "test_unique_corp_id"

        svc.verifications = verification_repo
        svc.configurations = configuration_test_client
        svc.members_2 = member_2_test_client
        svc.members_versioned = member_versioned_test_client

        # Create a list of VerificationData objects
        verified_at = datetime.datetime.now(tz=datetime.timezone.utc)

        test_member_2 = await member_2_test_client.persist(
            model=data_models.Member2Factory.create(
                id=1001,
                organization_id=test_config.organization_id,
                first_name=first_name,
                last_name=last_name,
                date_of_birth=date_of_birth,
                email=email,
                work_state=work_state,
                unique_corp_id=unique_corp_id,
            )
        )
        test_member_versioned = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                first_name=first_name,
                last_name=last_name,
                date_of_birth=date_of_birth,
                email=email,
                work_state=work_state,
                unique_corp_id=unique_corp_id,
            )
        )

        # Create a list of VerificationData objects
        verification_data_list = [
            mock_verification_data(
                eligibility_member_id=test_member_2.id,
                organization_id=test_member_2.organization_id,
                unique_corp_id=test_member_2.unique_corp_id,
                dependent_id=test_member_2.dependent_id,
                email=test_member_2.email,
                work_state=test_member_2.work_state,
                additional_fields={},
            )
        ]

        # When
        with mock.patch(
            "db.clients.configuration_client.Configurations.get",
            return_value=test_config,
        ), mock.patch(
            "db.clients.member_2_client.Member2Client.get",
            return_value=test_member_2,
        ), mock.patch(
            "db.clients.member_versioned_client.MembersVersioned.get",
            return_value=test_member_versioned,
        ):
            await svc.prepare_and_create_multiple_verifications_v2(
                user_id=user_id,
                verification_data_2_not_already_claimed=verification_data_list,
                verification_type="PRIMARY",
                date_of_birth=date_of_birth,
                first_name=first_name,
                last_name=last_name,
                verified_at=verified_at,
            )
            verifications_created = await svc.get_all_verifications_for_user(
                user_id=user_id
            )

            # Then
            assert len(verification_data_list) == len(verifications_created)
            for verification_data, verification_created in zip(
                verification_data_list, verifications_created
            ):
                assert (
                    verification_data.verification_id
                    == verification_created.verification_id
                )
                assert (
                    verification_data.organization_id
                    == verification_created.organization_id
                )


class TestCreateMultipleVerificationsForUser:
    @staticmethod
    def get_organization_ids_with_e9y_already_claimed():
        return [1, 3, 5]

    @staticmethod
    def eligibility_record_usable_side_effect(eligibility_member_id, organization_id):
        # Claim member records for given organizations
        # Verification records will not be created for these orgs
        if (
            eligibility_member_id in {1, 3}
            and organization_id
            in TestCreateMultipleVerificationsForUser.get_organization_ids_with_e9y_already_claimed()
        ):
            return False
        return True

    @staticmethod
    def assert_verification_data(
        verification_data_list,
        verifications_created,
        verified_at=None,
        verification_session=None,
    ):
        assert len(verification_data_list) == len(verifications_created)
        for verification_data, verification_created in zip(
            verification_data_list, verifications_created
        ):
            assert (
                verification_data.verification_id
                == verification_created.verification_id
            ), f"Verification ID mismatch: {verification_data.verification_id} != {verification_created.verification_id}"
            assert (
                verification_data.organization_id
                == verification_created.organization_id
            ), f"Organization ID mismatch: {verification_data.organization_id} != {verification_created.organization_id}"
            assert (
                verification_data.unique_corp_id == verification_created.unique_corp_id
            ), f"Unique Corp ID mismatch: {verification_data.unique_corp_id} != {verification_created.unique_corp_id}"
            assert (
                verification_data.dependent_id == verification_created.dependent_id
            ), f"Dependent ID mismatch: {verification_data.dependent_id} != {verification_created.dependent_id}"
            assert (
                verification_data.email == verification_created.email
            ), f"Email mismatch: {verification_data.email} != {verification_created.email}"
            assert (
                verification_data.work_state == verification_created.work_state
            ), f"Work State mismatch: {verification_data.work_state} != {verification_created.work_state}"
            assert (
                verification_data.additional_fields
                == verification_created.additional_fields
            ), f"Additional fields mismatch: {verification_data.additional_fields} != {verification_created.additional_fields}"

            if not verified_at:
                # verified_at should be set with default value
                assert verification_created.verified_at
            else:
                assert verification_created.verified_at == verified_at

            if not verification_session:
                assert verification_created.verification_session is None
            else:
                assert verification_created.verification_session == verification_session

    async def test_create_multiple_verifications_for_user(
        self,
        svc,
        multiple_test_members_versioned_from_test_config,
        verification_repo,
        mock_verification_data,
    ):
        # Given
        user_id = 12345
        first_name = "Don"
        last_name = "Joe"
        date_of_birth = datetime.date(1990, 1, 1)

        svc.verifications = verification_repo
        verified_at = datetime.datetime.now(tz=datetime.timezone.utc)

        # Create a list of VerificationData objects
        verification_data_list = [
            mock_verification_data(
                eligibility_member_id=t.id,
                organization_id=t.organization_id,
                unique_corp_id=t.unique_corp_id,
                dependent_id=t.dependent_id,
                email=t.email,
                work_state=t.work_state,
                additional_fields={},
            )
            for t in multiple_test_members_versioned_from_test_config
        ]

        # When
        verifications_created = await svc.create_multiple_verifications_for_user(
            user_id=user_id,
            verification_data_list=verification_data_list,
            verification_type="PRIMARY",
            date_of_birth=date_of_birth,
            first_name=first_name,
            last_name=last_name,
            verified_at=verified_at.isoformat(),
        )

        # Then
        self.assert_verification_data(
            verification_data_list,
            verifications_created,
            verified_at,
        )

    @staticmethod
    async def test_create_multiple_verifications_for_user_only_create_verification_when_e9y_record_not_already_claimed(
        svc,
        test_config,
        verification_repo,
        mock_verification_data,
        multiple_test_members_versioned_from_test_config,
    ):
        # Given
        user_id = 12345
        first_name = "Don"
        last_name = "Joe"
        date_of_birth = datetime.date(1990, 1, 1)

        svc.verifications = verification_repo
        verified_at = datetime.datetime.now(tz=datetime.timezone.utc)

        # Create a list of VerificationData objects
        verification_data_list = [
            mock_verification_data(
                eligibility_member_id=t.id,
                organization_id=t.organization_id,
                unique_corp_id=t.unique_corp_id,
                dependent_id=t.dependent_id,
                email=t.email,
                work_state=t.work_state,
                additional_fields={},
            )
            for t in multiple_test_members_versioned_from_test_config
        ]

        # When/Then
        with mock.patch(
            "app.eligibility.service.EligibilityService.verify_eligibility_record_usable",
            side_effect=TestCreateMultipleVerificationsForUser.eligibility_record_usable_side_effect,
        ):
            verifications_created = await svc.create_multiple_verifications_for_user(
                user_id=user_id,
                verification_data_list=verification_data_list,
                verification_type="PRIMARY",
                date_of_birth=date_of_birth,
                first_name=first_name,
                last_name=last_name,
                verified_at=verified_at.isoformat(),
            )
            verification_organization_ids = [
                v.organization_id for v in verifications_created
            ]
            assert (
                TestCreateMultipleVerificationsForUser.get_organization_ids_with_e9y_already_claimed()
                not in verification_organization_ids
            )

    @staticmethod
    async def test_create_multiple_verifications_for_user_all_e9y_records_already_claimed(
        svc,
        test_config,
        mock_verification_data,
        multiple_test_members_versioned_from_test_config,
    ):
        # Given
        user_id = 12345
        date_of_birth = datetime.date(1990, 1, 1)

        # Create a list of VerificationData objects
        verification_data_list = [
            mock_verification_data(
                eligibility_member_id=t.id,
                organization_id=t.organization_id,
                unique_corp_id=t.unique_corp_id,
                dependent_id=t.dependent_id,
                email=t.email,
                work_state=t.work_state,
                additional_fields={},
            )
            for t in multiple_test_members_versioned_from_test_config
        ]

        # When/Then
        with mock.patch(
            "app.eligibility.service.EligibilityService.verify_eligibility_record_usable",
            return_value=False,
        ):
            with pytest.raises(RecordAlreadyClaimedError) as e:
                await svc.create_multiple_verifications_for_user(
                    user_id=user_id,
                    verification_data_list=verification_data_list,
                    verification_type="PRIMARY",
                    date_of_birth=date_of_birth,
                )
            assert (
                str(e.value)
                == "Error persisting verification records - e9y records already claimed"
            )

    async def test_create_multiple_verifications_for_user_null_dates(
        self,
        svc,
        test_config,
        mock_verification_data,
        verification_repo,
        multiple_test_members_versioned_from_test_config,
    ):
        # Given
        user_id = 12345
        first_name = "Don"
        last_name = "Joe"
        date_of_birth = datetime.date(1990, 1, 1)

        svc.verifications = verification_repo

        # Create a list of VerificationData objects
        verification_data_list = [
            mock_verification_data(
                eligibility_member_id=t.id,
                organization_id=t.organization_id,
                unique_corp_id=t.unique_corp_id,
                dependent_id=t.dependent_id,
                email=t.email,
                work_state=t.work_state,
                additional_fields={},
            )
            for t in multiple_test_members_versioned_from_test_config
        ]

        # When
        verifications_created = await svc.create_multiple_verifications_for_user(
            user_id=user_id,
            verification_data_list=verification_data_list,
            verification_type="PRIMARY",
            date_of_birth=date_of_birth,
            first_name=first_name,
            last_name=last_name,
            verified_at=None,
        )

        # Then
        self.assert_verification_data(
            verification_data_list,
            verifications_created,
            verified_at=None,
        )

    @staticmethod
    async def test_create_multiple_verifications_for_user_verification_creation_error(
        svc,
        test_config,
        mock_verification_data,
        verification_repo,
        multiple_test_members_versioned_from_test_config,
    ):
        # Given
        user_id = 12345
        date_of_birth = datetime.date(1990, 1, 1)

        svc.verifications = verification_repo

        # Create a list of VerificationData objects
        verification_data_list = [
            mock_verification_data(
                eligibility_member_id=t.id,
                organization_id=9999,  # org_id doesn't exist
                unique_corp_id=t.unique_corp_id,
                dependent_id=t.dependent_id,
                email=t.email,
                work_state=t.work_state,
                additional_fields={},
            )
            for t in multiple_test_members_versioned_from_test_config
        ]

        # When/Then
        with pytest.raises(CreateVerificationError) as e:
            await svc.create_multiple_verifications_for_user(
                user_id=user_id,
                verification_data_list=verification_data_list,
                verification_type="PRIMARY",
                date_of_birth=date_of_birth,
            )
        assert str(e.value) == "Error persisting verification records"
        # verify no verification created
        verifications = (
            await svc.verifications.get_all_eligibility_verification_records_for_user(
                user_id=user_id
            )
        )
        assert not verifications

    @staticmethod
    async def test_create_multiple_verifications_for_user_verification_attempt_creation_error(
        svc,
        test_config,
        mock_verification_data,
        verification_repo,
        multiple_test_members_versioned_from_test_config,
    ):
        # Given
        user_id = 12345
        date_of_birth = datetime.date(1990, 1, 1)

        svc.verifications = verification_repo

        # Create a list of VerificationData objects
        verification_data_list = [
            mock_verification_data(
                eligibility_member_id=t.id,
                organization_id=t.organization_id,
                unique_corp_id=t.unique_corp_id,
                dependent_id=t.dependent_id,
                email=t.email,
                work_state=t.work_state,
                additional_fields={},
            )
            for t in multiple_test_members_versioned_from_test_config
        ]

        # When/Then
        with mock.patch(
            "verification.repository.verification.VerificationRepository.create_multiple_verification_attempts",
            side_effect=ValueError,
        ):
            # When/Then
            with pytest.raises(CreateVerificationError) as e:
                await svc.create_multiple_verifications_for_user(
                    user_id=user_id,
                    verification_data_list=verification_data_list,
                    verification_type="PRIMARY",
                    date_of_birth=date_of_birth,
                )
            assert str(e.value) == "Error persisting verification attempt records"

        # verify no verification created
        verifications = (
            await svc.verifications.get_all_eligibility_verification_records_for_user(
                user_id=user_id
            )
        )
        assert not verifications

    async def test_create_multiple_verifications_for_user_handle_value_error_creating_member_verification_records(
        self,
        svc,
        test_config,
        mock_verification_data,
        verification_repo,
        multiple_test_members_versioned_from_test_config,
    ):
        # Given
        user_id = 12345
        first_name = "Don"
        last_name = "Joe"
        date_of_birth = datetime.date(1990, 1, 1)
        svc.verifications = verification_repo

        # Create a list of VerificationData objects
        verification_data_list = [
            mock_verification_data(
                eligibility_member_id=None,
                organization_id=t.organization_id,
                unique_corp_id=t.unique_corp_id,
                dependent_id=t.dependent_id,
                email=t.email,
                work_state=t.work_state,
                additional_fields={},
            )
            for t in multiple_test_members_versioned_from_test_config
        ]

        verifications_created = await svc.create_multiple_verifications_for_user(
            user_id=user_id,
            verification_data_list=verification_data_list,
            verification_type="PRIMARY",
            date_of_birth=date_of_birth,
            first_name=first_name,
            last_name=last_name,
            verified_at=None,
        )

        # Then
        self.assert_verification_data(
            verification_data_list,
            verifications_created,
            verified_at=None,
        )

    async def test_create_multiple_verifications_for_user_skip_creating_member_verification_records(
        self,
        svc,
        test_config,
        mock_verification_data,
        verification_repo,
        multiple_test_members_versioned_from_test_config,
    ):
        # Given
        user_id = 12345
        first_name = "Don"
        last_name = "Joe"
        date_of_birth = datetime.date(1990, 1, 1)
        svc.verifications = verification_repo

        # Create a list of VerificationData objects
        verification_data_list = [
            mock_verification_data(
                eligibility_member_id=0,
                organization_id=t.organization_id,
                unique_corp_id=t.unique_corp_id,
                dependent_id=t.dependent_id,
                email=t.email,
                work_state=t.work_state,
                additional_fields={},
            )
            for t in multiple_test_members_versioned_from_test_config
        ]

        verifications_created = await svc.create_multiple_verifications_for_user(
            user_id=user_id,
            verification_data_list=verification_data_list,
            verification_type="CLIENT_SPECIFIC",
            date_of_birth=date_of_birth,
            first_name=first_name,
            last_name=last_name,
            verified_at=None,
        )

        # Then
        self.assert_verification_data(
            verification_data_list,
            verifications_created,
            verified_at=None,
        )

        # member_id is None - we shouldn't create member verifications
        with mock.patch(
            "app.eligibility.service.EligibilityService._create_member_verifications"
        ) as mock_create_member_verifications:
            mock_create_member_verifications.assert_not_called()

    async def test_create_multiple_verifications_for_user_member_verification_creation_error(
        self,
        svc,
        test_config,
        mock_verification_data,
        verification_repo,
        multiple_test_members_versioned_from_test_config,
    ):
        # Given
        user_id = 12345
        date_of_birth = datetime.date(1990, 1, 1)

        svc.verifications = verification_repo

        # Create a list of VerificationData objects
        verification_data_list = [
            mock_verification_data(
                eligibility_member_id=t.id,
                organization_id=t.organization_id,
                unique_corp_id=t.unique_corp_id,
                dependent_id=t.dependent_id,
                email=t.email,
                work_state=t.work_state,
                additional_fields={},
            )
            for t in multiple_test_members_versioned_from_test_config
        ]

        # When/Then
        with mock.patch(
            "verification.repository.verification.VerificationRepository.create_multiple_member_verifications",
            side_effect=Exception,
        ):
            # When/Then
            with pytest.raises(CreateVerificationError) as e:
                await svc.create_multiple_verifications_for_user(
                    user_id=user_id,
                    verification_data_list=verification_data_list,
                    verification_type="PRIMARY",
                    date_of_birth=date_of_birth,
                )
            assert str(e.value) == "Error persisting member_verification records"

        # transaction should roll back. verify no verification created for user
        verifications = (
            await svc.verifications.get_all_eligibility_verification_records_for_user(
                user_id=user_id
            )
        )
        assert not verifications

    async def test_create_multiple_verifications_for_user_with_verification_session(
        self,
        svc,
        multiple_test_members_versioned_from_test_config,
        verification_repo,
        mock_verification_data,
    ):
        # Given
        user_id = 12345
        first_name = "Don"
        last_name = "Joe"
        date_of_birth = datetime.date(1990, 1, 1)
        verification_session = "12345678-1234-5678-1234-567812345678"

        svc.verifications = verification_repo
        verified_at = datetime.datetime.now(tz=datetime.timezone.utc)

        # Create a list of VerificationData objects
        verification_data_list = [
            mock_verification_data(
                eligibility_member_id=t.id,
                organization_id=t.organization_id,
                unique_corp_id=t.unique_corp_id,
                dependent_id=t.dependent_id,
                email=t.email,
                work_state=t.work_state,
                additional_fields={},
                verification_session=verification_session,
            )
            for t in multiple_test_members_versioned_from_test_config
        ]

        # When
        verifications_created = await svc.create_multiple_verifications_for_user(
            user_id=user_id,
            verification_data_list=verification_data_list,
            verification_type="PRIMARY",
            date_of_birth=date_of_birth,
            first_name=first_name,
            last_name=last_name,
            verified_at=verified_at.isoformat(),
            verification_session=verification_session,
        )

        # Then
        self.assert_verification_data(
            verification_data_list,
            verifications_created,
            verified_at,
            verification_session,
        )

    @staticmethod
    async def test_create_multiple_verifications_write_disabled(
        svc,
        test_config,
        mock_verification_data,
        verification_repo,
        multiple_test_members_versioned_from_test_config,
    ):
        # Given
        user_id = 12345
        date_of_birth = datetime.date(1990, 1, 1)

        svc.verifications = verification_repo

        # Create a list of VerificationData objects
        verification_data_list = [
            mock_verification_data(
                eligibility_member_id=t.id,
                organization_id=t.organization_id,
                unique_corp_id=t.unique_corp_id,
                dependent_id=t.dependent_id,
                email=t.email,
                work_state=t.work_state,
                additional_fields={},
            )
            for t in multiple_test_members_versioned_from_test_config
        ]

        with mock.patch(
            "app.utils.feature_flag.is_write_disabled",
            return_value=True,
        ), pytest.raises(
            errors.CreateVerificationError,
            match="Creation is disabled due to feature flag",
        ):
            await svc.create_multiple_verifications_for_user(
                user_id=user_id,
                verification_data_list=verification_data_list,
                verification_type="PRIMARY",
                date_of_birth=date_of_birth,
            )


class TestCreateFailedVerification:
    @staticmethod
    async def test_create_failed_verification(
        svc, test_member_versioned, verification_repo
    ):
        # Given
        svc.verifications = verification_repo
        additional_fields = {"employee": True, "mavenID": "1234foo"}

        # When
        verification_attempt = await svc.create_failed_verification(
            user_id=1234,
            organization_id=test_member_versioned.organization_id,
            verification_type="PRIMARY",
            date_of_birth=test_member_versioned.date_of_birth,
            unique_corp_id=test_member_versioned.unique_corp_id,
            eligibility_member_id=test_member_versioned.id,
            verified_at=test_member_versioned.date_of_birth,
            additional_fields=additional_fields,
        )
        # Then
        assert verification_attempt.date_of_birth == test_member_versioned.date_of_birth
        assert (
            verification_attempt.unique_corp_id == test_member_versioned.unique_corp_id
        )
        # The DB will add timezone info to our datetime- we don't want to check against that
        assert (
            verification_attempt.verified_at.date()
            == test_member_versioned.date_of_birth
        )
        assert verification_attempt.additional_fields == additional_fields

    @staticmethod
    async def test_create_failed_verification_without_member_ids(
        svc, test_member_versioned, verification_repo
    ):
        # Given
        svc.verifications = verification_repo
        additional_fields = {"employee": True, "mavenID": "1234foo"}

        # When
        verification_attempt = await svc.create_failed_verification(
            user_id=1234,
            organization_id=test_member_versioned.organization_id,
            verification_type="PRIMARY",
            date_of_birth=test_member_versioned.date_of_birth,
            unique_corp_id=test_member_versioned.unique_corp_id,
            verified_at=test_member_versioned.date_of_birth,
            additional_fields=additional_fields,
        )
        # Then
        assert verification_attempt.eligibility_member_id is None
        assert verification_attempt.eligibility_member_2_id is None

    @staticmethod
    async def test_create_failed_verification_no_org(
        svc, test_member_versioned, verification_repo
    ):
        # Given
        svc.verifications = verification_repo
        additional_fields = {"employee": True, "mavenID": "1234foo"}

        # When
        verification_attempt = await svc.create_failed_verification(
            user_id=1234,
            organization_id=None,
            verification_type="PRIMARY",
            date_of_birth=test_member_versioned.date_of_birth,
            unique_corp_id=test_member_versioned.unique_corp_id,
            eligibility_member_id=test_member_versioned.id,
            verified_at=test_member_versioned.date_of_birth,
            additional_fields=additional_fields,
        )
        # Then
        assert verification_attempt.date_of_birth == test_member_versioned.date_of_birth
        assert (
            verification_attempt.unique_corp_id == test_member_versioned.unique_corp_id
        )
        # The DB will add timezone info to our datetime- we don't want to check against that
        assert (
            verification_attempt.verified_at.date()
            == test_member_versioned.date_of_birth
        )
        assert verification_attempt.additional_fields == additional_fields

    @staticmethod
    async def test_create_failed_verification_no_dates(
        svc, test_member_versioned, verification_repo
    ):
        # Given
        svc.verifications = verification_repo

        # When
        verification_attempt = await svc.create_failed_verification(
            user_id=1234,
            organization_id=test_member_versioned.organization_id,
            verification_type="PRIMARY",
            date_of_birth=None,
            unique_corp_id=test_member_versioned.unique_corp_id,
            eligibility_member_id=test_member_versioned.id,
            verified_at=None,
        )
        # Then
        assert verification_attempt.date_of_birth is None
        assert verification_attempt.date_of_birth is None
        assert (
            verification_attempt.unique_corp_id == test_member_versioned.unique_corp_id
        )

    @staticmethod
    async def test_create_failed_verification_v2(
        svc, test_member_versioned, verification_repo
    ):
        # Given
        svc.verifications = verification_repo

        # When
        with mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_write",
            return_value=True,
        ):
            verification_attempt = await svc.create_failed_verification(
                user_id=1234,
                organization_id=test_member_versioned.organization_id,
                verification_type="PRIMARY",
                date_of_birth=test_member_versioned.date_of_birth,
                unique_corp_id=test_member_versioned.unique_corp_id,
                eligibility_member_id=test_member_versioned.id,
                verified_at=test_member_versioned.date_of_birth,
            )

        # Then
        assert verification_attempt.verification_type == "PRIMARY"

    @staticmethod
    async def test_create_failed_verification_verification_attempt_creation_error(svc):
        # Given
        # When/Then
        with pytest.raises(CreateVerificationError):
            await svc.create_failed_verification(
                user_id=1234,
                organization_id=12345,  # pass in a fake orgID to cause error
                verification_type="PRIMARY",
                date_of_birth=datetime.date(2020, 1, 1),
                unique_corp_id="maven_foo",
            )

    @staticmethod
    async def test_create_failed_verification_member_verification_creation_error(
        svc, test_config, test_member_versioned
    ):
        # Given
        # When/Then
        with mock.patch(
            "verification.repository.verification.VerificationRepository.create_member_verification",
            side_effect=ValueError,
        ):
            with pytest.raises(CreateVerificationError):
                await svc.create_failed_verification(
                    user_id=1234,
                    organization_id=test_config.organization_id,
                    verification_type="PRIMARY",
                    date_of_birth=datetime.date(2020, 1, 1),
                    unique_corp_id="maven_foo",
                    eligibility_member_id=test_member_versioned.id,
                )

    @staticmethod
    async def test_create_failed_verifications_write_disabled(
        svc, test_config, test_member_versioned
    ):
        with mock.patch(
            "app.utils.feature_flag.is_write_disabled",
            return_value=True,
        ), pytest.raises(
            errors.CreateVerificationError,
            match="Creation is disabled due to feature flag",
        ):
            await svc.create_failed_verification(
                user_id=1234,
                organization_id=test_config.organization_id,
                verification_type="PRIMARY",
                date_of_birth=datetime.date(2020, 1, 1),
                unique_corp_id="maven_foo",
                eligibility_member_id=test_member_versioned.id,
            )


class TestValidateVerificationType:
    @staticmethod
    def test_validate_verification_type_failure(svc):
        # Given
        verification_type = "foobar"

        # When/Then
        with pytest.raises(service.ValidationError):
            svc._validate_verification_type(verification_type)

    @staticmethod
    @pytest.mark.parametrize(
        "verification_type", ("primary", "PRIMARY", "pRiMaRy", "secondary")
    )
    def test_validate_verification_type_pass(svc, verification_type):
        # Given
        verification_type = "primary"

        # When
        result = svc._validate_verification_type(verification_type)

        # Then
        assert result == verification_type.upper()


class TestGetWalletEnablementByUserID:
    @staticmethod
    async def test_get_wallet_enablement_by_user_id(
        svc,
        verification_repo,
        member_versioned_test_client,
        test_verification,
        test_member_verification,
        test_member_versioned,
    ):  # , test_member_versioned, test_member_verification):
        # Given
        svc.verifications = verification_repo
        svc.members_versioned = member_versioned_test_client
        user_id = test_verification.user_id

        with mock.patch(
            "verification.repository.verification.VerificationRepository.get_verification_key_for_user",
            return_value=db_model.VerificationKey(
                member_id=test_member_versioned.id,
                organization_id=test_member_versioned.organization_id,
            ),
        ):
            # When
            wallet_result = await svc.get_wallet_enablement_by_user_id(user_id=user_id)

            # Then
            assert wallet_result.unique_corp_id == test_member_versioned.unique_corp_id
            assert wallet_result.member_id == test_member_versioned.id

    @staticmethod
    async def test_get_wallet_enablement_by_user_id_no_member(
        svc,
        verification_repo,
        member_versioned_test_client,
        test_verification,
    ):  # , test_member_versioned, test_member_verification):
        # Given
        svc.verifications = verification_repo
        svc.members_versioned = member_versioned_test_client
        user_id = test_verification.user_id

        # When/Then
        with pytest.raises(GetMatchError):
            await svc.get_wallet_enablement_by_user_id(user_id=user_id)


class TestVerifyEligibilityRecordUsable:
    @staticmethod
    async def test_verify_eligibility_record_usable_record_unused(
        svc, verification_repo
    ):
        # Given
        svc.verifications = verification_repo

        # When
        eligible = await svc.verify_eligibility_record_usable(
            eligibility_member_id=1, organization_id=1
        )

        # Then
        assert eligible

    @staticmethod
    @pytest.mark.parametrize(
        [
            "employee_only",
            "medical_plan_only",
            "beneficiaries_enabled",
            "expected_result",
        ],
        [
            (True, True, True, False),  # employee_only
            (False, True, False, False),  # medical plan/no beneficiaries
            (False, True, True, True),  # medical plan w/ beneficiaries
            (False, False, False, True),  # non-medical plan w/o beneficiers
            (False, False, True, True),  # non-medical plan w/ beneficiaries
        ],
        ids=[
            "employee_only",
            "medical_plan_no_beneficiaries",
            "medical_plan_beneficiaries",
            "non_medical_plan_no_beneficiaries",
            "non_medical_plan_beneficiaries",
        ],
    )
    async def test_verify_eligibility_record_usable_record(
        svc,
        verification_repo,
        test_member_versioned,
        test_member_verification,
        member_2_test_client,
        verification_2_test_client,
        test_config,
        employee_only,
        medical_plan_only,
        beneficiaries_enabled,
        expected_result,
    ):
        # Given
        svc.verifications = verification_repo
        test_member_versioned.record["beneficiaries_enabled"] = beneficiaries_enabled
        test_config.medical_plan_only = medical_plan_only
        test_config.employee_only = employee_only

        # When
        with mock.patch(
            "db.clients.member_versioned_client.MembersVersioned.get",
            return_value=test_member_versioned,
        ):
            with mock.patch(
                "db.clients.configuration_client.Configurations.get",
                return_value=test_config,
            ):
                eligible = await svc.verify_eligibility_record_usable(
                    eligibility_member_id=test_member_versioned.id,
                    organization_id=test_config.organization_id,
                )

        # Then
        assert eligible == expected_result

        # Test 2.0
        test_member_2 = await member_2_test_client.persist(
            model=data_models.Member2Factory.create(
                id=1001,
                organization_id=test_config.organization_id,
            )
        )
        test_member_2.record["beneficiaries_enabled"] = beneficiaries_enabled
        test_verification_2 = await verification_2_test_client.persist(
            model=data_models.Verification2Factory.create(
                organization_id=test_config.organization_id,
                member_id=test_member_2.id,
                member_version=test_member_2.version,
            )
        )

        with mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_write",
            return_value=True,
        ), mock.patch(
            "db.clients.member_2_client.Member2Client.get",
            return_value=test_member_2,
        ), mock.patch(
            "db.clients.configuration_client.Configurations.get",
            return_value=test_config,
        ), mock.patch(
            "verification.repository.verification.VerificationRepository.get_verification_2_for_member",
            return_value=test_verification_2,
        ):
            eligible = await svc.verify_eligibility_record_usable(
                eligibility_member_id=test_member_2.id,
                organization_id=test_config.organization_id,
            )
            assert eligible == expected_result


class TestGetEligibleFeaturesForUser:
    @staticmethod
    @pytest.mark.usefixtures("mapped_sub_populations")
    @pytest.mark.parametrize(
        argnames="work_state,custom_attributes,feature_type,expected_feature_list",
        argvalues=[
            (
                "NY",
                '{"employment_status": "Part", "group_number": "1"}',
                pop_model.FeatureTypes.WALLET_FEATURE,
                [2102],
            ),
            (
                "ZZ",
                '{"employment_status": "Full", "group_number": "2"}',
                pop_model.FeatureTypes.TRACK_FEATURE,
                [1202, 1204, 1206, 1208, 1210],
            ),
            (
                "ZZ",
                '{"employment_status": "Part", "group_number": "3"}',
                pop_model.FeatureTypes.TRACK_FEATURE,
                [],
            ),
        ],
        ids=["NY-Part-3-Wallet", "ZZ-Full-2-Track", "ZZ-Part-3-Track"],
    )
    async def test_get_eligible_features_for_user(
        # test params
        work_state: str,
        custom_attributes: str,
        feature_type: int,
        # expected return
        expected_feature_list: List[int],
        # fixtures used in the test
        svc: service.EligibilityService,
        test_config: db_model.Configuration,
        # replacement clients & repo for the service
        population_test_client: population_client.Populations,
        sub_population_test_client: sub_population_client.SubPopulations,
        verification_repo: repository.VerificationRepository,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        # extra clients needed to write necessary data for test
        verification_test_client: verification_client.Verifications,
        member_verification_test_client: member_verification_client.MemberVerifications,
        member_2_test_client: member_2_client.Member2Client,
        verification_2_test_client: verification_2_client.Verification2Client,
    ):
        # Given
        svc.populations = population_test_client
        svc.sub_populations = sub_population_test_client
        svc.verifications = verification_repo
        svc.members_versioned = member_versioned_test_client
        svc.members_2 = member_2_test_client

        user_id = 1

        test_member_versioned = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                work_state=work_state,
                custom_attributes=custom_attributes,
            )
        )
        test_member_2 = await member_2_test_client.persist(
            model=data_models.Member2Factory.create(
                id=test_member_versioned.id,
                organization_id=test_config.organization_id,
                work_state=work_state,
                custom_attributes=custom_attributes,
            )
        )
        test_verification_2 = await verification_2_test_client.persist(
            model=data_models.Verification2Factory.create(
                organization_id=test_config.organization_id,
                member_id=test_member_2.id,
                user_id=user_id,
            )
        )
        test_verification = await verification_test_client.persist(
            model=data_models.VerificationFactory.create(
                user_id=user_id,
                organization_id=test_config.organization_id,
                verification_2_id=test_verification_2.id,
            )
        )
        await member_verification_test_client.persist(
            model=data_models.MemberVerificationFactory.create(
                member_id=test_member_versioned.id, verification_id=test_verification.id
            )
        )

        # When
        with mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
            return_value=False,
        ):
            returned_feature_list = await svc.get_eligible_features_for_user(
                user_id=test_verification.user_id,
                feature_type=int(feature_type),
            )

            # Then
            assert returned_feature_list == expected_feature_list

        with mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
            return_value=True,
        ):
            returned_feature_list = await svc.get_eligible_features_for_user(
                user_id=test_verification.user_id,
                feature_type=int(feature_type),
            )

            # Then
            assert returned_feature_list == expected_feature_list

    @staticmethod
    async def test_get_eligible_features_for_user_no_population_returns_none(
        # fixtures used in the test
        svc: service.EligibilityService,
        test_config: db_model.Configuration,
        # replacement clients & repo for the service
        population_test_client: population_client.Populations,
        sub_population_test_client: sub_population_client.SubPopulations,
        verification_repo: repository.VerificationRepository,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        # extra clients needed to write necessary data for test
        verification_test_client: verification_client.Verifications,
        member_verification_test_client: member_verification_client.MemberVerifications,
    ):
        # Given
        svc.populations = population_test_client
        svc.sub_populations = sub_population_test_client
        svc.verifications = verification_repo
        svc.members_versioned = member_versioned_test_client

        test_member_versioned = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                work_state="NY",
                custom_attributes='{"employment_status": "Part", "group_number": "1"}',
            )
        )
        test_verification = await verification_test_client.persist(
            model=data_models.VerificationFactory.create(
                organization_id=test_config.organization_id,
            )
        )
        await member_verification_test_client.persist(
            model=data_models.MemberVerificationFactory.create(
                member_id=test_member_versioned.id, verification_id=test_verification.id
            )
        )

        # When
        returned_feature_list = await svc.get_eligible_features_for_user(
            user_id=test_verification.user_id,
            feature_type=int(pop_model.FeatureTypes.TRACK_FEATURE),
        )

        # Then
        assert returned_feature_list is None

    @staticmethod
    @pytest.mark.usefixtures("mapped_sub_populations")
    async def test_get_eligible_features_for_user_inactive_population_returns_none(
        svc: service.EligibilityService,
        verified_member_zz_full_2: (db_model.MemberVersioned, db_model.Verification),
        mapped_population: pop_model.Population,
        population_test_client: population_client.Populations,
    ):
        # Given
        test_verification = verified_member_zz_full_2[1]

        # Deactivate the population
        await population_test_client.deactivate_population(
            population_id=mapped_population.id
        )

        # When
        returned_feature_list = await svc.get_eligible_features_for_user(
            user_id=test_verification.user_id,
            feature_type=int(pop_model.FeatureTypes.TRACK_FEATURE),
        )

        # Then
        assert returned_feature_list is None

    @staticmethod
    @pytest.mark.usefixtures("mapped_sub_populations")
    async def test_get_eligible_features_for_user_invalid_value_returns_empty_list(
        # fixtures used in the test
        svc: service.EligibilityService,
        test_config: db_model.Configuration,
        # replacement clients & repo for the service
        population_test_client: population_client.Populations,
        sub_population_test_client: sub_population_client.SubPopulations,
        verification_repo: repository.VerificationRepository,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        # extra clients needed to write necessary data for test
        verification_test_client: verification_client.Verifications,
        member_verification_test_client: member_verification_client.MemberVerifications,
    ):
        # Given
        svc.populations = population_test_client
        svc.sub_populations = sub_population_test_client
        svc.verifications = verification_repo
        svc.members_versioned = member_versioned_test_client

        test_member_versioned = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                work_state="YN",
                custom_attributes='{"employment_status": "Unemployed", "group_number": "-1"}',
            )
        )
        test_verification = await verification_test_client.persist(
            model=data_models.VerificationFactory.create(
                organization_id=test_config.organization_id,
            )
        )
        await member_verification_test_client.persist(
            model=data_models.MemberVerificationFactory.create(
                member_id=test_member_versioned.id, verification_id=test_verification.id
            )
        )

        # When
        returned_feature_list = await svc.get_eligible_features_for_user(
            user_id=test_verification.user_id,
            feature_type=int(pop_model.FeatureTypes.TRACK_FEATURE),
        )

        # Then
        assert len(returned_feature_list) == 0


class TestGetEligibleFeaturesForUserAndOrg:
    @staticmethod
    @pytest.mark.usefixtures("mapped_sub_populations")
    @pytest.mark.parametrize(
        argnames="work_state,custom_attributes,feature_type,expected_feature_list",
        argvalues=[
            (
                "NY",
                '{"employment_status": "Part", "group_number": "1"}',
                pop_model.FeatureTypes.WALLET_FEATURE,
                [2102],
            ),
            (
                "ZZ",
                '{"employment_status": "Full", "group_number": "2"}',
                pop_model.FeatureTypes.TRACK_FEATURE,
                [1202, 1204, 1206, 1208, 1210],
            ),
            (
                "ZZ",
                '{"employment_status": "Part", "group_number": "3"}',
                pop_model.FeatureTypes.TRACK_FEATURE,
                [],
            ),
        ],
        ids=["NY-Part-3-Wallet", "ZZ-Full-2-Track", "ZZ-Part-3-Track"],
    )
    async def test_get_eligible_features_for_user_and_org(
        # test params
        work_state: str,
        custom_attributes: str,
        feature_type: int,
        # expected return
        expected_feature_list: List[int],
        # fixtures used in the test
        svc: service.EligibilityService,
        test_config: db_model.Configuration,
        multiple_test_config: db_model.Configuration,
        # replacement clients & repo for the service
        population_test_client: population_client.Populations,
        sub_population_test_client: sub_population_client.SubPopulations,
        verification_repo: repository.VerificationRepository,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        # extra clients needed to write necessary data for test
        verification_test_client: verification_client.Verifications,
        member_verification_test_client: member_verification_client.MemberVerifications,
    ):
        # Given
        svc.populations = population_test_client
        svc.sub_populations = sub_population_test_client
        svc.verifications = verification_repo
        svc.members_versioned = member_versioned_test_client

        test_member_versioned = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                work_state=work_state,
                custom_attributes=custom_attributes,
            )
        )
        test_verification = await verification_test_client.persist(
            model=data_models.VerificationFactory.create(
                organization_id=test_config.organization_id,
            )
        )
        await member_verification_test_client.persist(
            model=data_models.MemberVerificationFactory.create(
                member_id=test_member_versioned.id, verification_id=test_verification.id
            )
        )

        test_member_versioned_2 = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=multiple_test_config[2].organization_id,
                work_state="NY",
                custom_attributes="{}",
            )
        )
        test_verification_2 = await verification_test_client.persist(
            model=data_models.VerificationFactory.create(
                user_id=test_verification.user_id,
                organization_id=multiple_test_config[2].organization_id,
                verified_at=datetime.date.today() + datetime.timedelta(days=3),
            )
        )
        await member_verification_test_client.persist(
            model=data_models.MemberVerificationFactory.create(
                member_id=test_member_versioned_2.id,
                verification_id=test_verification_2.id,
            )
        )

        # When
        returned_feature_list = await svc.get_eligible_features_for_user_and_org(
            user_id=test_verification.user_id,
            organization_id=test_verification.organization_id,
            feature_type=int(feature_type),
        )

        # Then
        assert returned_feature_list == expected_feature_list

    @staticmethod
    async def test_get_eligible_features_for_user_and_org_no_population_returns_none(
        # fixtures used in the test
        svc: service.EligibilityService,
        verified_member_zz_full_3: (db_model.MemberVersioned, db_model.Verification),
        # replacement clients & repo for the service
        population_test_client: population_client.Populations,
        sub_population_test_client: sub_population_client.SubPopulations,
        verification_repo: repository.VerificationRepository,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        # extra clients needed to write necessary data for test
        verification_test_client: verification_client.Verifications,
        member_verification_test_client: member_verification_client.MemberVerifications,
    ):
        # Given
        svc.populations = population_test_client
        svc.sub_populations = sub_population_test_client
        svc.verifications = verification_repo
        svc.members_versioned = member_versioned_test_client

        org_id = verified_member_zz_full_3[1][0].organization_id
        test_member_versioned = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=org_id,
                work_state="NY",
                custom_attributes='{"employment_status": "Part", "group_number": "1"}',
            )
        )
        test_verification = await verification_test_client.persist(
            model=data_models.VerificationFactory.create(
                user_id=verified_member_zz_full_3[1][0].user_id,
                organization_id=org_id,
            )
        )
        await member_verification_test_client.persist(
            model=data_models.MemberVerificationFactory.create(
                member_id=test_member_versioned.id, verification_id=test_verification.id
            )
        )

        # When
        returned_feature_list = await svc.get_eligible_features_for_user_and_org(
            user_id=test_verification.user_id,
            organization_id=test_verification.organization_id,
            feature_type=int(pop_model.FeatureTypes.TRACK_FEATURE),
        )

        # Then
        assert returned_feature_list is None

    @staticmethod
    @pytest.mark.usefixtures("mapped_sub_populations")
    async def test_get_eligible_features_for_user_inactive_population_returns_none(
        svc: service.EligibilityService,
        verified_member_zz_full_3: (db_model.MemberVersioned, db_model.Verification),
        mapped_population: pop_model.Population,
        population_test_client: population_client.Populations,
    ):
        # Given
        test_verification = verified_member_zz_full_3[1][0]

        # Deactivate the population
        await population_test_client.deactivate_population(
            population_id=mapped_population.id
        )

        # When
        returned_feature_list = await svc.get_eligible_features_for_user_and_org(
            user_id=test_verification.user_id,
            organization_id=test_verification.organization_id,
            feature_type=int(pop_model.FeatureTypes.TRACK_FEATURE),
        )

        # Then
        assert returned_feature_list is None

    @staticmethod
    @pytest.mark.usefixtures("mapped_sub_populations")
    async def test_get_eligible_features_for_user_and_org_invalid_value_returns_empty_list(
        # fixtures used in the test
        svc: service.EligibilityService,
        test_config: db_model.Configuration,
        # replacement clients & repo for the service
        population_test_client: population_client.Populations,
        sub_population_test_client: sub_population_client.SubPopulations,
        verification_repo: repository.VerificationRepository,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        # extra clients needed to write necessary data for test
        verification_test_client: verification_client.Verifications,
        member_verification_test_client: member_verification_client.MemberVerifications,
    ):
        # Given
        svc.populations = population_test_client
        svc.sub_populations = sub_population_test_client
        svc.verifications = verification_repo
        svc.members_versioned = member_versioned_test_client

        test_member_versioned = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                work_state="YN",
                custom_attributes='{"employment_status": "Unemployed", "group_number": "-1"}',
            )
        )
        test_verification = await verification_test_client.persist(
            model=data_models.VerificationFactory.create(
                organization_id=test_config.organization_id,
            )
        )
        await member_verification_test_client.persist(
            model=data_models.MemberVerificationFactory.create(
                member_id=test_member_versioned.id, verification_id=test_verification.id
            )
        )

        # When
        returned_feature_list = await svc.get_eligible_features_for_user_and_org(
            user_id=test_verification.user_id,
            organization_id=test_verification.organization_id,
            feature_type=int(pop_model.FeatureTypes.TRACK_FEATURE),
        )

        # Then
        assert len(returned_feature_list) == 0


class TestGetEligibleFeaturesBySubPopulationId:
    @staticmethod
    async def test_get_eligible_features_by_sub_population_id_returns_sub_pop_features(
        svc: service.EligibilityService,
        verified_member_zz_full_2: (db_model.MemberVersioned, db_model.Verification),
        sub_pop_zz_full_2_track_info: (int, List[int]),
    ):
        # Given
        target_sub_pop_id = sub_pop_zz_full_2_track_info[0]
        expected_feature_list = sub_pop_zz_full_2_track_info[1]

        # When
        returned_feature_list = await svc.get_eligible_features_by_sub_population_id(
            sub_population_id=target_sub_pop_id,
            feature_type=int(pop_model.FeatureTypes.TRACK_FEATURE),
        )

        # Then
        assert returned_feature_list == expected_feature_list

    @staticmethod
    async def test_get_eligible_features_by_sub_population_id_deactivated_population_returns_sub_pop_features(
        svc: service.EligibilityService,
        verified_member_zz_full_2: (db_model.MemberVersioned, db_model.Verification),
        sub_pop_zz_full_2_track_info: (int, List[int]),
    ):
        # Given
        target_sub_pop_id = sub_pop_zz_full_2_track_info[0]
        expected_feature_list = sub_pop_zz_full_2_track_info[1]

        # When
        returned_feature_list = await svc.get_eligible_features_by_sub_population_id(
            sub_population_id=target_sub_pop_id,
            feature_type=int(pop_model.FeatureTypes.TRACK_FEATURE),
        )

        # Then
        assert returned_feature_list == expected_feature_list


class TestGetSubPopulationIdForUser:
    @staticmethod
    async def test_get_sub_population_id_for_user(
        svc: service.EligibilityService,
        verified_member_zz_full_2: (db_model.MemberVersioned, db_model.Verification),
        mapped_sub_populations: List[pop_model.SubPopulation],
    ):
        # Given
        test_member = verified_member_zz_full_2[0]
        test_verification = verified_member_zz_full_2[1]

        expected_sub_pop_id = None
        for sub_pop in mapped_sub_populations:
            if sub_pop.feature_set_name == "fs_05":
                expected_sub_pop_id = sub_pop.id
                break
        assert expected_sub_pop_id is not None

        # When
        with mock.patch(
            "verification.repository.verification.VerificationRepository.get_verification_key_for_user",
            return_value=db_model.VerificationKey(
                member_id=test_member.id,
                organization_id=test_verification.organization_id,
            ),
        ), mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
            return_value=False,
        ):
            sub_population_id, is_active_pop = await svc.get_sub_population_id_for_user(
                user_id=test_verification.user_id,
            )

            # Then
            assert is_active_pop
            assert sub_population_id == expected_sub_pop_id

        with mock.patch(
            "verification.repository.verification.VerificationRepository.get_verification_key_for_user",
            return_value=db_model.VerificationKey(
                member_id=test_member.id,
                organization_id=test_verification.organization_id,
                member_2_id=test_member.id,
            ),
        ), mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
            return_value=True,
        ):
            sub_population_id, is_active_pop = await svc.get_sub_population_id_for_user(
                user_id=test_verification.user_id,
            )

            # Then
            assert is_active_pop
            assert sub_population_id == expected_sub_pop_id

        # Not found
        with mock.patch(
            "verification.repository.verification.VerificationRepository.get_verification_key_for_user",
            return_value=db_model.VerificationKey(
                member_id=test_member.id + 1,
                organization_id=test_verification.organization_id,
            ),
        ), mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
            return_value=True,
        ):
            sub_population_id, is_active_pop = await svc.get_sub_population_id_for_user(
                user_id=test_verification.user_id,
            )

            # Then
            assert is_active_pop
            assert sub_population_id is None

        # Fallback
        with mock.patch(
            "verification.repository.verification.VerificationRepository.get_verification_key_for_user",
            return_value=db_model.VerificationKey(
                member_id=test_member.id,
                organization_id=test_verification.organization_id,
                member_2_id=test_member.id + 100,
            ),
        ), mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
            return_value=True,
        ):
            sub_population_id, is_active_pop = await svc.get_sub_population_id_for_user(
                user_id=test_verification.user_id,
            )

            # Then
            assert is_active_pop
            assert sub_population_id == expected_sub_pop_id

    @staticmethod
    @pytest.mark.usefixtures("mapped_sub_populations")
    async def test_get_sub_population_id_for_user_unknown_user_returns_none(
        svc: service.EligibilityService,
    ):
        # Given
        # When
        sub_population_id, is_active_pop = await svc.get_sub_population_id_for_user(
            user_id=735773577357,
        )

        # Then
        assert not is_active_pop
        assert sub_population_id is None

    @staticmethod
    async def test_get_sub_population_id_for_user_no_population_returns_none(
        svc: service.EligibilityService,
        verified_member_zz_full_2: (db_model.MemberVersioned, db_model.Verification),
    ):
        # Given
        test_verification = verified_member_zz_full_2[1]

        # When
        sub_population_id, is_active_pop = await svc.get_sub_population_id_for_user(
            user_id=test_verification.user_id,
        )

        # Then
        assert not is_active_pop
        assert sub_population_id is None

    @staticmethod
    @pytest.mark.usefixtures("mapped_sub_populations")
    async def test_get_sub_population_id_for_user_not_in_lookup(
        svc: service.EligibilityService,
        test_config: db_model.Configuration,
        # replacement clients & repo for the service
        population_test_client: population_client.Populations,
        sub_population_test_client: sub_population_client.SubPopulations,
        verification_repo: repository.VerificationRepository,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        # extra clients needed to write necessary data for test
        verification_test_client: verification_client.Verifications,
        member_verification_test_client: member_verification_client.MemberVerifications,
    ):
        # Given
        svc.populations = population_test_client
        svc.sub_populations = sub_population_test_client
        svc.verifications = verification_repo
        svc.members_versioned = member_versioned_test_client

        await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                work_state="YN",
                custom_attributes='{"employment_status": "Full", "group_number": "2"}',
            )
        )
        test_verification = await verification_test_client.persist(
            model=data_models.VerificationFactory.create(
                organization_id=test_config.organization_id,
            )
        )

        # When
        sub_population_id, is_active_pop = await svc.get_sub_population_id_for_user(
            user_id=test_verification.user_id,
        )

        # Then
        assert is_active_pop
        assert sub_population_id is None


class TestGetSubPopulationIdForUserAndOrg:
    @staticmethod
    async def test_get_sub_population_id_for_user_and_org(
        svc: service.EligibilityService,
        verified_member_zz_full_3: (db_model.MemberVersioned, db_model.Verification),
        mapped_sub_populations: List[pop_model.SubPopulation],
    ):
        # Given
        test_member = verified_member_zz_full_3[0][0]
        test_verification = verified_member_zz_full_3[1][0]

        expected_sub_pop_id = None
        for sub_pop in mapped_sub_populations:
            if sub_pop.feature_set_name == "fs_05":
                expected_sub_pop_id = sub_pop.id
                break
        assert expected_sub_pop_id is not None

        # When
        with mock.patch(
            "verification.repository.verification.VerificationRepository.get_verification_key_2_for_user_and_org",
            return_value=db_model.VerificationKey(
                member_id=test_member.id,
                organization_id=test_verification.organization_id,
            ),
        ), mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
            return_value=False,
        ):
            (
                sub_population_id,
                is_active_pop,
            ) = await svc.get_sub_population_id_for_user_and_org(
                user_id=test_verification.user_id,
                organization_id=test_verification.organization_id,
            )

            # Then
            assert is_active_pop
            assert sub_population_id == expected_sub_pop_id

        with mock.patch(
            "verification.repository.verification.VerificationRepository.get_verification_key_2_for_user_and_org",
            return_value=db_model.VerificationKey(
                member_id=test_member.id,
                organization_id=test_verification.organization_id,
                member_2_id=test_member.id,
            ),
        ), mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
            return_value=True,
        ):
            (
                sub_population_id,
                is_active_pop,
            ) = await svc.get_sub_population_id_for_user_and_org(
                user_id=test_verification.user_id,
                organization_id=test_verification.organization_id,
            )

            # Then
            assert is_active_pop
            assert sub_population_id == expected_sub_pop_id

        # Not found
        with mock.patch(
            "verification.repository.verification.VerificationRepository.get_verification_key_2_for_user_and_org",
            return_value=db_model.VerificationKey(
                member_id=test_member.id + 100,
                member_2_id=test_member.id + 100,
                organization_id=test_verification.organization_id + 100,
            ),
        ), mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
            return_value=True,
        ), mock.patch(
            "verification.repository.verification.VerificationRepository.get_eligibility_member_id_for_user_and_org",
            return_value=None,
        ):
            (
                sub_population_id,
                is_active_pop,
            ) = await svc.get_sub_population_id_for_user_and_org(
                user_id=test_verification.user_id,
                organization_id=test_verification.organization_id,
            )

            # Then
            assert is_active_pop
            assert sub_population_id is None

        # Fallback
        with mock.patch(
            "verification.repository.verification.VerificationRepository.get_verification_key_2_for_user_and_org",
            return_value=db_model.VerificationKey(
                member_id=test_member.id + 100,
                organization_id=test_verification.organization_id + 100,
            ),
        ), mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
            return_value=True,
        ):
            (
                sub_population_id,
                is_active_pop,
            ) = await svc.get_sub_population_id_for_user_and_org(
                user_id=test_verification.user_id,
                organization_id=test_verification.organization_id,
            )

            # Then
            assert is_active_pop
            assert sub_population_id == expected_sub_pop_id

        # No verification key
        with mock.patch(
            "verification.repository.verification.VerificationRepository.get_verification_key_2_for_user_and_org",
            return_value=None,
        ), mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
            return_value=True,
        ), mock.patch(
            "verification.repository.verification.VerificationRepository.get_eligibility_member_id_for_user_and_org",
            return_value=None,
        ):
            (
                sub_population_id,
                is_active_pop,
            ) = await svc.get_sub_population_id_for_user_and_org(
                user_id=test_verification.user_id,
                organization_id=test_verification.organization_id,
            )

            # Then
            assert is_active_pop
            assert sub_population_id is None

    @staticmethod
    @pytest.mark.usefixtures("mapped_sub_populations")
    async def test_get_sub_population_id_for_user_and_org_unknown_user_returns_none(
        svc: service.EligibilityService,
    ):
        # Given
        # When
        (
            sub_population_id,
            is_active_pop,
        ) = await svc.get_sub_population_id_for_user_and_org(
            user_id=735773577357, organization_id=1
        )

        # Then
        assert not is_active_pop
        assert sub_population_id is None

    @staticmethod
    @pytest.mark.usefixtures("mapped_sub_populations")
    async def test_get_sub_population_id_for_user_and_org_unknown_org_returns_none(
        svc: service.EligibilityService,
        verified_member_zz_full_3: (db_model.MemberVersioned, db_model.Verification),
    ):
        # Given
        # When
        (
            sub_population_id,
            is_active_pop,
        ) = await svc.get_sub_population_id_for_user_and_org(
            user_id=verified_member_zz_full_3[1][0].user_id, organization_id=-100
        )

        # Then
        assert not is_active_pop
        assert sub_population_id is None

    @staticmethod
    async def test_get_sub_population_id_for_user_and_org_no_population_returns_none(
        svc: service.EligibilityService,
        verified_member_zz_full_3: (db_model.MemberVersioned, db_model.Verification),
    ):
        # Given
        test_verification = verified_member_zz_full_3[1][1]

        # When
        (
            sub_population_id,
            is_active_pop,
        ) = await svc.get_sub_population_id_for_user_and_org(
            user_id=test_verification.user_id,
            organization_id=test_verification.organization_id,
        )

        # Then
        assert not is_active_pop
        assert sub_population_id is None

    @staticmethod
    @pytest.mark.usefixtures("mapped_sub_populations")
    async def test_get_sub_population_id_for_user_and_org_not_in_lookup(
        svc: service.EligibilityService,
        test_config: db_model.Configuration,
        # replacement clients & repo for the service
        population_test_client: population_client.Populations,
        sub_population_test_client: sub_population_client.SubPopulations,
        verification_repo: repository.VerificationRepository,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        # extra clients needed to write necessary data for test
        verification_test_client: verification_client.Verifications,
        member_verification_test_client: member_verification_client.MemberVerifications,
    ):
        # Given
        svc.populations = population_test_client
        svc.sub_populations = sub_population_test_client
        svc.verifications = verification_repo
        svc.members_versioned = member_versioned_test_client

        await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                work_state="YN",
                custom_attributes='{"employment_status": "Full", "group_number": "2"}',
            )
        )
        test_verification = await verification_test_client.persist(
            model=data_models.VerificationFactory.create(
                organization_id=test_config.organization_id,
            )
        )

        # When
        (
            sub_population_id,
            is_active_pop,
        ) = await svc.get_sub_population_id_for_user_and_org(
            user_id=test_verification.user_id,
            organization_id=test_verification.organization_id,
        )

        # Then
        assert is_active_pop
        assert sub_population_id is None


class TestVerifyUser:
    @staticmethod
    async def test_alternative_eligibility_over_eligible_same_org_sort_by_updated_at(
        svc: service.EligibilityService,
        member_versioned_test_client,
        test_config: db_model.Configuration,
    ):
        # Given
        svc.members_versioned = member_versioned_test_client

        unique_corp_id = "foobar"
        search = dict(
            date_of_birth="1970-01-01",
            first_name="Princess",
            last_name="Zelda",
            work_state="Hyrule",
            unique_corp_id=unique_corp_id,
        )

        # Generate two valid records with the same org

        member_1 = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                unique_corp_id=unique_corp_id,
                date_of_birth=datetime.datetime.strptime(
                    "1970/01/01", "%Y/%m/%d"
                ).date(),
            )
        )
        member_2 = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                unique_corp_id=unique_corp_id,
                date_of_birth=member_1.date_of_birth,
            )
        )
        await member_versioned_test_client.set_updated_at(
            id=member_1.id,
            updated_at=datetime.datetime.strptime("2020/01/01", "%Y/%m/%d"),
        )
        await member_versioned_test_client.set_updated_at(
            id=member_2.id,
            updated_at=datetime.datetime.strptime("2020/01/02", "%Y/%m/%d"),
        )

        # When
        with mock.patch(
            "app.utils.eligibility_validation.is_cached_organization_active"
        ) as organization_active:
            organization_active.return_value = True
            found = await svc.check_alternate_eligibility(**search)

        # Then
        assert found.id == member_2.id

    @staticmethod
    async def test_alternative_eligibility_over_eligible_inactive_org(
        svc,
        test_config,
        test_inactive_config,
        configuration_test_client,
        member_versioned_test_client,
    ):
        # Given
        svc.members_versioned = member_versioned_test_client

        unique_corp_id = "foobar"
        search = dict(
            date_of_birth="1970-01-01",
            first_name="Princess",
            last_name="Zelda",
            work_state="Hyrule",
            unique_corp_id=unique_corp_id,
        )

        # Generate two records, with one belonging to inactive org
        member_1 = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                unique_corp_id=unique_corp_id,
                date_of_birth=datetime.datetime.strptime(
                    "1970/01/01", "%Y/%m/%d"
                ).date(),
            )
        )
        _ = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_inactive_config.organization_id,
                unique_corp_id=unique_corp_id,
                date_of_birth=member_1.date_of_birth,
            )
        )
        svc.configurations = configuration_test_client

        # When
        found = await svc.check_alternate_eligibility(**search)

        # Then
        assert found.id == member_1.id

    @staticmethod
    async def test_alternative_eligibility_over_eligible_all_inactive_org(
        svc,
        test_inactive_config,
        configuration_test_client,
        member_versioned_test_client,
    ):
        # Given
        svc.members_versioned = member_versioned_test_client

        unique_corp_id = "foobar"
        search = dict(
            date_of_birth="1970-01-01",
            first_name="Princess",
            last_name="Zelda",
            work_state="Hyrule",
            unique_corp_id=unique_corp_id,
        )

        # Generate two records, both in an inactive org
        member_1 = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_inactive_config.organization_id,
                unique_corp_id=unique_corp_id,
                date_of_birth=datetime.datetime.strptime(
                    "1970/01/01", "%Y/%m/%d"
                ).date(),
            )
        )
        _ = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_inactive_config.organization_id,
                unique_corp_id=unique_corp_id,
                date_of_birth=member_1.date_of_birth,
            )
        )
        svc.configurations = configuration_test_client

        # When/then
        with pytest.raises(Exception):
            await svc.check_alternate_eligibility(**search)


class TestOverEligibility:
    @staticmethod
    async def test_overeligibility_no_base_matches(
        svc: service.EligibilityService,
        member_versioned_test_client,
        test_member_versioned,
        member_2_test_client,
    ):
        # Given
        svc.members_versioned = member_versioned_test_client
        svc.members_2 = member_2_test_client
        search = dict(
            date_of_birth=test_member_versioned.date_of_birth,
            first_name=test_member_versioned.first_name,
            last_name=f"{test_member_versioned.last_name}_wont_pass",
            work_state=test_member_versioned.work_state,
            unique_corp_id=test_member_versioned.unique_corp_id,
            user_id=1,
            email=test_member_versioned.email,
        )

        # When/Then
        with pytest.raises(OverEligibilityError):
            await svc.check_overeligibility(**search)

        with mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_write",
            return_value=True,
        ):
            with pytest.raises(OverEligibilityError):
                await svc.check_overeligibility(**search)

    # region email filtering
    @staticmethod
    async def test_overeligibility_email_no_match(
        svc: service.EligibilityService,
        member_versioned_test_client,
        test_member_versioned,
        mock_is_overeligibility_enabled,
        mock_all_orgs_enabled_for_overeligibility,
        member_2_test_client,
    ):
        # Given
        svc.members_versioned = member_versioned_test_client
        svc.members_2 = member_2_test_client
        search = dict(
            date_of_birth=test_member_versioned.date_of_birth,
            first_name=test_member_versioned.first_name,
            last_name=test_member_versioned.last_name,
            work_state=test_member_versioned.work_state,
            unique_corp_id=test_member_versioned.unique_corp_id,
            user_id=1,
            email=f"{test_member_versioned.email}_wont_match",
        )

        mock_is_overeligibility_enabled(True)

        # When/Then
        with pytest.raises(OverEligibilityError):
            await svc.check_overeligibility(**search)

        with mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_write",
            return_value=True,
        ):
            with pytest.raises(OverEligibilityError):
                await svc.check_overeligibility(**search)

    @staticmethod
    @pytest.mark.parametrize(
        ["email_1", "email_2"],
        [
            ("foo@foobar.com", "foo@foobar.com"),
            ("foo@foobar.com", ""),  # record with empty email
        ],
        ids=["non-null-email", "null email"],
    )
    async def test_overeligibility_email_match_multiple_orgs(
        svc: service.EligibilityService,
        member_versioned_test_client,
        configuration_test_client,
        email_1,
        email_2,
        mock_is_overeligibility_enabled,
        mock_all_orgs_enabled_for_overeligibility,
        member_2_test_client,
    ):
        # Given
        test_config_1 = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create()
        )
        test_config_2 = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create()
        )
        member_versioned_1 = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_config_1.organization_id, email=email_1
            )
        )
        member_versioned_2 = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_config_2.organization_id,
                first_name=member_versioned_1.first_name,
                last_name=member_versioned_1.last_name,
                email=email_2,
                date_of_birth=member_versioned_1.date_of_birth,
            )
        )

        member_2_1 = await member_2_test_client.persist(
            model=data_models.Member2Factory.create(
                id=10001,
                organization_id=test_config_1.organization_id,
                email=email_1,
                first_name=member_versioned_1.first_name,
                last_name=member_versioned_1.last_name,
                date_of_birth=member_versioned_1.date_of_birth,
            )
        )
        member_2_2 = await member_2_test_client.persist(
            model=data_models.Member2Factory.create(
                id=10002,
                organization_id=test_config_2.organization_id,
                first_name=member_versioned_1.first_name,
                last_name=member_versioned_1.last_name,
                email=email_2,
                date_of_birth=member_versioned_1.date_of_birth,
            )
        )

        mock_is_overeligibility_enabled(True)

        svc.members_versioned = member_versioned_test_client
        svc.members_2 = member_2_test_client
        svc.configurations = configuration_test_client
        search = dict(
            date_of_birth=member_versioned_1.date_of_birth,
            first_name=member_versioned_1.first_name,
            last_name=member_versioned_1.last_name,
            work_state=member_versioned_1.work_state,
            unique_corp_id=None,
            user_id=1,
            email=email_1,
        )

        # When/Then
        member_list = await svc.check_overeligibility(**search)

        member_1_response = svc._convert_member_to_member_response(
            member_versioned_1, False, member_versioned_1.id, None
        )
        member_2_response = svc._convert_member_to_member_response(
            member_versioned_2, False, member_versioned_2.id, None
        )

        assert len(member_list) == 2
        assert member_1_response in member_list
        assert member_2_response in member_list

        with mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_write",
            return_value=True,
        ):
            member_list = await svc.check_overeligibility(**search)
            member_1_response = svc._convert_member_to_member_response(
                member_2_1, True, member_versioned_1.id, member_2_1.id
            )
            member_2_response = svc._convert_member_to_member_response(
                member_2_2, True, member_versioned_2.id, member_2_2.id
            )

            assert len(member_list) == 2
            assert member_1_response in member_list
            assert member_2_response in member_list

    # endregion email filtering

    # region healthplan filtering

    @staticmethod
    async def test_overeligibility_healthplan_match_multiple_orgs_all_match(
        svc: service.EligibilityService,
        member_versioned_test_client,
        configuration_test_client,
        mock_is_overeligibility_enabled,
        mock_all_orgs_enabled_for_overeligibility,
        member_2_test_client,
    ):
        # Given
        test_config = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create(eligibility_type="HEALTHPLAN")
        )
        test_config_2 = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create(eligibility_type="HEALTHPLAN")
        )

        member_versioned_1 = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_config.organization_id
            )
        )
        member_versioned_2 = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_config_2.organization_id,
                first_name=member_versioned_1.first_name,
                last_name=member_versioned_1.last_name,
                date_of_birth=member_versioned_1.date_of_birth,
                unique_corp_id=member_versioned_1.unique_corp_id,
            )
        )
        member_2_1 = await member_2_test_client.persist(
            model=data_models.Member2Factory.create(
                id=10001,
                organization_id=test_config.organization_id,
                first_name=member_versioned_1.first_name,
                last_name=member_versioned_1.last_name,
                date_of_birth=member_versioned_1.date_of_birth,
                unique_corp_id=member_versioned_1.unique_corp_id,
            )
        )
        member_2_2 = await member_2_test_client.persist(
            model=data_models.Member2Factory.create(
                id=10002,
                organization_id=test_config_2.organization_id,
                first_name=member_versioned_1.first_name,
                last_name=member_versioned_1.last_name,
                date_of_birth=member_versioned_1.date_of_birth,
                unique_corp_id=member_versioned_1.unique_corp_id,
            )
        )

        svc.members_versioned = member_versioned_test_client
        svc.members_2 = member_2_test_client
        svc.configurations = configuration_test_client
        mock_is_overeligibility_enabled(True)

        search = dict(
            date_of_birth=member_versioned_1.date_of_birth,
            first_name=member_versioned_1.first_name,
            last_name=member_versioned_1.last_name,
            work_state=member_versioned_1.work_state,
            unique_corp_id=member_versioned_1.unique_corp_id,
            user_id=1,
            email=None,
        )

        # When/Then
        member_list = await svc.check_overeligibility(**search)

        member_1_response = svc._convert_member_to_member_response(
            member_versioned_1, False, member_versioned_1.id, None
        )
        member_2_response = svc._convert_member_to_member_response(
            member_versioned_2, False, member_versioned_2.id, None
        )

        assert len(member_list) == 2
        assert member_1_response in member_list
        assert member_2_response in member_list

        with mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_write",
            return_value=True,
        ):
            member_list = await svc.check_overeligibility(**search)
            member_1_response = svc._convert_member_to_member_response(
                member_2_1, True, member_versioned_1.id, member_2_1.id
            )
            member_2_response = svc._convert_member_to_member_response(
                member_2_2, True, member_versioned_2.id, member_2_2.id
            )

            assert len(member_list) == 2
            assert member_1_response in member_list
            assert member_2_response in member_list

    @staticmethod
    async def test_overeligibility_healthplan_match_multiple_orgs_one_match(
        svc: service.EligibilityService,
        member_versioned_test_client,
        configuration_test_client,
        mock_is_overeligibility_enabled,
        mock_all_orgs_enabled_for_overeligibility,
        member_2_test_client,
    ):
        # Given
        test_config = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create(eligibility_type="HEALTHPLAN")
        )
        test_config_2 = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create(eligibility_type="HEALTHPLAN")
        )

        member_1 = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_config.organization_id
            )
        )
        _ = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_config_2.organization_id,
                first_name=member_1.first_name,
                last_name=member_1.last_name,
                date_of_birth=member_1.date_of_birth,
                unique_corp_id=f"{member_1.unique_corp_id}_wont_match",
            )
        )

        member_2 = await member_2_test_client.persist(
            model=data_models.Member2Factory.create(
                id=10001,
                organization_id=test_config.organization_id,
                first_name=member_1.first_name,
                last_name=member_1.last_name,
                date_of_birth=member_1.date_of_birth,
                unique_corp_id=member_1.unique_corp_id,
            )
        )

        _ = await member_2_test_client.persist(
            model=data_models.Member2Factory.create(
                id=10002,
                organization_id=test_config_2.organization_id,
                first_name=member_1.first_name,
                last_name=member_1.last_name,
                date_of_birth=member_1.date_of_birth,
                unique_corp_id=f"{member_1.unique_corp_id}_wont_match",
            )
        )

        svc.members_versioned = member_versioned_test_client
        svc.members_2 = member_2_test_client
        svc.configurations = configuration_test_client

        search = dict(
            date_of_birth=member_1.date_of_birth,
            first_name=member_1.first_name,
            last_name=member_1.last_name,
            work_state=member_1.work_state,
            unique_corp_id=member_1.unique_corp_id,
            user_id=1,
            email=None,
        )

        mock_is_overeligibility_enabled(True)

        # When
        member_list = await svc.check_overeligibility(**search)

        member_1_response = svc._convert_member_to_member_response(
            member_1, False, member_1.id, None
        )

        # Then
        assert member_list == [member_1_response]

        with mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_write",
            return_value=True,
        ):
            # When
            member_list = await svc.check_overeligibility(**search)

            member_2_response = svc._convert_member_to_member_response(
                member_2, True, member_1.id, member_2.id
            )

            # Then
            assert member_list == [member_2_response]

    @staticmethod
    async def test_overeligibility_healthplan_match_no_match_return_all_records(
        svc: service.EligibilityService,
        member_versioned_test_client,
        configuration_test_client,
        mock_is_overeligibility_enabled,
        mock_all_orgs_enabled_for_overeligibility,
        member_2_test_client,
    ):

        # If filtering on health plan records by unique corp ID would return no results, don't do filtering
        # Given
        svc.configurations = configuration_test_client
        test_config = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create(eligibility_type="HEALTHPLAN")
        )
        test_config_2 = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create(eligibility_type="HEALTHPLAN")
        )

        member_versioned_1 = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_config.organization_id
            )
        )
        member_versioned_2 = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_config_2.organization_id,
                first_name=member_versioned_1.first_name,
                last_name=member_versioned_1.last_name,
                date_of_birth=member_versioned_1.date_of_birth,
                unique_corp_id=member_versioned_1.unique_corp_id,
            )
        )
        member_2_1 = await member_2_test_client.persist(
            model=data_models.Member2Factory.create(
                id=10001,
                organization_id=test_config.organization_id,
                first_name=member_versioned_1.first_name,
                last_name=member_versioned_1.last_name,
                date_of_birth=member_versioned_1.date_of_birth,
                unique_corp_id=member_versioned_1.unique_corp_id,
            )
        )
        member_2_2 = await member_2_test_client.persist(
            model=data_models.Member2Factory.create(
                id=10002,
                organization_id=test_config_2.organization_id,
                first_name=member_versioned_1.first_name,
                last_name=member_versioned_1.last_name,
                date_of_birth=member_versioned_1.date_of_birth,
                unique_corp_id=member_versioned_1.unique_corp_id,
            )
        )
        svc.members_versioned = member_versioned_test_client
        svc.members_2 = member_2_test_client
        svc.configurations = configuration_test_client
        search = dict(
            date_of_birth=member_versioned_1.date_of_birth,
            first_name=member_versioned_1.first_name,
            last_name=member_versioned_1.last_name,
            work_state=member_versioned_1.work_state,
            unique_corp_id="no match here",
            user_id=1,
            email=None,
        )

        mock_is_overeligibility_enabled(True)

        # When
        member_list = await svc.check_overeligibility(**search)

        # Then
        member_1_response = svc._convert_member_to_member_response(
            member_versioned_1, False, member_versioned_1.id, None
        )
        member_2_response = svc._convert_member_to_member_response(
            member_versioned_2, False, member_versioned_2.id, None
        )

        assert len(member_list) == 2
        assert member_1_response in member_list
        assert member_2_response in member_list

        with mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_write",
            return_value=True,
        ):
            member_list = await svc.check_overeligibility(**search)
            member_1_response = svc._convert_member_to_member_response(
                member_2_1, True, member_versioned_1.id, member_2_1.id
            )
            member_2_response = svc._convert_member_to_member_response(
                member_2_2, True, member_versioned_2.id, member_2_2.id
            )

            assert len(member_list) == 2
            assert member_1_response in member_list
            assert member_2_response in member_list

    @staticmethod
    async def test_overeligibility_healthplan_match_multiple_orgs_multiple_eligibility_types(
        svc: service.EligibilityService,
        member_versioned_test_client,
        configuration_test_client,
        mock_is_overeligibility_enabled,
        mock_all_orgs_enabled_for_overeligibility,
        member_2_test_client,
    ):
        # If filtering on health plan records by unique corp ID would return no results, don't do filtering
        # Given
        svc.members_versioned = member_versioned_test_client
        svc.members_2 = member_2_test_client
        svc.configurations = configuration_test_client

        test_config = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create(eligibility_type="HEALTHPLAN")
        )
        test_config_2 = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create(eligibility_type="ALTERNATE")
        )

        member_versioned_1 = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_config.organization_id
            )
        )
        member_versioned_2 = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_config_2.organization_id,
                first_name=member_versioned_1.first_name,
                last_name=member_versioned_1.last_name,
                date_of_birth=member_versioned_1.date_of_birth,
                unique_corp_id=member_versioned_1.unique_corp_id,
            )
        )
        member_2_1 = await member_2_test_client.persist(
            model=data_models.Member2Factory.create(
                id=10001,
                organization_id=test_config.organization_id,
                first_name=member_versioned_1.first_name,
                last_name=member_versioned_1.last_name,
                date_of_birth=member_versioned_1.date_of_birth,
                unique_corp_id=member_versioned_1.unique_corp_id,
            )
        )
        member_2_2 = await member_2_test_client.persist(
            model=data_models.Member2Factory.create(
                id=10002,
                organization_id=test_config_2.organization_id,
                first_name=member_versioned_1.first_name,
                last_name=member_versioned_1.last_name,
                date_of_birth=member_versioned_1.date_of_birth,
                unique_corp_id=member_versioned_1.unique_corp_id,
            )
        )
        search = dict(
            date_of_birth=member_versioned_1.date_of_birth,
            first_name=member_versioned_1.first_name,
            last_name=member_versioned_1.last_name,
            work_state=member_versioned_1.work_state,
            unique_corp_id=member_versioned_1.unique_corp_id,
            user_id=1,
            email=None,
        )

        mock_is_overeligibility_enabled(True)

        # When
        member_list = await svc.check_overeligibility(**search)

        # Then
        member_1_response = svc._convert_member_to_member_response(
            member_versioned_1, False, member_versioned_1.id, None
        )
        member_2_response = svc._convert_member_to_member_response(
            member_versioned_2, False, member_versioned_2.id, None
        )

        assert len(member_list) == 2
        assert member_1_response in member_list
        assert member_2_response in member_list

        with mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_write",
            return_value=True,
        ):
            member_list = await svc.check_overeligibility(**search)
            member_1_response = svc._convert_member_to_member_response(
                member_2_1, True, member_versioned_1.id, member_2_1.id
            )
            member_2_response = svc._convert_member_to_member_response(
                member_2_2, True, member_versioned_2.id, member_2_2.id
            )

            assert len(member_list) == 2
            assert member_1_response in member_list
            assert member_2_response in member_list

    # endregion healthplan filtering

    # region org_active and multiple results per org

    @staticmethod
    async def test_overeligibility_non_active_org(
        svc: service.EligibilityService,
        member_versioned_test_client,
        configuration_test_client,
        test_inactive_config,
        mock_is_overeligibility_enabled,
        mock_all_orgs_enabled_for_overeligibility,
        member_2_test_client,
    ):
        # If filtering on health plan records by unique corp ID would return no results, don't do filtering
        # Given
        svc.members_versioned = member_versioned_test_client
        svc.members_2 = member_2_test_client
        svc.configurations = configuration_test_client

        member = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_inactive_config.organization_id
            )
        )

        search = dict(
            date_of_birth=member.date_of_birth,
            first_name=member.first_name,
            last_name=member.last_name,
            work_state=member.work_state,
            unique_corp_id=member.unique_corp_id,
            user_id=1,
            email=member.email,
        )

        mock_is_overeligibility_enabled(True)

        # When/Then
        with pytest.raises(OverEligibilityError):
            await svc.check_overeligibility(**search)

        with mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_write",
            return_value=True,
        ):
            with pytest.raises(OverEligibilityError):
                await svc.check_overeligibility(**search)

    @staticmethod
    async def test_overeligibility_mixed_active_orgs(
        svc: service.EligibilityService,
        member_versioned_test_client,
        configuration_test_client,
        test_inactive_config,
        mock_is_overeligibility_enabled,
        mock_all_orgs_enabled_for_overeligibility,
        member_2_test_client,
    ):
        # If filtering on health plan records by unique corp ID would return no results, don't do filtering
        # Given
        svc.members_versioned = member_versioned_test_client
        svc.members_2 = member_2_test_client
        svc.configurations = configuration_test_client

        inactive_member = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_inactive_config.organization_id
            )
        )

        active_config = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create(eligibility_type="ALTERNATE")
        )
        active_member = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=active_config.organization_id,
                first_name=inactive_member.first_name,
                last_name=inactive_member.last_name,
                date_of_birth=inactive_member.date_of_birth,
                email=inactive_member.email,
                unique_corp_id=inactive_member.unique_corp_id,
            )
        )
        _ = await member_2_test_client.persist(
            model=data_models.Member2Factory.create(
                id=10001,
                organization_id=test_inactive_config.organization_id,
                first_name=inactive_member.first_name,
                last_name=inactive_member.last_name,
                date_of_birth=inactive_member.date_of_birth,
                email=inactive_member.email,
                unique_corp_id=inactive_member.unique_corp_id,
            )
        )
        active_member_2 = await member_2_test_client.persist(
            model=data_models.Member2Factory.create(
                id=10002,
                organization_id=active_config.organization_id,
                first_name=inactive_member.first_name,
                last_name=inactive_member.last_name,
                date_of_birth=inactive_member.date_of_birth,
                email=inactive_member.email,
                unique_corp_id=inactive_member.unique_corp_id,
            )
        )

        search = dict(
            date_of_birth=inactive_member.date_of_birth,
            first_name=inactive_member.first_name,
            last_name=inactive_member.last_name,
            work_state=inactive_member.work_state,
            unique_corp_id=inactive_member.unique_corp_id,
            user_id=1,
            email=inactive_member.email,
        )

        mock_is_overeligibility_enabled(True)

        # When
        member_list = await svc.check_overeligibility(**search)

        active_member_response = svc._convert_member_to_member_response(
            active_member, False, active_member.id, None
        )

        # Then
        assert member_list == [active_member_response]

        with mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_write",
            return_value=True,
        ):
            member_list = await svc.check_overeligibility(**search)
            active_member_response = svc._convert_member_to_member_response(
                active_member_2, True, active_member.id, active_member_2.id
            )

            assert member_list == [active_member_response]

    @staticmethod
    async def test_overeligibility_multiple_results_same_org(
        svc: service.EligibilityService,
        member_versioned_test_client,
        configuration_test_client,
        test_config,
        test_member_versioned,
        mock_is_overeligibility_enabled,
        mock_all_orgs_enabled_for_overeligibility,
        member_2_test_client,
    ):
        # Given
        svc.members_versioned = member_versioned_test_client
        svc.members_2 = member_2_test_client
        svc.configurations = configuration_test_client

        newer_member = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_member_versioned.organization_id,
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                date_of_birth=test_member_versioned.date_of_birth,
                email=test_member_versioned.email,
                unique_corp_id=test_member_versioned.unique_corp_id,
            )
        )

        await member_versioned_test_client.set_updated_at(
            id=test_member_versioned.id,
            updated_at=datetime.datetime.strptime("2020/01/01", "%Y/%m/%d"),
        )
        await member_versioned_test_client.set_updated_at(
            id=newer_member.id,
            updated_at=datetime.datetime.strptime("2020/02/02", "%Y/%m/%d"),
        )

        member_2 = await member_2_test_client.persist(
            model=data_models.Member2Factory.create(
                id=10001,
                organization_id=test_member_versioned.organization_id,
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                date_of_birth=test_member_versioned.date_of_birth,
                email=test_member_versioned.email,
                unique_corp_id=test_member_versioned.unique_corp_id,
            )
        )
        newer_member_2 = await member_2_test_client.persist(
            model=data_models.Member2Factory.create(
                id=10002,
                organization_id=test_member_versioned.organization_id,
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                date_of_birth=test_member_versioned.date_of_birth,
                email=test_member_versioned.email,
                unique_corp_id=test_member_versioned.unique_corp_id,
            )
        )
        await member_2_test_client.set_updated_at(
            id=member_2.id,
            updated_at=datetime.datetime.strptime("2020/01/01", "%Y/%m/%d"),
        )
        await member_2_test_client.set_updated_at(
            id=newer_member_2.id,
            updated_at=datetime.datetime.strptime("2020/02/02", "%Y/%m/%d"),
        )

        search = dict(
            date_of_birth=test_member_versioned.date_of_birth,
            first_name=test_member_versioned.first_name,
            last_name=test_member_versioned.last_name,
            work_state=test_member_versioned.work_state,
            unique_corp_id=None,
            user_id=1,
            email=test_member_versioned.email,
        )

        mock_is_overeligibility_enabled(True)

        # When
        member_list = await svc.check_overeligibility(**search)

        # Then
        assert len(member_list) == 1
        assert member_list[0].id == newer_member.id

        with mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_write",
            return_value=True,
        ):
            member_list = await svc.check_overeligibility(**search)
            assert len(member_list) == 1
            assert member_list[0].id == newer_member_2.id

    @staticmethod
    async def test_overeligibility_multiple_results_same_org_different_secondary_identifier(
        svc: service.EligibilityService,
        member_versioned_test_client,
        configuration_test_client,
        test_config,
        test_member_versioned,
        mock_is_overeligibility_enabled,
        mock_all_orgs_enabled_for_overeligibility,
        member_2_test_client,
    ):

        # ensure if we return multiple results for an org that won't be filtered out by email/unique_corp, we return the more recent one
        # Given
        svc.members_versioned = member_versioned_test_client
        svc.members_2 = member_2_test_client
        svc.configurations = configuration_test_client

        newer_member = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_member_versioned.organization_id,
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                date_of_birth=test_member_versioned.date_of_birth,
                email=test_member_versioned.email,
                unique_corp_id=f"{test_member_versioned.unique_corp_id}_wont_match",
            )
        )

        await member_versioned_test_client.set_updated_at(
            id=test_member_versioned.id,
            updated_at=datetime.datetime.strptime("2020/01/01", "%Y/%m/%d"),
        )
        await member_versioned_test_client.set_updated_at(
            id=newer_member.id,
            updated_at=datetime.datetime.strptime("2020/02/02", "%Y/%m/%d"),
        )

        member_2 = await member_2_test_client.persist(
            model=data_models.Member2Factory.create(
                id=10001,
                organization_id=test_member_versioned.organization_id,
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                date_of_birth=test_member_versioned.date_of_birth,
                email=test_member_versioned.email,
                unique_corp_id=f"{test_member_versioned.unique_corp_id}",
            )
        )
        newer_member_2 = await member_2_test_client.persist(
            model=data_models.Member2Factory.create(
                id=10002,
                organization_id=test_member_versioned.organization_id,
                first_name=test_member_versioned.first_name,
                last_name=test_member_versioned.last_name,
                date_of_birth=test_member_versioned.date_of_birth,
                email=test_member_versioned.email,
                unique_corp_id=f"{test_member_versioned.unique_corp_id}_wont_match",
            )
        )

        await member_2_test_client.set_updated_at(
            id=member_2.id,
            updated_at=datetime.datetime.strptime("2020/01/01", "%Y/%m/%d"),
        )
        await member_2_test_client.set_updated_at(
            id=newer_member_2.id,
            updated_at=datetime.datetime.strptime("2020/02/02", "%Y/%m/%d"),
        )

        search = dict(
            date_of_birth=test_member_versioned.date_of_birth,
            first_name=test_member_versioned.first_name,
            last_name=test_member_versioned.last_name,
            work_state=test_member_versioned.work_state,
            unique_corp_id=None,
            user_id=1,
            email=test_member_versioned.email,
        )

        mock_is_overeligibility_enabled(True)

        # When
        member_list = await svc.check_overeligibility(**search)

        # Then
        assert len(member_list) == 1
        assert member_list[0].id == newer_member.id

        with mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_write",
            return_value=True,
        ):
            member_list = await svc.check_overeligibility(**search)
            assert member_2.updated_at == newer_member_2.updated_at
            assert len(member_list) == 1
            assert member_list[0].id == newer_member_2.id

    # endregion org_active and multiple results per org

    async def test_overeligibility_disabled(
        self,
        svc: service.EligibilityService,
        member_versioned_test_client,
        configuration_test_client,
        test_config,
        test_member_versioned,
        mock_is_overeligibility_enabled,
    ):
        # Given
        svc.members_versioned = member_versioned_test_client
        search = dict(
            date_of_birth=test_member_versioned.date_of_birth,
            first_name=test_member_versioned.first_name,
            last_name=test_member_versioned.last_name,
            work_state=test_member_versioned.work_state,
            unique_corp_id=test_member_versioned.unique_corp_id,
            user_id=1,
            email=f"{test_member_versioned.email}_wont_match",
        )

        mock_is_overeligibility_enabled(False)

        # When/Then
        with pytest.raises(OverEligibilityError):
            await svc.check_overeligibility(**search)

    def test_organizations_enabled_feature_flag_with_specific_orgs(self):
        # Given
        enabled_orgs = {1, 2, 3}

        def mock_function(organization_ids):
            return all(org_id in enabled_orgs for org_id in organization_ids)

        # Patch the feature flag function to use the mock function
        with mock.patch(
            "app.utils.feature_flag.are_all_organizations_enabled_for_overeligibility",
            side_effect=mock_function,
        ):
            organization_ids = frozenset({1, 2})

            # When
            result = feature_flag.are_all_organizations_enabled_for_overeligibility(
                organization_ids=organization_ids
            )

            # Then
            assert result is True

    async def test_overeligibility_all_orgs_disabled(
        self,
        svc: service.EligibilityService,
        member_versioned_test_client,
        configuration_test_client,
        test_config,
        test_member_versioned,
        mock_is_overeligibility_enabled,
    ):
        # Given
        svc.members_versioned = member_versioned_test_client
        svc.configurations = configuration_test_client

        test_config = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create(eligibility_type="HEALTHPLAN")
        )

        member_1 = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_config.organization_id
            )
        )

        search = dict(
            date_of_birth=member_1.date_of_birth,
            first_name=member_1.first_name,
            last_name=member_1.last_name,
            work_state=member_1.work_state,
            unique_corp_id=member_1.unique_corp_id,
            user_id=1,
            email=None,
        )

        mock_is_overeligibility_enabled(True)

        # When/Then
        with pytest.raises(OverEligibilityError):
            await svc.check_overeligibility(**search)

    async def test_overeligibility_more_than_one_org_disabled(
        self,
        svc: service.EligibilityService,
        member_versioned_test_client,
        configuration_test_client,
        test_config,
        test_member_versioned,
        mock_is_overeligibility_enabled,
    ):
        # Given
        svc.members_versioned = member_versioned_test_client
        svc.configurations = configuration_test_client

        test_config = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create(eligibility_type="HEALTHPLAN")
        )
        test_config_2 = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create(eligibility_type="ALTERNATE")
        )

        test_config_3 = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create(eligibility_type="STANDARD")
        )

        member_1 = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_config.organization_id
            )
        )
        member_2 = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_config_2.organization_id,
                first_name=member_1.first_name,
                last_name=member_1.last_name,
                date_of_birth=member_1.date_of_birth,
                unique_corp_id=member_1.unique_corp_id,
            )
        )

        member_3 = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_config_3.organization_id,
                first_name=member_1.first_name,
                last_name=member_1.last_name,
                date_of_birth=member_1.date_of_birth,
                unique_corp_id=member_1.unique_corp_id,
            )
        )

        search = dict(
            date_of_birth=member_1.date_of_birth,
            first_name=member_1.first_name,
            last_name=member_1.last_name,
            work_state=member_1.work_state,
            unique_corp_id=member_1.unique_corp_id,
            user_id=1,
            email=None,
        )

        mock_is_overeligibility_enabled(True)
        enabled_orgs = {3, 4, 5, 6}

        def mock_function(organization_ids):
            return all(org_id in enabled_orgs for org_id in organization_ids)

        # Patch the feature flag function to use the mock function
        with mock.patch(
            "app.utils.feature_flag.are_all_organizations_enabled_for_overeligibility",
            side_effect=mock_function,
            organization_ids={
                member_1.organization_id,
                member_2.organization_id,
                member_3.organization_id,
            },
        ):
            # When/Then
            with pytest.raises(OverEligibilityError):
                await svc.check_overeligibility(**search)
