import datetime
from typing import Any
from unittest import mock

import pytest
from ddtrace import tracer
from tests.factories import data_models
from tests.factories import data_models as factory
from tests.factories.data_models import (
    ConfigurationFactory,
    Member2Factory,
    MemberResponseFactory,
    MemberVersionedFactory,
    WalletEnablementResponseFactory,
)

import app.eligibility.errors
import app.utils.eligibility_validation
from app.eligibility import errors, service
from app.eligibility.service import EligibilityService
from app.utils.eligibility_validation import is_cached_organization_active
from db import model
from db.model import MemberVersioned

pytestmark = pytest.mark.asyncio


@pytest.fixture
def reset_organization_cache():
    app.utils.eligibility_validation.is_cached_organization_active.reset()


@pytest.fixture(scope="module")
def svc() -> service.EligibilityService:
    return service.EligibilityService()


@pytest.fixture(scope="module")
def test_svc() -> service.EligibilityTestUtilityService:
    return service.EligibilityTestUtilityService()


@pytest.fixture(autouse=True)
def trace_test(request):
    with tracer.trace(request.node.name) as span:
        yield span


@pytest.mark.usefixtures("reset_organization_cache")
@pytest.mark.parametrize(
    argnames="search,method_name,query_name,array_response,organization_id",
    argvalues=[
        (
            dict(date_of_birth="1970-01-01", email="foo@mail"),
            "check_standard_eligibility",
            "get_by_dob_and_email",
            True,
            1,
        ),
        (
            dict(
                date_of_birth="1970-01-01",
                first_name="Princess",
                last_name="Zelda",
                work_state="Hyrule",
                unique_corp_id=None,
            ),
            "check_alternate_eligibility",
            "get_by_secondary_verification",
            True,
            1,
        ),
        (
            dict(
                date_of_birth="1970-01-01",
                first_name="Princess",
                last_name="Zelda",
                work_state=None,
                unique_corp_id="corpid",
            ),
            "check_alternate_eligibility",
            "get_by_tertiary_verification",
            True,
            1,
        ),
        (
            dict(
                email="foo@mail",
                first_name="Princess",
                last_name="Zelda",
            ),
            "check_organization_specific_eligibility_without_dob",
            "get_by_email_and_name",
            True,
            685,
        ),
        # test when there is no match found for user_provided_organization_id,
        # but it belongs to an organization that doesn't send date of birth
        # expected to return the member record as a successful match
        (
            dict(
                email="foo@mail",
                first_name="Princess",
                last_name="Zelda",
            ),
            "check_organization_specific_eligibility_without_dob",
            "get_by_email_and_name",
            True,
            620,
        ),
        (dict(id=1), "get_by_member_id", "get", False, 1),
        (
            dict(
                organization_id=1,
                unique_corp_id="corpid",
                dependent_id="depid",
            ),
            "get_by_org_identity",
            "get_by_org_identity",
            False,
            1,
        ),
    ],
)
async def test_eligibility_checks(
    search: dict,
    method_name: str,
    query_name: str,
    member_versioned,
    array_response,
    organization_id,
    svc,
    members_versioned,
    client_specific_service,
    config,
    configs,
):
    # Given
    query_method = getattr(members_versioned, query_name)
    query_method.return_value = (
        member_versioned if not array_response else [member_versioned]
    )
    member_versioned.organization_id = organization_id
    search_method = getattr(svc, method_name)
    # When
    with mock.patch(
        "app.utils.eligibility_validation.is_organization_activated"
    ) as organization_active:
        organization_active.return_value = True
        found = await search_method(**search)
    # Then
    assert found.organization_id == member_versioned.organization_id
    assert found.first_name == member_versioned.first_name
    assert found.last_name == member_versioned.last_name
    assert found.created_at == member_versioned.created_at
    assert found.updated_at == member_versioned.updated_at


# region overeligible


@pytest.mark.usefixtures("reset_organization_cache")
@pytest.mark.parametrize(
    argnames="search,method_name,query_name",
    argvalues=[
        (
            dict(date_of_birth="1970-01-01", email="foo@foobar.com"),
            "check_standard_eligibility",
            "get_by_dob_and_email",
        ),
        (
            dict(
                date_of_birth="1970-01-01",
                first_name="Princess",
                last_name="Zelda",
                work_state="Hyrule",
                unique_corp_id=None,
            ),
            "check_alternate_eligibility",
            "get_by_secondary_verification",
        ),
        (
            dict(
                date_of_birth="1970-01-01",
                first_name="Princess",
                last_name="Zelda",
                work_state=None,
                unique_corp_id="corpid",
            ),
            "check_alternate_eligibility",
            "get_by_tertiary_verification",
        ),
    ],
)
async def test_eligibility_over_eligible_same_org(
    search: dict,
    method_name: str,
    query_name: str,
    svc,
    members_versioned,
    client_specific_service,
    config,
    configs,
):
    # Given
    # Generate two valid records with the same org
    member_1 = data_models.MemberVersionedFactory.create(
        organization_id=config.organization_id
    )
    member_2 = data_models.MemberVersionedFactory.create(
        organization_id=config.organization_id
    )
    query_method = getattr(members_versioned, query_name)
    query_method.return_value = [member_1, member_2]

    search_method = getattr(svc, method_name)
    # When
    with mock.patch(
        "app.utils.eligibility_validation.is_cached_organization_active"
    ) as organization_active:
        organization_active.return_value = True
        found = await search_method(**search)
    # Then
    assert found.first_name in [member_1.first_name, member_2.first_name]
    assert found.last_name in [member_1.last_name, member_2.last_name]
    assert found.date_of_birth in [member_1.date_of_birth, member_2.date_of_birth]


@pytest.mark.usefixtures("reset_organization_cache")
@pytest.mark.parametrize(
    argnames="search,method_name,query_name",
    argvalues=[
        (
            dict(
                date_of_birth="1970-01-01",
                first_name="Princess",
                last_name="Zelda",
                work_state="Hyrule",
                unique_corp_id=None,
            ),
            "check_alternate_eligibility",
            "get_by_secondary_verification",
        ),
        (
            dict(
                date_of_birth="1970-01-01",
                first_name="Princess",
                last_name="Zelda",
                work_state=None,
                unique_corp_id="corpid",
            ),
            "check_alternate_eligibility",
            "get_by_tertiary_verification",
        ),
    ],
)
async def test_alternative_eligibility_over_eligible_same_org(
    search: dict,
    method_name: str,
    query_name: str,
    svc,
    members_versioned,
    client_specific_service,
    config,
    configs,
):
    # Given
    # Generate two valid records with the same org
    member_1 = data_models.MemberVersionedFactory.create(
        organization_id=config.organization_id
    )
    member_2 = data_models.MemberVersionedFactory.create(
        organization_id=config.organization_id
    )
    query_method = getattr(members_versioned, query_name)
    query_method.return_value = [member_1, member_2]

    search_method = getattr(svc, method_name)
    # When
    with mock.patch(
        "app.utils.eligibility_validation.is_cached_organization_active"
    ) as organization_active:
        organization_active.return_value = True
        found = await search_method(**search)
    # Then
    assert found.first_name in [member_1.first_name, member_2.first_name]
    assert found.last_name in [member_1.last_name, member_2.last_name]
    assert found.date_of_birth in [member_1.date_of_birth, member_2.date_of_birth]


# endregion


@pytest.mark.parametrize(
    argnames="search,method_name,query_name,array_reply",
    argvalues=[
        (
            dict(
                is_employee=True,
                unique_corp_id="corpid",
                date_of_birth="1970-01-01",
                dependent_date_of_birth="",
                organization_id=1,
            ),
            "check_client_specific_eligibility",
            "get_by_client_specific_verification",
            False,
        ),
    ],
)
async def test_eligibility_checks_client_specific(
    search: dict,
    method_name: str,
    query_name: str,
    array_reply: bool,
    member_versioned,
    svc,
    members_versioned,
    client_specific_service,
    config_client_specific,
    configs,
):
    # Given
    query_method = getattr(members_versioned, query_name)
    query_method.side_effect = mock.AsyncMock(
        return_value=[member_versioned] if array_reply else member_versioned
    )

    search_method = getattr(svc, method_name)
    get_configuration_method = getattr(configs, "get")
    get_configuration_method.side_effect = mock.AsyncMock(
        return_value=config_client_specific
    )

    search_method = getattr(svc, method_name)

    # When
    found = await search_method(**search)
    # Then
    assert found.date_of_birth == member_versioned.date_of_birth
    assert found.first_name == member_versioned.first_name
    assert found.last_name == member_versioned.last_name
    assert found.unique_corp_id == member_versioned.unique_corp_id


@pytest.mark.parametrize(
    argnames="search,method_name,query_name,array_reply,organization_id",
    argvalues=[
        (
            dict(date_of_birth="1970-01-01", email="foo@mail"),
            "check_standard_eligibility",
            "get_by_dob_and_email",
            True,
            1,
        ),
        (
            dict(
                date_of_birth="1970-01-01",
                first_name="Princess",
                last_name="Zelda",
                work_state="Hyrule",
                unique_corp_id=None,
            ),
            "check_alternate_eligibility",
            "get_by_secondary_verification",
            True,
            1,
        ),
        (
            dict(
                date_of_birth="1970-01-01",
                first_name="Princess",
                last_name="Zelda",
                work_state=None,
                unique_corp_id="corpid",
            ),
            "check_alternate_eligibility",
            "get_by_tertiary_verification",
            True,
            1,
        ),
        (
            dict(
                email="foo@mail",
                first_name="Princess",
                last_name="Zelda",
            ),
            "check_organization_specific_eligibility_without_dob",
            "get_by_email_and_name",
            True,
            685,
        ),
        (
            dict(
                organization_id=1,
                unique_corp_id="corpid",
                dependent_id="depid",
            ),
            "get_by_org_identity",
            "get_by_org_identity",
            False,
            1,
        ),
    ],
)
async def test_eligibility_checks_terminated_organization(
    search: dict,
    method_name: str,
    query_name: str,
    array_reply: bool,
    organization_id: int,
    member,
    svc,
    members,
    terminated_config,
    configs,
):
    # Given
    query_method = getattr(members, query_name)
    member.organization_id = organization_id
    query_method.side_effect = mock.AsyncMock(
        return_value=[member] if array_reply else member
    )

    search_method = getattr(svc, method_name)

    # When
    with mock.patch(
        "app.utils.eligibility_validation.is_organization_activated"
    ) as organization_active:
        organization_active.return_value = False
        with pytest.raises(app.eligibility.errors.MatchError):
            await search_method(**search)


@pytest.mark.parametrize(
    argnames="search,method_name,query_name,array_reply",
    argvalues=[
        (
            dict(
                is_employee=True,
                unique_corp_id="corpid",
                date_of_birth="1970-01-01",
                dependent_date_of_birth="",
                organization_id=1,
            ),
            "check_client_specific_eligibility",
            "get_by_client_specific_verification",
            False,
        ),
    ],
)
async def test_eligibility_checks_terminated_organization_client_specific(
    search: dict,
    method_name: str,
    query_name: str,
    array_reply: bool,
    member,
    svc,
    members,
    terminated_config_client_specific,
    configs,
    client_specific_service,
):
    # Given
    query_method = getattr(members, query_name)
    query_method.side_effect = mock.AsyncMock(
        return_value=[member] if array_reply else member
    )

    search_method = getattr(svc, method_name)

    get_configuration_method = getattr(configs, "get")
    get_configuration_method.side_effect = mock.AsyncMock(
        return_value=terminated_config_client_specific
    )

    # When
    with pytest.raises(app.eligibility.errors.ClientSpecificConfigurationError):
        await search_method(**search)


@pytest.mark.parametrize(
    argnames="search,method_name,query_name",
    argvalues=[
        (
            dict(date_of_birth="1970-01-01", email="foo@mail"),
            "check_standard_eligibility",
            "get_by_dob_and_email",
        ),
        (
            dict(
                date_of_birth="1970-01-01",
                first_name="Princess",
                last_name="Zelda",
                work_state="Hyrule",
                unique_corp_id=None,
            ),
            "check_alternate_eligibility",
            "get_by_secondary_verification",
        ),
        (
            dict(
                date_of_birth="1970-01-01",
                first_name="Princess",
                last_name="Zelda",
                work_state=None,
                unique_corp_id="corpid",
            ),
            "check_alternate_eligibility",
            "get_by_tertiary_verification",
        ),
        (
            dict(id=1),
            "get_by_member_id",
            "get",
        ),
        (
            dict(
                organization_id=1,
                unique_corp_id="corpid",
                dependent_id="depid",
            ),
            "get_by_org_identity",
            "get_by_org_identity",
        ),
        (
            dict(
                email="foo@mail",
                first_name="Princess",
                last_name="Zelda",
            ),
            "check_organization_specific_eligibility_without_dob",
            "get_by_email_and_name",
        ),
    ],
)
async def test_eligibility_checks_no_match(
    search: dict,
    method_name: str,
    query_name: str,
    svc,
    members_versioned,
    client_specific_service,
):
    # Given
    query_method = getattr(members_versioned, query_name)
    query_method.return_value = []
    search_method = getattr(svc, method_name)

    # When/Then
    with mock.patch(
        "app.utils.eligibility_validation.is_cached_organization_active"
    ) as organization_active:
        organization_active.return_value = True
        with pytest.raises(app.eligibility.errors.MatchError):
            await search_method(**search)


@pytest.mark.parametrize(
    argnames="search,method_name,query_name,array_reply",
    argvalues=[
        (
            dict(
                is_employee=True,
                unique_corp_id="corpid",
                date_of_birth="1970-01-01",
                dependent_date_of_birth="",
                organization_id=1,
            ),
            "check_client_specific_eligibility",
            "get_by_client_specific_verification",
            False,
        ),
    ],
)
async def test_eligibility_checks_no_match_client_specific(
    search: dict,
    method_name: str,
    query_name: str,
    array_reply: bool,
    svc,
    members_versioned,
    client_specific_service,
    config_client_specific,
    configs,
):
    # Given
    query_method = getattr(members_versioned, query_name)
    query_method.side_effect = mock.AsyncMock(return_value=[] if array_reply else None)
    search_method = getattr(svc, method_name)
    get_configuration_method = getattr(configs, "get")
    get_configuration_method.side_effect = mock.AsyncMock(
        return_value=config_client_specific
    )

    # When/Then
    with mock.patch(
        "app.utils.eligibility_validation.is_cached_organization_active"
    ) as organization_active:
        organization_active.return_value = True
        with pytest.raises(app.eligibility.errors.MatchError):
            await search_method(**search)


@pytest.mark.parametrize(
    argnames="search,method_name,query_name",
    argvalues=[
        (
            dict(member_id=1),
            "get_wallet_enablement",
            "get_wallet_enablement",
        ),
        (
            dict(
                organization_id=1,
                unique_corp_id="corpid",
                dependent_id="depid",
            ),
            "get_wallet_enablement_by_identity",
            "get_wallet_enablement_by_identity",
        ),
    ],
)
async def test_wallet_enablement_checks(
    search: dict,
    method_name: str,
    query_name: str,
    svc,
    members_versioned,
):
    # Given
    with mock.patch(
        "app.eligibility.service.EligibilityService.get_by_member_id",
        return_value=member_versioned,
    ):
        enablement = factory.WalletEnablementResponseFactory.create()
        query_method = getattr(members_versioned, query_name)
        query_method.return_value = enablement
        search_method = getattr(svc, method_name)
        # When
        found = await search_method(**search)
        enablement.member_1_id = found.member_1_id
        enablement.is_v2 = False
        # Then
        assert found == enablement


@pytest.mark.parametrize(
    argnames="search,method_name,query_name",
    argvalues=[
        (
            dict(member_id=1),
            "get_wallet_enablement",
            "get_wallet_enablement",
        ),
        (
            dict(
                organization_id=1,
                unique_corp_id="corpid",
                dependent_id="depid",
            ),
            "get_wallet_enablement_by_identity",
            "get_wallet_enablement_by_identity",
        ),
    ],
)
async def test_wallet_enablement_checks_no_match(
    search: dict,
    method_name: str,
    query_name: str,
    svc,
    members_versioned,
):
    # Given
    query_method = getattr(members_versioned, query_name)
    query_method.return_value = None
    search_method = getattr(svc, method_name)
    # When/Then
    with pytest.raises(app.eligibility.errors.MatchError):
        await search_method(**search)


@pytest.mark.parametrize(
    argnames="search,method_name",
    argvalues=[
        (
            dict(date_of_birth="last week", email="foo@mail"),
            "check_standard_eligibility",
        ),
        (
            dict(
                date_of_birth="First of the Month",
                first_name="Princess",
                last_name="Zelda",
                work_state="Hyrule",
                unique_corp_id=None,
            ),
            "check_alternate_eligibility",
        ),
        (  # invalid date of birth
            dict(
                is_employee=True,
                date_of_birth="not-a-date",
                dependent_date_of_birth="",
                organization_id=1,
                unique_corp_id="corpid",
            ),
            "check_client_specific_eligibility",
        ),
        (  # null dob
            dict(
                is_employee=True,
                date_of_birth="",
                dependent_date_of_birth="1970-01-01",
                organization_id=1,
                unique_corp_id="corpid",
            ),
            "check_client_specific_eligibility",
        ),
        (  # dependent check, bad dependent dob
            dict(
                is_employee=False,
                date_of_birth="1970-01-01",
                dependent_date_of_birth="not-a-date",
                organization_id=1,
                unique_corp_id="corpid",
            ),
            "check_client_specific_eligibility",
        ),
        (  # dependent check, null dependent date of birth
            dict(
                is_employee=False,
                date_of_birth="1970-01-01",
                dependent_date_of_birth="",
                organization_id=1,
                unique_corp_id="corpid",
            ),
            "check_client_specific_eligibility",
        ),
        (  # no DOB or dep DOB
            dict(
                is_employee=True,
                date_of_birth="",
                dependent_date_of_birth="",
                organization_id=1,
                unique_corp_id="corpid",
            ),
            "check_client_specific_eligibility",
        ),
        (  # dependent check, missing member DOB
            dict(
                is_employee=False,
                date_of_birth="",
                dependent_date_of_birth="1970-01-01",
                organization_id=1,
                unique_corp_id="corpid",
            ),
            "check_client_specific_eligibility",
        ),
        (
            dict(organization_id=1, unique_corp_id="", dependent_id=""),
            "get_by_org_identity",
        ),
        (
            dict(organization_id=1, unique_corp_id="", dependent_id=""),
            "get_wallet_enablement_by_identity",
        ),
    ],
)
async def test_checks_bad_input(
    search: dict,
    method_name: str,
    svc,
    members,
):
    # Given
    search_method = getattr(svc, method_name)
    # When/Then
    with pytest.raises(service.ValidationError):
        await search_method(**search)


async def test_no_client_specific_implementation(
    svc,
    configs,
):
    # Given
    config = factory.ConfigurationFactory.create(implementation=None)
    configs.get.side_effect = mock.AsyncMock(return_value=config)
    # Then/When
    with pytest.raises(errors.ConfigurationError):
        await svc.check_client_specific_eligibility(
            is_employee=True,
            date_of_birth="",
            dependent_date_of_birth="",
            organization_id=1,
            unique_corp_id="corpid",
        )


async def test_check_organization_specific_eligibility_without_dob_bad_input(
    svc,
):
    # Given
    request_params = dict(
        email="foo@mail",
        first_name="Princess",
        last_name="Zelda",
    )
    expected_error = errors.NoDobMatchError
    method_name = "check_organization_specific_eligibility_without_dob"
    search_method = getattr(svc, method_name)
    # When/Then
    with pytest.raises(expected_error):
        await search_method(**request_params)


async def test_check_organization_specific_eligibility_without_dob_matching_records_of_org_with_dob(
    svc,
    member_versioned,
    members_versioned,
):
    # Given
    search = dict(
        email="foo@mail",
        first_name="Princess",
        last_name="Zelda",
    )
    method_name = "check_organization_specific_eligibility_without_dob"
    query_name = "get_by_email_and_name"
    organization_id = 1
    query_method = getattr(members_versioned, query_name)
    query_method.return_value = [member_versioned]
    member_versioned.organization_id = organization_id
    search_method = getattr(svc, method_name)

    member_versioned.organization_id = 1
    expected_error = errors.NoDobMatchError

    # When/Then
    with pytest.raises(expected_error):
        await search_method(**search)


async def test_check_organization_specific_eligibility_without_dob_returns_member(
    svc,
    member_versioned,
    members_versioned,
    members_2,
):
    # Given
    search = dict(
        email="foo@mail",
        first_name="Princess",
        last_name="Zelda",
    )
    method_name = "check_organization_specific_eligibility_without_dob"
    query_name = "get_by_email_and_name"
    organization_id = 620
    query_method = getattr(members_versioned, query_name)
    query_method.return_value = [member_versioned]
    member_versioned.organization_id = organization_id
    search_method = getattr(svc, method_name)

    # When/Then
    with mock.patch(
        "app.utils.feature_flag.organization_enabled_for_e9y_2_write",
        return_value=False,
    ), mock.patch(
        "app.utils.eligibility_validation.is_cached_organization_active",
        return_value=True,
    ):
        found = await search_method(**search)

        assert found.first_name == member_versioned.first_name
        assert found.last_name == member_versioned.last_name
        assert found.email == member_versioned.email
        assert found.organization_id == member_versioned.organization_id
        assert not found.is_v2

    with mock.patch(
        "app.utils.feature_flag.organization_enabled_for_e9y_2_write",
        return_value=True,
    ), mock.patch(
        "app.utils.eligibility_validation.is_cached_organization_active",
        return_value=True,
    ):
        member_2_record = factory.Member2Factory.create(
            id=100001,
            organization_id=organization_id,
        )
        members_2.get_by_email_and_name.side_effect = mock.AsyncMock(
            return_value=[member_2_record]
        )
        found = await search_method(**search)

        assert found.organization_id == member_2_record.organization_id
        assert found.is_v2

    expected_error = errors.NoDobMatchError
    with mock.patch(
        "app.utils.feature_flag.organization_enabled_for_e9y_2_write",
        return_value=True,
    ), mock.patch(
        "app.utils.eligibility_validation.is_cached_organization_active",
        return_value=True,
    ):
        member_2_record = factory.Member2Factory.create(
            id=100001,
            organization_id=organization_id + 1,
        )
        members_2.get_by_email_and_name.side_effect = mock.AsyncMock(
            return_value=[member_2_record]
        )
        with pytest.raises(expected_error):
            await search_method(**search)


@pytest.mark.usefixtures("reset_organization_cache")
async def test_get_cached_active_organizations_by_id(svc, configs):
    # Given
    config = factory.ConfigurationFactory.create(implementation=None)
    get_configuration_method = getattr(configs, "get")
    get_configuration_method.side_effect = mock.AsyncMock(return_value=config)

    # When
    await is_cached_organization_active(
        organization_id=config.organization_id,
        configs=configs,
    )

    # Then
    # Underlying async coroutine awaited once
    assert get_configuration_method.await_count == 1


@pytest.mark.usefixtures("reset_organization_cache")
async def test_get_cached_active_organizations_by_id_use_cache(svc, configs):
    # Given
    config = factory.ConfigurationFactory.create(implementation=None)
    get_configuration_method = getattr(configs, "get")
    get_configuration_method.side_effect = mock.AsyncMock(return_value=config)

    # When
    # Call our method twice- should use the cache on the second round
    await is_cached_organization_active(
        organization_id=config.organization_id,
        configs=configs,
    )
    await is_cached_organization_active(
        organization_id=config.organization_id,
        configs=configs,
    )

    # Then
    # Underlying async coroutine awaited once
    assert get_configuration_method.await_count == 1


@pytest.mark.usefixtures("reset_organization_cache")
async def test_get_cached_active_organizations_by_id_time_out(svc, configs):
    # Given
    config = factory.ConfigurationFactory.create(implementation=None)
    get_configuration_method = getattr(configs, "get")
    get_configuration_method.side_effect = mock.AsyncMock(return_value=config)

    with mock.patch(
        "app.utils.async_ttl_cache.AsyncTTLCache._InnerCache._get_time_to_live_value"
    ) as ttl_func_mock:
        # Given
        # Set TTL to be 0:30:01 ago so that a second call will already be considered expired
        # The TTL value is set when the initial call is made, so the mocked TTL value must
        # be set before the first call to get_cached_external_ids_by_value.
        ttl_func_mock.return_value = datetime.datetime.now() - datetime.timedelta(
            minutes=30, seconds=1
        )

        # When
        # Call our method twice- we will make two DB calls because the cache expires
        await is_cached_organization_active(
            organization_id=config.organization_id,
            configs=configs,
        )
        await is_cached_organization_active(
            organization_id=config.organization_id,
            configs=configs,
        )

    # Then
    # Underlying async coroutine awaited once
    assert get_configuration_method.await_count == 2


async def test_get_verification_for_user(svc):
    # Given
    verification = factory.EligibilityVerificationForUserFactory.create()
    user_id = verification.user_id
    search = {
        "user_id": user_id,
        "active_verifications_only": False,
        "organization_id": "",
    }
    search_method = getattr(svc, "get_verification_for_user")

    # When
    with mock.patch(
        "verification.repository.verification.VerificationRepository.get_eligibility_verification_record_for_user"
    ) as get_record:
        get_record.return_value = verification
        found_verification = await search_method(**search)

    # Then
    assert found_verification == verification


async def test_get_all_verifications_for_user(
    svc,
):
    # Given
    organization_ids = [1, 2, 3]
    user_id = 1234
    verifications = []
    for organization_id in organization_ids:
        verification = factory.EligibilityVerificationForUserFactory.create(
            user_id=user_id,
            organization_id=organization_id,
            record="",
            date_of_birth=datetime.date.today(),
            additional_fields="",
        )
        verifications.append(verification)

    search = {
        "user_id": user_id,
        "active_verifications_only": False,
        "organization_ids": "",
    }
    search_method = getattr(svc, "get_all_verifications_for_user")

    # When
    with mock.patch(
        "verification.repository.verification.VerificationRepository.get_all_eligibility_verification_records_for_user"
    ) as get_record:
        get_record.return_value = verifications
        found_verifications = await search_method(**search)

    # Then
    assert found_verifications == verifications


async def test_get_verification_for_user_no_result(
    svc,
):
    # Given
    search = {
        "user_id": 1234,
        "active_verifications_only": False,
        "organization_id": "",
    }
    search_method = getattr(svc, "get_verification_for_user")

    # When/Then
    with mock.patch(
        "verification.repository.verification.VerificationRepository.get_eligibility_verification_record_for_user"
    ) as get_record:
        get_record.return_value = None
        with pytest.raises(errors.GetMatchError):
            await search_method(**search)


async def test_get_all_verifications_for_user_no_result(svc):
    # Given
    search = {
        "user_id": 1234,
        "active_verifications_only": False,
        "organization_ids": "",
    }
    search_method = getattr(svc, "get_all_verifications_for_user")

    # When/Then
    with mock.patch(
        "verification.repository.verification.VerificationRepository.get_all_eligibility_verification_records_for_user"
    ) as get_record:
        get_record.return_value = None
        with pytest.raises(errors.GetMatchError):
            await search_method(**search)


async def test_get_verification_for_organization_filter(
    svc,
):
    # Given
    verification = factory.EligibilityVerificationForUserFactory.create()
    user_id = verification.user_id
    search = {
        "user_id": user_id,
        "active_verifications_only": True,
        "organization_id": "-1",
    }
    search_method = getattr(svc, "get_verification_for_user")

    # When/Then
    with mock.patch(
        "verification.repository.verification.VerificationRepository.get_eligibility_verification_record_for_user"
    ) as get_record:
        get_record.return_value = verification
        with pytest.raises(errors.GetMatchError):
            await search_method(**search)


async def test_get_all_verifications_for_organization_filter_matching_records(
    svc,
):
    # Given
    organization_ids = [1, 2, 3]
    user_id = 1234
    verifications = []
    for organization_id in organization_ids:
        verification = factory.EligibilityVerificationForUserFactory.create(
            user_id=user_id,
            organization_id=organization_id,
            record="",
            date_of_birth=datetime.date.today(),
            additional_fields="",
        )
        verifications.append(verification)

    organization_id_filter = 2
    search = {
        "user_id": user_id,
        "active_verifications_only": True,
        "organization_ids": [organization_id_filter],
    }
    search_method = getattr(svc, "get_all_verifications_for_user")

    # When
    with mock.patch(
        "verification.repository.verification.VerificationRepository.get_all_eligibility_verification_records_for_user"
    ) as get_record:
        get_record.return_value = verifications
        found_verifications = await search_method(**search)

        # Then
        assert found_verifications == [
            v for v in verifications if v.organization_id == organization_id_filter
        ]


async def test_get_all_verifications_for_organization_filter_no_matching_records(
    svc,
):
    # Given
    organization_ids = [1, 2, 3]
    user_id = 1234
    verifications = []
    for organization_id in organization_ids:
        verification = factory.EligibilityVerificationForUserFactory.create(
            user_id=user_id,
            organization_id=organization_id,
            record="",
            date_of_birth=datetime.date.today(),
            additional_fields="",
        )
        verifications.append(verification)

    search = {
        "user_id": user_id,
        "active_verifications_only": True,
        "organization_ids": [4],
    }
    search_method = getattr(svc, "get_all_verifications_for_user")

    # When
    with mock.patch(
        "verification.repository.verification.VerificationRepository.get_all_eligibility_verification_records_for_user"
    ) as result:
        result.return_value = verifications
        with pytest.raises(errors.GetMatchError):
            await search_method(**search)


async def test_get_verification_for_active_filter(svc):
    # Given
    verification = factory.EligibilityVerificationExpiredE9yForUserFactory.create()
    user_id = verification.user_id
    search = {
        "user_id": user_id,
        "active_verifications_only": True,
        "organization_id": "",
    }
    search_method = getattr(svc, "get_verification_for_user")

    # When/Then
    with mock.patch(
        "verification.repository.verification.VerificationRepository.get_eligibility_verification_record_for_user"
    ) as get_record:
        get_record.return_value = verification
        with pytest.raises(errors.GetMatchError):
            await search_method(**search)


async def test_get_all_verifications_for_user_with_active_filter(
    svc,
):
    # Given
    user_id = 1234
    organization_ids = [1, 2, 3]
    expired_verifications = []
    for _ in organization_ids:
        expired_verification = (
            factory.EligibilityVerificationExpiredE9yForUserFactory.create()
        )
        expired_verifications.append(expired_verification)

    search = {
        "user_id": user_id,
        "active_verifications_only": True,
        "organization_ids": [],
    }
    search_method = getattr(svc, "get_all_verifications_for_user")

    # When/Then
    with mock.patch(
        "verification.repository.verification.VerificationRepository.get_all_eligibility_verification_records_for_user"
    ) as get_record:
        get_record.return_value = expired_verifications
        with pytest.raises(errors.GetMatchError):
            await search_method(**search)


async def test_get_all_verifications_for_user_with_active_filter_expired_verifications(
    svc,
):
    # Given
    user_id = 1234
    organization_ids = [1, 2, 3]
    expired_verifications = []
    for _ in organization_ids:
        expired_verification = (
            factory.EligibilityVerificationExpiredE9yForUserFactory.create()
        )
        expired_verifications.append(expired_verification)

    search = {
        "user_id": user_id,
        "active_verifications_only": True,
        "organization_ids": [],
    }
    search_method = getattr(svc, "get_all_verifications_for_user")

    # When/Then
    with mock.patch(
        "verification.repository.verification.VerificationRepository.get_all_eligibility_verification_records_for_user"
    ) as get_record:
        get_record.return_value = expired_verifications
        with pytest.raises(errors.GetMatchError):
            await search_method(**search)


async def test_get_all_verifications_for_user_with_active_filter_no_member_record(
    svc,
):
    # Given
    user_id = 1234
    organization_ids = [1, 2, 3]
    memberless_verifications = []
    for organization_id in organization_ids:
        verification = (
            factory.verification
        ) = factory.EligibilityVerificationForUserFactory.create(
            user_id=user_id,
            organization_id=organization_id,
            eligibility_member_id=None,
        )
        memberless_verifications.append(verification)

    search = {
        "user_id": user_id,
        "active_verifications_only": True,
        "organization_ids": [],
    }
    search_method = getattr(svc, "get_all_verifications_for_user")

    # When/Then
    with mock.patch(
        "verification.repository.verification.VerificationRepository.get_all_eligibility_verification_records_for_user"
    ) as get_record:
        get_record.return_value = memberless_verifications
        found_verifications = await search_method(**search)

        # Then
        assert found_verifications == memberless_verifications


@pytest.mark.parametrize(
    argnames="service_method,args,repo,repo_method,repo_method_return",
    argvalues=[
        (
            "check_standard_eligibility",
            {
                "date_of_birth": datetime.date(year=2000, month=12, day=10),
                "email": "loganroy@waystar.com",
            },
            "members_versioned",
            "get_by_dob_and_email",
            [factory.MemberVersionedFactory.create()],
        ),
        (
            "check_alternate_eligibility",
            {
                "first_name": "Logan",
                "last_name": "Roy",
                "work_state": "NY",
                "date_of_birth": datetime.date(year=2000, month=12, day=10),
                "unique_corp_id": "9939023",
            },
            "members_versioned",
            "get_by_tertiary_verification",
            [factory.MemberVersionedFactory.create()],
        ),
        (
            "check_alternate_eligibility",
            {
                "first_name": "Logan",
                "last_name": "Roy",
                "work_state": "NY",
                "date_of_birth": datetime.date(year=2000, month=12, day=10),
                "unique_corp_id": "",
            },
            "members_versioned",
            "get_by_secondary_verification",
            [factory.MemberVersionedFactory.create()],
        ),
        (
            "get_by_member_id",
            {"id": 1},
            "members_versioned",
            "get",
            factory.MemberVersionedFactory.create(),
        ),
        (
            "get_by_org_identity",
            {
                "organization_id": 1,
                "unique_corp_id": "32324904",
                "dependent_id": "324353234",
            },
            "members_versioned",
            "get_by_org_identity",
            factory.MemberVersionedFactory.create(),
        ),
        (
            "get_wallet_enablement_by_identity",
            {
                "organization_id": 1,
                "unique_corp_id": "32324904",
                "dependent_id": "324353234",
            },
            "members_versioned",
            "get_wallet_enablement_by_identity",
            factory.WalletEnablementFactory.create(),
        ),
    ],
    ids=[
        "check_standard_eligibility-calls-members-versioned",
        "check_alternate_eligibility-calls-members_versioned-get_by_tertiary_verification-when-enabled-with-unique_corp_id",
        "check_alternate_eligibility-calls-members_versioned-get_by_secondary_verification-when-enabled-without-unique_corp_id",
        "get_by_member_id-calls-members_versioned-get-when-enabled",
        "get_by_org_identity-calls-members_versioned-get_by_org_identity-when-enabled",
        "get_wallet_enablement_by_identity-calls-members_versioned-get_wallet_enablement_by_identity-when-enabled",
    ],
)
async def test_service_function_calls_correct_repo_method(
    svc,
    service_method: str,
    args: dict,
    repo: str,
    repo_method: str,
    repo_method_return: Any,
):
    # Given

    with mock.patch(
        "app.utils.eligibility_validation.is_cached_organization_active"
    ) as mock_is_active:
        mock_is_active.return_value = True
        method = getattr(svc, service_method)
        mock_repo = getattr(svc, repo)
        mock_repo_method = getattr(mock_repo, repo_method)
        mock_repo_method.return_value = repo_method_return
        # When
        await method(**args)

        # Then
        mock_repo_method.assert_called()


class TestGetOtherUserIDsInFamily:
    @staticmethod
    async def test_get_other_user_ids_in_family_calls_client_method(svc):
        # Given
        user_id = 1
        verification = factory.EligibilityVerificationForUserFactory.create()

        # When
        with mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
            return_value=False,
        ), mock.patch(
            "verification.repository.verification.VerificationRepository.get_verification_key_for_user",
            return_value=verification,
        ):
            await svc.get_other_user_ids_in_family(user_id=user_id)
            # Then
            svc.members_versioned.get_other_user_ids_in_family.assert_called()

        with mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
            return_value=True,
        ), mock.patch(
            "verification.repository.verification.VerificationRepository.get_verification_key_for_user",
            return_value=verification,
        ):
            result = await svc.get_other_user_ids_in_family(user_id=user_id)
            # Then
            svc.members_2.get_other_user_ids_in_family.assert_called()
            assert result is not None

        # Fallback
        with mock.patch(
            "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
            return_value=True,
        ), mock.patch(
            "verification.repository.verification.VerificationRepository.get_verification_key_for_user",
            return_value=verification,
        ):
            await svc.get_other_user_ids_in_family(user_id=user_id + 100)
            # Then
            svc.members_2.get_other_user_ids_in_family.assert_called()
            svc.members_versioned.get_other_user_ids_in_family.assert_called()


class TestEligibilityTestUtilityService:
    @staticmethod
    async def test_create_a_member_for_organization(test_svc, members_versioned):
        # Given
        organization_id = 1
        first_name = "Unit"
        last_name = "Test"
        email = "test@test.com"
        date_of_birth = datetime.date(year=2000, month=12, day=10)
        dependent_id = "2"
        unique_corp_id = "TEST1"
        work_country = "US"
        effective_range = model.DateRange(
            lower=datetime.date(year=2024, month=1, day=1),
            upper_inc=datetime.date(year=2025, month=1, day=1),
        )
        test_record = model.MemberTestRecord(
            organization_id=str(organization_id),
            first_name=first_name,
            last_name=last_name,
            email=email,
            date_of_birth=date_of_birth,
            dependent_id=int(dependent_id),
            unique_corp_id=unique_corp_id,
            work_country=work_country,
            effective_range=effective_range,
        )
        persisted_member = MemberVersioned(
            organization_id=organization_id,
            first_name=first_name,
            last_name=last_name,
            email=email,
            date_of_birth=date_of_birth,
            dependent_id=str(dependent_id),
            unique_corp_id=unique_corp_id,
            work_country=work_country,
            effective_range=effective_range,
        )
        attributes_to_compare = {
            "organization_id",
            "first_name",
            "last_name",
            "email",
            "date_of_birth",
            "unique_corp_id",
            "work_country",
            "effective_range",
        }
        with mock.patch(
            "app.eligibility.service.EligibilityTestUtilityService.resolve_test_member"
        ) as resolved_test_member, mock.patch(
            "db.clients.member_versioned_client.MembersVersioned.bulk_persist"
        ):
            resolved_test_member.return_value = test_record

            # When
            await test_svc.create_members_for_organization(
                organization_id, [test_record]
            )
            # Then
            test_svc.resolve_test_member.assert_called_once_with(
                test_record=test_record
            )
            test_svc._members_versioned.bulk_persist.assert_called_once()

            # Create a dictionary containing only the subset of known attributes to compare
            # as the rest of the attributes are generated
            expected_values = {
                key: getattr(persisted_member, key) for key in attributes_to_compare
            }

            # inspect args passed to bulk_persist
            result = test_svc._members_versioned.bulk_persist.call_args_list[0].kwargs[
                "models"
            ][0]
            actual_values = {key: getattr(result, key) for key in attributes_to_compare}

            # Then
            assert expected_values == actual_values

    @staticmethod
    async def test_create_members_for_organization(test_svc, members_versioned):
        # Given
        organization_id = 1
        first_names = ["Unit1", "Unit2", "Unit3"]
        last_names = ["Test1", "Test2", "Test3"]
        emails = ["test1@test.com", "test2@test.com", "test3@test.com"]
        date_of_births = [
            datetime.date(year=2000, month=12, day=10),
            datetime.date(year=2001, month=12, day=10),
            datetime.date(year=2002, month=12, day=10),
        ]
        work_states = ["WA", "NY", "TX"]
        dependent_ids = [1, 2, 3]
        unique_corp_ids = ["TEST1", "TEST2", "TEST3"]
        work_countries = ["US", "US", "US"]
        effective_ranges = [
            model.DateRange(
                lower=datetime.date(year=2024, month=1, day=1),
                upper_inc=datetime.date(year=2025, month=1, day=1),
            ),
            model.DateRange(
                lower=datetime.date(year=2024, month=1, day=2),
                upper_inc=datetime.date(year=2025, month=1, day=1),
            ),
            model.DateRange(
                lower=datetime.date(year=2024, month=1, day=3),
                upper_inc=datetime.date(year=2025, month=1, day=1),
            ),
        ]
        test_records = []
        for i in range(3):
            test_record = model.MemberTestRecord(
                organization_id=str(organization_id),
                first_name=first_names[i],
                last_name=last_names[i],
                email=emails[i],
                date_of_birth=date_of_births[i],
                work_state=work_states[i],
                dependent_id=dependent_ids[i],
                unique_corp_id=unique_corp_ids[i],
                work_country=work_countries[i],
                effective_range=effective_ranges[i],
            )
            test_records.append(test_record)

        # When
        with mock.patch(
            "app.eligibility.service.EligibilityTestUtilityService.resolve_test_member",
        ):
            await test_svc.create_members_for_organization(
                str(organization_id), test_records
            )

            # Then
            test_svc.resolve_test_member.call_count = len(test_records)
            test_svc._members_versioned.bulk_persist.assert_called_once()

    @staticmethod
    async def test_create_members_for_organization_write_disabled(test_svc):
        organization_id = 1

        test_record = model.MemberTestRecord(
            organization_id=str(organization_id),
            first_name="Unit",
            last_name="Test",
            email="test@test.com",
            date_of_birth=datetime.date(year=2000, month=12, day=10),
            dependent_id=int("2"),
            unique_corp_id="TEST1",
            work_country="US",
            effective_range=model.DateRange(
                lower=datetime.date(year=2024, month=1, day=1),
                upper_inc=datetime.date(year=2025, month=1, day=1),
            ),
        )

        with mock.patch(
            "app.utils.feature_flag.is_write_disabled",
            return_value=True,
        ), pytest.raises(
            errors.CreateVerificationError,
            match="Creation is disabled due to feature flag",
        ):
            await test_svc.create_members_for_organization(
                organization_id=1, test_records=[test_record]
            )

    def test_resolve_test_member_defaults(self, test_svc):
        # Test with minimum required input
        test_record = model.MemberTestRecord(organization_id=1)
        resolved_attributes = test_svc.resolve_test_member(test_record)

        # Assert default values
        assert resolved_attributes["first_name"] == test_svc.DEFAULT_FIRST_NAME
        assert resolved_attributes["last_name"] == test_svc.DEFAULT_LAST_NAME
        assert resolved_attributes["email"] == test_svc.DEFAULT_EMAIL
        assert resolved_attributes["unique_corp_id"] == test_svc.DEFAULT_CORP_ID
        assert resolved_attributes["dependent_id"] == test_svc.DEFAULT_DEPENDENT_ID
        assert resolved_attributes["date_of_birth"] == test_svc.DEFAULT_DATE_OF_BIRTH
        assert resolved_attributes["work_country"] == test_svc.DEFAULT_WORK_COUNTRY

        # Assert effective_range is calculated correctly
        today = datetime.date.today()
        expected_lower = today - datetime.timedelta(days=1)
        expected_upper = today + datetime.timedelta(days=365)
        assert resolved_attributes["effective_range"].lower == expected_lower
        assert resolved_attributes["effective_range"].upper == expected_upper

    def test_resolve_test_member_input_from_request(self, test_svc):
        organization_id = 1
        first_name = "Unit"
        last_name = "Test"
        email = "test@test.com"
        date_of_birth = datetime.date(year=2000, month=12, day=10)
        work_state = "WA"
        dependent_id = 2
        unique_corp_id = "TEST1"
        work_country = "US"
        effective_range = model.DateRange(
            lower=datetime.date(year=2024, month=1, day=1),
            upper_inc=datetime.date(year=2025, month=1, day=1),
        )
        # Test with input set in request
        test_record = model.MemberTestRecord(
            organization_id=organization_id,
            first_name=first_name,
            last_name=last_name,
            email=email,
            date_of_birth=date_of_birth,
            work_state=work_state,
            dependent_id=dependent_id,
            work_country=work_country,
            unique_corp_id=unique_corp_id,
            effective_range=effective_range,
        )
        resolved_attributes = test_svc.resolve_test_member(test_record)

        # Assert non default values
        assert resolved_attributes["first_name"] == first_name
        assert resolved_attributes["last_name"] == last_name
        assert resolved_attributes["email"] == email
        assert resolved_attributes["unique_corp_id"] == unique_corp_id
        assert resolved_attributes["dependent_id"] == dependent_id
        assert resolved_attributes["date_of_birth"] == date_of_birth
        assert resolved_attributes["work_country"] == work_country


# region check_standard_eligibility
dob = datetime.date(1990, 1, 1)
email = "some@email.com"
member_versioned = MemberVersionedFactory.create(id=1, organization_id=1)
member_versioned_response = (
    service.EligibilityService._convert_member_to_member_response(
        member_versioned, False, member_versioned.id, None
    )
)
member_2 = Member2Factory.create(id=10001, organization_id=1)
member_2_response = service.EligibilityService._convert_member_to_member_response(
    member_2, True, member_versioned.id, member_2.id
)
member_2_wrong_org = Member2Factory.create(id=10001, organization_id=2)


async def test_check_standard_eligibility_return_member_versioned_if_no_need_check_member_2(
    svc,
):
    with mock.patch(
        "app.eligibility.service.EligibilityService._get_by_dob_and_email_v1",
        return_value=member_versioned,
    ), mock.patch(
        "app.eligibility.service.EligibilityService._get_by_dob_and_email_v2",
    ) as mock_get_v2, mock.patch(
        "app.utils.feature_flag.organization_enabled_for_e9y_2_write",
        return_value=False,
    ):
        result = await svc.check_standard_eligibility(date_of_birth=dob, email=email)
        assert result.first_name == member_versioned.first_name
        assert result.last_name == member_versioned.last_name
        assert result.date_of_birth == member_versioned.date_of_birth
        mock_get_v2.assert_not_called()


async def test_check_standard_eligibility_return_picked_when_org_enabled_for_e9y_2(
    svc,
):
    with mock.patch(
        "app.eligibility.service.EligibilityService._get_by_dob_and_email_v1",
        return_value=member_versioned,
    ), mock.patch(
        "app.eligibility.service.EligibilityService._get_by_dob_and_email_v2",
        return_value=member_2,
    ), mock.patch(
        "app.utils.feature_flag.organization_enabled_for_e9y_2_write",
        return_value=True,
    ):
        result = await svc.check_standard_eligibility(date_of_birth=dob, email=email)
        assert result.first_name == member_2.first_name
        assert result.last_name == member_2.last_name
        assert result.date_of_birth == member_2.date_of_birth


async def test_check_standard_eligibility_raise_error_when_not_fully_synced(
    svc,
):
    with mock.patch(
        "app.eligibility.service.EligibilityService._get_by_dob_and_email_v1",
        return_value=member_versioned,
    ), mock.patch(
        "app.eligibility.service.EligibilityService._get_by_dob_and_email_v2",
        return_value=member_2_wrong_org,
    ), mock.patch(
        "app.utils.feature_flag.organization_enabled_for_e9y_2_write",
        return_value=True,
    ):
        with pytest.raises(
            ValueError, match=r"member_versioned and member_2 not fully synced"
        ):
            _ = await svc.check_standard_eligibility(date_of_birth=dob, email=email)


async def test_check_alternate_eligibility_return_member_versioned_if_no_need_check_member_2(
    svc, members_versioned, members_2
):
    with mock.patch(
        "app.utils.eligibility_validation.is_cached_organization_active",
        return_value=True,
    ), mock.patch(
        "app.utils.feature_flag.organization_enabled_for_e9y_2_write",
        return_value=False,
    ):
        members_versioned.get_by_tertiary_verification.side_effect = mock.AsyncMock(
            return_value=[member_versioned]
        )
        result = await svc.check_alternate_eligibility(
            date_of_birth=dob,
            unique_corp_id=member_versioned.unique_corp_id,
            first_name=member_versioned.first_name,
            last_name=member_versioned.last_name,
            work_state=member_versioned.work_state,
        )
        assert result.first_name == member_versioned.first_name
        assert result.last_name == member_versioned.last_name
        assert result.date_of_birth == member_versioned.date_of_birth

    with mock.patch(
        "app.utils.eligibility_validation.is_cached_organization_active",
        return_value=True,
    ), mock.patch(
        "app.utils.feature_flag.organization_enabled_for_e9y_2_write",
        return_value=False,
    ):
        members_versioned.get_by_secondary_verification.side_effect = mock.AsyncMock(
            return_value=[member_versioned]
        )
        result = await svc.check_alternate_eligibility(
            date_of_birth=dob,
            unique_corp_id="",
            first_name=member_versioned.first_name,
            last_name=member_versioned.last_name,
            work_state=member_versioned.work_state,
        )
        assert result.first_name == member_versioned.first_name
        assert result.last_name == member_versioned.last_name
        assert result.date_of_birth == member_versioned.date_of_birth


async def test_check_alternate_eligibility_return_picked_when_org_enabled_for_e9y_2(
    svc, members_versioned, members_2
):
    with mock.patch(
        "app.utils.eligibility_validation.is_cached_organization_active",
        return_value=True,
    ), mock.patch(
        "app.utils.feature_flag.organization_enabled_for_e9y_2_write",
        return_value=True,
    ):
        members_versioned.get_by_tertiary_verification.side_effect = mock.AsyncMock(
            return_value=[member_versioned]
        )
        members_2.get_by_tertiary_verification.side_effect = mock.AsyncMock(
            return_value=[member_2]
        )
        result = await svc.check_alternate_eligibility(
            date_of_birth=dob,
            unique_corp_id=member_2.unique_corp_id,
            first_name=member_2.first_name,
            last_name=member_2.last_name,
            work_state=member_2.work_state,
        )
        assert result.first_name == member_2.first_name
        assert result.last_name == member_2.last_name
        assert result.date_of_birth == member_2.date_of_birth

    with mock.patch(
        "app.utils.eligibility_validation.is_cached_organization_active",
        return_value=True,
    ), mock.patch(
        "app.utils.feature_flag.organization_enabled_for_e9y_2_write",
        return_value=True,
    ):
        members_versioned.get_by_secondary_verification.side_effect = mock.AsyncMock(
            return_value=[member_versioned]
        )
        members_2.get_by_secondary_verification.side_effect = mock.AsyncMock(
            return_value=[member_2]
        )
        result = await svc.check_alternate_eligibility(
            date_of_birth=dob,
            unique_corp_id="",
            first_name=member_2.first_name,
            last_name=member_2.last_name,
            work_state=member_2.work_state,
        )
        assert result.first_name == member_2.first_name
        assert result.last_name == member_2.last_name
        assert result.date_of_birth == member_2.date_of_birth


async def test_check_alternate_eligibility_raise_error_when_not_fully_synced(
    svc, members_versioned, members_2
):
    with mock.patch(
        "app.utils.eligibility_validation.is_cached_organization_active",
        return_value=True,
    ), mock.patch(
        "app.utils.feature_flag.organization_enabled_for_e9y_2_write",
        return_value=True,
    ):
        members_versioned.get_by_tertiary_verification.side_effect = mock.AsyncMock(
            return_value=[member_versioned]
        )
        members_2.get_by_tertiary_verification.side_effect = mock.AsyncMock(
            return_value=[member_2_wrong_org]
        )

        with pytest.raises(
            ValueError, match=r"member_versioned and member_2 not fully synced"
        ):
            _ = await svc.check_alternate_eligibility(
                date_of_birth=dob,
                unique_corp_id=member_2.unique_corp_id,
                first_name=member_2.first_name,
                last_name=member_2.last_name,
                work_state=member_2.work_state,
            )

    with mock.patch(
        "app.utils.eligibility_validation.is_cached_organization_active",
        return_value=True,
    ), mock.patch(
        "app.utils.feature_flag.organization_enabled_for_e9y_2_write",
        return_value=True,
    ):
        members_versioned.get_by_tertiary_verification.side_effect = mock.AsyncMock(
            return_value=[member_versioned]
        )
        members_2.get_by_tertiary_verification.side_effect = mock.AsyncMock(
            return_value=[]
        )

        with pytest.raises(
            errors.AlternateMatchError,
            match=r"No member_2 records found for alternate eligibility",
        ):
            _ = await svc.check_alternate_eligibility(
                date_of_birth=dob,
                unique_corp_id=member_2.unique_corp_id,
                first_name=member_2.first_name,
                last_name=member_2.last_name,
                work_state=member_2.work_state,
            )


async def test_get_by_dob_and_email_v1_happy_path(svc, members_versioned):
    member_list = [member_versioned]
    members_versioned_get = members_versioned.get_by_dob_and_email
    members_versioned_get.side_effect = mock.AsyncMock(return_value=member_list)

    with mock.patch(
        "app.utils.eligibility_validation.is_cached_organization_active",
        return_value=True,
    ):
        result = await svc._get_by_dob_and_email_v1(dob, email)
        assert result == member_versioned


async def test_get_by_dob_and_email_v1_org_not_active(svc, members_versioned):
    member_list = [member_versioned]
    members_versioned_get = members_versioned.get_by_dob_and_email
    members_versioned_get.side_effect = mock.AsyncMock(return_value=member_list)

    with mock.patch(
        "app.utils.eligibility_validation.is_cached_organization_active",
        return_value=False,
    ):
        with pytest.raises(
            errors.MatchError, match=r"No active records found for user."
        ):
            _ = await svc._get_by_dob_and_email_v1(dob, email)


async def test_get_by_dob_and_email_v1_multiple_orgs(svc, members_versioned):
    member_ext = MemberVersionedFactory.create(
        organization_id=member_versioned.organization_id + 1
    )
    member_list = [member_versioned, member_ext]
    members_versioned_get = members_versioned.get_by_dob_and_email
    members_versioned_get.side_effect = mock.AsyncMock(return_value=member_list)

    with mock.patch(
        "app.utils.eligibility_validation.is_cached_organization_active",
        return_value=True,
    ):
        with pytest.raises(
            errors.MatchMultipleError,
            match=r"Multiple organization records found for user.",
        ):
            _ = await svc._get_by_dob_and_email_v1(dob, email)


async def test_get_by_dob_and_email_v2_happy_path(svc, members_2):
    member_2_client_get = members_2.get_by_dob_and_email
    member_2_client_get.side_effect = mock.AsyncMock(return_value=[member_2])

    with mock.patch(
        "app.utils.eligibility_validation.is_cached_organization_active",
        return_value=True,
    ):
        result = await svc._get_by_dob_and_email_v2(dob, email)
        assert result == member_2


async def test_get_by_dob_and_email_v2_org_inactive(svc, members_2, configs):
    member_2_client_get = members_2.get_by_dob_and_email
    member_2_client_get.side_effect = mock.AsyncMock(return_value=[member_2])

    with mock.patch(
        "app.utils.eligibility_validation.is_cached_organization_active",
        return_value=False,
    ):
        with pytest.raises(
            errors.MatchError, match=r"No active records found for user."
        ):
            _ = await svc._get_by_dob_and_email_v2(dob, email)


async def test_get_by_dob_and_email_v2_not_found(svc, members_2):
    member_2_client_get = members_2.get_by_dob_and_email
    member_2_client_get.side_effect = mock.AsyncMock(return_value=[])

    with pytest.raises(errors.StandardMatchError, match=r"Matching member not found."):
        _ = await svc._get_by_dob_and_email_v2(dob, email)


# endregion

# region helper functions
async def test_check_organization_active_status(svc, configs):
    configuration = ConfigurationFactory.create()
    get_configuration_method = getattr(configs, "get")
    get_configuration_method.side_effect = mock.AsyncMock(return_value=configuration)
    _ = await svc._check_organization_active_status(123)
    get_configuration_method.assert_called_once()
    get_configuration_method.reset_mock()

    # test cached, no db query called on second time.
    _ = svc._check_organization_active_status(123)
    get_configuration_method.assert_not_called()


# endregion

# region get_wallet_enablement_by_identity
wallet_enablement_v1 = WalletEnablementResponseFactory.create(
    member_id=10001,
    organization_id=1,
    unique_corp_id="mock_unique_corp_id",
    dependent_id="mock_dependent_id",
    enabled=True,
    insurance_plan="mock_plan_v1",
    is_v2=False,
    member_1_id=10001,
)
wallet_enablement_v2 = WalletEnablementResponseFactory.create(
    member_id=10002,
    organization_id=1,
    unique_corp_id="mock_unique_corp_id",
    dependent_id="mock_dependent_id",
    enabled=True,
    insurance_plan="mock_plan_v2",
    is_v2=True,
    member_2_id=10002,
)


async def test_get_wallet_enablement_by_identity(svc, members_versioned, members_2):
    with mock.patch(
        "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
        return_value=True,
    ):
        members_versioned.get_wallet_enablement_by_identity.side_effect = (
            mock.AsyncMock(return_value=wallet_enablement_v1)
        )
        members_2.get_wallet_enablement_by_identity.side_effect = mock.AsyncMock(
            return_value=wallet_enablement_v2
        )
        result = await svc.get_wallet_enablement_by_identity(
            organization_id=1,
            unique_corp_id="mock_unique_corp_id",
            dependent_id="mock_dependent_id",
        )
        wallet_enablement_v2.member_1_id = 10001
        assert result == wallet_enablement_v2

    with mock.patch(
        "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
        return_value=False,
    ):
        members_versioned.get_wallet_enablement_by_identity.side_effect = (
            mock.AsyncMock(return_value=wallet_enablement_v1)
        )
        result = await svc.get_wallet_enablement_by_identity(
            organization_id=1,
            unique_corp_id="mock_unique_corp_id",
            dependent_id="mock_dependent_id",
        )
        assert result == wallet_enablement_v1


# endregion

# region get_wallet_enablement
async def test_get_wallet_enablement(svc, members_versioned, members_2):
    member_record_v1 = MemberResponseFactory.create(
        id=10001, organization_id=1, member_1_id=10001
    )
    member_record_v2 = MemberResponseFactory.create(
        id=10002, organization_id=1, is_v2=True, member_2_id=10002
    )
    # Found case
    with mock.patch(
        "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
        return_value=True,
    ), mock.patch(
        "app.eligibility.service.EligibilityService.get_by_member_id",
        return_value=member_record_v2,
    ):
        members_versioned.get_wallet_enablement.side_effect = mock.AsyncMock(
            return_value=wallet_enablement_v1
        )
        members_2.get_wallet_enablement.side_effect = mock.AsyncMock(
            return_value=wallet_enablement_v2
        )
        result = await svc.get_wallet_enablement(member_id=10002)
        wallet_enablement_v2.member_1_id = 10002
        assert result == wallet_enablement_v2

    with mock.patch(
        "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
        return_value=False,
    ), mock.patch(
        "app.eligibility.service.EligibilityService.get_by_member_id",
        return_value=member_record_v1,
    ):
        members_versioned.get_wallet_enablement.side_effect = mock.AsyncMock(
            return_value=wallet_enablement_v1
        )
        result = await svc.get_wallet_enablement(member_id=10001)
        assert result == wallet_enablement_v1

    # Not found case
    with mock.patch(
        "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
        return_value=True,
    ), mock.patch(
        "app.eligibility.service.EligibilityService.get_by_member_id",
        return_value=member_record_v2,
    ), pytest.raises(
        errors.GetMatchError
    ):
        members_versioned.get_wallet_enablement.side_effect = mock.AsyncMock(
            return_value=[]
        )
        members_2.get_wallet_enablement.side_effect = mock.AsyncMock(return_value=[])
        result = await svc.get_wallet_enablement(member_id=10002)

    with mock.patch(
        "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
        return_value=False,
    ), mock.patch(
        "app.eligibility.service.EligibilityService.get_by_member_id",
        return_value=member_record_v1,
    ), pytest.raises(
        errors.GetMatchError
    ):
        members_versioned.get_wallet_enablement.side_effect = mock.AsyncMock(
            return_value=[]
        )
        result = await svc.get_wallet_enablement(member_id=10001)


# endregion

# region get_wallet_enablement_by_user_id
async def test_get_wallet_enablement_by_user_id(svc, members_versioned, members_2):
    member_record_v1 = MemberResponseFactory.create(id=10001, organization_id=1)
    member_record_v2 = MemberResponseFactory.create(id=10002, organization_id=1)

    verification_1 = model.VerificationKey(member_id=10001, organization_id=1)
    verification_2 = model.VerificationKey(
        member_id=10001, member_2_id=10002, organization_id=1
    )

    # Found case
    with mock.patch(
        "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
        return_value=True,
    ), mock.patch(
        "app.eligibility.service.EligibilityService.get_by_member_id",
        return_value=member_record_v2,
    ), mock.patch(
        "verification.repository.verification.VerificationRepository.get_verification_key_for_user",
        return_value=verification_2,
    ):
        members_2.get_wallet_enablement.side_effect = mock.AsyncMock(
            return_value=wallet_enablement_v2
        )
        result = await svc.get_wallet_enablement_by_user_id(user_id=1)
        wallet_enablement_v2.member_1_id = 10001
        assert result == wallet_enablement_v2

    with mock.patch(
        "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
        return_value=False,
    ), mock.patch(
        "app.eligibility.service.EligibilityService.get_by_member_id",
        return_value=member_record_v1,
    ), mock.patch(
        "verification.repository.verification.VerificationRepository.get_verification_key_for_user",
        return_value=verification_1,
    ):
        members_versioned.get_wallet_enablement.side_effect = mock.AsyncMock(
            return_value=wallet_enablement_v1
        )
        result = await svc.get_wallet_enablement_by_user_id(user_id=1)
        wallet_enablement_v1.is_v2 = False
        wallet_enablement_v1.member_2_id = None
        assert result == wallet_enablement_v1

    # Not found case
    with mock.patch(
        "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
        return_value=True,
    ), mock.patch(
        "app.eligibility.service.EligibilityService.get_by_member_id",
        return_value=member_record_v2,
    ), mock.patch(
        "verification.repository.verification.VerificationRepository.get_verification_key_for_user",
        return_value=verification_2,
    ), pytest.raises(
        errors.GetMatchError
    ):
        members_versioned.get_wallet_enablement.side_effect = mock.AsyncMock(
            return_value=[]
        )
        members_2.get_wallet_enablement.side_effect = mock.AsyncMock(return_value=[])
        result = await svc.get_wallet_enablement_by_user_id(user_id=2)

    with mock.patch(
        "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
        return_value=False,
    ), mock.patch(
        "app.eligibility.service.EligibilityService.get_by_member_id",
        return_value=member_record_v1,
    ), mock.patch(
        "verification.repository.verification.VerificationRepository.get_verification_key_for_user",
        return_value=verification_1,
    ), pytest.raises(
        errors.GetMatchError
    ):
        members_versioned.get_wallet_enablement.side_effect = mock.AsyncMock(
            return_value=[]
        )
        result = await svc.get_wallet_enablement_by_user_id(user_id=1)

    # Fallback case
    with mock.patch(
        "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
        return_value=True,
    ), mock.patch(
        "verification.repository.verification.VerificationRepository.get_verification_key_for_user",
        return_value=verification_2,
    ):
        members_2.get_wallet_enablement.side_effect = mock.AsyncMock(return_value=None)
        members_versioned.get_wallet_enablement.side_effect = mock.AsyncMock(
            return_value=wallet_enablement_v1
        )
        result = await svc.get_wallet_enablement_by_user_id(user_id=1)
        assert result == wallet_enablement_v1


# endregion

# region get_by_org_identity


@pytest.mark.parametrize(
    argnames="should_check, expected",
    argvalues=[
        (True, member_2_response),
        (False, member_versioned_response),
    ],
)
async def test_get_by_org_identity_return_based_on_org_id(svc, should_check, expected):
    with mock.patch(
        "app.eligibility.service.EligibilityService._get_by_org_identity_v1",
        return_value=member_versioned,
    ), mock.patch(
        "app.eligibility.service.EligibilityService._get_by_org_identity_v2",
        return_value=member_2,
    ), mock.patch(
        "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
        return_value=should_check,
    ):
        result = await svc.get_by_org_identity(
            organization_id=1,
            unique_corp_id="mock_unique_corp_id",
            dependent_id="mock_dependent_id",
        )
        assert result == expected


async def test_get_by_org_identity_fallback_v1(svc):
    with mock.patch(
        "app.eligibility.service.EligibilityService._get_by_org_identity_v1",
        return_value=member_versioned,
    ), mock.patch(
        "app.eligibility.service.EligibilityService._get_by_org_identity_v2",
        return_value=None,
    ), mock.patch(
        "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
        return_value=True,
    ):
        result = await svc.get_by_org_identity(
            organization_id=1,
            unique_corp_id="mock_unique_corp_id",
            dependent_id="mock_dependent_id",
        )
        assert result == member_versioned_response


@pytest.mark.usefixtures("reset_organization_cache")
async def test_get_by_org_identity_v1_happy_path(svc, members_versioned):
    members_versioned.get_by_org_identity.side_effect = mock.AsyncMock(
        return_value=member_versioned
    )

    with mock.patch(
        "app.utils.eligibility_validation.is_organization_activated",
        return_value=True,
    ):
        result = await svc._get_by_org_identity_v1(
            identity=model.OrgIdentity(
                organization_id=1,
                unique_corp_id="mock_unique_corp_id",
                dependent_id="mock_dependent_id",
            )
        )
        assert result == member_versioned


@pytest.mark.usefixtures("reset_organization_cache")
async def test_get_by_org_identity_v1_raise_error_if_not_found(svc, members_versioned):
    members_versioned.get_by_org_identity.side_effect = mock.AsyncMock(
        return_value=None
    )

    with mock.patch(
        "app.utils.eligibility_validation.is_organization_activated",
        return_value=True,
    ), pytest.raises(errors.IdentityMatchError, match=r"Matching member not found."):
        _ = await svc._get_by_org_identity_v1(
            identity=model.OrgIdentity(
                organization_id=1,
                unique_corp_id="mock_unique_corp_id",
                dependent_id="mock_dependent_id",
            )
        )


@pytest.mark.usefixtures("reset_organization_cache")
async def test_get_by_org_identity_v1_raise_error_if_org_not_active(
    svc, members_versioned
):
    members_versioned.get_by_org_identity.side_effect = mock.AsyncMock(
        return_value=member_versioned
    )

    with mock.patch(
        "app.utils.eligibility_validation.is_organization_activated",
        return_value=False,
    ), pytest.raises(errors.IdentityMatchError, match=r"Matching member not found."):
        _ = await svc._get_by_org_identity_v1(
            identity=model.OrgIdentity(
                organization_id=1,
                unique_corp_id="mock_unique_corp_id",
                dependent_id="mock_dependent_id",
            )
        )


async def test_get_by_org_identity_v2_happy_path(svc, members_2):
    members_2.get_by_org_identity.side_effect = mock.AsyncMock(return_value=member_2)

    with mock.patch(
        "app.eligibility.service.EligibilityService._check_organization_active_status",
        return_value=True,
    ):
        result = await svc._get_by_org_identity_v2(
            identity=model.OrgIdentity(
                organization_id=1,
                unique_corp_id="mock_unique_corp_id",
                dependent_id="mock_dependent_id",
            )
        )
        assert result == member_2


async def test_get_by_org_identity_v2_returns_none_if_not_found(svc, members_2):
    members_2.get_by_org_identity.side_effect = mock.AsyncMock(return_value=None)

    with mock.patch(
        "app.eligibility.service.EligibilityService._check_organization_active_status",
        return_value=True,
    ):
        result = await svc._get_by_org_identity_v2(
            identity=model.OrgIdentity(
                organization_id=1,
                unique_corp_id="mock_unique_corp_id",
                dependent_id="mock_dependent_id",
            )
        )
        assert result is None


async def test_get_by_org_identity_v2_raise_error_if_org_not_active(svc, members_2):
    members_2.get_by_org_identity.side_effect = mock.AsyncMock(return_value=member_2)

    with mock.patch(
        "app.eligibility.service.EligibilityService._check_organization_active_status",
        return_value=False,
    ), pytest.raises(errors.IdentityMatchError, match=r"Organization not active"):
        _ = await svc._get_by_org_identity_v2(
            identity=model.OrgIdentity(
                organization_id=1,
                unique_corp_id="mock_unique_corp_id",
                dependent_id="mock_dependent_id",
            )
        )


# endregion

# region create_verification_for_user


@pytest.mark.parametrize(
    argnames="org_enabled_e9y_2,create_v1_call_count,create_v2_call_count",
    argvalues=[
        (True, 0, 1),
        (False, 1, 0),
    ],
)
async def test_create_verification_for_user_based_on_org_enabled_for_e9y_2(
    svc, org_enabled_e9y_2, create_v1_call_count, create_v2_call_count
):
    with mock.patch(
        "app.utils.feature_flag.organization_enabled_for_e9y_2_write",
        return_value=org_enabled_e9y_2,
    ), mock.patch(
        "app.eligibility.service.EligibilityService._create_verification_for_user_v1",
        return_value=org_enabled_e9y_2,
    ) as mock_v1, mock.patch(
        "app.eligibility.service.EligibilityService._create_verification_for_user_v2",
        return_value=org_enabled_e9y_2,
    ) as mock_v2, mock.patch(
        "app.eligibility.service.EligibilityService.get_verification_for_user",
        return_value=None,
    ):
        _ = await svc.create_verification_for_user(
            organization_id=1,
            verification_type="STANDARD",
            unique_corp_id="mocked",
            user_id=1001,
        )
        assert mock_v1.call_count == create_v1_call_count
        assert mock_v2.call_count == create_v2_call_count


class TestCanCreateVerification2:
    verification_2 = data_models.Verification2Factory.create(
        user_id=10,
        organization_id=102,
        member_id=1234,
        member_version=1001,
    )
    configuration = data_models.ConfigurationFactory.create(
        employee_only=False,
        medical_plan_only=False,
    )
    employee_only_configuration = data_models.ConfigurationFactory.create(
        employee_only=True
    )
    medical_plan_only_configuration = data_models.ConfigurationFactory.create(
        medical_plan_only=True,
    )
    beneficiaries_enabled_member_2 = data_models.Member2Factory.create(
        id=100, record={"beneficiaries_enabled": True}
    )
    beneficiaries_disabled_member_2 = data_models.Member2Factory.create(
        id=101, record={"beneficiaries_enabled": False}
    )

    @staticmethod
    @pytest.mark.parametrize(
        argnames="verification_2,configuration,member_2,expected",
        argvalues=[
            (ValueError("mocked"), None, None, True),
            (verification_2, employee_only_configuration, None, False),
            (
                verification_2,
                medical_plan_only_configuration,
                beneficiaries_disabled_member_2,
                False,
            ),
            (verification_2, configuration, beneficiaries_disabled_member_2, True),
            (
                verification_2,
                medical_plan_only_configuration,
                beneficiaries_enabled_member_2,
                True,
            ),
        ],
        ids=[
            "existing_verification_not_found",
            "employee_only_org",
            "medical_plan_only_org_and_beneficiaries_disabled_member",
            "common_org_and_beneficiaries_disabled_member",
            "medical_plan_only_org_and_beneficiaries_enabled_member",
        ],
    )
    async def test_can_create_verification_2(
        verification_2, configuration, member_2, expected
    ):
        res = EligibilityService.can_create_verification_2(
            verification_2, configuration, member_2
        )
        assert res == expected


class TestCreateVerificationForUserV2:
    organization_id = 1
    verification_type = "STANDARD"
    unique_corp_id = "mock_unique_corp_id"
    dependent_id = "mock_dependent_id"
    first_name = "mock_first_name"
    last_name = "mock_last_name"
    email = "mock_email"
    work_state = "mock_work_state"
    user_id = 1001
    date_of_birth = datetime.date(1999, 1, 12)
    additional_fields = {}
    verification_session = None
    eligibility_member_id = 10002

    configuration = data_models.ConfigurationFactory.create()
    member_2 = data_models.Member2Factory.create(id=199999)
    member_versioned = data_models.MemberVersionedFactory.create(id=199)

    @staticmethod
    async def test_create_without_eligibility_member_id(svc):
        with mock.patch(
            "verification.repository.verification.VerificationRepository.create_verification_dual_write"
        ) as mock_repo_create:
            _ = await svc._create_verification_for_user_v2(
                organization_id=TestCreateVerificationForUserV2.organization_id,
                verification_type=TestCreateVerificationForUserV2.verification_type,
                unique_corp_id=TestCreateVerificationForUserV2.unique_corp_id,
                dependent_id=TestCreateVerificationForUserV2.dependent_id,
                first_name=TestCreateVerificationForUserV2.first_name,
                last_name=TestCreateVerificationForUserV2.last_name,
                email=TestCreateVerificationForUserV2.email,
                work_state=TestCreateVerificationForUserV2.work_state,
                user_id=TestCreateVerificationForUserV2.user_id,
                date_of_birth=TestCreateVerificationForUserV2.date_of_birth,
                additional_fields=TestCreateVerificationForUserV2.additional_fields,
                verification_session=TestCreateVerificationForUserV2.verification_session,
            )
            mock_repo_create.assert_called_once_with(
                user_id=TestCreateVerificationForUserV2.user_id,
                verification_type=TestCreateVerificationForUserV2.verification_type,
                organization_id=TestCreateVerificationForUserV2.organization_id,
                unique_corp_id=TestCreateVerificationForUserV2.unique_corp_id,
                dependent_id=TestCreateVerificationForUserV2.dependent_id,
                first_name=TestCreateVerificationForUserV2.first_name,
                last_name=TestCreateVerificationForUserV2.last_name,
                email=TestCreateVerificationForUserV2.email,
                work_state=TestCreateVerificationForUserV2.work_state,
                date_of_birth=TestCreateVerificationForUserV2.date_of_birth,
                additional_fields=TestCreateVerificationForUserV2.additional_fields,
                verification_session=TestCreateVerificationForUserV2.verification_session,
                verified_at=mock.ANY,
                deactivated_at=None,
                eligibility_member_1_id=None,
                eligibility_member_2_id=None,
                eligibility_member_2_version=None,
            )

    @staticmethod
    async def test_create_with_eligibility_member_id_when_org_not_found(
        svc, configs, members_2
    ):
        with mock.patch(
            "verification.repository.verification.VerificationRepository.get_verification_2_for_member",
            return_value=None,
        ), pytest.raises(
            errors.CreateVerificationError,
            match=r"Configuration not found for organization_id=1",
        ):
            configs.get.side_effect = mock.AsyncMock(return_value=None)
            members_2.get.side_effect = mock.AsyncMock(return_value=None)
            _ = await svc._create_verification_for_user_v2(
                organization_id=TestCreateVerificationForUserV2.organization_id,
                verification_type=TestCreateVerificationForUserV2.verification_type,
                unique_corp_id=TestCreateVerificationForUserV2.unique_corp_id,
                dependent_id=TestCreateVerificationForUserV2.dependent_id,
                first_name=TestCreateVerificationForUserV2.first_name,
                last_name=TestCreateVerificationForUserV2.last_name,
                email=TestCreateVerificationForUserV2.email,
                work_state=TestCreateVerificationForUserV2.work_state,
                user_id=TestCreateVerificationForUserV2.user_id,
                date_of_birth=TestCreateVerificationForUserV2.date_of_birth,
                additional_fields=TestCreateVerificationForUserV2.additional_fields,
                verification_session=TestCreateVerificationForUserV2.verification_session,
                eligibility_member_id=TestCreateVerificationForUserV2.eligibility_member_id,
            )

    @staticmethod
    async def test_create_with_eligibility_member_id_when_member_2_not_found(
        svc, configs, members_2
    ):
        with mock.patch(
            "app.eligibility.service.EligibilityService.get_by_member_id",
            return_value=None,
        ), pytest.raises(
            errors.CreateVerificationError,
            match=r"Member not found for eligibility_member_id*",
        ):
            configs.get.side_effect = mock.AsyncMock(
                return_value=TestCreateVerificationForUserV2.configuration
            )
            members_2.get.side_effect = mock.AsyncMock(return_value=None)
            _ = await svc._create_verification_for_user_v2(
                organization_id=TestCreateVerificationForUserV2.organization_id,
                verification_type=TestCreateVerificationForUserV2.verification_type,
                unique_corp_id=TestCreateVerificationForUserV2.unique_corp_id,
                dependent_id=TestCreateVerificationForUserV2.dependent_id,
                first_name=TestCreateVerificationForUserV2.first_name,
                last_name=TestCreateVerificationForUserV2.last_name,
                email=TestCreateVerificationForUserV2.email,
                work_state=TestCreateVerificationForUserV2.work_state,
                user_id=TestCreateVerificationForUserV2.user_id,
                date_of_birth=TestCreateVerificationForUserV2.date_of_birth,
                additional_fields=TestCreateVerificationForUserV2.additional_fields,
                verification_session=TestCreateVerificationForUserV2.verification_session,
                eligibility_member_id=TestCreateVerificationForUserV2.eligibility_member_id,
            )

    @staticmethod
    async def test_create_with_eligibility_member_id_when_aleady_claimed(
        svc, configs, members_2
    ):
        with mock.patch(
            "verification.repository.verification.VerificationRepository.get_verification_2_for_member",
            return_value=data_models.Verification2Factory.create(),
        ), mock.patch(
            "app.eligibility.service.EligibilityService.can_create_verification_2",
            return_value=False,
        ), mock.patch(
            "verification.repository.verification.VerificationRepository.get_verification_key_1_for_verification_2_id",
            return_value=model.VerificationKey(
                member_id=1,
                organization_id=1,
            ),
        ), pytest.raises(
            errors.CreateVerificationError,
            match=r"Failed can_create_verification_2 check for eligibility_member_id*",
        ):
            configs.get.side_effect = mock.AsyncMock(
                return_value=TestCreateVerificationForUserV2.configuration
            )
            members_2.get.side_effect = mock.AsyncMock(
                return_value=TestCreateVerificationForUserV2.member_2
            )
            _ = await svc._create_verification_for_user_v2(
                organization_id=TestCreateVerificationForUserV2.organization_id,
                verification_type=TestCreateVerificationForUserV2.verification_type,
                unique_corp_id=TestCreateVerificationForUserV2.unique_corp_id,
                dependent_id=TestCreateVerificationForUserV2.dependent_id,
                first_name=TestCreateVerificationForUserV2.first_name,
                last_name=TestCreateVerificationForUserV2.last_name,
                email=TestCreateVerificationForUserV2.email,
                work_state=TestCreateVerificationForUserV2.work_state,
                user_id=TestCreateVerificationForUserV2.user_id,
                date_of_birth=TestCreateVerificationForUserV2.date_of_birth,
                additional_fields=TestCreateVerificationForUserV2.additional_fields,
                verification_session=TestCreateVerificationForUserV2.verification_session,
                eligibility_member_id=TestCreateVerificationForUserV2.eligibility_member_id,
            )

    @staticmethod
    async def test_create_with_eligibility_member_id_exception_logged_1(
        svc, configs, members_2
    ):
        db_exception = Exception("db failed")
        configs.get.side_effect = db_exception
        members_2.get.side_effect = mock.AsyncMock(
            return_value=TestCreateVerificationForUserV2.member_2
        )

        with mock.patch(
            "verification.repository.verification.VerificationRepository.get_verification_2_for_member",
            return_value=None,
        ), mock.patch("app.eligibility.service.logger") as mock_logger:
            try:
                _ = await svc._create_verification_for_user_v2(
                    organization_id=TestCreateVerificationForUserV2.organization_id,
                    verification_type=TestCreateVerificationForUserV2.verification_type,
                    unique_corp_id=TestCreateVerificationForUserV2.unique_corp_id,
                    dependent_id=TestCreateVerificationForUserV2.dependent_id,
                    first_name=TestCreateVerificationForUserV2.first_name,
                    last_name=TestCreateVerificationForUserV2.last_name,
                    email=TestCreateVerificationForUserV2.email,
                    work_state=TestCreateVerificationForUserV2.work_state,
                    user_id=TestCreateVerificationForUserV2.user_id,
                    date_of_birth=TestCreateVerificationForUserV2.date_of_birth,
                    additional_fields=TestCreateVerificationForUserV2.additional_fields,
                    verification_session=TestCreateVerificationForUserV2.verification_session,
                    eligibility_member_id=TestCreateVerificationForUserV2.eligibility_member_id,
                )
            except Exception:
                mock_logger.error.assert_called_once_with(
                    "Failed to get configuration of v2",
                    organization_id=1,
                    details=db_exception,
                )

    @staticmethod
    async def test_create_with_eligibility_member_id_exception_logged_2(
        svc, configs, members_2, members_versioned
    ):
        db_exception = Exception("db failed")
        configs.get.side_effect = mock.AsyncMock(
            return_value=TestCreateVerificationForUserV2.configuration
        )
        members_2.get.side_effect = mock.AsyncMock(
            return_value=TestCreateVerificationForUserV2.member_2
        )
        members_versioned.get_by_member_2.side_effect = mock.AsyncMock(
            return_value=TestCreateVerificationForUserV2.member_versioned
        )

        with mock.patch(
            "verification.repository.verification.VerificationRepository.get_verification_2_for_member",
            side_effect=db_exception,
        ), mock.patch(
            "verification.repository.verification.VerificationRepository.create_verification_dual_write",
        ), mock.patch(
            "app.eligibility.service.logger"
        ) as mock_logger:
            _ = await svc._create_verification_for_user_v2(
                organization_id=TestCreateVerificationForUserV2.organization_id,
                verification_type=TestCreateVerificationForUserV2.verification_type,
                unique_corp_id=TestCreateVerificationForUserV2.unique_corp_id,
                dependent_id=TestCreateVerificationForUserV2.dependent_id,
                first_name=TestCreateVerificationForUserV2.first_name,
                last_name=TestCreateVerificationForUserV2.last_name,
                email=TestCreateVerificationForUserV2.email,
                work_state=TestCreateVerificationForUserV2.work_state,
                user_id=TestCreateVerificationForUserV2.user_id,
                date_of_birth=TestCreateVerificationForUserV2.date_of_birth,
                additional_fields=TestCreateVerificationForUserV2.additional_fields,
                verification_session=TestCreateVerificationForUserV2.verification_session,
                eligibility_member_id=TestCreateVerificationForUserV2.eligibility_member_id,
            )
            mock_logger.error.assert_called_once_with(
                "Failed to get existing verification_2 of v2",
                member_id=10002,
                details=db_exception,
            )

    @staticmethod
    async def test_create_with_eligibility_member_id_happy_path(
        svc, configs, members_2, members_versioned
    ):
        with mock.patch(
            "verification.repository.verification.VerificationRepository.get_verification_2_for_member",
            return_value=data_models.Verification2Factory.create(),
        ), mock.patch(
            "app.eligibility.service.EligibilityService.verify_eligibility_record_usable",
            return_value=True,
        ), mock.patch(
            "verification.repository.verification.VerificationRepository.create_verification_dual_write"
        ) as mock_repo_create, mock.patch(
            "app.eligibility.service.EligibilityService.get_by_member_id",
            return_value=member_2_response,
        ):
            configs.get.side_effect = mock.AsyncMock(
                return_value=TestCreateVerificationForUserV2.configuration
            )
            members_2.get.side_effect = mock.AsyncMock(
                return_value=TestCreateVerificationForUserV2.member_2
            )
            members_versioned.get_by_member_2.side_effect = mock.AsyncMock(
                return_value=member_versioned
            )
            _ = await svc._create_verification_for_user_v2(
                organization_id=TestCreateVerificationForUserV2.organization_id,
                verification_type=TestCreateVerificationForUserV2.verification_type,
                unique_corp_id=TestCreateVerificationForUserV2.unique_corp_id,
                dependent_id=TestCreateVerificationForUserV2.dependent_id,
                first_name=TestCreateVerificationForUserV2.first_name,
                last_name=TestCreateVerificationForUserV2.last_name,
                email=TestCreateVerificationForUserV2.email,
                work_state=TestCreateVerificationForUserV2.work_state,
                user_id=TestCreateVerificationForUserV2.user_id,
                date_of_birth=TestCreateVerificationForUserV2.date_of_birth,
                additional_fields=TestCreateVerificationForUserV2.additional_fields,
                verification_session=TestCreateVerificationForUserV2.verification_session,
                eligibility_member_id=TestCreateVerificationForUserV2.eligibility_member_id,
            )
            mock_repo_create.assert_called_once_with(
                user_id=TestCreateVerificationForUserV2.user_id,
                verification_type=TestCreateVerificationForUserV2.verification_type,
                organization_id=TestCreateVerificationForUserV2.organization_id,
                unique_corp_id=TestCreateVerificationForUserV2.unique_corp_id,
                dependent_id=TestCreateVerificationForUserV2.dependent_id,
                first_name=TestCreateVerificationForUserV2.first_name,
                last_name=TestCreateVerificationForUserV2.last_name,
                email=TestCreateVerificationForUserV2.email,
                work_state=TestCreateVerificationForUserV2.work_state,
                date_of_birth=TestCreateVerificationForUserV2.date_of_birth,
                additional_fields=TestCreateVerificationForUserV2.additional_fields,
                verification_session=TestCreateVerificationForUserV2.verification_session,
                verified_at=mock.ANY,
                deactivated_at=None,
                eligibility_member_1_id=member_2_response.member_1_id,
                eligibility_member_2_id=member_2_response.member_2_id,
                eligibility_member_2_version=member_2_response.version,
            )


# endregion


# region get_by_member_id
@pytest.mark.parametrize(
    argnames="member_versioned_found, member_2_found, org_enabled_e9y_2, expected",
    argvalues=[
        (member_versioned, member_2, True, member_2_response),
        (member_versioned, member_2, False, member_versioned_response),
        (member_versioned, None, True, member_versioned_response),
        (member_versioned, None, False, member_versioned_response),
        (None, member_2, True, None),
        (None, member_2, False, None),
        (None, None, True, None),
        (None, None, False, None),
    ],
    ids=[
        "return member_2 when member_versioned found, member_2 found, org enabled for e9y2",
        "return member_versioned when member_versioned found, member_2 found, org not enabled for e9y2",
        "raise error when member_versioned found, member_2 not found, org enabled for e9y2",
        "return member_versioned when member_versioned found, member_2 not found, org not enabled for e9y2",
        "raise error when member_versioned not found, member_2 found, org enabled for e9y2",
        "raise error when member_versioned not found, member_2 found, org not enabled for e9y2",
        "raise error when member_versioned not found, member_2 not found, org enabled for e9y2",
        "raise error when member_versioned not found, member_2 not found, org not enabled for e9y2",
    ],
)
async def test_get_by_member_id(
    svc,
    members_versioned,
    members_2,
    member_versioned_found,
    member_2_found,
    org_enabled_e9y_2,
    expected,
):
    with mock.patch(
        "app.utils.feature_flag.organization_enabled_for_e9y_2_read",
        return_value=org_enabled_e9y_2,
    ):
        members_versioned.get.side_effect = mock.AsyncMock(
            return_value=member_versioned_found
        )
        members_2.get_by_member_versioned.side_effect = mock.AsyncMock(
            return_value=member_2_found
        )
        if expected is None:
            with pytest.raises(errors.MatchError):
                _ = await svc.get_by_member_id(id=1)
        else:
            result = await svc.get_by_member_id(id=1)
            assert result == expected


# endregion
