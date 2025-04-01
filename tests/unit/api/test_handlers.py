import datetime
from typing import List, Tuple
from unittest import mock
from unittest.mock import patch

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.wrappers_pb2 import Int64Value
from grpclib import Status
from maven_schemas import eligibility_pb2 as e9ypb
from maven_schemas.eligibility import eligibility_test_utility_pb2 as teste9ypb
from maven_schemas.eligibility import pre_eligibility_pb2 as pre9ypb
from tests.factories import data_models
from tests.factories import data_models as factory
from tests.factories.data_models import Member2Factory
from tests.unit.eligibility.test_translate import (
    active_member,
    future_date,
    inactive_member_different_org,
    inactive_member_same_org,
    past_date,
)

from api import handlers
from app.eligibility import errors, service, translate
from app.eligibility.constants import MatchType
from db import model

pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="module", autouse=True)
def MockStream():
    with mock.patch(
        "grpclib.server.Stream", autospec=True, spec_set=True
    ) as MockStream:
        yield MockStream


@pytest.fixture(scope="module", autouse=True)
def MockInternalEligibilityService():
    with mock.patch(
        "app.eligibility.service.EligibilityService", autospec=True
    ) as MockService:
        yield MockService


@pytest.fixture(scope="module", autouse=True)
def MockInternalEligibilityQueryService():
    with mock.patch(
        "app.eligibility.query_service.EligibilityQueryService", autospec=True
    ) as MockService:
        yield MockService


@pytest.fixture(scope="module", autouse=True)
def MockInternalPreEligibilityService():
    with mock.patch(
        "app.eligibility.service.PreEligibilityService", autospec=True
    ) as MockService:
        yield MockService


@pytest.fixture(scope="module", autouse=True)
def MockInternalEligibilityTestUtilityService():
    with mock.patch(
        "app.eligibility.service.EligibilityTestUtilityService", autospec=True
    ) as MockService:
        yield MockService


@pytest.fixture
def svc(MockInternalEligibilityService):
    yield MockInternalEligibilityService.return_value
    MockInternalEligibilityService.reset_mock()


@pytest.fixture
def query_svc(MockInternalEligibilityQueryService):
    yield MockInternalEligibilityQueryService.return_value
    MockInternalEligibilityQueryService.reset_mock()


@pytest.fixture
def pre9y_svc(MockInternalPreEligibilityService):
    yield MockInternalPreEligibilityService.return_value
    MockInternalPreEligibilityService.reset_mock()


@pytest.fixture
def teste9y_svc(MockInternalEligibilityTestUtilityService):
    yield MockInternalEligibilityTestUtilityService.return_value
    MockInternalEligibilityTestUtilityService.reset_mock()


@pytest.fixture(scope="function")
def wallet():
    return factory.WalletEnablementFactory.create()


@pytest.fixture(scope="function")
def wallet_response():
    return factory.WalletEnablementResponseFactory.create()


@pytest.fixture(scope="function")
def wallet_no_end_date():
    return factory.WalletEnablementFactory.create(
        effective_range=factory.DateRangeFactory.create(upper=None)
    )


@pytest.fixture(scope="function")
def wallet_response_no_end_date():
    return factory.WalletEnablementResponseFactory.create(
        effective_range=factory.DateRangeFactory.create(upper=None)
    )


@pytest.fixture(scope="function")
def eligibility_verification_for_user():
    return factory.EligibilityVerificationForUserFactory.create(
        record="", date_of_birth=datetime.date.today(), additional_fields=""
    )


@pytest.fixture(scope="function")
def eligibility_verifications_for_user_multiple_orgs(
    active_organizations: List[Tuple[int, bool]]
):
    verifications = []
    for organization_id, is_active in active_organizations:
        verification_deactivated_at = datetime.datetime.today() + datetime.timedelta(
            days=2
        )
        if not is_active:
            verification_deactivated_at = (
                datetime.datetime.today() - datetime.timedelta(days=2)
            )
        verification = factory.EligibilityVerificationForUserFactory.create(
            organization_id=organization_id,
            record="",
            date_of_birth=datetime.date.today(),
            additional_fields="",
            verification_deactivated_at=verification_deactivated_at,
        )
        verifications.append(verification)
    return verifications


def _datetime_to_timestamp(dt: datetime) -> Timestamp:
    """Converts a Python datetime object to a Protobuf Timestamp."""
    return Timestamp(seconds=int(dt.timestamp()), nanos=dt.microsecond * 1000)


@pytest.fixture(scope="function")
def failed_verification_attempt():
    return factory.FailedVerificationAttemptResponseFactory.create(
        date_of_birth=datetime.date.today()
    )


@pytest.fixture(scope="function")
def pre_eligibility_organization():
    return factory.PreEligibilityOrganizationFactory.create()


@pytest.fixture(scope="function")
def pre_eligibility_response():
    return factory.PreEligibilityResponseFactory.create()


def test_create_member_resolve_effective_range(member):
    # Given
    member.effective_range = model.DateRange(upper=datetime.date.today())
    # When
    response = handlers._create_member_response(member)
    # Then
    assert response.effective_range
    assert response.effective_range.upper == member.effective_range.upper.isoformat()


def test_create_member_non_string_custom_attributes_types(member):
    # Given
    member.custom_attributes = {
        "one": "1",
        "two": 2,
        "three": {"inner_key": "inner_value"},
    }
    # When
    response = handlers._create_member_response(member)
    # Then
    assert response.custom_attributes == {
        "one": "1",
        "two": "2",
        "three": '{"inner_key":"inner_value"}',
    }


def test_create_member_with_no_custom_attributes(member):
    # Given
    member.custom_attributes = None
    # When
    response = handlers._create_member_response(member)
    # Then
    assert not response.custom_attributes


def test_create_member_with_member_version_0_for_v1(member):
    response = handlers._create_member_response(member)
    assert response.version == 0


def test_create_member_with_correct_member_version_for_v2():
    member_2 = Member2Factory.create(id=1001, version=1000)
    response = handlers._create_member_response(member_2)
    assert response.version == 1000


@pytest.fixture
def expected_response(member):
    lower = member.effective_range.lower and member.effective_range.lower.isoformat()
    upper = member.effective_range.upper and member.effective_range.upper.isoformat()
    created_at, updated_at = Timestamp(), Timestamp()
    created_at.FromDatetime(member.created_at)
    updated_at.FromDatetime(member.updated_at)
    return e9ypb.Member(
        id=member.id,
        record=translate.dump(member.record),
        custom_attributes=member.custom_attributes,
        organization_id=member.organization_id,
        file_id=member.file_id,
        first_name=member.first_name,
        last_name=member.last_name,
        date_of_birth=member.date_of_birth.isoformat(),
        work_state=member.work_state,
        work_country=member.work_country,
        email=member.email,
        unique_corp_id=member.unique_corp_id,
        dependent_id=member.dependent_id,
        effective_range=e9ypb.DateRange(
            lower=lower,
            upper=upper,
            lower_inc=member.effective_range.lower_inc,
            upper_inc=member.effective_range.upper_inc,
        ),
        employer_assigned_id=member.employer_assigned_id,
        created_at=created_at,
        updated_at=updated_at,
    )


@pytest.fixture
def expected_member_list_response(member_list):
    return_val = []
    for m in member_list:
        lower = m.effective_range.lower and m.effective_range.lower.isoformat()
        upper = m.effective_range.upper and m.effective_range.upper.isoformat()
        created_at, updated_at = Timestamp(), Timestamp()
        created_at.FromDatetime(m.created_at)
        updated_at.FromDatetime(m.updated_at)
        return_val.append(
            e9ypb.Member(
                id=m.id,
                record=translate.dump(m.record),
                custom_attributes=m.custom_attributes,
                organization_id=m.organization_id,
                file_id=m.file_id,
                first_name=m.first_name,
                last_name=m.last_name,
                date_of_birth=m.date_of_birth.isoformat(),
                work_state=m.work_state,
                work_country=m.work_country,
                email=m.email,
                unique_corp_id=m.unique_corp_id,
                dependent_id=m.dependent_id,
                effective_range=e9ypb.DateRange(
                    lower=lower,
                    upper=upper,
                    lower_inc=m.effective_range.lower_inc,
                    upper_inc=m.effective_range.upper_inc,
                ),
                employer_assigned_id=m.employer_assigned_id,
                created_at=created_at,
                updated_at=updated_at,
            )
        )
    return e9ypb.MemberList(member_list=return_val)


@pytest.fixture
def expected_wallet_response(wallet_response):
    created_at, updated_at = Timestamp(), Timestamp()
    created_at.FromDatetime(wallet_response.created_at)
    updated_at.FromDatetime(wallet_response.updated_at)
    eligibility_end_date = (
        wallet_response.effective_range.upper
        and wallet_response.effective_range.upper.isoformat()
    )
    return e9ypb.WalletEnablement(
        member_id=wallet_response.member_id,
        organization_id=wallet_response.organization_id,
        enabled=wallet_response.enabled,
        insurance_plan=wallet_response.insurance_plan,
        start_date=wallet_response.start_date.isoformat(),
        eligibility_date=wallet_response.eligibility_date.isoformat(),
        created_at=created_at,
        updated_at=updated_at,
        eligibility_end_date=eligibility_end_date,
        is_v2=wallet_response.is_v2,
        member_1_id=wallet_response.member_1_id,
        member_2_id=wallet_response.member_2_id,
    )


@pytest.fixture
def expected_verification_for_user_response(eligibility_verification_for_user):
    return _convert_to_verification_response(eligibility_verification_for_user)


def expected_verifications_for_user_multiple_orgs_response(verification_records):
    response = []
    for record in verification_records:
        response.append(_convert_to_verification_response(record))
    return e9ypb.VerificationList(verification_list=response)


def _convert_to_verification_response(eligibility_verification_for_user):
    lower = eligibility_verification_for_user.effective_range.lower
    upper = eligibility_verification_for_user.effective_range.upper
    effective_range = e9ypb.DateRange(
        lower=lower and lower.isoformat(),
        upper=upper and upper.isoformat(),
        lower_inc=eligibility_verification_for_user.effective_range.lower_inc,
        upper_inc=eligibility_verification_for_user.effective_range.upper_inc,
    )
    (
        verification_created_at,
        verification_updated_at,
        verification_deactivated_at,
        verification_verified_at,
    ) = (
        Timestamp(),
        Timestamp(),
        Timestamp(),
        Timestamp(),
    )
    verification_created_at.FromDatetime(
        eligibility_verification_for_user.verification_created_at
    )
    verification_updated_at.FromDatetime(
        eligibility_verification_for_user.verification_updated_at
    )
    verification_deactivated_at.FromDatetime(
        eligibility_verification_for_user.verification_deactivated_at
    )
    verification_verified_at.FromDatetime(eligibility_verification_for_user.verified_at)

    return e9ypb.VerificationForUser(
        verification_id=eligibility_verification_for_user.verification_id,
        user_id=eligibility_verification_for_user.user_id,
        organization_id=eligibility_verification_for_user.organization_id,
        unique_corp_id=eligibility_verification_for_user.unique_corp_id,
        eligibility_member_id=str(
            eligibility_verification_for_user.eligibility_member_id
        ),
        record="{}",
        additional_fields="{}",
        dependent_id=eligibility_verification_for_user.dependent_id,
        first_name=eligibility_verification_for_user.first_name,
        last_name=eligibility_verification_for_user.last_name,
        date_of_birth=eligibility_verification_for_user.date_of_birth.isoformat(),
        work_state=eligibility_verification_for_user.work_state,
        email=eligibility_verification_for_user.email,
        effective_range=effective_range,
        verification_type=eligibility_verification_for_user.verification_type,
        employer_assigned_id=eligibility_verification_for_user.employer_assigned_id,
        verification_created_at=verification_created_at,
        verification_updated_at=verification_updated_at,
        verification_deactivated_at=verification_deactivated_at,
        gender_code=eligibility_verification_for_user.gender_code,
        do_not_contact=eligibility_verification_for_user.do_not_contact,
        verified_at=verification_verified_at,
        verification_session=eligibility_verification_for_user.verification_session,
        is_v2=eligibility_verification_for_user.is_v2,
        verification_1_id=eligibility_verification_for_user.verification_1_id,
        verification_2_id=eligibility_verification_for_user.verification_2_id,
    )


@pytest.fixture
def expected_failed_verification_response(failed_verification_attempt):
    created_at, updated_at, verified_at = (Timestamp(), Timestamp(), Timestamp())
    created_at.FromDatetime(failed_verification_attempt.created_at)
    updated_at.FromDatetime(failed_verification_attempt.updated_at)
    verified_at.FromDatetime(failed_verification_attempt.verified_at)

    return e9ypb.VerificationAttempt(
        organization_id=str(failed_verification_attempt.organization_id),
        unique_corp_id=failed_verification_attempt.unique_corp_id,
        dependent_id=failed_verification_attempt.dependent_id,
        first_name=failed_verification_attempt.first_name,
        last_name=failed_verification_attempt.last_name,
        date_of_birth=failed_verification_attempt.date_of_birth.isoformat(),
        work_state=failed_verification_attempt.work_state,
        email=failed_verification_attempt.email,
        verification_type=failed_verification_attempt.verification_type,
        created_at=created_at,
        updated_at=updated_at,
        policy_used=failed_verification_attempt.policy_used,
        successful_verification=failed_verification_attempt.successful_verification,
        verified_at=verified_at,
        additional_fields="{}",
        is_v2=failed_verification_attempt.is_v2,
        verification_attempt_1_id=failed_verification_attempt.verification_attempt_1_id,
        verification_attempt_2_id=failed_verification_attempt.verification_attempt_2_id,
        eligibility_member_id=failed_verification_attempt.eligibility_member_id,
        eligibility_member_2_id=failed_verification_attempt.eligibility_member_2_id,
    )


@pytest.fixture
def expected_eligible_features_response():
    return factory.GetEligibleFeaturesForUserResponse.create()


@pytest.fixture
def expected_deactivated_record():
    return factory.DeactivatedVerificationFactory.create()


@pytest.fixture
def expected_pre_eligibility_organization(pre_eligibility_organization):
    eligibility_end_date = Timestamp()
    eligibility_end_date.FromDatetime(pre_eligibility_organization.eligibility_end_date)
    return pre9ypb.PreEligibilityOrganization(
        organization_id=pre_eligibility_organization.organization_id,
        eligibility_end_date=eligibility_end_date,
    )


@pytest.fixture
def expected_pre_eligibility_response(pre_eligibility_response):
    match_type = MatchType.UNKNOWN_ELIGIBILITY
    return pre9ypb.PreEligibilityResponse(
        match_type=match_type, pre_eligibility_organizations=list()
    )


@pytest.mark.parametrize(
    argnames="handler_name,query_name,message",
    argvalues=[
        (
            "CheckStandardEligibility",
            "check_standard_eligibility",
            e9ypb.StandardEligibilityRequest(
                date_of_birth=datetime.date.today().isoformat(),
                company_email="foo@email.net",
            ),
        ),
        (
            "CheckAlternateEligibility",
            "check_alternate_eligibility",
            e9ypb.AlternateEligibilityRequest(
                date_of_birth=datetime.date.today().isoformat(),
                first_name="Foo",
                last_name="Bar",
                work_state="OK",
            ),
        ),
        (
            "CheckAlternateEligibility",
            "check_alternate_eligibility",
            e9ypb.AlternateEligibilityRequest(
                date_of_birth=datetime.date.today().isoformat(),
                first_name="Foo",
                last_name="Bar",
                unique_corp_id="corpid",
            ),
        ),
        (
            "CheckAlternateEligibility",
            "check_alternate_eligibility",
            e9ypb.AlternateEligibilityRequest(
                date_of_birth=datetime.date.today().isoformat(),
                first_name="Foo",
                last_name="Bar",
            ),
        ),
        (
            "CheckClientSpecificEligibility",
            "check_client_specific_eligibility",
            e9ypb.ClientSpecificEligibilityRequest(
                date_of_birth=datetime.date.today().isoformat(),
                unique_corp_id="foo",
                organization_id=1,
            ),
        ),
        (
            "CheckNoDOBEligibility",
            "check_organization_specific_eligibility_without_dob",
            e9ypb.NoDOBEligibilityRequest(
                email="foo@email.net",
                first_name="Foo",
                last_name="Bar",
            ),
        ),
        (
            "GetMemberById",
            "get_by_member_id",
            e9ypb.MemberIdRequest(
                id=1,
            ),
        ),
        (
            "GetMemberByOrgIdentity",
            "get_by_org_identity",
            e9ypb.OrgIdentityRequest(
                organization_id=1,
                unique_corp_id="foo",
                dependent_id="bar",
            ),
        ),
    ],
)
async def test_user_verification_found(
    e9y,
    svc,
    member: model.Member,
    handler_name,
    query_name,
    message,
    expected_response,
    grpc_stream,
):
    # Given
    search = getattr(svc, query_name)
    search.side_effect = mock.AsyncMock(return_value=member)
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    handler = getattr(e9y, handler_name)
    # When
    await handler(grpc_stream)
    # Then
    assert grpc_stream.send_message.call_args == mock.call(expected_response)


@pytest.mark.parametrize(
    argnames="handler_name,query_name,message",
    argvalues=[
        (
            "CheckHealthPlanEligibility",
            "check_healthplan_eligibility",
            e9ypb.HealthPlanEligibilityRequest(
                date_of_birth=datetime.date.today().isoformat(),
                first_name="Foo",
                last_name="Bar",
                subscriber_id="corpid",
            ),
        ),
    ],
)
async def test_query_service_member_found(
    e9y,
    query_svc,
    member: model.Member,
    handler_name,
    query_name,
    message,
    expected_response,
    grpc_stream,
):
    # Given
    search = getattr(query_svc, query_name)
    search.side_effect = mock.AsyncMock(return_value=member)
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    handler = getattr(e9y, handler_name)
    # When
    await handler(grpc_stream)
    # Then
    assert grpc_stream.send_message.call_args == mock.call(expected_response)


@pytest.mark.parametrize(
    argnames="handler_name,query_name,message,match_multiple_error",
    argvalues=[
        (
            "CheckStandardEligibility",
            "check_standard_eligibility",
            e9ypb.StandardEligibilityRequest(
                date_of_birth=datetime.date.today().isoformat(),
                company_email="foo@email.net",
            ),
            errors.StandardMatchMultipleError,
        ),
        (
            "CheckAlternateEligibility",
            "check_alternate_eligibility",
            e9ypb.AlternateEligibilityRequest(
                date_of_birth=datetime.date.today().isoformat(),
                first_name="Foo",
                last_name="Bar",
                work_state="OK",
            ),
            errors.AlternateMatchMultipleError,
        ),
        (
            "CheckAlternateEligibility",
            "check_alternate_eligibility",
            e9ypb.AlternateEligibilityRequest(
                date_of_birth=datetime.date.today().isoformat(),
                first_name="Foo",
                last_name="Bar",
                unique_corp_id="corpid",
            ),
            errors.AlternateMatchMultipleError,
        ),
        (
            "CheckAlternateEligibility",
            "check_alternate_eligibility",
            e9ypb.AlternateEligibilityRequest(
                date_of_birth=datetime.date.today().isoformat(),
                first_name="Foo",
                last_name="Bar",
            ),
            errors.AlternateMatchMultipleError,
        ),
    ],
)
async def test_multi_match_error(
    e9y,
    svc,
    member_list,
    expected_member_list_response,
    handler_name,
    query_name,
    message,
    match_multiple_error,
    grpc_stream,
):
    # Given
    search = getattr(svc, query_name)
    search.side_effect = mock.AsyncMock(side_effect=match_multiple_error)
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    handler = getattr(e9y, handler_name)
    expected_response = mock.call(
        status=Status.NOT_FOUND,
        status_message="multiple members found.",
        status_details=mock.ANY,
    )

    # When
    await handler(grpc_stream)
    # Then
    assert grpc_stream.send_trailing_metadata.call_args == expected_response


async def test_overeligiblity_found(
    e9y,
    svc,
    member_list,
    expected_member_list_response,
    grpc_stream,
):
    # Given
    search = getattr(svc, "check_overeligibility")
    search.side_effect = mock.AsyncMock(return_value=member_list)
    message = e9ypb.EligibilityOverEligibilityRequest(
        date_of_birth=datetime.date.today().isoformat(),
        first_name="Foo",
        last_name="Bar",
        work_state="OK",
        unique_corp_id="1234",
        company_email="foo@foobar.com",
        user_id="1234",
    )
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    handler = getattr(e9y, "CheckEligibilityOverEligibility")
    # When
    await handler(grpc_stream)
    # Then
    assert grpc_stream.send_message.call_args == mock.call(
        expected_member_list_response
    )


async def test_basic_verification_found(
    e9y,
    query_svc,
    member_list,
    expected_member_list_response,
    grpc_stream,
):
    # Given
    search = getattr(query_svc, "check_basic_eligibility")
    search.side_effect = mock.AsyncMock(return_value=member_list)
    message = e9ypb.EmployerEligibilityRequest(
        date_of_birth=datetime.date.today().isoformat(),
        first_name="Foo",
        last_name="Bar",
        work_state="OK",
        company_email="foo@foobar.com",
    )
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    handler = getattr(e9y, "CheckBasicEligibility")
    # When
    await handler(grpc_stream)
    # Then
    assert grpc_stream.send_message.call_args == mock.call(
        expected_member_list_response
    )


@pytest.mark.parametrize(
    argnames="handler_name,query_name,message",
    argvalues=[
        (
            "CheckStandardEligibility",
            "check_standard_eligibility",
            e9ypb.StandardEligibilityRequest(
                date_of_birth=datetime.date.today().isoformat(),
                company_email="foo@email.net",
            ),
        ),
        (
            "CheckAlternateEligibility",
            "check_alternate_eligibility",
            e9ypb.AlternateEligibilityRequest(
                date_of_birth=datetime.date.today().isoformat(),
                first_name="Foo",
                last_name="Bar",
                work_state="OK",
            ),
        ),
        (
            "CheckAlternateEligibility",
            "check_alternate_eligibility",
            e9ypb.AlternateEligibilityRequest(
                date_of_birth=datetime.date.today().isoformat(),
                first_name="Foo",
                last_name="Bar",
                unique_corp_id="corpid",
            ),
        ),
        (
            "CheckAlternateEligibility",
            "check_alternate_eligibility",
            e9ypb.AlternateEligibilityRequest(
                date_of_birth=datetime.date.today().isoformat(),
                first_name="Foo",
                last_name="Bar",
            ),
        ),
        (
            "CheckClientSpecificEligibility",
            "check_client_specific_eligibility",
            e9ypb.ClientSpecificEligibilityRequest(
                date_of_birth=datetime.date.today().isoformat(),
                unique_corp_id="foo",
                organization_id=1,
            ),
        ),
        (
            "CheckNoDOBEligibility",
            "check_organization_specific_eligibility_without_dob",
            e9ypb.NoDOBEligibilityRequest(
                email="foo@email.net",
                first_name="Foo",
                last_name="Bar",
            ),
        ),
        (
            "GetMemberById",
            "get_by_member_id",
            e9ypb.MemberIdRequest(
                id=1,
            ),
        ),
        (
            "GetMemberByOrgIdentity",
            "get_by_org_identity",
            e9ypb.OrgIdentityRequest(
                organization_id=1,
                unique_corp_id="foo",
                dependent_id="bar",
            ),
        ),
        (
            "CheckEligibilityOverEligibility",
            "check_overeligibility",
            e9ypb.EligibilityOverEligibilityRequest(
                date_of_birth=datetime.date.today().isoformat(),
                first_name="Foo",
                last_name="Bar",
                work_state="OK",
                unique_corp_id="1234",
                company_email="foo@foobar.com",
                user_id="1234",
            ),
        ),
    ],
)
async def test_user_verification_not_found(
    e9y,
    svc,
    handler_name,
    query_name,
    message,
    grpc_stream,
):
    # Given
    search = getattr(svc, query_name)
    search.side_effect = mock.AsyncMock(side_effect=errors.StandardMatchError)
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    expected_response = mock.call(
        status=Status.NOT_FOUND,
        status_message="Matching member not found.",
        status_details=mock.ANY,
    )
    handler = getattr(e9y, handler_name)

    # When
    await handler(grpc_stream)
    # Then
    assert grpc_stream.send_trailing_metadata.call_args == expected_response


@pytest.mark.parametrize(
    argnames="handler_name,query_name,message",
    argvalues=[
        (
            "CheckBasicEligibility",
            "check_basic_eligibility",
            e9ypb.BasicEligibilityRequest(
                date_of_birth=datetime.date.today().isoformat(),
                first_name="Foo",
                last_name="Bar",
                user_id=1,
            ),
        ),
        (
            "CheckHealthPlanEligibility",
            "check_healthplan_eligibility",
            e9ypb.HealthPlanEligibilityRequest(
                date_of_birth=datetime.date.today().isoformat(),
                first_name="Foo",
                last_name="Bar",
                subscriber_id="corpid",
                user_id=1,
            ),
        ),
    ],
)
async def test_query_service_member_record_not_found(
    e9y,
    query_svc,
    handler_name,
    query_name,
    message,
    grpc_stream,
):
    # Given
    search = getattr(query_svc, query_name)
    search.side_effect = mock.AsyncMock(side_effect=errors.MatchError)
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    expected_response = mock.call(
        status=Status.NOT_FOUND,
        status_message="Matching member not found.",
        status_details=mock.ANY,
    )
    handler = getattr(e9y, handler_name)

    # When
    await handler(grpc_stream)
    # Then
    assert grpc_stream.send_trailing_metadata.call_args == expected_response


async def test_invalid_org_identity(
    e9y,
    svc,
    grpc_stream,
):
    # Given
    message = e9ypb.OrgIdentityRequest(organization_id=1)
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)

    def raises(*args, **kwargs):
        raise service.ValidationError(
            "Test message",
            unique_corp_id=message.unique_corp_id,
            organization_id=message.organization_id,
            dependent_id=message.dependent_id,
        )

    svc.get_by_org_identity.side_effect = raises

    expected_response = mock.call(
        status=Status.INVALID_ARGUMENT,
        status_message="Test message",
        status_details=mock.ANY,
    )
    handler = getattr(e9y, "GetMemberByOrgIdentity")

    # When
    await handler(grpc_stream)
    # Then
    assert grpc_stream.send_trailing_metadata.call_args == expected_response


@pytest.mark.parametrize(
    argnames="handler_name,query_name,message",
    argvalues=[
        (
            "GetWalletEnablementById",
            "get_wallet_enablement",
            e9ypb.MemberIdRequest(id=1),
        ),
        (
            "GetWalletEnablementByOrgIdentity",
            "get_wallet_enablement_by_identity",
            e9ypb.OrgIdentityRequest(
                organization_id=1,
                unique_corp_id="foo",
                dependent_id="bar",
            ),
        ),
        (
            "GetWalletEnablementByUserId",
            "get_wallet_enablement_by_user_id",
            e9ypb.UserIdRequest(id=1),
        ),
    ],
)
async def test_wallet_enablement_found(
    e9y,
    svc,
    wallet_response,
    handler_name,
    query_name,
    message,
    expected_wallet_response,
    grpc_stream,
):
    # Given
    search = getattr(svc, query_name)
    search.side_effect = mock.AsyncMock(return_value=wallet_response)
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    handler = getattr(e9y, handler_name)
    # When
    await handler(grpc_stream)
    # Then
    assert grpc_stream.send_message.call_args == mock.call(expected_wallet_response)


@pytest.mark.parametrize(
    argnames="handler_name,query_name,message",
    argvalues=[
        (
            "GetWalletEnablementById",
            "get_wallet_enablement",
            e9ypb.MemberIdRequest(id=1),
        ),
        (
            "GetWalletEnablementByOrgIdentity",
            "get_wallet_enablement_by_identity",
            e9ypb.OrgIdentityRequest(
                organization_id=1,
                unique_corp_id="foo",
                dependent_id="bar",
            ),
        ),
        (
            "GetWalletEnablementByUserId",
            "get_wallet_enablement_by_user_id",
            e9ypb.UserIdRequest(id=1),
        ),
    ],
)
async def test_wallet_enablement_found_no_end_date(
    e9y,
    svc,
    handler_name,
    query_name,
    message,
    grpc_stream,
    wallet_response_no_end_date,
):
    # Given
    created_at, updated_at = Timestamp(), Timestamp()
    created_at.FromDatetime(wallet_response_no_end_date.created_at)
    updated_at.FromDatetime(wallet_response_no_end_date.updated_at)

    expected_response = e9ypb.WalletEnablement(
        member_id=wallet_response_no_end_date.member_id,
        organization_id=wallet_response_no_end_date.organization_id,
        enabled=wallet_response_no_end_date.enabled,
        insurance_plan=wallet_response_no_end_date.insurance_plan,
        start_date=wallet_response_no_end_date.start_date.isoformat(),
        eligibility_date=wallet_response_no_end_date.eligibility_date.isoformat(),
        created_at=created_at,
        updated_at=updated_at,
        eligibility_end_date="",
        is_v2=wallet_response_no_end_date.is_v2,
        member_1_id=wallet_response_no_end_date.member_1_id,
        member_2_id=wallet_response_no_end_date.member_2_id,
    )

    search = getattr(svc, query_name)
    search.side_effect = mock.AsyncMock(return_value=wallet_response_no_end_date)
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    handler = getattr(e9y, handler_name)
    # When
    await handler(grpc_stream)
    # Then
    assert grpc_stream.send_message.call_args == mock.call(expected_response)


@pytest.mark.parametrize(
    argnames="handler_name,query_name,message",
    argvalues=[
        (
            "GetWalletEnablementById",
            "get_wallet_enablement",
            e9ypb.MemberIdRequest(id=1),
        ),
        (
            "GetWalletEnablementByOrgIdentity",
            "get_wallet_enablement_by_identity",
            e9ypb.OrgIdentityRequest(
                organization_id=1,
                unique_corp_id="foo",
                dependent_id="bar",
            ),
        ),
        (
            "GetWalletEnablementByUserId",
            "get_wallet_enablement_by_user_id",
            e9ypb.UserIdRequest(id=1),
        ),
    ],
)
async def test_wallet_enablement_not_found(
    e9y,
    svc,
    handler_name,
    query_name,
    message,
    grpc_stream,
):
    # Given
    search = getattr(svc, query_name)
    search.side_effect = mock.AsyncMock(side_effect=errors.GetMatchError)
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    expected_response = mock.call(
        status=Status.NOT_FOUND,
        status_message="Matching member not found.",
        status_details=mock.ANY,
    )
    handler = getattr(e9y, handler_name)

    # When
    await handler(grpc_stream)
    # Then
    assert grpc_stream.send_trailing_metadata.call_args == expected_response


@pytest.mark.parametrize(
    argnames="error",
    argvalues=[
        errors.ClientSpecificConfigurationError(),
        errors.UpstreamClientSpecificException(
            model.ClientSpecificImplementation.MICROSOFT, Exception("BOOM!")
        ),
    ],
)
async def test_client_specific_error_states(e9y, svc, error, grpc_stream):
    # Given
    message = e9ypb.ClientSpecificEligibilityRequest(
        organization_id=1,
        unique_corp_id="corpid",
        date_of_birth="1970-01-01",
        is_employee=True,
    )
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    svc.check_client_specific_eligibility.side_effect = mock.AsyncMock(
        side_effect=error
    )
    expected_response = mock.call(
        status=(
            Status.UNIMPLEMENTED
            if isinstance(error, errors.ClientSpecificConfigurationError)
            else Status.UNAVAILABLE
        ),
        status_message=mock.ANY,
        status_details=mock.ANY,
    )
    # When
    await e9y.CheckClientSpecificEligibility(grpc_stream)
    # Then
    assert grpc_stream.send_trailing_metadata.call_args == expected_response


@pytest.mark.parametrize(
    argnames="error",
    argvalues=[
        errors.NoDobMatchError(),
    ],
)
async def test_no_dob_error_states(e9y, svc, error, grpc_stream):
    # Given
    message = e9ypb.NoDOBEligibilityRequest(
        email="foo@email.net",
        first_name="foo",
        last_name="bar",
    )
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    svc.check_organization_specific_eligibility_without_dob.side_effect = (
        mock.AsyncMock(side_effect=error)
    )
    expected_response = mock.call(
        status=(
            Status.NOT_FOUND
            if isinstance(error, errors.NoDobMatchError)
            else Status.UNAVAILABLE
        ),
        status_message=mock.ANY,
        status_details=mock.ANY,
    )
    # When
    await e9y.CheckNoDOBEligibility(grpc_stream)
    # Then
    assert grpc_stream.send_trailing_metadata.call_args == expected_response


@pytest.mark.parametrize(
    argnames="handler_name,query_name,message",
    argvalues=[
        (
            "GetVerificationForUser",
            "get_verification_for_user",
            e9ypb.GetVerificationForUserRequest(
                user_id=1, organization_id="", active_verifications_only=False
            ),
        ),
    ],
)
async def test_get_verification_for_user(
    e9y,
    svc,
    eligibility_verification_for_user,
    handler_name,
    query_name,
    message,
    expected_verification_for_user_response,
    grpc_stream,
):
    # Given
    search = getattr(svc, query_name)
    search.side_effect = mock.AsyncMock(return_value=eligibility_verification_for_user)
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    handler = getattr(e9y, handler_name)
    # When
    await handler(grpc_stream)
    # Then
    assert grpc_stream.send_message.call_args == mock.call(
        expected_verification_for_user_response
    )


@pytest.mark.parametrize(
    argnames="handler_name,query_name,message,active_organizations",
    argvalues=[
        (
            "GetAllVerificationsForUser",
            "get_all_verifications_for_user",
            e9ypb.GetAllVerificationsForUserRequest(
                user_id=1, organization_ids=[], active_verifications_only=False
            ),
            [(1, True), (2, True), (3, True)],
        ),
        (
            "GetAllVerificationsForUser",
            "get_all_verifications_for_user",
            e9ypb.GetAllVerificationsForUserRequest(
                user_id=1, organization_ids=[], active_verifications_only=True
            ),
            [(1, False), (2, True), (3, True)],
        ),
        (
            "GetAllVerificationsForUser",
            "get_all_verifications_for_user",
            e9ypb.GetAllVerificationsForUserRequest(
                user_id=1, organization_ids=[1], active_verifications_only=True
            ),
            [(1, True), (2, True)],
        ),
    ],
)
async def test_get_all_verifications_for_user(
    e9y,
    svc,
    eligibility_verifications_for_user_multiple_orgs,
    handler_name,
    query_name,
    message,
    grpc_stream,
):
    # Given
    search = getattr(svc, query_name)
    active_verifications = _filter_inactive_verifications(
        eligibility_verifications_for_user_multiple_orgs
    )
    search.side_effect = mock.AsyncMock(return_value=active_verifications)
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    handler = getattr(e9y, handler_name)
    response = expected_verifications_for_user_multiple_orgs_response(
        active_verifications
    )
    # When
    await handler(grpc_stream)
    # Then
    assert grpc_stream.send_message.call_args == mock.call(response)


def _filter_inactive_verifications(verifications):
    result = []
    for verification in verifications:
        verification_deactivated_at = verification.verification_deactivated_at
        if verification_deactivated_at >= datetime.datetime.today():
            result.append(verification)
    return result


@pytest.mark.parametrize(
    argnames="handler_name,query_name",
    argvalues=[
        ("CreateVerificationForUser", "create_verification_for_user"),
    ],
)
async def test_create_verification_for_user(
    e9y,
    svc,
    eligibility_verification_for_user,
    handler_name,
    query_name,
    grpc_stream,
    member,
    expected_verification_for_user_response,
):
    # Given
    message = e9ypb.CreateVerificationForUserRequest(
        user_id=str(1),
        eligibility_member_id=str(1234),
        organization_id=member.organization_id,
        verification_type="PRIMARY",
        unique_corp_id=member.unique_corp_id,
        dependent_id=member.dependent_id,
        first_name=member.first_name,
        last_name=member.last_name,
        date_of_birth=member.date_of_birth.isoformat(),
        email=member.email,
        work_state=member.work_state,
        verified_at=member.date_of_birth.isoformat(),
    )
    # expected_response()
    search = getattr(svc, query_name)
    search.side_effect = mock.AsyncMock(return_value=eligibility_verification_for_user)
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    handler = getattr(e9y, handler_name)
    # When
    await handler(grpc_stream)
    # Then
    assert grpc_stream.send_message.call_args == mock.call(
        expected_verification_for_user_response
    )


@pytest.fixture
def create_multiple_verifications_request(
    verification_info,
    user_id,
    verification_type,
    first_name,
    last_name,
    date_of_birth,
    verification_session,
):
    verification_data_list = []
    for info in verification_info:
        (
            eligibility_member_id,
            organization_id,
            verification_type,
            unique_corp_id,
            dependent_id,
            email,
            work_state,
            additional_fields,
        ) = info
        # Create individual VerificationData objects
        verification_data = e9ypb.VerificationData(
            eligibility_member_id=Int64Value(value=eligibility_member_id),
            organization_id=organization_id,
            unique_corp_id=unique_corp_id,
            dependent_id=dependent_id,
            email=email,
            work_state=work_state,
            additional_fields=additional_fields,
        )
        verification_data_list.append(verification_data)

    # Prepare the request with the list of VerificationData
    return e9ypb.CreateMultipleVerificationsForUserRequest(
        user_id=Int64Value(value=user_id),
        verification_data_list=verification_data_list,
        verification_type=verification_type,
        first_name=first_name,
        last_name=last_name,
        date_of_birth=date_of_birth,  # YYYY-MM-DD
        verified_at=datetime.datetime.now().isoformat(),  # ISO 8601 format
        deactivated_at="",
        verification_session=verification_session,
    )


@pytest.mark.parametrize(
    argnames="handler_name,query_name,verification_info,active_organizations,user_id,verification_type, first_name,last_name,date_of_birth,verification_session",
    argvalues=[
        (
            "CreateMultipleVerificationsForUser",
            "create_multiple_verifications_for_user",
            [
                (
                    12345,
                    1,
                    "standard",
                    "corp001",
                    "001",
                    "corp1@test.net",
                    "CA",
                    "test_additional_fields_1",
                ),
                (
                    56789,
                    2,
                    "standard",
                    "corp002",
                    "002",
                    "corp2@test.net",
                    "NY",
                    "test_additional_fields_2",
                ),
                (
                    00000,
                    3,
                    "standard",
                    "corp003",
                    "003",
                    "corp3@test.net",
                    "TX",
                    "test_additional_fields_3",
                ),
            ],
            [(1, True), (2, True), (3, True)],
            999,
            "standard",
            "John",
            "Doe",
            "1990-01-01",
            "UUID-1234-56789",
        ),
    ],
)
async def test_create_multiple_verifications_for_user(
    e9y,
    svc,
    eligibility_verifications_for_user_multiple_orgs,
    handler_name,
    query_name,
    grpc_stream,
    create_multiple_verifications_request,
    member,
):
    # Given
    message = create_multiple_verifications_request
    active_verifications = _filter_inactive_verifications(
        eligibility_verifications_for_user_multiple_orgs
    )
    search = getattr(svc, query_name)
    search.side_effect = mock.AsyncMock(return_value=active_verifications)
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    handler = getattr(e9y, handler_name)
    response = expected_verifications_for_user_multiple_orgs_response(
        active_verifications
    )
    # When
    await handler(grpc_stream)
    # Then
    assert grpc_stream.send_message.call_args == mock.call(response)


@pytest.mark.parametrize(
    argnames="handler_name,query_name",
    argvalues=[
        ("CreateFailedVerification", "create_failed_verification"),
    ],
)
async def test_create_failed_verification(
    e9y,
    svc,
    handler_name,
    query_name,
    grpc_stream,
    member,
    failed_verification_attempt,
    expected_failed_verification_response,
):
    # Given
    message = e9ypb.CreateFailedVerificationRequest(
        eligibility_member_id=str(1234),
        organization_id=str(member.organization_id),
        verification_type="PRIMARY",
        unique_corp_id=member.unique_corp_id,
        dependent_id=member.dependent_id,
        first_name=member.first_name,
        last_name=member.last_name,
        date_of_birth=member.date_of_birth.isoformat(),
        email=member.email,
        work_state=member.work_state,
        policy_used="",
        verified_at=member.date_of_birth.isoformat(),
    )
    # expected_response()
    search = getattr(svc, query_name)
    search.side_effect = mock.AsyncMock(return_value=failed_verification_attempt)
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    handler = getattr(e9y, handler_name)
    # When
    await handler(grpc_stream)
    # Then
    assert grpc_stream.send_message.call_args == mock.call(
        expected_failed_verification_response
    )


@pytest.mark.parametrize(
    argnames="handler_name,query_name",
    argvalues=[
        ("GetEligibleFeaturesForUser", "get_eligible_features_for_user"),
    ],
)
async def test_get_eligible_features_for_user_handle_error(
    e9y,
    svc,
    handler_name,
    query_name,
    grpc_stream,
    expected_eligible_features_response,
):
    # Given
    message = e9ypb.GetEligibleFeaturesForUserRequest(
        user_id=None,
        feature_type=int(1),
    )

    def raises(*args, **kwargs):
        raise service.ValidationError(
            "Test message", user_id=message.user_id, feature_type=message.feature_type
        )

    # expected_response()
    search = getattr(svc, query_name)
    search.side_effect = raises
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    expected_response = mock.call(
        status=Status.INVALID_ARGUMENT,
        status_message="Test message",
        status_details=mock.ANY,
    )
    handler = getattr(e9y, handler_name)
    # When
    await handler(grpc_stream)
    # Then
    assert grpc_stream.send_trailing_metadata.call_args == expected_response


@pytest.mark.parametrize(
    argnames="handler_name,query_name",
    argvalues=[
        ("GetEligibleFeaturesForUser", "get_eligible_features_for_user"),
    ],
)
async def test_get_eligible_features_for_user(
    e9y,
    svc,
    handler_name,
    query_name,
    grpc_stream,
    expected_eligible_features_response,
):
    # Given
    message = e9ypb.GetEligibleFeaturesForUserRequest(
        user_id=int(1),
        feature_type=int(1),
    )
    test_features = expected_eligible_features_response.features
    # expected_response()
    search = getattr(svc, query_name)
    search.side_effect = mock.AsyncMock(return_value=test_features)
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    handler = getattr(e9y, handler_name)
    # When
    await handler(grpc_stream)
    # Then
    assert grpc_stream.send_message.call_args[0][0].features == test_features


@pytest.mark.parametrize(
    argnames="handler_name,query_name",
    argvalues=[
        ("DeactivateVerificationForUser", "deactivate_verification_for_user"),
    ],
)
async def test_deactivate_verification_record_for_user_handle_error(
    e9y,
    svc,
    handler_name,
    query_name,
    grpc_stream,
    expected_deactivated_record,
):
    # Given
    message = e9ypb.DeactivateVerificationForUserRequest(
        verification_id=None,
        user_id=None,
    )

    def raises(*args, **kwargs):
        raise service.ValidationError(
            "Test message",
            verification_id=message.verification_id,
            user_id=message.user_id,
        )

    # expected_response()
    search = getattr(svc, query_name)
    search.side_effect = raises
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    expected_response = mock.call(
        status=Status.INVALID_ARGUMENT,
        status_message="Test message",
        status_details=mock.ANY,
    )
    handler = getattr(e9y, handler_name)
    # When
    await handler(grpc_stream)
    # Then
    assert grpc_stream.send_trailing_metadata.call_args == expected_response


@pytest.mark.parametrize(
    argnames="handler_name,query_name",
    argvalues=[
        ("DeactivateVerificationForUser", "deactivate_verification_for_user"),
    ],
)
async def test_deactivate_verification_record_for_user(
    e9y,
    svc,
    handler_name,
    query_name,
    grpc_stream,
    expected_deactivated_record,
):
    # Given
    message = e9ypb.DeactivateVerificationForUserRequest(
        verification_id=int(1),
        user_id=int(1),
    )
    result = getattr(svc, query_name)
    result.side_effect = mock.AsyncMock(return_value=expected_deactivated_record)
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    handler = getattr(e9y, handler_name)

    # When
    await handler(grpc_stream)
    # Then
    assert (
        grpc_stream.send_message.call_args[0][0].verification_id
        == expected_deactivated_record.id
    )


@pytest.mark.parametrize(
    argnames="handler_name,query_name,message",
    argvalues=[
        (
            "CheckPreEligibility",
            "get_members_by_name_and_date_of_birth",
            pre9ypb.PreEligibilityRequest(
                user_id=None,
                member_id=None,
                first_name="foo",
                last_name="bar",
                date_of_birth=datetime.date.today().isoformat(),
            ),
        ),
    ],
)
async def test_pre_eligibility_record_unknown_eligibility(
    pre9y,
    pre9y_svc,
    handler_name,
    query_name,
    message,
    expected_pre_eligibility_response,
    grpc_stream,
):
    # Given
    pre_eligibility_search = getattr(pre9y_svc, query_name)
    pre_eligibility_search.side_effect = mock.AsyncMock(return_value=[])
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    handler = getattr(pre9y, handler_name)

    # When
    await handler(grpc_stream)

    # Then
    assert grpc_stream.send_message.call_args == mock.call(
        expected_pre_eligibility_response
    )


@pytest.mark.parametrize(
    argnames="handler_name,query_name,message",
    argvalues=[
        (
            "CheckPreEligibility",
            "get_members_by_name_and_date_of_birth",
            pre9ypb.PreEligibilityRequest(
                user_id=None,
                member_id=1,
                first_name="foo",
                last_name="bar",
                date_of_birth=datetime.date.today().isoformat(),
            ),
        ),
    ],
)
async def test_pre_eligibility_no_matching_records(
    e9y,
    pre9y,
    pre9y_svc,
    svc,
    expected_pre_eligibility_response,
    expected_pre_eligibility_organization,
    handler_name,
    query_name,
    message,
    active_member,
    inactive_member_same_org,
    grpc_stream,
):
    # Given
    # member_search
    member_search_request = e9ypb.MemberIdRequest(
        id=1,
    )
    member_search_grpc_stream = grpc_stream
    member_search = svc.get_by_member_id_from_member
    member_search.side_effect = mock.AsyncMock(return_value=active_member)
    member_search_grpc_stream.recv_message.side_effect = mock.AsyncMock(
        return_value=member_search_request
    )
    member_search_handler = e9y.GetMemberById
    pre_eligibility_search = getattr(pre9y_svc, query_name)
    matching_records = []

    # When
    await member_search_handler(member_search_grpc_stream)
    pre_eligibility_search.side_effect = mock.AsyncMock(return_value=matching_records)
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    handler = getattr(pre9y, handler_name)
    await handler(grpc_stream)

    # Then
    assert (
        grpc_stream.send_message.call_args[0][0].match_type
        == MatchType.UNKNOWN_ELIGIBILITY
    )


@pytest.mark.parametrize(
    argnames="handler_name,query_name,message",
    argvalues=[
        (
            "CheckPreEligibility",
            "get_members_by_name_and_date_of_birth",
            pre9ypb.PreEligibilityRequest(
                user_id=None,
                member_id=None,
                first_name="foo",
                last_name="bar",
                date_of_birth=datetime.date.today().isoformat(),
            ),
        ),
    ],
)
async def test_pre_eligibility_record_potential_eligibility(
    e9y,
    pre9y,
    pre9y_svc,
    svc,
    expected_pre_eligibility_response,
    expected_pre_eligibility_organization,
    handler_name,
    query_name,
    message,
    member,
    grpc_stream,
):
    # Given
    pre_eligibility_search = getattr(pre9y_svc, query_name)
    pre_eligibility_search.side_effect = mock.AsyncMock(return_value=[member])
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    handler = getattr(pre9y, handler_name)

    # When
    await handler(grpc_stream)

    # Then
    assert grpc_stream.send_message.call_args[0][0].match_type == MatchType.POTENTIAL


@pytest.mark.parametrize(
    argnames="handler_name,query_name,message",
    argvalues=[
        (
            "CheckPreEligibility",
            "get_members_by_name_and_date_of_birth",
            pre9ypb.PreEligibilityRequest(
                user_id=None,
                member_id=1,
                first_name="foo",
                last_name="bar",
                date_of_birth=datetime.date.today().isoformat(),
            ),
        ),
    ],
)
async def test_pre_eligibility_record_existing_eligibility(
    e9y,
    pre9y,
    pre9y_svc,
    svc,
    expected_pre_eligibility_response,
    expected_pre_eligibility_organization,
    handler_name,
    query_name,
    message,
    member,
    grpc_stream,
):
    # Given
    # member_search
    member_search_request = e9ypb.MemberIdRequest(
        id=1,
    )
    member_search_grpc_stream = grpc_stream
    member_search = svc.get_by_member_id_from_member
    member_search.side_effect = mock.AsyncMock(return_value=member)
    member_search_grpc_stream.recv_message.side_effect = mock.AsyncMock(
        return_value=member_search_request
    )
    member_search_handler = e9y.GetMemberById
    pre_eligibility_search = getattr(pre9y_svc, query_name)

    # When
    await member_search_handler(member_search_grpc_stream)
    pre_eligibility_search.side_effect = mock.AsyncMock(return_value=[member])
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    handler = getattr(pre9y, handler_name)
    await handler(grpc_stream)

    # Then
    assert (
        grpc_stream.send_message.call_args[0][0].match_type
        == MatchType.EXISTING_ELIGIBILITY
    )


@pytest.mark.parametrize(
    argnames="handler_name,query_name,message",
    argvalues=[
        (
            "CheckPreEligibility",
            "get_members_by_name_and_date_of_birth",
            pre9ypb.PreEligibilityRequest(
                user_id=None,
                member_id=1,
                first_name="foo",
                last_name="bar",
                date_of_birth=datetime.date.today().isoformat(),
            ),
        ),
    ],
)
async def test_pre_eligibility_record_potential_current_organization(
    e9y,
    pre9y,
    pre9y_svc,
    svc,
    expected_pre_eligibility_response,
    expected_pre_eligibility_organization,
    handler_name,
    query_name,
    message,
    active_member,
    inactive_member_same_org,
    grpc_stream,
):
    # Given
    # member_search
    member_search_request = e9ypb.MemberIdRequest(
        id=1,
    )
    member_search_grpc_stream = grpc_stream
    member_search = svc.get_by_member_id_from_member
    member_search.side_effect = mock.AsyncMock(return_value=inactive_member_same_org)
    member_search_grpc_stream.recv_message.side_effect = mock.AsyncMock(
        return_value=member_search_request
    )
    member_search_handler = e9y.GetMemberById
    pre_eligibility_search = getattr(pre9y_svc, query_name)
    matching_records = [active_member, inactive_member_same_org]

    # When
    await member_search_handler(member_search_grpc_stream)
    pre_eligibility_search.side_effect = mock.AsyncMock(return_value=matching_records)
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    handler = getattr(pre9y, handler_name)
    await handler(grpc_stream)

    # Then
    assert (
        grpc_stream.send_message.call_args[0][0].match_type
        == MatchType.POTENTIAL_CURRENT_ORGANIZATION
    )


@pytest.mark.parametrize(
    argnames="handler_name,query_name,message",
    argvalues=[
        (
            "CheckPreEligibility",
            "get_members_by_name_and_date_of_birth",
            pre9ypb.PreEligibilityRequest(
                user_id=None,
                member_id=1,
                first_name="foo",
                last_name="bar",
                date_of_birth=datetime.date.today().isoformat(),
            ),
        ),
    ],
)
async def test_pre_eligibility_record_potential_other_organization(
    e9y,
    pre9y,
    pre9y_svc,
    svc,
    expected_pre_eligibility_response,
    expected_pre_eligibility_organization,
    handler_name,
    query_name,
    message,
    inactive_member_different_org,
    member,
    grpc_stream,
):
    # Given
    # member_search
    member_search_request = e9ypb.MemberIdRequest(
        id=1,
    )
    member_search_grpc_stream = grpc_stream
    member_search = svc.get_by_member_id_from_member
    member_search.side_effect = mock.AsyncMock(
        return_value=inactive_member_different_org
    )
    member_search_grpc_stream.recv_message.side_effect = mock.AsyncMock(
        return_value=member_search_request
    )
    member_search_handler = e9y.GetMemberById
    pre_eligibility_search = getattr(pre9y_svc, query_name)
    matching_records = [member, inactive_member_different_org]

    # When
    await member_search_handler(member_search_grpc_stream)
    pre_eligibility_search.side_effect = mock.AsyncMock(return_value=matching_records)
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    handler = getattr(pre9y, handler_name)
    await handler(grpc_stream)

    # Then
    assert (
        grpc_stream.send_message.call_args[0][0].match_type
        == MatchType.POTENTIAL_OTHER_ORGANIZATION
    )


@pytest.mark.parametrize(
    argnames="member, matching_records, expected",
    argvalues=[
        (None, [active_member], pre9ypb.MatchType.POTENTIAL),
        (active_member, [], pre9ypb.MatchType.UNKNOWN_ELIGIBILITY),
        (None, [], pre9ypb.MatchType.UNKNOWN_ELIGIBILITY),
        (active_member, [active_member], pre9ypb.MatchType.EXISTING_ELIGIBILITY),
        (
            inactive_member_same_org,
            [active_member],
            pre9ypb.MatchType.POTENTIAL_CURRENT_ORGANIZATION,
        ),
        (
            inactive_member_different_org,
            [active_member],
            pre9ypb.MatchType.POTENTIAL_OTHER_ORGANIZATION,
        ),
    ],
    ids=[
        "no_member",
        "no_match",
        "no_member_no_match",
        "active_member",
        "inactive_member_same_org",
        "inactive_member_different_org",
    ],
)
def test_get_match_type(member, matching_records, expected):
    assert (
        handlers.get_match_type(member=member, matching_records=matching_records)
        == expected
    )


@pytest.mark.parametrize(
    argnames="effective_range_upper, organizations_expected",
    argvalues=[
        (None, 1),
        (past_date, 0),
        (future_date, 1),
    ],
    ids=[
        "no_effective_range_upper",
        "past_effective_range_upper",
        "future_effective_range_upper",
    ],
)
def test_get_pre_eligibility_organizations(
    effective_range_upper, organizations_expected
):
    # Given
    test_member = data_models.MemberFactory.create(
        id=1,
        organization_id=1,
        record={"record_source": "census"},
        effective_range=model.DateRange(upper=effective_range_upper),
    )

    expected_pre_eligibility_organizations = []
    if organizations_expected:
        eligibility_end_date = None
        if effective_range_upper is not None:
            end_time = datetime.datetime.combine(
                effective_range_upper, datetime.time.min
            )
            eligibility_end_date = Timestamp()
            eligibility_end_date.FromDatetime(end_time)

        expected_pre_eligibility_organizations.append(
            pre9ypb.PreEligibilityOrganization(
                organization_id=1,
                eligibility_end_date=eligibility_end_date,
            )
        )

    # When/Then
    assert (
        handlers.get_pre_eligibility_organizations([test_member])
        == expected_pre_eligibility_organizations
    )


async def test_create_eligibility_members_for_organization_prod(
    teste9y_svc,
    grpc_stream,
    member,
    teste9y,
):
    # Given
    with patch(
        "api.handlers.EligibilityTestUtilityService.app_settings"
    ) as mock_app_settings:
        # set environment to production
        mock_app_settings.environment = "production"
        handler_name = "CreateEligibilityMemberTestRecordsForOrganization"
        message = teste9ypb.CreateEligibilityMemberTestRecordsForOrganizationRequest(
            organization_id=1, test_member_records=[]
        )
        # When
        grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
        handler = getattr(teste9y, handler_name)
        await handler(grpc_stream)

        # Then
        assert grpc_stream.send_message.call_args is None


async def test_create_eligibility_members_for_organization_non_prod_non_existent_org(
    teste9y_svc,
    grpc_stream,
    member,
    teste9y,
):
    # Given
    query_name = "create_members_for_organization"
    create_member = getattr(teste9y_svc, query_name)
    handler_name = "CreateEligibilityMemberTestRecordsForOrganization"
    message = teste9ypb.CreateEligibilityMemberTestRecordsForOrganizationRequest(
        test_member_records=[]
    )
    create_member.side_effect = mock.AsyncMock(side_effect=errors.OrganizationNotFound)
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    expected_response = mock.call(
        status=Status.INVALID_ARGUMENT,
        status_message="Organization not found",
        status_details=mock.ANY,
    )
    handler = getattr(teste9y, handler_name)

    # When
    await handler(grpc_stream)
    # Then
    assert grpc_stream.send_trailing_metadata.call_args == expected_response


async def test_create_eligibility_members_for_organization_non_prod(
    teste9y_svc,
    grpc_stream,
    member,
    active_member,
    teste9y,
):
    # Given
    handler_name = "CreateEligibilityMemberTestRecordsForOrganization"
    query_name = "create_members_for_organization"
    message = teste9ypb.CreateEligibilityMemberTestRecordsForOrganizationRequest(
        organization_id=1,
        test_member_records=[
            teste9ypb.EligibilityMemberTestRecord(
                first_name="test1_first_name",
                last_name="test1_last_name",
                date_of_birth="test1_date_of_birth",
            ),
            teste9ypb.EligibilityMemberTestRecord(
                first_name="test2_first_name",
                last_name="test2_last_name",
                date_of_birth="test2_date_of_birth",
            ),
        ],
    )
    create_member = getattr(teste9y_svc, query_name)
    expected_members_response = [member, active_member]
    create_member.side_effect = mock.AsyncMock(return_value=expected_members_response)
    grpc_stream.recv_message.side_effect = mock.AsyncMock(return_value=message)
    handler = getattr(teste9y, handler_name)

    # When
    await handler(grpc_stream)
    # Then
    assert len(grpc_stream.send_message.call_args[0][0].members) == len(
        expected_members_response
    )
