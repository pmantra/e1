import datetime
from unittest import mock

import pytest
from tests.factories import data_models

from api import handlers
from db import model


@pytest.fixture(scope="module", autouse=True)
def MockStream():
    with mock.patch(
        "grpclib.server.Stream", autospec=True, spec_set=True
    ) as MockStream:
        yield MockStream


@pytest.fixture(scope="function")
def grpc_stream(MockStream):
    stream = MockStream.return_value
    yield stream
    MockStream.reset_mock()


@pytest.fixture(scope="module")
def e9y() -> handlers.EligibilityService:
    return handlers.EligibilityService()


@pytest.fixture(scope="module")
def pre9y() -> handlers.PreEligibilityService:
    return handlers.PreEligibilityService()


@pytest.fixture(scope="module")
def teste9y() -> handlers.EligibilityTestUtilityService:
    return handlers.EligibilityTestUtilityService()


@pytest.fixture(scope="function")
def member():
    return data_models.MemberFactory.create(
        id=1, organization_id=1, record={"record_source": "census"}
    )


@pytest.fixture(scope="function")
def member_list():
    member_list = []
    for i in range(3):
        member = data_models.MemberFactory.create(
            id=i, organization_id=1, record={"record_source": "census"}
        )
        member_list.append(member)

    return member_list


@pytest.fixture(scope="function")
def active_member():
    future_date = datetime.date.today() + datetime.timedelta(days=10)
    return data_models.MemberFactory.create(
        id=1,
        organization_id=1,
        record={"record_source": "census"},
        effective_range=model.DateRange(upper=future_date),
    )


@pytest.fixture(scope="function")
def inactive_member_same_org():
    yesterday = datetime.date.today() - datetime.timedelta(days=10)
    return data_models.MemberFactory.create(
        id=1,
        organization_id=1,
        record={"record_source": "census"},
        effective_range=model.DateRange(upper=yesterday),
    )


@pytest.fixture(scope="function")
def inactive_member_different_org():
    yesterday = datetime.date.today() - datetime.timedelta(days=10)
    return data_models.MemberFactory.create(
        id=1,
        organization_id=2,
        record={"record_source": "census"},
        effective_range=model.DateRange(upper=yesterday),
    )
