import msal
import orjson
import pytest
import typic
from tests.factories import data_models as factory

from app.eligibility.client_specific import base, microsoft
from app.eligibility.client_specific.microsoft import MicrosoftResponse

pytestmark = pytest.mark.asyncio


@pytest.fixture
def caller() -> microsoft.MicrosoftSpecificCaller:
    def _no_init(self):
        """skip initialization"""
        self.auth = msal.ConfidentialClientApplication(
            client_id="mock_client_id",
            authority="mock-authority",
            client_credential="mock-client-credential",
        )
        self.url = "mock-url"
        self.auth_url = "mock-auth-url"
        self.service = "mock-service"
        self.auth_scopes = ["mock-scope"]
        self.resource = "mock-resource"
        self.validator = typic.get_constraints(MicrosoftResponse)

    microsoft.MicrosoftSpecificCaller.__init__ = _no_init
    return microsoft.MicrosoftSpecificCaller()


@pytest.fixture
def client() -> microsoft.MicrosoftSpecificProtocol:
    return microsoft.MicrosoftSpecificProtocol()


class TestMicrosoftSpecificCaller:
    @staticmethod
    async def test_get_token_cached(
        caller,
    ):
        # Given
        expected = "token"
        caller.auth.acquire_token_silent.return_value = {caller._TOKEN_KEY: expected}
        # When
        token = await caller.get_token()
        # Then
        assert token == expected

    @staticmethod
    async def test_get_token_acquire(caller):
        # Given
        expected = "token"
        caller.auth.acquire_token_silent.return_value = None
        caller.auth.acquire_token_for_client.return_value = {
            caller._TOKEN_KEY: expected
        }
        # When
        token = await caller.get_token()
        # Then
        assert token == expected

    @staticmethod
    async def test_get_token_acquire_error(caller):
        # Given
        caller.auth.acquire_token_silent.return_value = None
        caller.auth.acquire_token_for_client.return_value = {}
        # Then/When
        with pytest.raises(microsoft.MicrosoftAuthError):
            await caller.get_token()

    @staticmethod
    async def test_call_valid_employee(caller, response_mock):
        # Given
        expected = factory.MicrosoftResponseFactory.create()
        request = factory.ClientSpecificEmployeeRequestFactory.create()
        response_mock.post(caller.url, status=200, body=orjson.dumps(expected))
        # When
        response = await caller.call(request)
        # Then
        assert response == expected

    @staticmethod
    async def test_call_valid_dependent(caller, response_mock):
        # Given
        expected = factory.MicrosoftResponseFactory.create()
        request = factory.ClientSpecificDependentRequestFactory.create()
        response_mock.post(caller.url, status=200, body=orjson.dumps(expected))
        # When
        response = await caller.call(request)
        # Then
        assert response == expected

    def test_get_payload_employee(self, caller):
        # Given
        request = factory.ClientSpecificEmployeeRequestFactory.create()

        # When
        returned_payload = caller._get_payload(request)

        # Then
        assert returned_payload == {
            "EmployeeId": request.unique_corp_id,
            "DateOfBirth": request.date_of_birth.isoformat(),
            "IsEmployee": request.is_employee,
            "DependentDateOfBirth": None,
        }

    def test_get_payload_dependent(self, caller):
        # Given
        request = factory.ClientSpecificDependentRequestFactory.create()

        # When
        returned_payload = caller._get_payload(request)

        # Then
        assert returned_payload == {
            "EmployeeId": request.unique_corp_id,
            "DateOfBirth": request.date_of_birth.isoformat(),
            "IsEmployee": request.is_employee,
            "DependentDateOfBirth": request.dependent_date_of_birth.isoformat(),
        }

    @staticmethod
    @pytest.mark.parametrize(
        argnames="response",
        argvalues=[{}, {"insuranceType": "Covered"}],
    )
    async def test_validation_fails(caller, response):
        # When/Then
        with pytest.raises(base.ResponseValidationError):
            caller._do_validate(response)


class TestMicrosoftSpecificProtocol:
    @staticmethod
    def test_check_eligibility(client):
        # Given
        response = factory.MicrosoftResponseFactory.create()
        # When
        checked = client.check_eligibility(response)
        # Then
        assert checked == response

    @staticmethod
    @pytest.mark.parametrize(
        argnames="value", argvalues=[*microsoft.MicrosoftSpecificProtocol._INELIGIBLE]
    )
    def test_check_eligibility_fails(client, value):
        # Given
        response = factory.MicrosoftResponseFactory.create(insuranceType=value)
        # When
        checked = client.check_eligibility(response)
        # Then
        assert checked is None
