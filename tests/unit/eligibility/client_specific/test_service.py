from unittest import mock

import pytest
from tests.factories import data_models as factory

from app.eligibility import errors
from app.eligibility.client_specific import ClientSpecificService
from db import model

pytestmark = pytest.mark.asyncio


class TestClientSpecificService:
    async def test_perform_client_specific_check(
        self, mock_msft_check, client_specific_service
    ):
        # Given
        client_specific_service._get_mode.return_value = (
            model.ClientSpecificMode.ONLY_CLIENT_CHECK
        )
        response = factory.MicrosoftResponseFactory.create()
        request = factory.ClientSpecificEmployeeRequestFactory.create()
        mock_msft_check.verify.return_value = response
        # When
        member = await client_specific_service.perform_client_specific_verification(
            is_employee=request.is_employee,
            date_of_birth=request.date_of_birth,
            dependent_date_of_birth=request.dependent_date_of_birth,
            organization_id=1,
            unique_corp_id=request.unique_corp_id,
            implementation=model.ClientSpecificImplementation.MICROSOFT,
        )
        # Then
        assert isinstance(member, model.Member)

    async def test_perform_client_specific_check_error(
        self, mock_msft_check, client_specific_service
    ):
        # Given
        client_specific_service._get_mode.return_value = (
            model.ClientSpecificMode.ONLY_CLIENT_CHECK
        )
        request = factory.ClientSpecificEmployeeRequestFactory.create()
        mock_msft_check.verify.side_effect = Exception("BOOM!")
        # Then/When
        with pytest.raises(errors.UpstreamClientSpecificException):
            await client_specific_service.perform_client_specific_verification(
                is_employee=request.is_employee,
                date_of_birth=request.date_of_birth,
                dependent_date_of_birth=request.dependent_date_of_birth,
                organization_id=1,
                unique_corp_id=request.unique_corp_id,
                implementation=model.ClientSpecificImplementation.MICROSOFT,
            )

    async def test_perform_client_specific_check_no_match(
        self, mock_msft_check, client_specific_service
    ):
        # Given
        client_specific_service._get_mode.return_value = (
            model.ClientSpecificMode.ONLY_CLIENT_CHECK
        )
        request = factory.ClientSpecificEmployeeRequestFactory.create()
        mock_msft_check.verify.return_value = None
        # Then/When
        with pytest.raises(errors.ClientSpecificMatchError):
            await client_specific_service.perform_client_specific_verification(
                is_employee=request.is_employee,
                date_of_birth=request.date_of_birth,
                dependent_date_of_birth=request.dependent_date_of_birth,
                organization_id=1,
                unique_corp_id=request.unique_corp_id,
                implementation=model.ClientSpecificImplementation.MICROSOFT,
            )

    async def test_perform_client_specific_check_census_fallback(
        self, members, client_specific_service, mock_msft_check
    ):
        # Given
        response = factory.MicrosoftResponseFactory.create()
        mock_msft_check.verify.return_value = response
        client_specific_service._get_mode.return_value = (
            model.ClientSpecificMode.FALLBACK_TO_CENSUS
        )
        request = factory.ClientSpecificEmployeeRequestFactory.create()
        expected = factory.MemberFactory.create()
        members.get_by_client_specific_verification.return_value = mock.AsyncMock(
            return_value=expected
        )
        # When
        member = await client_specific_service.perform_client_specific_verification(
            is_employee=request.is_employee,
            date_of_birth=request.date_of_birth,
            dependent_date_of_birth=request.date_of_birth,
            organization_id=1,
            unique_corp_id=request.unique_corp_id,
            implementation=model.ClientSpecificImplementation.MICROSOFT,
        )
        # Then
        assert isinstance(member, model.Member)

    async def test_perform_client_check_specific_census_fallback_caller_fail(
        self, members_versioned, client_specific_service, mock_msft_check
    ):
        # Given
        mock_msft_check.verify.return_value = None
        client_specific_service._get_mode.return_value = (
            model.ClientSpecificMode.FALLBACK_TO_CENSUS
        )
        request = factory.ClientSpecificEmployeeRequestFactory.create()
        expected = factory.MemberVersionedFactory.create()
        members_versioned.get_by_client_specific_verification = mock.AsyncMock(
            return_value=expected
        )
        # When
        member = await client_specific_service.perform_client_specific_verification(
            is_employee=request.is_employee,
            date_of_birth=request.date_of_birth,
            dependent_date_of_birth=request.dependent_date_of_birth,
            organization_id=1,
            unique_corp_id=request.unique_corp_id,
            implementation=model.ClientSpecificImplementation.MICROSOFT,
        )
        # Then
        assert isinstance(member, model.MemberVersioned)

    async def test_perform_client_specific_check_census_fallback_all_fail(
        self, members_versioned, client_specific_service, mock_msft_check
    ):
        # Given
        mock_msft_check.verify.return_value = None
        client_specific_service._get_mode.return_value = (
            model.ClientSpecificMode.FALLBACK_TO_CENSUS
        )
        request = factory.ClientSpecificEmployeeRequestFactory.create()
        members_versioned.get_by_client_specific_verification = mock.AsyncMock(
            return_value=None
        )
        # Then/When
        with pytest.raises(errors.ClientSpecificMatchError):
            await client_specific_service.perform_client_specific_verification(
                is_employee=request.is_employee,
                date_of_birth=request.date_of_birth,
                dependent_date_of_birth=request.date_of_birth,
                organization_id=1,
                unique_corp_id=request.unique_corp_id,
                implementation=model.ClientSpecificImplementation.MICROSOFT,
            )

    async def test_get_mode(self):
        # Given a valid implementation
        implementation: model.ClientSpecificImplementation = (
            model.ClientSpecificImplementation.MICROSOFT
        )

        # When
        mode: model.ClientSpecificMode = ClientSpecificService._get_mode(implementation)

        # Then
        assert model.ClientSpecificMode.ONLY_CLIENT_CHECK == mode

    async def test_perform_census_check_calls_member_when_disabled(
        self, client_specific_service
    ):
        # Given
        request = factory.ClientSpecificEmployeeRequestFactory.create()
        implementation: model.ClientSpecificImplementation = (
            model.ClientSpecificImplementation.MICROSOFT
        )

        # When
        client_specific_service.members_versioned.get_by_client_specific_verification.return_value = (
            factory.MemberFactory.create()
        )

        await client_specific_service._perform_census_check(request, 1, implementation)

        # Then
        client_specific_service.members_versioned.get_by_client_specific_verification.assert_called()

    async def test_perform_census_check_calls_member_versioned_when_enabled(
        self, client_specific_service
    ):
        # Given
        request = factory.ClientSpecificEmployeeRequestFactory.create()
        implementation: model.ClientSpecificImplementation = (
            model.ClientSpecificImplementation.MICROSOFT
        )

        # When
        await client_specific_service._perform_census_check(request, 1, implementation)

        # Then
        client_specific_service.members_versioned.get_by_client_specific_verification.assert_called()
