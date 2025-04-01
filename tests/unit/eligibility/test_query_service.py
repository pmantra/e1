from unittest import mock
from unittest.mock import AsyncMock, create_autospec

import pytest
from tests.factories import data_models

from app.eligibility.constants import EligibilityMethod
from app.eligibility.query_framework import EligibilityQueryExecutor, EligibilityResult
from app.eligibility.query_framework.errors import MatchMultipleError, MemberSearchError
from app.eligibility.query_framework.types import MultipleRecordType
from app.eligibility.query_service import EligibilityQueryService
from app.eligibility.service import ValidationError
from db import model


class TestBasicEligibility:
    @pytest.fixture
    def mock_query_executor(self):
        """Mock the query executor with AsyncMock for async methods."""
        executor = create_autospec(EligibilityQueryExecutor)
        executor.perform_eligibility_check = AsyncMock()
        return executor

    @pytest.fixture
    def query_service(self, mock_query_executor):
        """Create EligibilityQueryService with mocked dependencies."""
        return EligibilityQueryService(query_executor=mock_query_executor)

    @pytest.mark.asyncio
    async def test_check_basic_eligibility_validation_error(self, query_service):
        """Test validation error is raised when parameters are invalid."""
        # Given
        first_name = "Princess"
        last_name = "Zelda"
        date_of_birth = "invalid-date"

        # Use ValidationError from errors module
        query_service.query_executor.perform_eligibility_check.side_effect = ValidationError(
            f"No valid queries for {EligibilityMethod.BASIC.value} with provided parameters."
        )

        # When/Then
        with pytest.raises(MemberSearchError) as exc_info:
            await query_service.check_basic_eligibility(
                first_name=first_name, last_name=last_name, date_of_birth=date_of_birth
            )

        assert "No valid queries" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_check_basic_eligibility_no_results(self, query_service):
        """Test MemberSearchError is raised when no results are found."""
        # Given
        first_name = "Princess"
        last_name = "Zelda"
        date_of_birth = "1970-01-01"

        # Set up MemberSearchError for empty results
        query_service.query_executor.perform_eligibility_check.side_effect = (
            MemberSearchError(
                "No member records found matching the provided criteria.",
                method=EligibilityMethod.BASIC,
            )
        )

        # When/Then - Should raise a MemberSearchError
        with pytest.raises(MemberSearchError) as exc_info:
            await query_service.check_basic_eligibility(
                first_name=first_name, last_name=last_name, date_of_birth=date_of_birth
            )

        # Assert the error has the correct attributes
        assert "No member records found" in str(exc_info.value)
        assert exc_info.value.method == EligibilityMethod.BASIC

    @pytest.mark.asyncio
    async def test_check_basic_eligibility_unexpected_error(self, query_service):
        """Test that unexpected errors are converted to MemberSearchError."""
        # Given
        query_service.query_executor.perform_eligibility_check.side_effect = (
            RuntimeError("Unexpected error")
        )

        # When/Then - Should raise MemberSearchError due to error handler
        with pytest.raises(MemberSearchError) as exc_info:
            await query_service.check_basic_eligibility(
                first_name="John", last_name="Doe", date_of_birth="1990-01-01"
            )

        # Check error was properly wrapped
        assert "Error during eligibility check" in str(exc_info.value)
        assert "Unexpected error" in str(exc_info.value)
        assert exc_info.value.method == EligibilityMethod.BASIC

    @pytest.mark.asyncio
    async def test_check_basic_eligibility_member_conversion_error(self, query_service):
        """Test ValueError when member conversion fails."""
        # Given - Create an EligibilityResult with data that will fail conversion
        mock_result = EligibilityResult(
            records=[object()], v1_id=1  # Not a valid member type
        )
        query_service.query_executor.perform_eligibility_check.return_value = (
            mock_result
        )

        # When/Then
        with pytest.raises(MemberSearchError) as exc_info:
            await query_service.check_basic_eligibility(
                first_name="Princess", last_name="Zelda", date_of_birth="1970-01-01"
            )

        assert "Error during eligibility check" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_check_basic_eligibility_multiple_members(
        self, query_service, config
    ):
        """Test basic eligibility check returning multiple members."""
        # Given
        first_name = "Princess"
        last_name = "Zelda"
        date_of_birth = "1970-01-01"

        # Create mock members
        mock_members = [
            data_models.MemberResponseFactory.create(
                id="1",
                organization_id=f"{config.organization_id}_{i}",
                first_name=first_name,
                last_name=last_name,
                date_of_birth=date_of_birth,
            )
            for i in range(3)
        ]

        # Return EligibilityResult instead of tuple
        query_service.query_executor.perform_eligibility_check.return_value = (
            EligibilityResult(records=mock_members, v1_id=1)
        )

        # When
        result = await query_service.check_basic_eligibility(
            first_name=first_name, last_name=last_name, date_of_birth=date_of_birth
        )

        # Then
        assert isinstance(result, list)
        assert len(result) == 3
        for member_response in result:
            assert isinstance(member_response, model.MemberResponse)
            assert member_response.first_name == first_name
            assert member_response.last_name == last_name

        # Verify executor was called with correct parameters
        query_service.query_executor.perform_eligibility_check.assert_called_once_with(
            method=EligibilityMethod.BASIC,
            params={
                "first_name": first_name,
                "last_name": last_name,
                "date_of_birth": date_of_birth,
                "user_id": None,
            },
            expected_type=MultipleRecordType,
        )

    @pytest.mark.asyncio
    async def test_check_basic_eligibility_missing_params(self, query_service):
        """Test validation error when required parameters are missing."""
        # Given
        query_service.query_executor.perform_eligibility_check.side_effect = ValidationError(
            f"No valid queries for {EligibilityMethod.BASIC.value} with provided parameters."
        )

        # When/Then
        with pytest.raises(MemberSearchError) as exc_info:
            await query_service.check_basic_eligibility(
                first_name="",  # Empty string
                last_name="Zelda",
                date_of_birth="1970-01-01",
            )

        assert "No valid queries" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_check_basic_eligibility_single_member(self, query_service, config):
        """Test basic eligibility check returning a single member response."""
        # Given
        first_name = "Princess"
        last_name = "Zelda"
        date_of_birth = "1970-01-01"

        mock_response = data_models.MemberResponseFactory.create(
            id="1",
            organization_id=config.organization_id,
            first_name=first_name,
            last_name=last_name,
            date_of_birth=date_of_birth,
        )

        # Return EligibilityResult instead of tuple
        query_service.query_executor.perform_eligibility_check.return_value = (
            EligibilityResult(records=[mock_response], v1_id=1)
        )

        # When
        result = await query_service.check_basic_eligibility(
            first_name=first_name, last_name=last_name, date_of_birth=date_of_birth
        )

        # Then
        assert isinstance(result, list)
        assert len(result) == 1
        assert isinstance(result[0], model.MemberResponse)
        assert result[0].first_name == first_name
        assert result[0].last_name == last_name


class TestEmployerEligibility:
    email = "mock_email"
    date_of_birth = "1998-10-12"
    dependent_date_of_birth = "2008-12-11"
    employee_first_name = "mock_employee_first_name"
    employee_last_name = "mock_employee_last_name"
    first_name = "mock_first_name"
    last_name = "mock_last_name"
    work_state = "mock_workstate"

    @pytest.fixture
    def mock_query_executor(self):
        """Mock the query executor with AsyncMock for async methods."""
        executor = create_autospec(EligibilityQueryExecutor)
        executor.perform_eligibility_check = AsyncMock()
        return executor

    @pytest.fixture
    def query_service(self, mock_query_executor):
        """Create EligibilityQueryService with mocked dependencies."""
        return EligibilityQueryService(query_executor=mock_query_executor)

    @pytest.mark.asyncio
    async def test_check_employer_eligibility_found_member(self, query_service):
        """Test successful member eligibility check."""
        # Create a mock member
        member_found = data_models.MemberResponseFactory.create(
            id=1,
            organization_id=1,
            first_name=TestEmployerEligibility.first_name,
            last_name=TestEmployerEligibility.last_name,
        )

        # Set up the mock query result
        mock_result = EligibilityResult(records=member_found, v1_id=1)
        query_service.query_executor.perform_eligibility_check.return_value = (
            mock_result
        )

        # Call the service method
        res = await query_service.check_employer_eligibility(
            email=TestEmployerEligibility.email,
            date_of_birth=TestEmployerEligibility.date_of_birth,
            dependent_date_of_birth=TestEmployerEligibility.dependent_date_of_birth,
            employee_first_name=TestEmployerEligibility.employee_first_name,
            employee_last_name=TestEmployerEligibility.employee_last_name,
            first_name=TestEmployerEligibility.first_name,
            last_name=TestEmployerEligibility.last_name,
            work_state=TestEmployerEligibility.work_state,
        )

        # Verify results
        assert isinstance(res, model.MemberResponse)
        assert res.organization_id == member_found.organization_id
        assert res.first_name == TestEmployerEligibility.first_name
        assert res.last_name == TestEmployerEligibility.last_name

    @pytest.mark.asyncio
    async def test_check_employer_partner_eligibility_with_basic(
        self, query_service, member
    ):
        """Test employer eligibility with basic parameters."""
        member = data_models.MemberResponseFactory.create(
            id=1,
            organization_id=1,
            first_name=self.first_name,
            last_name=self.last_name,
            date_of_birth=self.date_of_birth,
        )

        # Set up the mock query result
        mock_result = EligibilityResult(records=member, v1_id=1)
        query_service.query_executor.perform_eligibility_check.return_value = (
            mock_result
        )

        result = await query_service.check_employer_eligibility(
            employee_first_name=self.first_name,
            employee_last_name=self.last_name,
            date_of_birth=self.date_of_birth,
        )

        assert isinstance(result, model.MemberResponse)
        assert result.date_of_birth == self.date_of_birth
        assert result.first_name == self.first_name
        assert result.last_name == self.last_name

    @pytest.mark.asyncio
    async def test_check_employer_eligibility(self, query_service):
        """Test employer eligibility when v2 is disabled."""
        member_found = data_models.MemberResponseFactory.create(
            id=1,
            organization_id=1,
            first_name=TestEmployerEligibility.first_name,
            last_name=TestEmployerEligibility.last_name,
        )

        # Set up the mock query result
        mock_result = EligibilityResult(records=member_found, v1_id=1)
        query_service.query_executor.perform_eligibility_check.return_value = (
            mock_result
        )

        with mock.patch("maven.feature_flags.json_variation", return_value=[]):
            result = await query_service.check_employer_eligibility(
                email=TestEmployerEligibility.email,
                first_name=TestEmployerEligibility.first_name,
                last_name=TestEmployerEligibility.last_name,
            )

        assert isinstance(result, model.MemberResponse)
        assert result.first_name == TestEmployerEligibility.first_name
        assert result.last_name == TestEmployerEligibility.last_name

    @pytest.mark.asyncio
    async def test_check_employer_eligibility_validation_error(
        self, query_service, caplog
    ):
        """Test validation error handling with re-raised exception."""
        # Create a validation error
        member_search_error = MemberSearchError("mock validate error")
        query_service.query_executor.perform_eligibility_check.side_effect = (
            member_search_error
        )

        # Call the service method and expect the error to be re-raised
        with pytest.raises(MemberSearchError) as exc_info:
            await query_service.check_employer_eligibility(
                email=TestEmployerEligibility.email,
                date_of_birth=TestEmployerEligibility.date_of_birth,
                user_id=123,
            )

        # Verify the exception is the same one we raised
        assert exc_info.value is member_search_error

    @pytest.mark.asyncio
    async def test_check_employer_eligibility_member_search_error(
        self, query_service, caplog
    ):
        """Test MemberSearchError handling with re-raised exception and method enrichment."""
        # Create a member search error without a method
        search_error = MemberSearchError("No matching records found")
        assert not hasattr(search_error, "method") or search_error.method is None

        query_service.query_executor.perform_eligibility_check.side_effect = (
            search_error
        )

        # Call the service method and expect the error to be re-raised
        with pytest.raises(MemberSearchError) as exc_info:
            await query_service.check_employer_eligibility(
                first_name=TestEmployerEligibility.first_name,
                last_name=TestEmployerEligibility.last_name,
                user_id=456,
            )

        # Verify the exception is the same one we raised
        assert exc_info.value is search_error

        # Verify the method was set on the exception
        assert exc_info.value.method == EligibilityMethod.EMPLOYER

        # Verify the error was logged correctly
        assert "Member search error in employer eligibility check" in caplog.text
        assert "No matching records found" in caplog.text
        assert "456" in caplog.text  # user_id should be logged

    @pytest.mark.asyncio
    async def test_check_employer_eligibility_generic_exception(
        self, query_service, caplog
    ):
        """Test generic exception handling with conversion to MemberSearchError."""
        # Create a generic exception
        generic_error = RuntimeError("Something went wrong")
        query_service.query_executor.perform_eligibility_check.side_effect = (
            generic_error
        )

        # Call the service method and expect a MemberSearchError to be raised
        with pytest.raises(MemberSearchError) as exc_info:
            await query_service.check_employer_eligibility(
                first_name=TestEmployerEligibility.first_name,
                last_name=TestEmployerEligibility.last_name,
                user_id=789,
            )

        # Verify a new MemberSearchError was created with the original exception as cause
        assert exc_info.value.__cause__ is generic_error
        assert "Error during eligibility check: Something went wrong" in str(
            exc_info.value
        )
        assert exc_info.value.method == EligibilityMethod.EMPLOYER

        # Verify the error was logged correctly
        assert "Error in employer eligibility check" in caplog.text
        assert "Something went wrong" in caplog.text
        assert "RuntimeError" in caplog.text
        assert "789" in caplog.text  # user_id should be logged

    @pytest.mark.asyncio
    async def test_check_employer_partner_eligibility_with_employee_name_and_dob_multiple_results(
        self, query_service
    ):
        """Test employer eligibility handles multiple results correctly."""
        query_service.query_executor.perform_eligibility_check.side_effect = (
            MatchMultipleError("Multiple active records found")
        )

        with pytest.raises(MemberSearchError) as exc_info:
            await query_service.check_employer_eligibility(
                employee_first_name=self.first_name,
                employee_last_name=self.last_name,
                date_of_birth=self.date_of_birth,
            )

        assert "Multiple active records found" in str(exc_info.value)
        assert exc_info.value.method == EligibilityMethod.EMPLOYER


class TestHealthPlanEligibility:
    # Test constants
    FIRST_NAME = "Princess"
    LAST_NAME = "Zelda"
    UNIQUE_CORP_ID = "hyrule_123"
    DATE_OF_BIRTH = "1990-01-01"
    DEPENDENT_DOB = "2010-01-01"

    @pytest.fixture
    def mock_query_executor(self):
        executor = create_autospec(EligibilityQueryExecutor)
        executor.perform_eligibility_check = AsyncMock()
        return executor

    @pytest.fixture
    def query_service(self, mock_query_executor):
        """Create EligibilityQueryService with mocked dependencies."""
        return EligibilityQueryService(query_executor=mock_query_executor)

    @pytest.mark.asyncio
    async def test_check_healthplan_eligibility_with_name_and_id(
        self, query_service, member
    ):
        """Test successful eligibility check with name and unique_corp_id."""
        member = data_models.MemberResponseFactory.create(
            id=1,
            organization_id=1,
            first_name=self.FIRST_NAME,
            last_name=self.LAST_NAME,
            unique_corp_id=self.UNIQUE_CORP_ID,
        )

        # Set up the mock query result
        mock_result = EligibilityResult(records=member, v1_id=1)
        query_service.query_executor.perform_eligibility_check.return_value = (
            mock_result
        )

        result = await query_service.check_healthplan_eligibility(
            unique_corp_id=self.UNIQUE_CORP_ID,
            first_name=self.FIRST_NAME,
            last_name=self.LAST_NAME,
        )

        assert isinstance(result, model.MemberResponse)
        assert result.first_name == self.FIRST_NAME
        assert result.last_name == self.LAST_NAME
        assert result.unique_corp_id == self.UNIQUE_CORP_ID

    @pytest.mark.asyncio
    async def test_check_healthplan_eligibility_with_dob(self, query_service, member):
        """Test eligibility check with date of birth."""
        member = data_models.MemberResponseFactory.create(
            id=1,
            organization_id=1,
            first_name=self.FIRST_NAME,
            last_name=self.LAST_NAME,
            date_of_birth=self.DATE_OF_BIRTH,
            unique_corp_id=self.UNIQUE_CORP_ID,
        )

        # Set up the mock query result
        mock_result = EligibilityResult(records=member, v1_id=1)
        query_service.query_executor.perform_eligibility_check.return_value = (
            mock_result
        )

        result = await query_service.check_healthplan_eligibility(
            unique_corp_id=self.UNIQUE_CORP_ID, date_of_birth=self.DATE_OF_BIRTH
        )

        assert isinstance(result, model.MemberResponse)
        assert result.date_of_birth == self.DATE_OF_BIRTH
        assert result.first_name == self.FIRST_NAME

    @pytest.mark.asyncio
    async def test_check_healthplan_eligibility_validation_error(
        self, query_service, caplog
    ):
        """Test handling of validation error with proper logging."""
        # Create a validation error
        validation_error = MemberSearchError("Invalid parameters")
        query_service.query_executor.perform_eligibility_check.side_effect = (
            validation_error
        )

        user_id = 123

        with pytest.raises(MemberSearchError) as exc_info:
            await query_service.check_healthplan_eligibility(
                unique_corp_id=self.UNIQUE_CORP_ID, user_id=user_id
            )

        # Verify the exception is the same one we raised
        assert exc_info.value is validation_error

    @pytest.mark.asyncio
    async def test_check_healthplan_eligibility_no_match(self, query_service, caplog):
        """Test when no matching member is found."""
        # Create a search error with method already set
        search_error = MemberSearchError(
            "No matching member found", method=EligibilityMethod.HEALTH_PLAN
        )
        query_service.query_executor.perform_eligibility_check.side_effect = (
            search_error
        )

        user_id = 456

        # When/Then
        with pytest.raises(MemberSearchError) as exc_info:
            await query_service.check_healthplan_eligibility(
                unique_corp_id=self.UNIQUE_CORP_ID,
                first_name=self.FIRST_NAME,
                last_name=self.LAST_NAME,
                user_id=user_id,
            )

        # Verify the exception is the same one we raised
        assert exc_info.value is search_error

    @pytest.mark.asyncio
    async def test_check_healthplan_eligibility_unexpected_error(
        self, query_service, caplog
    ):
        """Test handling of unexpected errors with conversion to MemberSearchError."""
        # Create a generic exception
        generic_error = RuntimeError("Unexpected error")
        query_service.query_executor.perform_eligibility_check.side_effect = (
            generic_error
        )

        user_id = 789

        with pytest.raises(MemberSearchError) as exc_info:
            await query_service.check_healthplan_eligibility(
                unique_corp_id=self.UNIQUE_CORP_ID,
                first_name=self.FIRST_NAME,
                last_name=self.LAST_NAME,
                user_id=user_id,
            )

        # Verify a new MemberSearchError was created with the original exception as cause
        assert exc_info.value.__cause__ is generic_error
        assert "Error during eligibility check: Unexpected error" in str(exc_info.value)
        assert exc_info.value.method == EligibilityMethod.HEALTH_PLAN

    @pytest.mark.asyncio
    async def test_check_healthplan_eligibility_with_basic(self, query_service, member):
        """Test eligibility check with date of birth."""
        member = data_models.MemberResponseFactory.create(
            id=1,
            organization_id=1,
            first_name=self.FIRST_NAME,
            last_name=self.LAST_NAME,
            date_of_birth=self.DATE_OF_BIRTH,
        )

        # Set up the mock query result
        mock_result = EligibilityResult(records=member, v1_id=1)
        query_service.query_executor.perform_eligibility_check.return_value = (
            mock_result
        )

        result = await query_service.check_healthplan_eligibility(
            first_name=self.FIRST_NAME,
            last_name=self.LAST_NAME,
            date_of_birth=self.DATE_OF_BIRTH,
        )

        assert isinstance(result, model.MemberResponse)
        assert result.date_of_birth == self.DATE_OF_BIRTH
        assert result.first_name == self.FIRST_NAME
        assert result.last_name == self.LAST_NAME

    @pytest.mark.asyncio
    async def test_check_healthplan_eligibility_with_name_and_dob_multiple_results(
        self, query_service
    ):
        """Test health plan eligibility handles multiple results correctly."""
        query_service.query_executor.perform_eligibility_check.side_effect = (
            MatchMultipleError("Multiple active records found")
        )

        with pytest.raises(MemberSearchError):
            await query_service.check_healthplan_eligibility(
                first_name=self.FIRST_NAME,
                last_name=self.LAST_NAME,
                date_of_birth=self.DATE_OF_BIRTH,
            )

    @pytest.mark.asyncio
    async def test_check_healthplan_partner_eligibility_with_basic(
        self, query_service, member
    ):
        """Test eligibility check with date of birth."""
        member = data_models.MemberResponseFactory.create(
            id=1,
            organization_id=1,
            first_name=self.FIRST_NAME,
            last_name=self.LAST_NAME,
            date_of_birth=self.DATE_OF_BIRTH,
        )

        # Set up the mock query result
        mock_result = EligibilityResult(records=member, v1_id=1)
        query_service.query_executor.perform_eligibility_check.return_value = (
            mock_result
        )

        result = await query_service.check_healthplan_eligibility(
            employee_first_name=self.FIRST_NAME,
            employee_last_name=self.LAST_NAME,
            date_of_birth=self.DATE_OF_BIRTH,
        )

        assert isinstance(result, model.MemberResponse)
        assert result.date_of_birth == self.DATE_OF_BIRTH
        assert result.first_name == self.FIRST_NAME
        assert result.last_name == self.LAST_NAME

    @pytest.mark.asyncio
    async def test_check_healthplan_partner_eligibility_with_employee_name_and_dob_multiple_results(
        self, query_service
    ):
        """Test health plan eligibility handles multiple results correctly."""
        query_service.query_executor.perform_eligibility_check.side_effect = (
            MatchMultipleError("Multiple active records found")
        )

        with pytest.raises(MemberSearchError):
            await query_service.check_healthplan_eligibility(
                employee_first_name=self.FIRST_NAME,
                employee_last_name=self.LAST_NAME,
                date_of_birth=self.DATE_OF_BIRTH,
            )

    @pytest.mark.asyncio
    async def test_check_healthplan_eligibility_with_v2(self, query_service, config):
        """Test healthplan eligibility when v2 is enabled."""
        member = data_models.MemberResponseFactory.create(
            id=1,
            organization_id=config.organization_id,
            first_name=TestHealthPlanEligibility.FIRST_NAME,
            last_name=TestHealthPlanEligibility.LAST_NAME,
            unique_corp_id=TestHealthPlanEligibility.UNIQUE_CORP_ID,
        )

        # Set up the mock query result
        mock_result = EligibilityResult(records=member, v1_id=1)
        query_service.query_executor.perform_eligibility_check.return_value = (
            mock_result
        )

        with mock.patch(
            "maven.feature_flags.json_variation", return_value=[config.organization_id]
        ):
            result = await query_service.check_healthplan_eligibility(
                unique_corp_id=TestHealthPlanEligibility.UNIQUE_CORP_ID,
                first_name=TestHealthPlanEligibility.FIRST_NAME,
                last_name=TestHealthPlanEligibility.LAST_NAME,
            )

        assert isinstance(result, model.MemberResponse)
        assert result.first_name == self.FIRST_NAME
        assert result.last_name == self.LAST_NAME
        assert result.unique_corp_id == self.UNIQUE_CORP_ID

    @pytest.mark.asyncio
    async def test_healthplan_eligibility_error_handler_preserves_function_metadata(
        self, query_service
    ):
        """Test that the error handler decorator preserves the function's metadata."""
        # Check that the docstring is preserved
        assert (
            "Check health-plan eligibility."
            in query_service.check_healthplan_eligibility.__doc__
        )
        assert "Returns:" in query_service.check_healthplan_eligibility.__doc__
        assert "Raises:" in query_service.check_healthplan_eligibility.__doc__

        # Check that the function name is preserved
        assert (
            query_service.check_healthplan_eligibility.__name__
            == "check_healthplan_eligibility"
        )
