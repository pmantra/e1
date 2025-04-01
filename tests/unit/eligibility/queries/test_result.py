from unittest.mock import MagicMock, patch

import pytest

from app.eligibility.constants import EligibilityMethod
from app.eligibility.query_framework.errors import ValidationError
from app.eligibility.query_framework.registry import QueryDefinition

# Import the classes we're testing
from app.eligibility.query_framework.result import EligibilityResult, QueryResult
from db import model


class TestQueryResult:
    """Tests for the QueryResult class."""

    def test_is_success_with_valid_single_result(self):
        """Test is_success returns True with a valid single result."""
        # Create a mock MemberType
        mock_member = MagicMock(spec=model.MemberVersioned)

        # Given
        result = QueryResult(
            result=mock_member, query_name="test_query", query_type="basic", error=None
        )

        # When & Then
        assert result.is_success is True

    def test_is_success_with_valid_list_result(self):
        """Test is_success returns True with a valid list result."""
        # Create mock members
        mock_members = [
            MagicMock(spec=model.MemberVersioned),
            MagicMock(spec=model.MemberVersioned),
        ]

        # Given
        result = QueryResult(
            result=mock_members, query_name="test_query", query_type="basic", error=None
        )

        # When & Then
        assert result.is_success is True

    def test_is_success_with_error(self):
        """Test is_success returns False when error is present."""
        # Create a mock MemberType
        mock_member = MagicMock(spec=model.MemberVersioned)

        # Given
        result = QueryResult(
            result=mock_member,
            query_name="test_query",
            query_type="basic",
            error="Some error occurred",
        )

        # When & Then
        assert result.is_success is False

    def test_is_success_with_none_result(self):
        """Test is_success returns False when result is None."""
        # Given
        result = QueryResult(
            result=None, query_name="test_query", query_type="basic", error=None
        )

        # When & Then
        assert result.is_success is False

    def test_is_success_with_empty_list_result(self):
        """Test is_success returns False when result is an empty list."""
        # Given
        result = QueryResult(
            result=[], query_name="test_query", query_type="basic", error=None
        )

        # When & Then
        assert result.is_success is False

    def test_is_multiple_results_with_multiple_records(self):
        """Test is_multiple_results returns True with multiple records."""
        # Create mock members
        mock_members = [
            MagicMock(spec=model.MemberVersioned),
            MagicMock(spec=model.MemberVersioned),
        ]

        # Given
        result = QueryResult(
            result=mock_members, query_name="test_query", query_type="basic", error=None
        )

        # When & Then
        assert result.is_multiple_results is True

    def test_is_multiple_results_with_single_record_list(self):
        """Test is_multiple_results returns False with a single record list."""
        # Create a mock member
        mock_member = MagicMock(spec=model.MemberVersioned)

        # Given
        result = QueryResult(
            result=[mock_member],
            query_name="test_query",
            query_type="basic",
            error=None,
        )

        # When & Then
        assert result.is_multiple_results is False

    def test_is_multiple_results_with_single_record(self):
        """Test is_multiple_results returns False with a single record."""
        # Create a mock member
        mock_member = MagicMock(spec=model.MemberVersioned)

        # Given
        result = QueryResult(
            result=mock_member, query_name="test_query", query_type="basic", error=None
        )

        # When & Then
        assert result.is_multiple_results is False

    def test_is_multiple_results_with_none_result(self):
        """Test is_multiple_results returns False with None result."""
        # Given
        result = QueryResult(
            result=None, query_name="test_query", query_type="basic", error=None
        )

        # When & Then
        assert result.is_multiple_results is False

    def test_first_result_with_list(self):
        """Test first_result returns first item from list result."""
        # Create mock members
        mock_member1 = MagicMock(spec=model.MemberVersioned)
        mock_member2 = MagicMock(spec=model.MemberVersioned)

        # Given
        result = QueryResult(
            result=[mock_member1, mock_member2],
            query_name="test_query",
            query_type="basic",
            error=None,
        )

        # When & Then
        assert result.first_result == mock_member1

    def test_first_result_with_single_item(self):
        """Test first_result returns the single item when result is not a list."""
        # Create a mock member
        mock_member = MagicMock(spec=model.MemberVersioned)

        # Given
        result = QueryResult(
            result=mock_member, query_name="test_query", query_type="basic", error=None
        )

        # When & Then
        assert result.first_result == mock_member

    def test_first_result_with_none_result(self):
        """Test first_result returns None when result is None."""
        # Given
        result = QueryResult(
            result=None, query_name="test_query", query_type="basic", error=None
        )

        # When & Then
        assert result.first_result is None

    def test_first_result_with_empty_list(self):
        """Test first_result returns None when result is an empty list."""
        # Given
        result = QueryResult(
            result=[], query_name="test_query", query_type="basic", error=None
        )

        # When & Then
        assert result.first_result is None

    def test_get_failure_reason_with_error(self):
        """Test _get_failure_reason returns correct message with error."""
        # Given
        result = QueryResult(
            result=MagicMock(spec=model.MemberVersioned),
            query_name="test_query",
            query_type="basic",
            error="Test error message",
        )

        # Act
        failure_reason = result._get_failure_reason()

        # Assert
        assert failure_reason == "Error: Test error message"

    def test_get_failure_reason_with_none_result(self):
        """Test _get_failure_reason returns correct message with None result."""
        # Given
        result = QueryResult(
            result=None, query_name="test_query", query_type="basic", error=None
        )

        # Act
        failure_reason = result._get_failure_reason()

        # Assert
        assert failure_reason == "Result is None"

    def test_get_failure_reason_with_empty_list(self):
        """Test _get_failure_reason returns correct message with empty list result."""
        # Given
        result = QueryResult(
            result=[], query_name="test_query", query_type="basic", error=None
        )

        # Act
        failure_reason = result._get_failure_reason()

        # Assert
        assert failure_reason == "Empty result list"

    def test_get_failure_reason_with_unexpected_state(self):
        """Test _get_failure_reason returns correct message for unexpected state."""
        # Given
        mock_member = MagicMock(spec=model.MemberVersioned)
        result = QueryResult(
            result=mock_member, query_name="test_query", query_type="basic", error=None
        )

        # Act
        failure_reason = result._get_failure_reason()

        # Assert
        assert "Unexpected state: result=" in failure_reason
        assert "error=None" in failure_reason

    def test_log_execution_success(self):
        """Test log_execution logs correctly for success cases."""
        # Create a mock member
        mock_member = MagicMock(spec=model.MemberVersioned)

        # Given
        result = QueryResult(
            result=mock_member,
            query_name="test_query",
            query_type="basic",
            v_id=123,
            organization_id="org_1",
            error=None,
        )
        method = EligibilityMethod.BASIC
        params = {"user_id": 456, "first_name": "Test", "last_name": "User"}

        # Patch the logger.info method directly
        with patch(
            "app.eligibility.query_framework.result.logger.info"
        ) as mock_info, patch(
            "app.eligibility.query_framework.result.logger.warning"
        ) as mock_warning:
            # Act
            result.log_execution(method, params)

            # Assert
            mock_info.assert_called_once_with(
                "Query succeeded",
                extra={
                    "query": "test_query",
                    "query_type": "basic",
                    "method": method.value,
                    "user_id": 456,
                    "organization_id": "org_1",
                },
            )
            mock_warning.assert_not_called()

    def test_log_execution_failure_with_error(self):
        """Test log_execution logs correctly for failure cases with error."""
        # Given
        result = QueryResult(
            result=None,
            query_name="test_query",
            query_type="basic",
            v_id=123,
            organization_id="org_1",
            error="Test error message",
        )
        method = EligibilityMethod.EMPLOYER
        params = {"user_id": 456, "first_name": "Test", "last_name": "User"}

        # Patch the logger methods directly
        with patch(
            "app.eligibility.query_framework.result.logger.info"
        ) as mock_info, patch(
            "app.eligibility.query_framework.result.logger.warning"
        ) as mock_warning:
            # Act
            result.log_execution(method, params)

            # Assert
            mock_info.assert_not_called()
            mock_warning.assert_called_once_with(
                "Query failed",
                extra={
                    "query": "test_query",
                    "query_type": "basic",
                    "error": "Test error message",
                    "failure_reason": "Error: Test error message",
                    "result_type": None,
                    "user_id": 456,
                    "method": method.value,
                },
            )

    def test_log_execution_failure_with_none_result(self):
        """Test log_execution logs correctly for failure with None result."""
        # Given
        result = QueryResult(
            result=None,
            query_name="test_query",
            query_type="basic",
            v_id=123,
            organization_id="org_1",
            error=None,
        )
        method = EligibilityMethod.HEALTH_PLAN
        params = {"user_id": 456, "first_name": "Test", "last_name": "User"}

        # Patch the logger methods directly
        with patch(
            "app.eligibility.query_framework.result.logger.info"
        ) as mock_info, patch(
            "app.eligibility.query_framework.result.logger.warning"
        ) as mock_warning:
            # Act
            result.log_execution(method, params)

            # Assert
            mock_info.assert_not_called()
            mock_warning.assert_called_once_with(
                "Query failed",
                extra={
                    "query": "test_query",
                    "query_type": "basic",
                    "error": None,
                    "failure_reason": "Result is None",
                    "result_type": None,
                    "user_id": 456,
                    "method": method.value,
                },
            )

    def test_log_execution_failure_with_empty_list(self):
        """Test log_execution logs correctly for failure with empty list result."""
        # Given
        result = QueryResult(
            result=[],
            query_name="test_query",
            query_type="basic",
            v_id=123,
            organization_id="org_1",
            error=None,
        )
        method = EligibilityMethod.BASIC
        params = {"user_id": 456, "first_name": "Test", "last_name": "User"}

        # Patch the logger methods directly
        with patch(
            "app.eligibility.query_framework.result.logger.info"
        ) as mock_info, patch(
            "app.eligibility.query_framework.result.logger.warning"
        ) as mock_warning:
            # Act
            result.log_execution(method, params)

            # Assert
            mock_info.assert_not_called()
            mock_warning.assert_called_once_with(
                "Query failed",
                extra={
                    "query": "test_query",
                    "query_type": "basic",
                    "error": None,
                    "failure_reason": "Empty result list",
                    "result_type": "list",
                    "user_id": 456,
                    "method": method.value,
                },
            )


class TestEligibilityResult:
    """Tests for the EligibilityResult class."""

    def test_first_record_with_list(self):
        """Test first_record returns first item from list records."""
        # Create mock members
        mock_member1 = MagicMock(spec=model.MemberVersioned)
        mock_member2 = MagicMock(spec=model.MemberVersioned)

        # Given
        result = EligibilityResult(records=[mock_member1, mock_member2], v1_id=123)

        # When & Then
        assert result.first_record == mock_member1

    def test_first_record_with_single_item(self):
        """Test first_record returns the single record when not a list."""
        # Create a mock member
        mock_member = MagicMock(spec=model.MemberVersioned)

        # Given
        result = EligibilityResult(records=mock_member, v1_id=123)

        # When & Then
        assert result.first_record == mock_member

    def test_first_record_with_empty_list(self):
        """Test first_record returns None when records is an empty list."""
        # Given
        result = EligibilityResult(records=[], v1_id=123)

        # When & Then
        assert result.first_record is None

    def test_first_record_with_none_records(self):
        """Test first_record returns None when records is None."""
        # Given
        result = EligibilityResult(records=None, v1_id=123)

        # When & Then
        assert result.first_record is None


class TestFilterParams:
    """Tests for the filter_params method in QueryDefinition."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        # Create a simple QueryDefinition instance for testing
        # Patch the post_init method to avoid auto-detection
        with patch.object(QueryDefinition, "__post_init__"):
            self.query_def = QueryDefinition(
                method_name="test_method",
                required_params={"user_id", "email"},
                optional_params={"date_of_birth", "end_date", "first_name"},
            )

    def test_filter_params_basic(self):
        """Test basic parameter filtering without any date conversion."""
        # Input parameters
        params = {
            "user_id": 123,
            "email": "test@example.com",
            "first_name": "John",
            "irrelevant_param": "should be filtered out",
        }

        # Execute the method
        result = self.query_def.filter_params(params)

        # Verify results
        assert "user_id" in result
        assert "email" in result
        assert "first_name" in result
        assert "irrelevant_param" not in result
        assert result["user_id"] == 123
        assert result["email"] == "test@example.com"
        assert result["first_name"] == "John"

    def test_filter_params_none_and_missing(self):
        """Test filtering when parameters are None or missing."""
        # Input parameters with some None values and missing required params
        params = {
            "user_id": 123,
            "email": None,  # None value should be filtered out
            # first_name is missing but optional
            "irrelevant_param": "should be filtered out",
        }

        # Execute the method
        result = self.query_def.filter_params(params)

        # Verify results
        assert "user_id" in result
        assert "email" not in result  # Should be filtered out because it's None
        assert "first_name" not in result  # Missing optional param
        assert "irrelevant_param" not in result
        assert result["user_id"] == 123

    @patch("app.eligibility.convert.to_date")
    def test_filter_params_date_conversion_success(self, mock_to_date):
        """Test successful date parameter conversion."""
        # Setup mock for date conversion
        mock_to_date.return_value = "2023-01-15"  # Successfully converted date

        # Input parameters with date fields
        params = {
            "user_id": 123,
            "email": "test@example.com",
            "date_of_birth": "15/01/2023",  # Should be converted
            "end_date": "2023-01-31",  # Should also be converted
        }

        # Execute the method
        result = self.query_def.filter_params(params)

        # Verify results
        assert "date_of_birth" in result
        assert "end_date" in result
        assert result["date_of_birth"] == "2023-01-15"  # Converted value
        assert result["end_date"] == "2023-01-15"  # Converted value

        # Verify to_date was called for both date fields
        mock_to_date.assert_any_call("15/01/2023")
        mock_to_date.assert_any_call("2023-01-31")

    @patch("app.eligibility.convert.to_date")
    def test_filter_params_date_conversion_unknown(self, mock_to_date):
        """Test date parameter conversion with DATE_UNKNOWN result."""
        # Import convert to get access to DATE_UNKNOWN
        from app.eligibility import convert

        # Setup mock for date conversion
        mock_to_date.return_value = (
            convert.DATE_UNKNOWN
        )  # Conversion returned unknown date

        # Input parameters with date fields
        params = {
            "user_id": 123,
            "email": "test@example.com",
            "date_of_birth": "invalid-date",  # Will return DATE_UNKNOWN
            "end_date": "also-invalid",  # Will also return DATE_UNKNOWN
        }

        # Execute the method
        result = self.query_def.filter_params(params)

        # Verify results
        assert "date_of_birth" not in result  # Should be filtered out
        assert "end_date" not in result  # Should be filtered out
        assert "user_id" in result
        assert "email" in result

    @patch("app.eligibility.convert.to_date")
    def test_filter_params_date_validation_error(self, mock_to_date):
        """Test date parameter conversion with ValidationError."""
        # Import the correct ValidationError class
        from app.eligibility.service import ValidationError as ServiceValidationError

        # Setup mock for date conversion to raise ValidationError
        mock_to_date.side_effect = ServiceValidationError("Invalid date format")

        # Input parameters with date fields
        params = {
            "user_id": 123,
            "email": "test@example.com",
            "date_of_birth": "invalid-date",  # Will raise ValidationError
            "end_date": "also-invalid",  # Will also raise ValidationError
        }

        # Execute the method
        result = self.query_def.filter_params(params)

        # Verify results
        assert "date_of_birth" not in result  # Should be filtered out
        assert "end_date" not in result  # Should be filtered out
        assert "user_id" in result
        assert "email" in result

    def test_filter_params_mixed_scenario(self):
        """Test a mix of different parameter scenarios."""
        # Import convert to patch to_date
        from app.eligibility import convert

        # Create a more complex patching for different date parameter behaviors
        def mock_to_date_side_effect(value):
            if value == "2023-01-15":
                return "2023-01-15"  # Valid date, returns as is
            elif value == "invalid-date":
                return convert.DATE_UNKNOWN  # Unknown date
            elif value == "error-date":
                raise ValidationError("Invalid date format")  # Raises error
            else:
                return value  # Default case

        # Patch the to_date function
        with patch(
            "app.eligibility.convert.to_date", side_effect=mock_to_date_side_effect
        ):
            # Input parameters with mixed scenarios
            params = {
                "user_id": 123,
                "email": "test@example.com",
                "date_of_birth": "2023-01-15",  # Valid date
                "end_date": "invalid-date",  # Returns DATE_UNKNOWN
                "first_name": None,  # None value
                "non_existent": "filtered out",  # Not in valid_params
                "other_date": "error-date",  # Would raise ValidationError but not in valid_params
            }

            # Execute the method
            result = self.query_def.filter_params(params)

            # Verify results
            assert "user_id" in result
            assert "email" in result
            assert "date_of_birth" in result
            assert "end_date" not in result  # Filtered out due to DATE_UNKNOWN
            assert "first_name" not in result  # Filtered out due to None
            assert "non_existent" not in result  # Not in valid_params
            assert "other_date" not in result  # Not in valid_params
            assert result["date_of_birth"] == "2023-01-15"  # Successfully converted

    def test_filter_params_empty_input(self):
        """Test with empty input parameters."""
        # Execute the method with empty dict
        result = self.query_def.filter_params({})

        # Verify results
        assert result == {}

        # Try with None (though this should never happen in practice)
        with pytest.raises(TypeError, match="'NoneType' is not iterable"):
            self.query_def.filter_params(None)

    def test_filter_params_non_date_fields(self):
        """Test that non-date fields are not processed by to_date."""
        # Create a diverse set of parameters
        params = {"user_id": 123, "email": "test@example.com", "first_name": "John"}

        # Patch to_date to track if it's called for non-date fields
        with patch("app.eligibility.convert.to_date") as mock_to_date:
            # Execute the method
            result = self.query_def.filter_params(params)

            # Verify to_date was not called for any field
            mock_to_date.assert_not_called()

            # Verify all parameters are in the result
            assert "user_id" in result
            assert "email" in result
            assert "first_name" in result
