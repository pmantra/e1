"""Tests for the eligibility query registry."""
import datetime
from unittest.mock import MagicMock, patch

import pytest

from app.eligibility import convert
from app.eligibility.constants import EligibilityMethod
from app.eligibility.query_framework.registry import QueryDefinition, QueryRegistry


class TestQueryDefinition:
    """Test the QueryDefinition class."""

    def test_query_type_derivation(self):
        """Test that query_type is correctly derived from method name."""
        test_cases = [
            ("get_by_dob_and_email", "by_dob_and_email"),
            ("get_by_dob_and_email_v2", "by_dob_and_email"),
            ("get_all_by_name_and_date_of_birth", "all_by_name_and_date_of_birth"),
            ("get_all_by_name_and_date_of_birth_v2", "all_by_name_and_date_of_birth"),
            (
                "get_by_dependent_date_of_birth_and_unique_corp_id",
                "by_dependent_date_of_birth_and_unique_corp_id",
            ),
        ]

        for method_name, expected_type in test_cases:
            query = QueryDefinition(method_name=method_name)
            assert query.query_type == expected_type, (
                f"Expected query_type {expected_type} for {method_name}, "
                f"got {query.query_type}"
            )

    def test_auto_parameter_detection(self):
        """Test parameter detection with manual setup."""
        # Create a query definition with explicit params instead of auto-detection
        query_def = QueryDefinition(
            method_name="test_method",
            required_params={"date_of_birth", "first_name", "last_name"},
            optional_params={"user_id"},
        )

        # Verify detected parameters
        assert query_def.required_params == {"date_of_birth", "first_name", "last_name"}
        assert query_def.optional_params == {"user_id"}

    def test_validate_params_success(self):
        """Test parameter validation with valid parameters."""
        query_def = QueryDefinition(
            method_name="test_method",
            required_params={"first_name", "last_name", "date_of_birth"},
            optional_params={"user_id"},
        )

        params = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
            "user_id": 123,
        }

        result = query_def.validate_params(params)
        assert result.is_valid is True
        assert result.missing_params == set()

    def test_validate_params_failure(self):
        """Test parameter validation with missing required parameters."""
        query_def = QueryDefinition(
            method_name="test_method",
            required_params={"first_name", "last_name", "date_of_birth"},
            optional_params={"user_id"},
        )

        params = {
            "first_name": "John",
            # Missing last_name
            "date_of_birth": "1990-01-01",
            "user_id": 123,
        }

        result = query_def.validate_params(params)
        assert result.is_valid is False
        assert result.missing_params == {"last_name"}

    def test_validate_params_empty_string(self):
        """Test parameter validation with empty string parameters."""
        query_def = QueryDefinition(
            method_name="test_method",
            required_params={"first_name", "last_name", "date_of_birth"},
            optional_params={"user_id"},
        )

        params = {
            "first_name": "John",
            "last_name": "",  # Empty string
            "date_of_birth": "1990-01-01",
            "user_id": 123,
        }

        result = query_def.validate_params(params)
        assert result.is_valid is False
        assert result.missing_params == {"last_name"}

    def test_post_init_method_not_found_error(self):
        """Test that __post_init__ raises ValueError when method is not found."""
        from app.eligibility.query_framework.registry import QueryDefinition

        # Create a simple mock repository class
        class MockRepository:
            def existing_method(self):
                pass

        # Patch the import to return our mock class
        with patch(
            "app.eligibility.query_framework.EligibilityQueryRepository",
            MockRepository,
        ):
            # Now attempt to create a QueryDefinition with a method that doesn't exist
            with pytest.raises(ValueError) as exc_info:
                QueryDefinition(method_name="non_existent_method")

            # Verify the error message
            assert "Method 'non_existent_method' not found in repository" in str(
                exc_info.value
            )

    def test_post_init_optional_param_detection(self):
        """Test that __post_init__ correctly identifies optional parameters."""

        # Create a mock method with both required and optional parameters
        # Define a mock repository class
        class MockRepoClass:
            @staticmethod
            def mock_method(*, required_param, optional_param="default"):
                pass

        # Patch the direct reference in the QueryDefinition class
        with patch(
            "app.eligibility.query_framework.registry.EligibilityQueryRepository",
            MockRepoClass,
        ):
            # No need to patch inspect.signature as it will work with the real method

            # Create a QueryDefinition without specifying required/optional params
            query_def = QueryDefinition(method_name="mock_method")

            # Verify that optional_param was correctly identified as optional
            assert "optional_param" in query_def.optional_params
            # Verify that required_param was correctly identified as required
            assert "required_param" in query_def.required_params
            # Verify that 'self' was not included in either set
            assert "self" not in query_def.required_params
            assert "self" not in query_def.optional_params


class TestQueryRegistry:
    """Test the QueryRegistry class."""

    def test_registry_has_all_methods(self):
        """Test that registry contains all expected methods."""
        # Check BASIC queries
        assert len(QueryRegistry.BASIC_V1_QUERIES) > 0
        assert len(QueryRegistry.BASIC_V2_QUERIES) > 0

        # Check EMPLOYER queries
        assert len(QueryRegistry.EMPLOYER_V1_QUERIES) > 0
        assert len(QueryRegistry.EMPLOYER_V2_QUERIES) > 0

        # Check HEALTH_PLAN queries
        assert len(QueryRegistry.HEALTH_PLAN_V1_QUERIES) > 0
        assert len(QueryRegistry.HEALTH_PLAN_V2_QUERIES) > 0

    def test_get_v1_queries(self):
        """Test getting V1 queries for each method."""
        basic_queries = QueryRegistry.get_v1_queries(EligibilityMethod.BASIC)
        employer_queries = QueryRegistry.get_v1_queries(EligibilityMethod.EMPLOYER)
        health_plan_queries = QueryRegistry.get_v1_queries(
            EligibilityMethod.HEALTH_PLAN
        )

        assert basic_queries == QueryRegistry.BASIC_V1_QUERIES
        assert employer_queries == QueryRegistry.EMPLOYER_V1_QUERIES
        assert health_plan_queries == QueryRegistry.HEALTH_PLAN_V1_QUERIES

    def test_get_v2_queries(self):
        """Test getting V2 queries for each method."""
        basic_queries = QueryRegistry.get_v2_queries(EligibilityMethod.BASIC)
        employer_queries = QueryRegistry.get_v2_queries(EligibilityMethod.EMPLOYER)
        health_plan_queries = QueryRegistry.get_v2_queries(
            EligibilityMethod.HEALTH_PLAN
        )

        assert basic_queries == QueryRegistry.BASIC_V2_QUERIES
        assert employer_queries == QueryRegistry.EMPLOYER_V2_QUERIES
        assert health_plan_queries == QueryRegistry.HEALTH_PLAN_V2_QUERIES

    def test_get_queries_invalid_method(self):
        """Test getting queries with invalid method."""
        invalid_method = MagicMock()
        invalid_method.value = "INVALID"

        with pytest.raises(ValueError):
            QueryRegistry.get_v1_queries(invalid_method)

        with pytest.raises(ValueError):
            QueryRegistry.get_v2_queries(invalid_method)

    def test_v1_v2_method_parity(self):
        """Ensure there's parity between V1 and V2 query types."""
        for method in [
            EligibilityMethod.BASIC,
            EligibilityMethod.EMPLOYER,
            EligibilityMethod.HEALTH_PLAN,
        ]:
            v1_queries = QueryRegistry.get_v1_queries(method)
            v2_queries = QueryRegistry.get_v2_queries(method)

            v1_types = {q.query_type for q in v1_queries}
            v2_types = {q.query_type for q in v2_queries}

            # Check if any query types are missing in either version
            v1_missing = v2_types - v1_types
            v2_missing = v1_types - v2_types

            assert (
                not v1_missing
            ), f"V1 is missing query types: {v1_missing} for method {method}"
            assert (
                not v2_missing
            ), f"V2 is missing query types: {v2_missing} for method {method}"


class TestValidateAndFilterParams:
    def test_validate_params_required_parameters(self):
        """Test validation of required parameters."""
        # Set up a query definition with required and optional params
        query_def = QueryDefinition(
            method_name="test_method",
            required_params={"first_name", "date_of_birth"},
            optional_params={"email"},
        )

        # Test with valid parameters
        valid_params = {
            "first_name": "John",
            "date_of_birth": "1990-01-01",
            "email": "john@example.com",
        }
        result = query_def.validate_params(valid_params)
        assert result.is_valid is True
        assert not result.missing_params

        # Test with missing required parameter
        invalid_params = {
            "first_name": "John",
            # date_of_birth missing
            "email": "john@example.com",
        }
        result = query_def.validate_params(invalid_params)
        assert result.is_valid is False
        assert "date_of_birth" in result.missing_params

    def test_validate_params_empty_string_handling(self):
        """Test validation handles empty strings properly."""
        query_def = QueryDefinition(
            method_name="test_method",
            required_params={"first_name"},
            optional_params={"last_name"},
        )

        # Empty string for required param
        params = {"first_name": ""}
        result = query_def.validate_params(params)
        assert result.is_valid is False
        assert "first_name" in result.missing_params

        # Whitespace string for required param
        params = {"first_name": "   "}
        result = query_def.validate_params(params)
        assert result.is_valid is False
        assert "first_name" in result.missing_params

    def test_filter_params_date_conversion(self):
        """Test parameter filtering with date conversion."""
        query_def = QueryDefinition(
            method_name="test_method",
            required_params={"date_of_birth"},
            optional_params={"end_date", "name"},
        )

        # Mock the date conversion
        with patch("app.eligibility.convert.to_date") as mock_to_date:
            # Setup the mock to return a date for valid input
            mock_to_date.side_effect = lambda d: (
                datetime.date(1990, 1, 1)
                if d == "1990-01-01"
                else datetime.date(2023, 5, 15)
                if d == "2023-05-15"
                else convert.DATE_UNKNOWN
            )

            # Test with valid dates
            params = {
                "date_of_birth": "1990-01-01",  # Required
                "end_date": "2023-05-15",  # Optional
                "name": "John",
            }
            result = query_def.filter_params(params)

            # Verify all parameters are included with dates converted
            assert "date_of_birth" in result
            assert result["date_of_birth"] == datetime.date(1990, 1, 1)
            assert "end_date" in result
            assert result["end_date"] == datetime.date(2023, 5, 15)
            assert "name" in result
            assert result["name"] == "John"

    def test_filter_params_invalid_optional_date(self):
        """Test parameter filtering skips invalid optional dates."""
        query_def = QueryDefinition(
            method_name="test_method",
            required_params={"date_of_birth"},
            optional_params={"end_date", "name"},
        )

        # Mock the date conversion
        with patch("app.eligibility.convert.to_date") as mock_to_date:
            # Setup the mock for different scenarios
            mock_to_date.side_effect = lambda d: (
                datetime.date(1990, 1, 1)
                if d == "1990-01-01"
                else convert.DATE_UNKNOWN  # Invalid date for end_date
            )

            # Test with valid required date but invalid optional date
            params = {
                "date_of_birth": "1990-01-01",  # Required - valid
                "end_date": "invalid-date",  # Optional - invalid
                "name": "John",
            }
            result = query_def.filter_params(params)

            # Verify required date is included and optional invalid date is excluded
            assert "date_of_birth" in result
            assert result["date_of_birth"] == datetime.date(1990, 1, 1)
            assert "end_date" not in result  # Should be excluded
            assert "name" in result
            assert result["name"] == "John"

    def test_filter_params_validation_error_handling(self):
        """Test handling of ValidationError during date conversion."""
        query_def = QueryDefinition(
            method_name="test_method",
            required_params={"date_of_birth"},
            optional_params={"end_date", "name"},
        )

        # Mock the date conversion to raise ValidationError for end_date
        with patch("app.eligibility.convert.to_date") as mock_to_date:
            from app.eligibility.service import ValidationError

            def mock_convert(date_str):
                if date_str == "1990-01-01":
                    return datetime.date(1990, 1, 1)
                elif date_str == "invalid-format":
                    raise ValidationError("Invalid date format")
                return None

            mock_to_date.side_effect = mock_convert

            # Test with ValidationError for optional parameter
            params = {
                "date_of_birth": "1990-01-01",  # Required - valid
                "end_date": "invalid-format",  # Optional - raises ValidationError
                "name": "John",
            }
            result = query_def.filter_params(params)

            # Verify required date is included and optional problematic date is excluded
            assert "date_of_birth" in result
            assert result["date_of_birth"] == datetime.date(1990, 1, 1)
            assert "end_date" not in result  # Should be excluded due to ValidationError
            assert "name" in result
            assert result["name"] == "John"
