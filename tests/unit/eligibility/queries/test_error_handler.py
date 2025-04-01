from unittest.mock import AsyncMock, patch

import pytest

from app.eligibility.constants import EligibilityMethod
from app.eligibility.query_framework import eligibility_error_handler
from app.eligibility.query_framework.errors import MemberSearchError, ValidationError


class TestEligibilityErrorHandler:
    """Tests for the eligibility error handler decorator."""

    @pytest.mark.asyncio
    async def test_successful_execution(self):
        """Test that the decorator allows successful function execution."""
        # Create a mock function that returns successfully
        mock_func = AsyncMock(return_value="success")

        # Apply the decorator
        decorated_func = eligibility_error_handler(EligibilityMethod.BASIC)(mock_func)

        # Call the decorated function
        result = await decorated_func(user_id=123)

        # Verify results
        assert result == "success"
        mock_func.assert_called_once_with(user_id=123)

    @pytest.mark.asyncio
    async def test_validation_error_handling(self):
        """Test that ValidationError is re-raised with proper logging."""
        # Create a ValidationError
        validation_error = ValidationError("Invalid parameters")

        # Create a simple async function that accepts user_id and raises ValidationError
        async def test_function(user_id=None):
            raise validation_error

        # Wrap it with our decorator
        wrapped_function = eligibility_error_handler(EligibilityMethod.EMPLOYER)(
            test_function
        )

        # Mock the logger to check for logging
        with patch(
            "app.eligibility.query_framework.error_handler.logger"
        ) as mock_logger:
            # Call the function and verify the error is re-raised
            with pytest.raises(ValidationError) as exc_info:
                await wrapped_function(user_id=456)

            # Verify the error is the same one we raised
            assert exc_info.value is validation_error

            # Verify logging occurred
            mock_logger.warning.assert_called_once()
            log_msg = mock_logger.warning.call_args[0][0]
            assert "Validation error in employer eligibility check" in log_msg

    @pytest.mark.asyncio
    async def test_member_search_error_handling(self):
        """Test that MemberSearchError is re-raised with method set."""
        # Create a MemberSearchError without method
        search_error = MemberSearchError("No matching records found")
        mock_func = AsyncMock(side_effect=search_error)

        # Apply the decorator
        decorated_func = eligibility_error_handler(EligibilityMethod.HEALTH_PLAN)(
            mock_func
        )

        # Call the decorated function
        with pytest.raises(MemberSearchError) as exc_info:
            await decorated_func(user_id=789)

        # Verify that it's the same exception but with method set
        assert exc_info.value is search_error
        assert exc_info.value.method == EligibilityMethod.HEALTH_PLAN

    @pytest.mark.asyncio
    async def test_generic_exception_handling(self):
        """Test that generic exceptions are converted to MemberSearchError."""
        # Create a generic exception
        generic_error = RuntimeError("Something went wrong")
        mock_func = AsyncMock(side_effect=generic_error)

        # Apply the decorator
        decorated_func = eligibility_error_handler(EligibilityMethod.BASIC)(mock_func)

        # Call the decorated function
        with pytest.raises(MemberSearchError) as exc_info:
            await decorated_func(user_id=123)

        # Verify exception details
        assert "Error during eligibility check: Something went wrong" in str(
            exc_info.value
        )
        assert exc_info.value.method == EligibilityMethod.BASIC
        assert exc_info.value.__cause__ is generic_error

    @pytest.mark.asyncio
    async def test_logging_for_validation_error(self):
        """Test logging for validation errors."""
        # Create a ValidationError with fields
        error = ValidationError("Field validation failed", field1="invalid")

        # Create a function that accepts user_id and raises the error
        async def test_function(user_id=None):
            raise error

        # Apply the decorator
        decorated_func = eligibility_error_handler(EligibilityMethod.BASIC)(
            test_function
        )

        # Mock the logger
        with patch(
            "app.eligibility.query_framework.error_handler.logger"
        ) as mock_logger:
            # Call the decorated function and expect ValidationError
            with pytest.raises(ValidationError) as exc_info:
                await decorated_func(user_id=123)

            # Verify it's the same error
            assert exc_info.value is error

            # Verify logger was called correctly
            mock_logger.warning.assert_called_once()
            log_msg = mock_logger.warning.call_args[0][0]
            assert "Validation error in basic eligibility check" in log_msg

            log_extra = mock_logger.warning.call_args[1]["extra"]
            assert log_extra["error"] == "Field validation failed"
            assert log_extra["fields"] == {"field1": "invalid"}
            assert log_extra["user_id"] == 123

    @pytest.mark.asyncio
    async def test_logging_for_member_search_error(self):
        """Test logging for MemberSearchError."""
        # Create a MemberSearchError
        error = MemberSearchError(
            "No matching records", method=EligibilityMethod.EMPLOYER
        )
        mock_func = AsyncMock(side_effect=error)

        # Apply the decorator
        decorated_func = eligibility_error_handler(EligibilityMethod.EMPLOYER)(
            mock_func
        )

        # Mock the logger
        with patch(
            "app.eligibility.query_framework.error_handler.logger"
        ) as mock_logger:
            # Call the decorated function (will raise, but we're catching it)
            with pytest.raises(MemberSearchError):
                await decorated_func(user_id=456)

            # Verify logger was called correctly
            mock_logger.warning.assert_called_once()
            log_msg = mock_logger.warning.call_args[0][0]
            assert "Member search error in employer eligibility check" in log_msg

            log_extra = mock_logger.warning.call_args[1]["extra"]
            assert log_extra["error"] == "No matching records"
            assert log_extra["method"] == EligibilityMethod.EMPLOYER.value
            assert log_extra["user_id"] == 456

    @pytest.mark.asyncio
    async def test_logging_for_generic_exception(self):
        """Test logging for generic exceptions."""
        # Create a generic exception
        error = ValueError("Invalid value")

        # Create a function that accepts user_id and raises the error
        async def test_function(user_id=None):
            raise error

        # Apply the decorator
        decorated_func = eligibility_error_handler(EligibilityMethod.HEALTH_PLAN)(
            test_function
        )

        # Mock the logger
        with patch(
            "app.eligibility.query_framework.error_handler.logger"
        ) as mock_logger:
            # Call the decorated function (will raise, but we're catching it)
            with pytest.raises(MemberSearchError):
                await decorated_func(user_id=789)

            # Verify logger was called correctly
            mock_logger.error.assert_called_once()
            log_msg = mock_logger.error.call_args[0][0]

            # Use a more flexible check to handle different formatting
            assert "Error in " in log_msg
            assert "health" in log_msg.lower()
            assert "plan" in log_msg.lower()
            assert "eligibility check" in log_msg

            log_extra = mock_logger.error.call_args[1]["extra"]
            assert log_extra["error"] == "Invalid value"
            assert log_extra["error_type"] == "ValueError"
            assert log_extra["user_id"] == 789
