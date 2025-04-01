from unittest import mock
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from app.eligibility.constants import EligibilityMethod
from app.eligibility.errors import MatchError
from app.eligibility.query_framework import (
    EligibilityQueryExecutor,
    EligibilityResult,
    QueryResult,
    errors,
)
from app.eligibility.query_framework.errors import UnsupportedReturnTypeError
from app.eligibility.query_framework.executor import QueryVersioningResult
from app.eligibility.query_framework.types import MultipleRecordType, SingleRecordType
from db import model


class TestEligibilityQueryExecutor:
    """Tests for the EligibilityQueryExecutor class."""

    @pytest.fixture
    def mock_repository(self):
        """Create a mock repository for testing."""
        mock_repo = MagicMock()
        mock_repo.get_by_dob_and_email = AsyncMock()
        mock_repo.get_all_by_name_and_date_of_birth = AsyncMock()
        mock_repo.get_by_name_and_unique_corp_id = AsyncMock()
        return mock_repo

    @pytest.fixture
    def mock_configurations(self):
        """Mock the configurations client."""
        return MagicMock()

    @pytest.fixture
    def executor(self, mock_repository):
        """Create an EligibilityQueryExecutor with mocked dependencies."""
        executor = EligibilityQueryExecutor(query_repository=mock_repository)
        executor.configurations = MagicMock()
        return executor

    @patch("app.eligibility.query_framework.executor.QueryRegistry")
    async def test_perform_eligibility_check_success(self, mock_registry, executor):
        """Test successful eligibility check with V1 query."""
        # Mock the query registry
        mock_query = MagicMock()
        mock_query.method_name = "get_all_by_name_and_date_of_birth"
        mock_query.validate_params.return_value.is_valid = True
        mock_query.filter_params.return_value = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

        mock_registry.get_v1_queries.return_value = [mock_query]
        mock_registry.get_v2_queries.return_value = [mock_query]

        # Create mock member record
        mock_member = MagicMock(spec=model.MemberVersioned)
        mock_member.id = 12345
        mock_member.organization_id = 1
        mock_member.first_name = "John"
        mock_member.last_name = "Doe"
        mock_member.date_of_birth = "1990-01-01"
        mock_member.unique_corp_id = "abc123"
        mock_member.dependent_id = None

        # Set up repository method to return mock member
        executor.repository.get_all_by_name_and_date_of_birth.return_value = [
            mock_member
        ]

        # Mock the _filter_active_records method
        with patch.object(
            executor, "_filter_active_records", return_value=[mock_member]
        ) as mock_filter, patch.object(
            executor, "_is_v2_enabled", return_value=False
        ) as mock_v2_enabled:
            # Call the method
            params = {
                "first_name": "John",
                "last_name": "Doe",
                "date_of_birth": "1990-01-01",
            }
            result = await executor.perform_eligibility_check(
                method=EligibilityMethod.BASIC,
                params=params,
                expected_type=MultipleRecordType,
            )

            # Verify the result
            assert isinstance(result, EligibilityResult)
            assert result.records == [mock_member]
            assert result.v1_id == 12345

            # Verify the correct methods were called
            mock_registry.get_v1_queries.assert_called_once_with(
                EligibilityMethod.BASIC
            )
            mock_v2_enabled.assert_called_once_with("1")
            mock_filter.assert_called_once()

    @patch("app.eligibility.query_framework.executor.QueryRegistry")
    async def test_perform_eligibility_check_no_queries(self, mock_registry, executor):
        """Test eligibility check when no queries are configured."""
        # Set up the query registry to return no queries
        mock_registry.get_v1_queries.return_value = []

        # Call the method and expect an error
        with pytest.raises(errors.ValidationError) as exc_info:
            await executor.perform_eligibility_check(
                method=EligibilityMethod.BASIC,
                params={"first_name": "John", "last_name": "Doe"},
                expected_type=MultipleRecordType,
            )

        assert "No queries configured for method" in str(exc_info.value)

    @patch("app.eligibility.query_framework.executor.QueryRegistry")
    async def test_perform_eligibility_check_validation_failure(
        self, mock_registry, executor
    ):
        """Test eligibility check when all queries fail validation."""
        # Mock the query registry
        mock_query = MagicMock()
        mock_query.method_name = "get_all_by_name_and_date_of_birth"
        mock_query.validate_params.return_value.is_valid = False
        mock_query.validate_params.return_value.missing_params = {"date_of_birth"}

        mock_registry.get_v1_queries.return_value = [mock_query]

        # Call the method and expect a ValidationError
        with pytest.raises(errors.ValidationError) as exc_info:
            await executor.perform_eligibility_check(
                method=EligibilityMethod.BASIC,
                params={
                    "first_name": "John",
                    "last_name": "Doe",
                },  # Missing date_of_birth
                expected_type=MultipleRecordType,
            )

        assert "missing parameters" in str(exc_info.value).lower()

    @patch("app.eligibility.query_framework.executor.QueryRegistry")
    async def test_perform_eligibility_check_no_results(self, mock_registry, executor):
        """Test eligibility check when query returns no results."""
        # Mock the query registry
        mock_query = MagicMock()
        mock_query.method_name = "get_all_by_name_and_date_of_birth"
        mock_query.validate_params.return_value.is_valid = True
        mock_query.filter_params.return_value = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

        mock_registry.get_v1_queries.return_value = [mock_query]

        # Set up repository method to return empty result
        executor.repository.get_all_by_name_and_date_of_birth.return_value = []

        # Call the method and expect a MemberSearchError
        with pytest.raises(errors.MemberSearchError) as exc_info:
            await executor.perform_eligibility_check(
                method=EligibilityMethod.BASIC,
                params={
                    "first_name": "John",
                    "last_name": "Doe",
                    "date_of_birth": "1990-01-01",
                },
                expected_type=MultipleRecordType,
            )

        assert "No matching records found" in str(exc_info.value)

    @patch("app.eligibility.query_framework.executor.QueryRegistry")
    async def test_perform_eligibility_check_inactive_organization(
        self, mock_registry, executor
    ):
        """Test eligibility check when organization is inactive."""
        # Mock the query registry
        mock_query = MagicMock()
        mock_query.method_name = "get_all_by_name_and_date_of_birth"
        mock_query.validate_params.return_value.is_valid = True
        mock_query.filter_params.return_value = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

        mock_registry.get_v1_queries.return_value = [mock_query]

        # Create mock member record
        mock_member = MagicMock(spec=model.MemberVersioned)
        mock_member.id = 12345
        mock_member.organization_id = 1

        # Set up repository method to return mock member
        executor.repository.get_all_by_name_and_date_of_birth.return_value = [
            mock_member
        ]

        # Mock the _filter_active_records method to raise InactiveOrganizationError
        with patch.object(
            executor,
            "_filter_active_records",
            side_effect=errors.InactiveOrganizationError(
                method=EligibilityMethod.BASIC
            ),
        ):
            # Call the method and expect an InactiveOrganizationError
            with pytest.raises(errors.InactiveOrganizationError):
                await executor.perform_eligibility_check(
                    method=EligibilityMethod.BASIC,
                    params={
                        "first_name": "John",
                        "last_name": "Doe",
                        "date_of_birth": "1990-01-01",
                    },
                    expected_type=MultipleRecordType,
                )

    @patch("app.eligibility.query_framework.executor.QueryRegistry")
    async def test_perform_eligibility_check_v2_enabled(self, mock_registry, executor):
        """Test eligibility check with V2 query when organization is V2 enabled."""
        # Mock the query registry
        mock_v1_query = MagicMock()
        mock_v1_query.method_name = "get_all_by_name_and_date_of_birth"
        mock_v1_query.validate_params.return_value.is_valid = True
        mock_v1_query.filter_params.return_value = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

        mock_v2_query = MagicMock()
        mock_v2_query.method_name = "get_all_by_name_and_date_of_birth_v2"
        mock_v2_query.validate_params.return_value.is_valid = True
        mock_v2_query.filter_params.return_value = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

        mock_registry.get_v1_queries.return_value = [mock_v1_query]
        mock_registry.get_v2_queries.return_value = [mock_v2_query]

        # Create mock member records
        mock_v1_member = MagicMock(spec=model.MemberVersioned)
        mock_v1_member.id = 12345
        mock_v1_member.organization_id = 1
        mock_v1_member.first_name = "John"
        mock_v1_member.last_name = "Doe"
        mock_v1_member.date_of_birth = "1990-01-01"
        mock_v1_member.unique_corp_id = "abc123"
        mock_v1_member.dependent_id = None

        mock_v2_member = MagicMock(spec=model.Member2)
        mock_v2_member.id = 67890
        mock_v2_member.organization_id = 1
        mock_v2_member.first_name = "John"
        mock_v2_member.last_name = "Doe"
        mock_v2_member.date_of_birth = "1990-01-01"
        mock_v2_member.unique_corp_id = "abc123"
        mock_v2_member.dependent_id = None

        # Set up repository methods to return mock members
        executor.repository.get_all_by_name_and_date_of_birth.return_value = [
            mock_v1_member
        ]
        executor.repository.get_all_by_name_and_date_of_birth_v2.return_value = [
            mock_v2_member
        ]

        # Mock the necessary methods
        with patch.object(
            executor, "_filter_active_records", return_value=[mock_v2_member]
        ) as mock_filter, patch.object(
            executor, "_is_v2_enabled", return_value=True
        ) as mock_v2_enabled, patch.object(
            executor, "_validate_results", return_value=True
        ) as mock_validate:
            # Call the method
            params = {
                "first_name": "John",
                "last_name": "Doe",
                "date_of_birth": "1990-01-01",
            }
            result = await executor.perform_eligibility_check(
                method=EligibilityMethod.BASIC,
                params=params,
                expected_type=MultipleRecordType,
            )

            # Verify the result uses V2 data
            assert isinstance(result, EligibilityResult)
            assert result.records == [mock_v2_member]
            assert result.v1_id == 12345  # V1 ID is still used as reference

            # Verify the correct methods were called
            mock_registry.get_v1_queries.assert_called_once_with(
                EligibilityMethod.BASIC
            )
            mock_registry.get_v2_queries.assert_called_once_with(
                EligibilityMethod.BASIC
            )
            mock_v2_enabled.assert_called_once_with("1")
            mock_validate.assert_called_once_with(mock_v1_member, mock_v2_member)
            mock_filter.assert_called_once()

    @patch("app.eligibility.query_framework.executor.QueryRegistry")
    async def test_perform_eligibility_check_v2_validation_failure(
        self, mock_registry, executor
    ):
        """Test eligibility check when V2 validation fails."""
        # Mock the query registry
        mock_v1_query = MagicMock()
        mock_v1_query.method_name = "get_all_by_name_and_date_of_birth"
        mock_v1_query.validate_params.return_value.is_valid = True
        mock_v1_query.filter_params.return_value = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

        mock_v2_query = MagicMock()
        mock_v2_query.method_name = "get_all_by_name_and_date_of_birth_v2"
        mock_v2_query.validate_params.return_value.is_valid = True
        mock_v2_query.filter_params.return_value = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

        mock_registry.get_v1_queries.return_value = [mock_v1_query]
        mock_registry.get_v2_queries.return_value = [mock_v2_query]

        # Create mock member records
        mock_v1_member = MagicMock(spec=model.MemberVersioned)
        mock_v1_member.id = 12345
        mock_v1_member.organization_id = 1
        mock_v1_member.first_name = "John"
        mock_v1_member.last_name = "Doe"

        mock_v2_member = MagicMock(spec=model.Member2)
        mock_v2_member.id = 67890
        mock_v2_member.organization_id = 1
        mock_v2_member.first_name = "Johnny"  # Different first name
        mock_v2_member.last_name = "Doe"

        # Set up repository methods to return mock members
        executor.repository.get_all_by_name_and_date_of_birth.return_value = [
            mock_v1_member
        ]
        executor.repository.get_all_by_name_and_date_of_birth_v2.return_value = [
            mock_v2_member
        ]

        # Mock the necessary methods
        with patch.object(
            executor, "_filter_active_records", return_value=[mock_v1_member]
        ), patch.object(
            executor, "_is_v2_enabled", return_value=True
        ) as mock_v2_enabled, patch.object(
            executor, "_validate_results", return_value=False
        ) as mock_validate:
            # Call the method
            params = {
                "first_name": "John",
                "last_name": "Doe",
                "date_of_birth": "1990-01-01",
            }
            result = await executor.perform_eligibility_check(
                method=EligibilityMethod.BASIC,
                params=params,
                expected_type=MultipleRecordType,
            )

            # Verify the result falls back to V1 data
            assert isinstance(result, EligibilityResult)
            assert result.records == [mock_v1_member]
            assert result.v1_id == 12345

            # Verify the correct methods were called
            mock_registry.get_v1_queries.assert_called_once_with(
                EligibilityMethod.BASIC
            )
            mock_registry.get_v2_queries.assert_called_once_with(
                EligibilityMethod.BASIC
            )
            mock_v2_enabled.assert_called_once_with("1")
            mock_validate.assert_called_once_with(mock_v1_member, mock_v2_member)

    async def test_execute_single_query_success(self, executor):
        """Test execution of a single query with successful result."""
        # Create mock query definition
        mock_query = MagicMock()
        mock_query.method_name = "get_by_dob_and_email"
        mock_query.query_type = "dob_email"

        # Create mock member record
        mock_member = MagicMock(spec=model.MemberVersioned)
        mock_member.id = 12345
        mock_member.organization_id = 1

        # Set up repository method to return mock member
        executor.repository.get_by_dob_and_email.return_value = mock_member

        # Call the method
        params = {
            "date_of_birth": "1990-01-01",
            "email": "john@example.com",
            "user_id": 456,
        }
        result = await executor._execute_single_query(
            query=mock_query, params=params, method=EligibilityMethod.EMPLOYER
        )

        # Verify the result
        assert isinstance(result, QueryResult)
        assert result.result == mock_member
        assert result.query_name == "get_by_dob_and_email"
        assert result.query_type == "dob_email"
        assert result.v_id == 12345
        assert result.organization_id == "1"
        assert result.error is None
        assert result.is_success is True

    async def test_execute_single_query_empty_result(self, executor):
        """Test execution of a single query with empty result."""
        # Create mock query definition
        mock_query = MagicMock()
        mock_query.method_name = "get_by_dob_and_email"
        mock_query.query_type = "dob_email"

        # Set up repository method to return None
        executor.repository.get_by_dob_and_email.return_value = None

        # Call the method
        params = {"date_of_birth": "1990-01-01", "email": "john@example.com"}
        result = await executor._execute_single_query(
            query=mock_query, params=params, method=EligibilityMethod.EMPLOYER
        )

        # Verify the result
        assert isinstance(result, QueryResult)
        assert result.result is None
        assert result.query_name == "get_by_dob_and_email"
        assert result.query_type == "dob_email"
        assert result.error is None
        assert result.is_success is False

    async def test_execute_in_parallel(self, executor):
        """Test execution of queries in parallel."""
        # Create mock queries and results
        mock_query1 = MagicMock()
        mock_query1.method_name = "get_by_dob_and_email"
        mock_params1 = {"date_of_birth": "1990-01-01", "email": "john@example.com"}

        mock_query2 = MagicMock()
        mock_query2.method_name = "get_all_by_name_and_date_of_birth"
        mock_params2 = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

        executable_queries = [(mock_query1, mock_params1), (mock_query2, mock_params2)]

        # Set up _execute_single_query to return different results
        mock_result1 = QueryResult(
            result=None,
            query_name="get_by_dob_and_email",
            query_type="dob_email",
            error="Not found",
        )
        mock_result2 = QueryResult(
            result=MagicMock(),
            query_name="get_all_by_name_and_date_of_birth",
            query_type="name_dob",
            error=None,
        )

        with patch.object(
            executor, "_execute_single_query", side_effect=[mock_result1, mock_result2]
        ) as mock_execute:
            # Call the method
            result = await executor._execute_in_parallel(
                executable_queries=executable_queries,
                method=EligibilityMethod.EMPLOYER,
                params={},
            )

            # Verify the result is the successful one (mock_result2)
            assert result == mock_result2

            # Verify _execute_single_query was called for both queries
            assert mock_execute.call_count == 2
            mock_execute.assert_has_calls(
                [
                    call(mock_query1, mock_params1, EligibilityMethod.EMPLOYER),
                    call(mock_query2, mock_params2, EligibilityMethod.EMPLOYER),
                ],
                any_order=True,
            )

    async def test_execute_sequentially_first_success(self, executor):
        """Test sequential execution when first query succeeds."""
        # Create mock queries and results
        mock_query1 = MagicMock()
        mock_query1.method_name = "get_by_dob_and_email"
        mock_params1 = {"date_of_birth": "1990-01-01", "email": "john@example.com"}

        mock_query2 = MagicMock()
        mock_query2.method_name = "get_all_by_name_and_date_of_birth"
        mock_params2 = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

        executable_queries = [(mock_query1, mock_params1), (mock_query2, mock_params2)]

        # Set up _execute_single_query to return successful result for first query
        mock_result1 = QueryResult(
            result=MagicMock(),
            query_name="get_by_dob_and_email",
            query_type="dob_email",
            error=None,
        )

        with patch.object(
            executor, "_execute_single_query", return_value=mock_result1
        ) as mock_execute:
            # Call the method
            result = await executor._execute_sequentially(
                executable_queries=executable_queries,
                method=EligibilityMethod.EMPLOYER,
                params={},
            )

            # Verify the result is the successful one (mock_result1)
            assert result == mock_result1

            # Verify _execute_single_query was called only for the first query
            assert mock_execute.call_count == 1
            mock_execute.assert_called_once_with(
                mock_query1, mock_params1, EligibilityMethod.EMPLOYER
            )

    async def test_execute_sequentially_all_fail(self, executor):
        """Test sequential execution when all queries fail."""
        # Create mock queries and results
        mock_query1 = MagicMock()
        mock_query1.method_name = "get_by_dob_and_email"
        mock_params1 = {"date_of_birth": "1990-01-01", "email": "john@example.com"}

        mock_query2 = MagicMock()
        mock_query2.method_name = "get_all_by_name_and_date_of_birth"
        mock_params2 = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

        executable_queries = [(mock_query1, mock_params1), (mock_query2, mock_params2)]

        # Set up _execute_single_query to return failed results
        mock_result1 = QueryResult(
            result=None,
            query_name="get_by_dob_and_email",
            query_type="dob_email",
            error="Not found",
        )
        mock_result2 = QueryResult(
            result=None,
            query_name="get_all_by_name_and_date_of_birth",
            query_type="name_dob",
            error="Not found",
        )

        with patch.object(
            executor, "_execute_single_query", side_effect=[mock_result1, mock_result2]
        ) as mock_execute:
            # Call the method
            result = await executor._execute_sequentially(
                executable_queries=executable_queries,
                method=EligibilityMethod.EMPLOYER,
                params={},
            )

            # Verify the result is a failure result
            assert result.is_success is False
            assert result.query_name == "all_queries_failed"
            assert "All queries failed" in result.error

            # Verify _execute_single_query was called for both queries
            assert mock_execute.call_count == 2
            mock_execute.assert_has_calls(
                [
                    call(mock_query1, mock_params1, EligibilityMethod.EMPLOYER),
                    call(mock_query2, mock_params2, EligibilityMethod.EMPLOYER),
                ]
            )

    def test_validate_results_success(self, executor):
        """Test validation of matching V1 and V2 results."""
        # Create mock member records with matching attributes
        mock_v1_member = MagicMock(spec=model.MemberVersioned)
        mock_v1_member.unique_corp_id = "abc123"
        mock_v1_member.organization_id = 1
        mock_v1_member.first_name = "John"
        mock_v1_member.last_name = "Doe"
        mock_v1_member.date_of_birth = "1990-01-01"
        mock_v1_member.dependent_id = None

        mock_v2_member = MagicMock(spec=model.Member2)
        mock_v2_member.unique_corp_id = "abc123"
        mock_v2_member.organization_id = 1
        mock_v2_member.first_name = "John"
        mock_v2_member.last_name = "Doe"
        mock_v2_member.date_of_birth = "1990-01-01"
        mock_v2_member.dependent_id = None

        # Call the method
        result = executor._validate_results(mock_v1_member, mock_v2_member)

        # Verify the result
        assert result is True

    def test_validate_results_mismatch(self, executor):
        """Test validation of mismatched V1 and V2 results."""
        # Create mock member records with mismatched attributes
        mock_v1_member = MagicMock(spec=model.MemberVersioned)
        mock_v1_member.unique_corp_id = "abc123"
        mock_v1_member.organization_id = 1
        mock_v1_member.first_name = "John"
        mock_v1_member.last_name = "Doe"
        mock_v1_member.date_of_birth = "1990-01-01"
        mock_v1_member.dependent_id = None

        mock_v2_member = MagicMock(spec=model.Member2)
        mock_v2_member.unique_corp_id = "xyz456"  # Different ID
        mock_v2_member.organization_id = 1
        mock_v2_member.first_name = "John"
        mock_v2_member.last_name = "Doe"
        mock_v2_member.date_of_birth = "1990-01-01"
        mock_v2_member.dependent_id = None

        # Call the method
        result = executor._validate_results(mock_v1_member, mock_v2_member)

        # Verify the result
        assert result is False

    def test_validate_results_exception(self, executor):
        """Test validation when an exception occurs."""
        # Create mock member records where one is missing an attribute
        mock_v1_member = MagicMock(spec=model.MemberVersioned)
        # Remove the unique_corp_id attribute
        del mock_v1_member.unique_corp_id

        mock_v2_member = MagicMock(spec=model.Member2)
        mock_v2_member.unique_corp_id = "abc123"

        # Call the method
        result = executor._validate_results(mock_v1_member, mock_v2_member)

        # Verify the result
        assert result is False

    @patch("app.eligibility.query_framework.executor.feature_flag")
    def test_is_v2_enabled(self, mock_feature_flag, executor):
        """Test checking if V2 is enabled for an organization."""
        # Set up the feature flag module
        mock_feature_flag.organization_enabled_for_e9y_2_write.return_value = True

        # Call the method
        result = executor._is_v2_enabled("123")

        # Verify the result
        assert result is True
        mock_feature_flag.organization_enabled_for_e9y_2_write.assert_called_once_with(
            123
        )

    async def test_prepare_queries_valid(self, executor):
        """Test preparation of valid queries."""
        # Create mock queries
        mock_query1 = MagicMock()
        mock_query1.method_name = "get_by_dob_and_email"
        mock_query1.query_type = "dob_email"
        mock_query1.validate_params.return_value.is_valid = True
        mock_query1.filter_params.return_value = {
            "date_of_birth": "1990-01-01",
            "email": "john@example.com",
        }

        mock_query2 = MagicMock()
        mock_query2.method_name = "get_all_by_name_and_date_of_birth"
        mock_query2.query_type = "name_dob"
        mock_query2.validate_params.return_value.is_valid = True
        mock_query2.filter_params.return_value = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

        queries = [mock_query1, mock_query2]

        # Call the method
        params = {
            "date_of_birth": "1990-01-01",
            "email": "john@example.com",
            "first_name": "John",
            "last_name": "Doe",
            "user_id": 456,
        }
        result = executor._prepare_queries(queries, params, EligibilityMethod.EMPLOYER)

        # Verify the result
        assert len(result) == 2
        assert result[0][0] == mock_query1
        assert result[0][1] == {
            "date_of_birth": "1990-01-01",
            "email": "john@example.com",
        }
        assert result[1][0] == mock_query2
        assert result[1][1] == {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

        # Verify validate_params and filter_params were called for both queries
        mock_query1.validate_params.assert_called_once_with(params)
        mock_query1.filter_params.assert_called_once_with(params)
        mock_query2.validate_params.assert_called_once_with(params)
        mock_query2.filter_params.assert_called_once_with(params)

    def test_prepare_queries_no_valid_queries(self, executor):
        """Test preparation of queries when none are valid."""
        # Create mock queries that fail validation
        mock_query1 = MagicMock()
        mock_query1.method_name = "get_by_dob_and_email"
        mock_query1.query_type = "dob_email"
        mock_query1.validate_params.return_value.is_valid = False
        mock_query1.validate_params.return_value.missing_params = {"date_of_birth"}

        mock_query2 = MagicMock()
        mock_query2.method_name = "get_all_by_name_and_date_of_birth"
        mock_query2.query_type = "name_dob"
        mock_query2.validate_params.return_value.is_valid = False
        mock_query2.validate_params.return_value.missing_params = {"first_name"}

        queries = [mock_query1, mock_query2]

        # Call the method and expect a ValidationError
        params = {"email": "john@example.com", "last_name": "Doe"}
        with pytest.raises(errors.ValidationError) as exc_info:
            executor._prepare_queries(queries, params, EligibilityMethod.EMPLOYER)

        # Verify the error message contains information about all missing parameters
        error_message = str(exc_info.value)
        assert "get_by_dob_and_email" in error_message
        assert "date_of_birth" in error_message
        assert "get_all_by_name_and_date_of_birth" in error_message
        assert "first_name" in error_message

    def test_prepare_queries_some_valid(self, executor):
        """Test preparation of queries when some are valid."""
        # Create mock queries, one valid and one invalid
        mock_query1 = MagicMock()
        mock_query1.method_name = "get_by_dob_and_email"
        mock_query1.query_type = "dob_email"
        mock_query1.validate_params.return_value.is_valid = False
        mock_query1.validate_params.return_value.missing_params = {"date_of_birth"}

        mock_query2 = MagicMock()
        mock_query2.method_name = "get_all_by_name_and_date_of_birth"
        mock_query2.query_type = "name_dob"
        mock_query2.validate_params.return_value.is_valid = True
        mock_query2.filter_params.return_value = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

        queries = [mock_query1, mock_query2]

        # Call the method
        params = {
            "email": "john@example.com",
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }
        result = executor._prepare_queries(queries, params, EligibilityMethod.EMPLOYER)

        # Verify the result only includes the valid query
        assert len(result) == 1
        assert result[0][0] == mock_query2
        assert result[0][1] == {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

    @patch("app.eligibility.query_framework.executor.eligibility_validation")
    async def test_filter_active_records_single(self, mock_validation, executor):
        """Test filtering active records for a single record."""
        # Create mock member record
        mock_member = MagicMock(spec=model.MemberVersioned)

        # Set up validation to return the same record
        mock_validation.check_member_org_active.return_value = mock_member

        # Call the method
        result = await executor._filter_active_records(
            member_records=mock_member,
            method=EligibilityMethod.BASIC,
            expected_type=SingleRecordType,
        )

        # Verify the result
        assert result == mock_member
        mock_validation.check_member_org_active.assert_called_once_with(
            configuration_client=executor.configurations, member=mock_member
        )

    @patch("app.eligibility.query_framework.executor.eligibility_validation")
    async def test_filter_active_records_multiple(self, mock_validation, executor):
        """Test filtering active records for multiple records."""
        # Create mock member records
        mock_members = [
            MagicMock(spec=model.MemberVersioned),
            MagicMock(spec=model.MemberVersioned),
        ]

        # Set up validation to return the same records
        mock_validation.check_member_org_active_and_overeligibility.return_value = (
            mock_members
        )

        # Call the method
        result = await executor._filter_active_records(
            member_records=mock_members,
            method=EligibilityMethod.BASIC,
            expected_type=MultipleRecordType,
        )

        # Verify the result
        assert result == mock_members
        mock_validation.check_member_org_active_and_overeligibility.assert_called_once_with(
            configuration_client=executor.configurations, member_list=mock_members
        )

    @patch("app.eligibility.query_framework.executor.eligibility_validation")
    async def test_filter_active_records_single_list_expected(
        self, mock_validation, executor
    ):
        """Test filtering active records for a list when the expected type is SingleRecordType."""
        # Create mock member records
        mock_members = [
            MagicMock(spec=model.MemberVersioned),
            MagicMock(spec=model.MemberVersioned),
        ]

        # Set up validation to return a single record
        single_member = MagicMock(spec=model.MemberVersioned)
        mock_validation.check_member_org_active_and_single_org.return_value = (
            single_member
        )

        # Call the method
        result = await executor._filter_active_records(
            member_records=mock_members,
            method=EligibilityMethod.EMPLOYER,
            expected_type=SingleRecordType,
        )

        # Verify the result
        assert result == single_member
        mock_validation.check_member_org_active_and_single_org.assert_called_once_with(
            configuration_client=executor.configurations, member_list=mock_members
        )

    async def test_filter_active_records_unsupported_type(self, executor):
        """Test filtering active records with an unsupported expected type."""
        # Create mock member record
        mock_member = MagicMock(spec=model.MemberVersioned)

        # Call the method with an unsupported type
        with pytest.raises(errors.UnsupportedReturnTypeError):
            await executor._filter_active_records(
                member_records=mock_member,
                method=EligibilityMethod.BASIC,
                expected_type=str,  # Not a valid expected type
            )

    def test_determine_query_version_multiple_results(self, executor):
        """Test determining query version with multiple results."""
        # Create a mock QueryResult with multiple results
        mock_result = MagicMock()
        mock_result.is_multiple_results = True

        # Call the method
        use_v1, reason = executor._determine_query_version(
            mock_result, "123", EligibilityMethod.BASIC
        )

        # Verify the result
        assert use_v1 is True
        assert "multiple V1 records" in reason

    def test_determine_query_version_v2_not_enabled(self, executor):
        """Test determining query version when V2 is not enabled for org."""
        # Create a mock QueryResult with a single result
        mock_result = MagicMock()
        mock_result.is_multiple_results = False

        # Mock the _is_v2_enabled method to return False
        with patch.object(
            executor, "_is_v2_enabled", return_value=False
        ) as mock_v2_enabled:
            # Call the method
            use_v1, reason = executor._determine_query_version(
                mock_result, "123", EligibilityMethod.BASIC
            )

            # Verify the result
            assert use_v1 is True
            assert "organization not V2 enabled" in reason
            mock_v2_enabled.assert_called_once_with("123")

    def test_determine_query_version_v2_enabled(self, executor):
        """Test determining query version when V2 is enabled for org."""
        # Create a mock QueryResult with a single result
        mock_result = MagicMock()
        mock_result.is_multiple_results = False

        # Mock the _is_v2_enabled method to return True
        with patch.object(
            executor, "_is_v2_enabled", return_value=True
        ) as mock_v2_enabled:
            # Call the method
            use_v1, reason = executor._determine_query_version(
                mock_result, "123", EligibilityMethod.BASIC
            )

            # Verify the result
            assert use_v1 is False
            assert reason is None
            mock_v2_enabled.assert_called_once_with("123")

    async def test_try_v2_queries_success(self, executor):
        """Test trying V2 queries with successful validation."""
        # Mock dependencies
        mock_v1_record = MagicMock(spec=model.MemberVersioned)
        mock_v1_record.id = 12345

        mock_v2_record = MagicMock(spec=model.Member2)
        mock_v2_record.id = 67890

        mock_query = MagicMock()
        mock_query.method_name = "get_all_by_name_and_date_of_birth_v2"

        # Set up QueryRegistry to return the mock query
        with patch(
            "app.eligibility.query_framework.executor.QueryRegistry"
        ) as mock_registry, patch.object(
            executor, "_execute_queries"
        ) as mock_execute_queries, patch.object(
            executor, "_validate_results", return_value=True
        ) as mock_validate:
            mock_registry.get_v2_queries.return_value = [mock_query]

            # Set up the query result
            mock_result = MagicMock()
            mock_result.is_success = True
            mock_result.first_result = mock_v2_record
            mock_result.result = [mock_v2_record]
            mock_execute_queries.return_value = mock_result

            # Call the method
            params = {
                "first_name": "John",
                "last_name": "Doe",
                "date_of_birth": "1990-01-01",
            }
            result = await executor._try_v2_queries(
                v1_record=mock_v1_record, method=EligibilityMethod.BASIC, params=params
            )

            # Verify the result
            assert result == [mock_v2_record]
            mock_registry.get_v2_queries.assert_called_once_with(
                EligibilityMethod.BASIC
            )
            mock_execute_queries.assert_called_once_with(
                queries=mock_registry.get_v2_queries.return_value,
                method=EligibilityMethod.BASIC,
                params=params,
            )
            mock_validate.assert_called_once_with(mock_v1_record, mock_v2_record)

    async def test_try_v2_queries_validation_failure(self, executor):
        """Test trying V2 queries with failed validation."""
        # Mock dependencies
        mock_v1_record = MagicMock(spec=model.MemberVersioned)
        mock_v2_record = MagicMock(spec=model.Member2)
        mock_query = MagicMock()

        # Set up QueryRegistry to return the mock query
        with patch(
            "app.eligibility.query_framework.executor.QueryRegistry"
        ) as mock_registry, patch.object(
            executor, "_execute_queries"
        ) as mock_execute_queries, patch.object(
            executor, "_validate_results", return_value=False
        ) as mock_validate:
            mock_registry.get_v2_queries.return_value = [mock_query]

            # Set up the query result
            mock_result = MagicMock()
            mock_result.is_success = True
            mock_result.first_result = mock_v2_record
            mock_execute_queries.return_value = mock_result

            # Call the method
            params = {
                "first_name": "John",
                "last_name": "Doe",
                "date_of_birth": "1990-01-01",
            }
            result = await executor._try_v2_queries(
                v1_record=mock_v1_record, method=EligibilityMethod.BASIC, params=params
            )

            # Verify the result is None, indicating validation failure
            assert result is None
            mock_validate.assert_called_once_with(mock_v1_record, mock_v2_record)

    async def test_try_v2_queries_execution_failure(self, executor):
        """Test trying V2 queries with failed query execution."""
        # Mock dependencies
        mock_v1_record = MagicMock(spec=model.MemberVersioned)
        mock_query = MagicMock()

        # Set up QueryRegistry to return the mock query
        with patch(
            "app.eligibility.query_framework.executor.QueryRegistry"
        ) as mock_registry, patch.object(
            executor, "_execute_queries"
        ) as mock_execute_queries:
            mock_registry.get_v2_queries.return_value = [mock_query]

            # Set up the query result to indicate failure
            mock_result = MagicMock()
            mock_result.is_success = False
            mock_execute_queries.return_value = mock_result

            # Call the method
            params = {
                "first_name": "John",
                "last_name": "Doe",
                "date_of_birth": "1990-01-01",
            }
            result = await executor._try_v2_queries(
                v1_record=mock_v1_record, method=EligibilityMethod.BASIC, params=params
            )

            # Verify the result is None, indicating execution failure
            assert result is None

    def test_log_result_usage(self, executor):
        """Test logging result usage."""
        # Set up a mock logger
        with patch("app.eligibility.query_framework.executor.logger") as mock_logger:
            # Mock result
            mock_result = MagicMock()
            mock_result.result = [MagicMock(), MagicMock()]  # Two mock members

            # Call the method for V1 usage
            executor._log_result_usage(
                use_v1_results=True,
                reason="test reason",
                method=EligibilityMethod.BASIC,
                v1_result=mock_result,
                org_id="123",
            )

            # Verify the log call
            mock_logger.info.assert_called_once_with(
                "Using V1 results: test reason",
                extra={
                    "method": EligibilityMethod.BASIC.value,
                    "record_count": 2,
                    "organization_id": "123",
                },
            )

            # Reset the mock
            mock_logger.reset_mock()

            # Call the method for V2 usage
            executor._log_result_usage(
                use_v1_results=False,
                reason="test reason",
                method=EligibilityMethod.BASIC,
                v1_result=mock_result,
                org_id="123",
            )

            # Verify the log call
            mock_logger.info.assert_called_once_with(
                "Using V2 results: test reason",
                extra={
                    "method": EligibilityMethod.BASIC.value,
                    "record_count": 2,
                    "organization_id": "123",
                },
            )

    def test_log_result_usage_no_reason(self, executor):
        """Test logging result usage with no reason provided."""
        # Set up a mock logger
        with patch("app.eligibility.query_framework.executor.logger") as mock_logger:
            # Mock result
            mock_result = MagicMock()

            # Call the method with no reason
            executor._log_result_usage(
                use_v1_results=True,
                reason=None,
                method=EligibilityMethod.BASIC,
                v1_result=mock_result,
                org_id="123",
            )

            # Verify no log call was made
            mock_logger.info.assert_not_called()

    def test_get_run_in_parallel_flag(self, executor):
        """Test getting the run_in_parallel flag."""
        # The method is currently hard-coded to return False
        assert executor._get_run_in_parallel_flag() is False

    @patch("app.eligibility.query_framework.executor.QueryRegistry")
    async def test_perform_eligibility_check_v2_enabled_but_query_fails(
        self, mock_registry, executor
    ):
        """Test eligibility check with V2 query that fails when organization is V2 enabled."""
        # Mock the query registry
        mock_v1_query = MagicMock()
        mock_v1_query.method_name = "get_all_by_name_and_date_of_birth"
        mock_v1_query.validate_params.return_value.is_valid = True
        mock_v1_query.filter_params.return_value = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

        mock_v2_query = MagicMock()
        mock_v2_query.method_name = "get_all_by_name_and_date_of_birth_v2"
        mock_v2_query.validate_params.return_value.is_valid = True
        mock_v2_query.filter_params.return_value = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

        mock_registry.get_v1_queries.return_value = [mock_v1_query]
        mock_registry.get_v2_queries.return_value = [mock_v2_query]

        # Create mock member records
        mock_v1_member = MagicMock(spec=model.MemberVersioned)
        mock_v1_member.id = 12345
        mock_v1_member.organization_id = 1
        mock_v1_member.first_name = "John"
        mock_v1_member.last_name = "Doe"
        mock_v1_member.date_of_birth = "1990-01-01"
        mock_v1_member.unique_corp_id = "abc123"
        mock_v1_member.dependent_id = None

        # Set up repository methods
        executor.repository.get_all_by_name_and_date_of_birth.return_value = [
            mock_v1_member
        ]
        executor.repository.get_all_by_name_and_date_of_birth_v2.side_effect = (
            ValueError("Database error")
        )

        # Mock the necessary methods
        with patch.object(
            executor, "_filter_active_records", return_value=[mock_v1_member]
        ), patch.object(
            executor, "_is_v2_enabled", return_value=True
        ) as mock_v2_enabled, patch.object(
            executor, "_log_result_usage"
        ) as mock_log_usage:
            # Call the method
            params = {
                "first_name": "John",
                "last_name": "Doe",
                "date_of_birth": "1990-01-01",
            }
            result = await executor.perform_eligibility_check(
                method=EligibilityMethod.BASIC,
                params=params,
                expected_type=MultipleRecordType,
            )

            # Verify the result falls back to V1 data
            assert isinstance(result, EligibilityResult)
            assert result.records == [mock_v1_member]
            assert result.v1_id == 12345

            # Verify the correct methods were called
            mock_registry.get_v1_queries.assert_called_once()
            mock_registry.get_v2_queries.assert_called_once()
            mock_v2_enabled.assert_called_once()

            # Verify log_result_usage was called with the correct fallback message
            mock_log_usage.assert_called_once()
            fallback_call = mock_log_usage.call_args
            assert fallback_call[0][0] is True  # use_v1_results = True
            assert (
                "V2 query failed or validation failed" in fallback_call[0][1]
            )  # reason should include fallback message

    @patch("app.eligibility.query_framework.executor.QueryRegistry")
    async def test_perform_eligibility_check_empty_filtered_records(
        self, mock_registry, executor
    ):
        """Test eligibility check when filtering results in no active records."""
        # Mock the query registry
        mock_query = MagicMock()
        mock_query.method_name = "get_all_by_name_and_date_of_birth"
        mock_query.validate_params.return_value.is_valid = True
        mock_query.filter_params.return_value = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

        mock_registry.get_v1_queries.return_value = [mock_query]

        # Create mock member record
        mock_member = MagicMock(spec=model.MemberVersioned)
        mock_member.id = 12345
        mock_member.organization_id = 1

        # Set up repository method to return mock member
        executor.repository.get_all_by_name_and_date_of_birth.return_value = [
            mock_member
        ]

        # Mock the filter_active_records method to return empty list (not None, which would raise InactiveOrganizationError)
        with patch.object(
            executor, "_filter_active_records", return_value=[]
        ), patch.object(executor, "_is_v2_enabled", return_value=False):
            # Call the method and expect a MemberSearchError
            with pytest.raises(errors.MemberSearchError) as exc_info:
                await executor.perform_eligibility_check(
                    method=EligibilityMethod.BASIC,
                    params={
                        "first_name": "John",
                        "last_name": "Doe",
                        "date_of_birth": "1990-01-01",
                    },
                    expected_type=MultipleRecordType,
                )

            assert "No member records found matching the provided criteria" in str(
                exc_info.value
            )

    @patch("app.eligibility.query_framework.executor.QueryRegistry")
    async def test_execute_queries_all_invalid(self, mock_registry, executor):
        """Test execute_queries when validation fails for all queries."""
        # Mock queries
        v1_queries = [MagicMock(), MagicMock()]

        # Mock _prepare_queries to raise ValidationError
        validation_error = errors.ValidationError("Missing required parameters")
        with patch.object(
            executor, "_prepare_queries", side_effect=validation_error
        ) as mock_prepare:
            # Call the method
            params = {"some_param": "value"}

            # This should re-raise the ValidationError
            with pytest.raises(errors.ValidationError) as exc_info:
                await executor._execute_queries(
                    queries=v1_queries, method=EligibilityMethod.BASIC, params=params
                )

            # Verify _prepare_queries was called
            mock_prepare.assert_called_once_with(
                v1_queries, params, EligibilityMethod.BASIC
            )
            assert exc_info.value == validation_error

    @patch("app.eligibility.query_framework.executor.QueryRegistry")
    async def test_perform_eligibility_check_specific_error_paths(
        self, mock_registry, executor
    ):
        """Test specific error paths in perform_eligibility_check method."""
        # 1. Test when v1_queries is empty
        mock_registry.get_v1_queries.return_value = []

        with pytest.raises(errors.ValidationError) as exc_info:
            await executor.perform_eligibility_check(
                method=EligibilityMethod.BASIC,
                params={},
                expected_type=MultipleRecordType,
            )

        assert "No queries configured for method" in str(exc_info.value)

        # 2. Test when v1_result has an error
        mock_registry.get_v1_queries.return_value = [MagicMock()]
        mock_error_result = MagicMock()
        mock_error_result.error = "Query execution failed"

        with patch.object(executor, "_execute_queries", return_value=mock_error_result):
            with pytest.raises(errors.MemberSearchError) as exc_info:
                await executor.perform_eligibility_check(
                    method=EligibilityMethod.BASIC,
                    params={},
                    expected_type=MultipleRecordType,
                )

            assert "No matching records found for" in str(exc_info.value)
            assert "details: Query execution failed" in str(exc_info.value)

    @pytest.mark.asyncio
    @patch("app.eligibility.query_framework.executor.QueryRegistry")
    async def test_execute_queries_no_executable_queries(self, mock_registry, executor):
        """Test the scenario where _prepare_queries returns an empty list but doesn't raise ValidationError."""
        # Mock queries
        v1_queries = [MagicMock(), MagicMock()]

        # Mock _prepare_queries to return an empty list
        with patch.object(
            executor, "_prepare_queries", return_value=[]
        ) as mock_prepare:
            # Call the method
            params = {"some_param": "value"}
            result = await executor._execute_queries(
                queries=v1_queries, method=EligibilityMethod.BASIC, params=params
            )

            # Verify result shows error
            assert isinstance(result, QueryResult)
            assert result.result is None
            assert result.query_name == "no_executable_queries"
            assert result.query_type == "validation_error"
            assert "No valid queries available" in result.error
            assert result.is_success is False

            # Verify _prepare_queries was called
            mock_prepare.assert_called_once_with(
                v1_queries, params, EligibilityMethod.BASIC
            )

    @pytest.mark.asyncio
    @patch("app.eligibility.query_framework.executor.QueryRegistry")
    async def test_try_v2_queries_success_with_logger(self, mock_registry, executor):
        """Test _try_v2_queries with logger verification when successful."""
        # Mock dependencies
        mock_v1_record = MagicMock(spec=model.MemberVersioned)
        mock_v1_record.id = 12345

        mock_v2_record = MagicMock(spec=model.Member2)
        mock_v2_record.id = 67890

        mock_query = MagicMock()
        mock_query.method_name = "get_all_by_name_and_date_of_birth_v2"

        # Set up the mock logger
        with patch("app.eligibility.query_framework.executor.logger") as mock_logger:
            # Set up QueryRegistry to return the mock query
            mock_registry.get_v2_queries.return_value = [mock_query]

            # Set up the query result
            mock_result = MagicMock()
            mock_result.is_success = True
            mock_result.first_result = mock_v2_record
            mock_result.result = [mock_v2_record]

            # Mock _execute_queries and _validate_results
            with patch.object(
                executor, "_execute_queries", return_value=mock_result
            ), patch.object(executor, "_validate_results", return_value=True):
                # Call the method
                params = {
                    "first_name": "John",
                    "last_name": "Doe",
                    "date_of_birth": "1990-01-01",
                }
                result = await executor._try_v2_queries(
                    v1_record=mock_v1_record,
                    method=EligibilityMethod.BASIC,
                    params=params,
                )

                # Verify the result
                assert result == [mock_v2_record]

                # Verify logger was called
                mock_logger.info.assert_called_once_with(
                    "Using V2 results",
                    extra={"v1_id": mock_v1_record.id, "v2_id": mock_v2_record.id},
                )

    @pytest.mark.asyncio
    @patch("app.eligibility.query_framework.executor.QueryRegistry")
    async def test_run_in_parallel_execution(self, mock_registry, executor):
        """Test that _execute_queries respects _get_run_in_parallel_flag."""
        # Mock queries
        mock_query = MagicMock()
        mock_query.validate_params.return_value.is_valid = True
        mock_query.filter_params.return_value = {"param": "value"}

        v1_queries = [mock_query]
        params = {"param": "value"}

        # Mock _prepare_queries to return valid queries
        valid_queries = [(mock_query, {"param": "value"})]

        # Test with parallel execution
        with patch.object(
            executor, "_prepare_queries", return_value=valid_queries
        ), patch.object(
            executor, "_get_run_in_parallel_flag", return_value=True
        ), patch.object(
            executor, "_execute_in_parallel"
        ) as mock_parallel, patch.object(
            executor, "_execute_sequentially"
        ) as mock_sequential:
            # Set return value for _execute_in_parallel
            expected_result = QueryResult(
                result=MagicMock(), query_name="test", error=None
            )
            mock_parallel.return_value = expected_result

            # Call the method
            result = await executor._execute_queries(
                queries=v1_queries, method=EligibilityMethod.BASIC, params=params
            )

            # Verify result
            assert result == expected_result

            # Verify correct execution method was called
            mock_parallel.assert_called_once_with(
                valid_queries, EligibilityMethod.BASIC, params
            )
            mock_sequential.assert_not_called()

    @pytest.mark.asyncio
    @patch("app.eligibility.query_framework.executor.QueryRegistry")
    async def test_v1_result_with_error(self, mock_registry, executor):
        """Test perform_eligibility_check when v1_result has an error without mocking _execute_queries."""
        # Mock the query registry
        mock_query = MagicMock()
        mock_registry.get_v1_queries.return_value = [mock_query]

        # Create an error response for the execute_queries call
        error_response = QueryResult(
            result=None,
            query_name="failed_query",
            query_type="test",
            error="Query execution failed",
        )

        # Ensure _execute_queries returns the error response
        with patch.object(executor, "_execute_queries", return_value=error_response):
            # Call the method and expect MemberSearchError
            with pytest.raises(errors.MemberSearchError) as exc_info:
                await executor.perform_eligibility_check(
                    method=EligibilityMethod.BASIC,
                    params={"param": "value"},
                    expected_type=MultipleRecordType,
                )

            # Verify the error includes the details from v1_result.error
            error_message = str(exc_info.value)
            assert "No matching records found for" in error_message
            assert "details: Query execution failed" in error_message
            assert exc_info.value.method == EligibilityMethod.BASIC

    @pytest.mark.asyncio
    @patch("app.eligibility.query_framework.executor.QueryRegistry")
    async def test_v2_fallback_when_v2_query_returns_none(
        self, mock_registry, executor
    ):
        """Test fallback to V1 results when V2 query returns None."""
        # Mock the query registry
        mock_v1_query = MagicMock()
        mock_registry.get_v1_queries.return_value = [mock_v1_query]

        # Set up mock V1 result
        mock_v1_member = MagicMock(spec=model.MemberVersioned)
        mock_v1_member.id = 12345
        mock_v1_member.organization_id = 1

        v1_result = QueryResult(
            result=mock_v1_member, query_name="v1_query", query_type="test", error=None
        )

        # Mock necessary dependencies
        with patch.object(
            executor, "_execute_queries", return_value=v1_result
        ), patch.object(
            executor, "_determine_query_version", return_value=(False, "test reason")
        ), patch.object(
            executor, "_try_v2_queries", return_value=None
        ), patch.object(
            executor, "_log_result_usage"
        ) as mock_log, patch.object(
            executor, "_filter_active_records", return_value=mock_v1_member
        ):
            # Call the method
            result = await executor.perform_eligibility_check(
                method=EligibilityMethod.BASIC,
                params={"param": "value"},
                expected_type=SingleRecordType,
            )

            # Verify we got a result based on V1 data
            assert isinstance(result, EligibilityResult)
            assert result.records == mock_v1_member
            assert result.v1_id == 12345

            # Verify _log_result_usage was called with correct parameters
            mock_log.assert_called_once()
            call_args = mock_log.call_args[0]
            assert call_args[0] is True  # use_v1_results should be True
            assert call_args[1] == "V2 query failed or validation failed"

    @pytest.mark.asyncio
    @patch("app.eligibility.query_framework.executor.QueryRegistry")
    async def test_v2_success_path(self, mock_registry, executor):
        """Test successful V2 path where V2 query returns a result and we use it."""
        # Mock the query registry
        mock_v1_query = MagicMock()
        mock_registry.get_v1_queries.return_value = [mock_v1_query]

        # Set up mock V1 result
        mock_v1_member = MagicMock(spec=model.MemberVersioned)
        mock_v1_member.id = 12345
        mock_v1_member.organization_id = 1

        v1_result = QueryResult(
            result=mock_v1_member, query_name="v1_query", query_type="test", error=None
        )

        # Set up mock V2 result
        mock_v2_member = MagicMock(spec=model.Member2)

        # Mock necessary dependencies
        with patch.object(
            executor, "_execute_queries", return_value=v1_result
        ), patch.object(
            executor, "_determine_query_version", return_value=(False, "test reason")
        ), patch.object(
            executor, "_try_v2_queries", return_value=mock_v2_member
        ), patch.object(
            executor, "_log_result_usage"
        ) as mock_log, patch.object(
            executor, "_filter_active_records", return_value=mock_v2_member
        ):
            # Call the method
            result = await executor.perform_eligibility_check(
                method=EligibilityMethod.BASIC,
                params={"param": "value"},
                expected_type=SingleRecordType,
            )

            # Verify we got a result based on V2 data
            assert isinstance(result, EligibilityResult)
            assert result.records == mock_v2_member
            assert result.v1_id == 12345

            # Verify _log_result_usage was called with correct parameters
            mock_log.assert_called_once()
            call_args = mock_log.call_args[0]
            assert call_args[0] is False  # use_v1_results should be False
            assert call_args[1] == "V2 validation successful"

    @pytest.mark.asyncio
    @patch("app.eligibility.query_framework.executor.QueryRegistry")
    async def test_execute_queries_real_implementation(self, mock_registry, executor):
        """Test execute_queries without mocking internal methods to ensure real code path is covered."""
        # Create a real query definition
        query_def = MagicMock()
        query_def.method_name = "get_all_by_name_and_date_of_birth"
        query_def.validate_params.return_value.is_valid = True
        query_def.filter_params.return_value = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

        # Mock the repository method to return a result
        mock_member = MagicMock(spec=model.MemberVersioned)
        mock_member.id = 12345
        mock_member.organization_id = 1
        executor.repository.get_all_by_name_and_date_of_birth.return_value = [
            mock_member
        ]

        # Call execute_queries directly
        result = await executor._execute_queries(
            queries=[query_def],
            method=EligibilityMethod.BASIC,
            params={
                "first_name": "John",
                "last_name": "Doe",
                "date_of_birth": "1990-01-01",
            },
        )

        # Verify result
        assert isinstance(result, QueryResult)
        assert result.is_success is True
        assert result.result == [mock_member]
        assert result.query_name == "get_all_by_name_and_date_of_birth"
        assert result.v_id == 12345
        assert result.organization_id == "1"
        assert result.error is None

    @pytest.mark.asyncio
    async def test_end_to_end_eligibility_check(self, executor):
        """Test a more complete end-to-end eligibility check with minimal mocking to ensure full code coverage."""
        # Create mock repository methods
        mock_v1_member = MagicMock(spec=model.MemberVersioned)
        mock_v1_member.id = 12345
        mock_v1_member.organization_id = 1
        mock_v1_member.first_name = "John"
        mock_v1_member.last_name = "Doe"
        mock_v1_member.date_of_birth = "1990-01-01"
        mock_v1_member.unique_corp_id = "abc123"
        mock_v1_member.dependent_id = None

        mock_v2_member = MagicMock(spec=model.Member2)
        mock_v2_member.id = 67890
        mock_v2_member.organization_id = 1
        mock_v2_member.first_name = "John"
        mock_v2_member.last_name = "Doe"
        mock_v2_member.date_of_birth = "1990-01-01"
        mock_v2_member.unique_corp_id = "abc123"
        mock_v2_member.dependent_id = None

        # Set up repository methods
        executor.repository.get_all_by_name_and_date_of_birth.return_value = [
            mock_v1_member
        ]
        executor.repository.get_all_by_name_and_date_of_birth_v2.return_value = [
            mock_v2_member
        ]

        # Mock QueryRegistry
        with patch(
            "app.eligibility.query_framework.executor.QueryRegistry"
        ) as mock_registry:
            # Set up mock query
            mock_query = MagicMock()
            mock_query.method_name = "get_all_by_name_and_date_of_birth"
            mock_query.validate_params.return_value.is_valid = True
            mock_query.filter_params.return_value = {
                "first_name": "John",
                "last_name": "Doe",
                "date_of_birth": "1990-01-01",
            }

            mock_v2_query = MagicMock()
            mock_v2_query.method_name = "get_all_by_name_and_date_of_birth_v2"
            mock_v2_query.validate_params.return_value.is_valid = True
            mock_v2_query.filter_params.return_value = {
                "first_name": "John",
                "last_name": "Doe",
                "date_of_birth": "1990-01-01",
            }

            mock_registry.get_v1_queries.return_value = [mock_query]
            mock_registry.get_v2_queries.return_value = [mock_v2_query]

            # Mock enough to make test work but not too much
            with patch.object(
                executor, "_is_v2_enabled", return_value=True
            ), patch.object(
                executor, "_filter_active_records", return_value=[mock_v2_member]
            ):
                # Call the method
                result = await executor.perform_eligibility_check(
                    method=EligibilityMethod.BASIC,
                    params={
                        "first_name": "John",
                        "last_name": "Doe",
                        "date_of_birth": "1990-01-01",
                    },
                    expected_type=MultipleRecordType,
                )

                # Verify we got a result
                assert isinstance(result, EligibilityResult)
                assert result.records == [mock_v2_member]
                assert result.v1_id == 12345

    @pytest.mark.asyncio
    @patch("app.eligibility.query_framework.executor.eligibility_validation")
    async def test_filter_active_records_single_member(self, mock_validation, executor):
        """Test filtering a single member record."""
        # Create a mock member
        mock_member = MagicMock(spec=model.MemberVersioned)
        mock_member.id = 12345
        mock_member.organization_id = 1

        # Set up validation mock using AsyncMock
        mock_validation.check_member_org_active = AsyncMock(return_value=mock_member)

        # Call the method
        result = await executor._filter_active_records(
            member_records=mock_member,
            method=EligibilityMethod.BASIC,
            expected_type=SingleRecordType,
        )

        # Verify the result and that the correct validation function was called
        assert result == mock_member
        mock_validation.check_member_org_active.assert_called_once_with(
            configuration_client=executor.configurations, member=mock_member
        )

    @pytest.mark.asyncio
    @patch("app.eligibility.query_framework.executor.eligibility_validation")
    async def test_filter_active_records_list_multiple(self, mock_validation, executor):
        """Test filtering a list with expected multiple records (overeligibility)."""
        # Create mock members
        mock_members = [
            MagicMock(spec=model.MemberVersioned),
            MagicMock(spec=model.MemberVersioned),
        ]

        # Set up validation mock using AsyncMock
        mock_validation.check_member_org_active_and_overeligibility = AsyncMock(
            return_value=mock_members
        )

        # Call the method with MultipleRecordType (list expected)
        result = await executor._filter_active_records(
            member_records=mock_members,
            method=EligibilityMethod.BASIC,
            expected_type=MultipleRecordType,
        )

        # Verify the result and that the correct validation function was called
        assert result == mock_members
        mock_validation.check_member_org_active_and_overeligibility.assert_called_once_with(
            configuration_client=executor.configurations, member_list=mock_members
        )

    @pytest.mark.asyncio
    @patch("app.eligibility.query_framework.executor.eligibility_validation")
    async def test_filter_active_records_list_single(self, mock_validation, executor):
        """Test filtering a list when single record is expected."""
        # Create mock members
        mock_members = [
            MagicMock(spec=model.MemberVersioned),
            MagicMock(spec=model.MemberVersioned),
        ]

        # Single result after filtering
        mock_single_result = MagicMock(spec=model.MemberVersioned)

        # Set up validation mock using AsyncMock
        mock_validation.check_member_org_active_and_single_org = AsyncMock(
            return_value=mock_single_result
        )

        # Call the method with SingleRecordType (single result expected)
        result = await executor._filter_active_records(
            member_records=mock_members,
            method=EligibilityMethod.EMPLOYER,
            expected_type=SingleRecordType,
        )

        # Verify the result and that the correct validation function was called
        assert result == mock_single_result
        mock_validation.check_member_org_active_and_single_org.assert_called_once_with(
            configuration_client=executor.configurations, member_list=mock_members
        )

    @pytest.mark.asyncio
    @patch("app.eligibility.query_framework.executor.eligibility_validation")
    async def test_filter_active_records_inactive_organization(
        self, mock_validation, executor
    ):
        """Test filtering when all organizations are inactive."""
        # Create mock members
        mock_members = [
            MagicMock(spec=model.MemberVersioned),
            MagicMock(spec=model.MemberVersioned),
        ]

        # Set up validation mock using AsyncMock to return None (inactive)
        mock_validation.check_member_org_active_and_overeligibility = AsyncMock(
            return_value=None
        )

        # Call the method and expect InactiveOrganizationError
        with pytest.raises(errors.InactiveOrganizationError) as exc_info:
            await executor._filter_active_records(
                member_records=mock_members,
                method=EligibilityMethod.BASIC,
                expected_type=MultipleRecordType,
            )

        # Verify the error has the right method
        assert exc_info.value.method == EligibilityMethod.BASIC

    @pytest.mark.asyncio
    @patch("app.eligibility.query_framework.executor.eligibility_validation")
    async def test_filter_active_records_empty_result(self, mock_validation, executor):
        """Test filtering when validation returns empty list."""
        # Create mock members
        mock_members = [
            MagicMock(spec=model.MemberVersioned),
            MagicMock(spec=model.MemberVersioned),
        ]

        # Set up validation mock using AsyncMock to return empty list
        mock_validation.check_member_org_active_and_overeligibility = AsyncMock(
            return_value=[]
        )

        # Call the method and expect InactiveOrganizationError
        with pytest.raises(errors.InactiveOrganizationError) as exc_info:
            await executor._filter_active_records(
                member_records=mock_members,
                method=EligibilityMethod.BASIC,
                expected_type=MultipleRecordType,
            )

        # Verify the error has the right method
        assert exc_info.value.method == EligibilityMethod.BASIC

    @pytest.mark.asyncio
    @patch("app.eligibility.query_framework.executor.logger")
    async def test_execute_single_query_success_single(self, mock_logger, executor):
        """Test successful execution of a query with a single result."""
        # Create a mock query
        mock_query = MagicMock()
        mock_query.method_name = "get_by_dob_and_email"
        mock_query.query_type = "dob_email"

        # Create mock member
        mock_member = MagicMock(spec=model.MemberVersioned)
        mock_member.id = 12345
        mock_member.organization_id = 1

        # Set up repository method
        executor.repository.get_by_dob_and_email = AsyncMock(return_value=mock_member)

        # Call the method
        params = {
            "date_of_birth": "1990-01-01",
            "email": "john@example.com",
            "user_id": 456,
        }
        result = await executor._execute_single_query(
            query=mock_query, params=params, method=EligibilityMethod.EMPLOYER
        )

        # Verify result
        assert isinstance(result, QueryResult)
        assert result.result == mock_member
        assert result.query_name == "get_by_dob_and_email"
        assert result.query_type == "dob_email"
        assert result.v_id == 12345
        assert result.organization_id == "1"
        assert result.error is None

        # Verify logger calls
        mock_logger.info.assert_any_call(
            "Executing query: get_by_dob_and_email",
            extra={
                "user_id": 456,
                "method": EligibilityMethod.EMPLOYER.value,
                "query_type": "dob_email",
                "parameters": {
                    "date_of_birth": "***",
                    "email": "***",
                    "user_id": 456,
                },
            },
        )

        mock_logger.info.assert_any_call(
            "Query successful: get_by_dob_and_email",
            extra={
                "execution_time_ms": mock.ANY,
                "result_count": 1,
                "query_type": "dob_email",
                "organization_id": "1",
                "method": EligibilityMethod.EMPLOYER.value,
                "v1_id": 12345,
            },
        )

    @pytest.mark.asyncio
    @patch("app.eligibility.query_framework.executor.logger")
    async def test_execute_single_query_success_list(self, mock_logger, executor):
        """Test successful execution of a query with multiple results."""
        # Create a mock query
        mock_query = MagicMock()
        mock_query.method_name = "get_all_by_name_and_date_of_birth"
        mock_query.query_type = "name_dob"

        # Create mock members
        mock_members = [
            MagicMock(spec=model.MemberVersioned),
            MagicMock(spec=model.MemberVersioned),
        ]
        mock_members[0].id = 12345
        mock_members[0].organization_id = 1
        mock_members[1].id = 67890
        mock_members[1].organization_id = 1

        # Set up repository method
        executor.repository.get_all_by_name_and_date_of_birth = AsyncMock(
            return_value=mock_members
        )

        # Call the method
        params = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }
        result = await executor._execute_single_query(
            query=mock_query, params=params, method=EligibilityMethod.BASIC
        )

        # Verify result
        assert isinstance(result, QueryResult)
        assert result.result == mock_members
        assert result.query_name == "get_all_by_name_and_date_of_birth"
        assert result.query_type == "name_dob"
        assert result.v_id == 12345  # Should take ID from first record
        assert result.organization_id == "1"
        assert result.error is None

        # Verify logger calls
        mock_logger.info.assert_any_call(
            "Executing query: get_all_by_name_and_date_of_birth",
            extra={
                "user_id": None,
                "method": EligibilityMethod.BASIC.value,
                "query_type": "name_dob",
                "parameters": {
                    "first_name": "***",
                    "last_name": "***",
                    "date_of_birth": "***",
                },
            },
        )

        mock_logger.info.assert_any_call(
            "Query successful: get_all_by_name_and_date_of_birth",
            extra={
                "execution_time_ms": mock.ANY,
                "result_count": 2,
                "query_type": "name_dob",
                "organization_id": "1",
                "method": EligibilityMethod.BASIC.value,
                "v1_id": 12345,
            },
        )

    @pytest.mark.asyncio
    @patch("app.eligibility.query_framework.executor.logger")
    async def test_execute_single_query_empty_list(self, mock_logger, executor):
        """Test execution of a query that returns an empty list."""
        # Create a mock query
        mock_query = MagicMock()
        mock_query.method_name = "get_all_by_name_and_date_of_birth"
        mock_query.query_type = "name_dob"

        # Set up repository method to return empty list
        executor.repository.get_all_by_name_and_date_of_birth = AsyncMock(
            return_value=[]
        )

        # Call the method
        params = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }
        result = await executor._execute_single_query(
            query=mock_query, params=params, method=EligibilityMethod.BASIC
        )

        # Verify result
        assert isinstance(result, QueryResult)
        assert result.result == []
        assert result.query_name == "get_all_by_name_and_date_of_birth"
        assert result.query_type == "name_dob"
        assert result.error is None
        assert result.is_success is False  # Empty list should not be considered success

        # Verify logger calls
        mock_logger.warning.assert_called_once_with(
            "Query returned no results: get_all_by_name_and_date_of_birth",
            extra={
                "execution_time_ms": mock.ANY,
                "query_type": "name_dob",
                "method": EligibilityMethod.BASIC.value,
                "parameters": {
                    "first_name": "***",
                    "last_name": "***",
                    "date_of_birth": "***",
                },
                "is_empty_list": True,
                "result_is_none": False,
            },
        )

    @pytest.mark.asyncio
    @patch("app.eligibility.query_framework.executor.logger")
    async def test_execute_single_query_none_result(self, mock_logger, executor):
        """Test execution of a query that returns None."""
        # Create a mock query
        mock_query = MagicMock()
        mock_query.method_name = "get_by_dob_and_email"
        mock_query.query_type = "dob_email"

        # Set up repository method to return None
        executor.repository.get_by_dob_and_email = AsyncMock(return_value=None)

        # Call the method
        params = {"date_of_birth": "1990-01-01", "email": "john@example.com"}
        result = await executor._execute_single_query(
            query=mock_query, params=params, method=EligibilityMethod.EMPLOYER
        )

        # Verify result
        assert isinstance(result, QueryResult)
        assert result.result is None
        assert result.query_name == "get_by_dob_and_email"
        assert result.query_type == "dob_email"
        assert result.error is None
        assert result.is_success is False  # None should not be considered success

        # Verify logger calls
        mock_logger.warning.assert_called_once_with(
            "Query returned no results: get_by_dob_and_email",
            extra={
                "execution_time_ms": mock.ANY,
                "query_type": "dob_email",
                "method": EligibilityMethod.EMPLOYER.value,
                "parameters": {"date_of_birth": "***", "email": "***"},
                "is_empty_list": False,
                "result_is_none": True,
            },
        )

    @pytest.mark.asyncio
    @patch("app.eligibility.query_framework.executor.logger")
    async def test_execute_single_query_exception(self, mock_logger, executor):
        """Test execution of a query that raises an exception."""
        # Create a mock query
        mock_query = MagicMock()
        mock_query.method_name = "get_by_dob_and_email"
        mock_query.query_type = "dob_email"

        # Set up repository method to raise an exception
        error_message = "Invalid date format"
        executor.repository.get_by_dob_and_email = AsyncMock(
            side_effect=ValueError(error_message)
        )

        # Call the method
        params = {"date_of_birth": "invalid-date", "email": "john@example.com"}
        result = await executor._execute_single_query(
            query=mock_query, params=params, method=EligibilityMethod.EMPLOYER
        )
        # Verify result
        assert isinstance(result, QueryResult)
        assert result.result is None
        assert result.query_name == "get_by_dob_and_email"
        assert result.query_type == "dob_email"
        assert result.error == error_message
        assert result.is_success is False

        # Verify logger calls
        mock_logger.warning.assert_called_once_with(
            f"Query get_by_dob_and_email failed with error: {error_message}",
            extra={
                "execution_time_ms": mock.ANY,
                "error": error_message,
                "error_type": "ValueError",
                "query_type": "dob_email",
                "method": EligibilityMethod.EMPLOYER.value,
                "parameters": {"date_of_birth": "***", "email": "***"},
            },
        )

    @pytest.mark.asyncio
    @patch("app.eligibility.query_framework.executor.logger")
    @patch("app.eligibility.query_framework.executor.time")
    async def test_execute_single_query_timing(self, mock_time, mock_logger, executor):
        """Test that execution timing is correctly measured and logged."""
        # Set up time mock
        start_time = 1000.0
        end_time = 1001.5  # 1.5 seconds later
        mock_time.time.side_effect = [start_time, end_time]

        # Create a mock query
        mock_query = MagicMock()
        mock_query.method_name = "get_by_dob_and_email"
        mock_query.query_type = "dob_email"

        # Create mock member
        mock_member = MagicMock(spec=model.MemberVersioned)
        mock_member.id = 12345
        mock_member.organization_id = 1

        # Set up repository method
        executor.repository.get_by_dob_and_email = AsyncMock(return_value=mock_member)

        # Call the method
        params = {"date_of_birth": "1990-01-01", "email": "john@example.com"}
        await executor._execute_single_query(
            query=mock_query, params=params, method=EligibilityMethod.EMPLOYER
        )

        # Verify timing in logger calls
        # The execution_time_ms should be 1500 (1.5 seconds in milliseconds)
        for call_c in mock_logger.info.call_args_list:
            if "Query successful" in call_c[0][0]:
                assert call_c[1]["extra"]["execution_time_ms"] == 1500

    @pytest.mark.asyncio
    @patch("app.eligibility.query_framework.executor.logger")
    async def test_execute_single_query_parameter_redaction(
        self, mock_logger, executor
    ):
        """Test that sensitive parameters are properly redacted in logs."""
        # Create a mock query
        mock_query = MagicMock()
        mock_query.method_name = "get_by_dob_and_email"
        mock_query.query_type = "dob_email"

        # Create mock member
        mock_member = MagicMock(spec=model.MemberVersioned)
        mock_member.id = 12345
        mock_member.organization_id = 1

        # Set up repository method
        executor.repository.get_by_dob_and_email = AsyncMock(return_value=mock_member)

        # Call the method with all sensitive parameters
        params = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
            "dependent_date_of_birth": "2010-01-01",
            "email": "john@example.com",  # Not sensitive
            "user_id": 456,  # Not sensitive
        }
        await executor._execute_single_query(
            query=mock_query, params=params, method=EligibilityMethod.EMPLOYER
        )

        # Verify parameters were redacted properly
        for call_c in mock_logger.info.call_args_list:
            if "Executing query" in call_c[0][0]:
                log_params = call_c[1]["extra"]["parameters"]
                assert log_params["first_name"] == "***"
                assert log_params["last_name"] == "***"
                assert log_params["date_of_birth"] == "***"
                assert log_params["dependent_date_of_birth"] == "***"
                assert log_params["email"] == "***"
                assert log_params["user_id"] == 456  # Not redacted

    @pytest.mark.asyncio
    async def test_execute_in_parallel_first_query_succeeds(self, executor):
        """Test parallel execution where the first query succeeds."""
        # Create mock queries
        mock_query1 = MagicMock()
        mock_query1.method_name = "get_by_dob_and_email"
        mock_params1 = {"date_of_birth": "1990-01-01", "email": "john@example.com"}

        mock_query2 = MagicMock()
        mock_query2.method_name = "get_all_by_name_and_date_of_birth"
        mock_params2 = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

        executable_queries = [(mock_query1, mock_params1), (mock_query2, mock_params2)]

        # Create a successful result for the first query and a failed result for the second
        success_result = QueryResult(
            result=MagicMock(spec=model.MemberVersioned),
            query_name="get_by_dob_and_email",
            query_type="test",
            error=None,
        )

        failed_result = QueryResult(
            result=None,
            query_name="get_all_by_name_and_date_of_birth",
            query_type="test",
            error="No matching record",
        )

        # Patch _execute_single_query to return the appropriate results
        # We'll set it up so both are called (due to parallel execution)
        with patch.object(
            executor,
            "_execute_single_query",
            side_effect=[success_result, failed_result],
        ) as mock_execute:
            # Call the method
            result = await executor._execute_in_parallel(
                executable_queries=executable_queries,
                method=EligibilityMethod.EMPLOYER,
                params={"some": "params"},
            )

            # Verify results - should get the successful one regardless of order
            assert result is success_result

            # Verify that _execute_single_query was called for both queries
            assert mock_execute.call_count == 2
            mock_execute.assert_any_call(
                mock_query1, mock_params1, EligibilityMethod.EMPLOYER
            )
            mock_execute.assert_any_call(
                mock_query2, mock_params2, EligibilityMethod.EMPLOYER
            )

    @pytest.mark.asyncio
    async def test_execute_in_parallel_second_query_succeeds(self, executor):
        """Test parallel execution where the second query succeeds but the first fails."""
        # Create mock queries
        mock_query1 = MagicMock()
        mock_query1.method_name = "get_by_dob_and_email"
        mock_params1 = {"date_of_birth": "1990-01-01", "email": "john@example.com"}

        mock_query2 = MagicMock()
        mock_query2.method_name = "get_all_by_name_and_date_of_birth"
        mock_params2 = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

        executable_queries = [(mock_query1, mock_params1), (mock_query2, mock_params2)]

        # Create results for each query
        failed_result = QueryResult(
            result=None,
            query_name="get_by_dob_and_email",
            query_type="test",
            error="No matching record",
        )

        success_result = QueryResult(
            result=MagicMock(spec=model.MemberVersioned),
            query_name="get_all_by_name_and_date_of_birth",
            query_type="test",
            error=None,
        )

        # Patch _execute_single_query to return the appropriate results
        with patch.object(
            executor,
            "_execute_single_query",
            side_effect=[failed_result, success_result],
        ) as mock_execute:
            # Call the method
            result = await executor._execute_in_parallel(
                executable_queries=executable_queries,
                method=EligibilityMethod.EMPLOYER,
                params={"some": "params"},
            )

            # Verify results - should get the successful one (the second one)
            assert result is success_result

            # Verify that _execute_single_query was called for both queries
            assert mock_execute.call_count == 2

    @pytest.mark.asyncio
    async def test_execute_in_parallel_all_queries_fail(self, executor):
        """Test parallel execution where all queries fail."""
        # Create mock queries
        mock_query1 = MagicMock()
        mock_query1.method_name = "get_by_dob_and_email"
        mock_params1 = {"date_of_birth": "1990-01-01", "email": "john@example.com"}

        mock_query2 = MagicMock()
        mock_query2.method_name = "get_all_by_name_and_date_of_birth"
        mock_params2 = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

        executable_queries = [(mock_query1, mock_params1), (mock_query2, mock_params2)]

        # Create failed results for each query
        failed_result1 = QueryResult(
            result=None,
            query_name="get_by_dob_and_email",
            query_type="test",
            error="No matching record 1",
        )

        failed_result2 = QueryResult(
            result=None,
            query_name="get_all_by_name_and_date_of_birth",
            query_type="test",
            error="No matching record 2",
        )

        # Patch _execute_single_query to return failed results for both queries
        with patch.object(
            executor,
            "_execute_single_query",
            side_effect=[failed_result1, failed_result2],
        ) as mock_execute:
            # Call the method
            result = await executor._execute_in_parallel(
                executable_queries=executable_queries,
                method=EligibilityMethod.EMPLOYER,
                params={"some": "params"},
            )

            # Verify results - should get the last failed result
            assert result is failed_result2

            # Verify that _execute_single_query was called for both queries
            assert mock_execute.call_count == 2

    @pytest.mark.asyncio
    async def test_execute_in_parallel_empty_queries_list(self, executor):
        """Test parallel execution with an empty list of queries."""
        # Call the method with an empty list of queries
        result = await executor._execute_in_parallel(
            executable_queries=[],
            method=EligibilityMethod.EMPLOYER,
            params={"some": "params"},
        )

        # Verify results
        assert isinstance(result, QueryResult)
        assert result.is_success is False
        assert result.query_name == "all_queries_failed"
        assert result.query_type == "execution_error"
        assert result.error == "All queries failed to execute"

    @pytest.mark.asyncio
    async def test_execute_in_parallel_with_actual_calls(self, executor):
        """Test parallel execution with actual calls to _execute_single_query."""
        # Create mock queries
        mock_query1 = MagicMock()
        mock_query1.method_name = "get_by_dob_and_email"
        mock_params1 = {"date_of_birth": "1990-01-01", "email": "john@example.com"}

        mock_query2 = MagicMock()
        mock_query2.method_name = "get_all_by_name_and_date_of_birth"
        mock_params2 = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

        executable_queries = [(mock_query1, mock_params1), (mock_query2, mock_params2)]

        # Set up repository methods
        mock_member = MagicMock(spec=model.MemberVersioned)
        mock_member.id = 12345
        mock_member.organization_id = 1

        # First method returns None (failure), second returns a member (success)
        executor.repository.get_by_dob_and_email = AsyncMock(return_value=None)
        executor.repository.get_all_by_name_and_date_of_birth = AsyncMock(
            return_value=[mock_member]
        )

        # Call the method directly without patching _execute_single_query
        with patch(
            "app.eligibility.query_framework.executor.logger"
        ):  # Just patch logger to avoid actual logging
            result = await executor._execute_in_parallel(
                executable_queries=executable_queries,
                method=EligibilityMethod.EMPLOYER,
                params={"some": "params"},
            )

            # Verify results
            assert isinstance(result, QueryResult)
            assert result.is_success is True
            assert result.query_name == "get_all_by_name_and_date_of_birth"
            assert result.result == [mock_member]

    @pytest.mark.asyncio
    async def test_execute_in_parallel_order_preservation(self, executor):
        """Test that success results are returned in the correct order (first success found)."""
        # Create mock queries
        mock_query1 = MagicMock()
        mock_query1.method_name = "get_by_dob_and_email"
        mock_params1 = {"date_of_birth": "1990-01-01", "email": "john@example.com"}

        mock_query2 = MagicMock()
        mock_query2.method_name = "get_all_by_name_and_date_of_birth"
        mock_params2 = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

        mock_query3 = MagicMock()
        mock_query3.method_name = "get_by_name_and_unique_corp_id"
        mock_params3 = {
            "first_name": "John",
            "last_name": "Doe",
            "unique_corp_id": "123",
        }

        executable_queries = [
            (mock_query1, mock_params1),
            (mock_query2, mock_params2),
            (mock_query3, mock_params3),
        ]

        # Create results with different successes
        mock_member1 = MagicMock(spec=model.MemberVersioned)
        mock_member1.id = 12345
        mock_member1.organization_id = 1

        mock_member2 = MagicMock(spec=model.MemberVersioned)
        mock_member2.id = 67890
        mock_member2.organization_id = 2

        # Create different successful results
        success_result1 = QueryResult(
            result=mock_member1,
            query_name="get_by_dob_and_email",
            query_type="test1",
            error=None,
        )

        success_result2 = QueryResult(
            result=mock_member2,
            query_name="get_all_by_name_and_date_of_birth",
            query_type="test2",
            error=None,
        )

        success_result3 = QueryResult(
            result=MagicMock(spec=model.MemberVersioned),
            query_name="get_by_name_and_unique_corp_id",
            query_type="test3",
            error=None,
        )

        # We'll test with different ordering to ensure the first success is always returned

        # Test case 1: All succeed, first result should be returned
        with patch.object(
            executor,
            "_execute_single_query",
            side_effect=[success_result1, success_result2, success_result3],
        ):
            result = await executor._execute_in_parallel(
                executable_queries=executable_queries,
                method=EligibilityMethod.EMPLOYER,
                params={"some": "params"},
            )

            # Should return the first success result
            assert result is success_result1

        # Test case 2: Results in different order (success first, third)
        with patch.object(
            executor,
            "_execute_single_query",
            side_effect=[
                success_result1,
                QueryResult(
                    result=None, query_name="fail", query_type="test", error="Error"
                ),
                success_result3,
            ],
        ):
            result = await executor._execute_in_parallel(
                executable_queries=executable_queries,
                method=EligibilityMethod.EMPLOYER,
                params={"some": "params"},
            )

            # Should return the first success result
            assert result is success_result1

    @pytest.mark.asyncio
    async def test_execute_sequentially_first_query_succeeds(self, executor):
        """Test sequential execution where the first query succeeds."""
        # Create mock queries
        mock_query1 = MagicMock()
        mock_query1.method_name = "get_by_dob_and_email"
        mock_params1 = {"date_of_birth": "1990-01-01", "email": "john@example.com"}

        mock_query2 = MagicMock()
        mock_query2.method_name = "get_all_by_name_and_date_of_birth"
        mock_params2 = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

        executable_queries = [(mock_query1, mock_params1), (mock_query2, mock_params2)]

        # Create a successful result for the first query
        success_result = QueryResult(
            result=MagicMock(spec=model.MemberVersioned),
            query_name="get_by_dob_and_email",
            query_type="test",
            error=None,
        )

        # Patch _execute_single_query to return the successful result for the first query
        with patch.object(
            executor, "_execute_single_query", return_value=success_result
        ) as mock_execute:
            # Call the method
            result = await executor._execute_sequentially(
                executable_queries=executable_queries,
                method=EligibilityMethod.EMPLOYER,
                params={"some": "params"},
            )

            # Verify results
            assert result is success_result

            # Verify that _execute_single_query was called only once (for the first query)
            assert mock_execute.call_count == 1
            mock_execute.assert_called_once_with(
                mock_query1, mock_params1, EligibilityMethod.EMPLOYER
            )

    @pytest.mark.asyncio
    async def test_execute_sequentially_second_query_succeeds(self, executor):
        """Test sequential execution where the second query succeeds after the first fails."""
        # Create mock queries
        mock_query1 = MagicMock()
        mock_query1.method_name = "get_by_dob_and_email"
        mock_params1 = {"date_of_birth": "1990-01-01", "email": "john@example.com"}

        mock_query2 = MagicMock()
        mock_query2.method_name = "get_all_by_name_and_date_of_birth"
        mock_params2 = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

        executable_queries = [(mock_query1, mock_params1), (mock_query2, mock_params2)]

        # Create results for each query
        failed_result = QueryResult(
            result=None,
            query_name="get_by_dob_and_email",
            query_type="test",
            error="No matching record",
        )

        success_result = QueryResult(
            result=MagicMock(spec=model.MemberVersioned),
            query_name="get_all_by_name_and_date_of_birth",
            query_type="test",
            error=None,
        )

        # Patch _execute_single_query to return the appropriate result for each query
        with patch.object(
            executor,
            "_execute_single_query",
            side_effect=[failed_result, success_result],
        ) as mock_execute:
            # Call the method
            result = await executor._execute_sequentially(
                executable_queries=executable_queries,
                method=EligibilityMethod.EMPLOYER,
                params={"some": "params"},
            )

            # Verify results
            assert result is success_result

            # Verify that _execute_single_query was called twice (both queries)
            assert mock_execute.call_count == 2
            mock_execute.assert_has_calls(
                [
                    mock.call(mock_query1, mock_params1, EligibilityMethod.EMPLOYER),
                    mock.call(mock_query2, mock_params2, EligibilityMethod.EMPLOYER),
                ]
            )

    @pytest.mark.asyncio
    async def test_execute_sequentially_all_queries_fail(self, executor):
        """Test sequential execution where all queries fail."""
        # Create mock queries
        mock_query1 = MagicMock()
        mock_query1.method_name = "get_by_dob_and_email"
        mock_params1 = {"date_of_birth": "1990-01-01", "email": "john@example.com"}

        mock_query2 = MagicMock()
        mock_query2.method_name = "get_all_by_name_and_date_of_birth"
        mock_params2 = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

        executable_queries = [(mock_query1, mock_params1), (mock_query2, mock_params2)]

        # Create failed results for each query
        failed_result1 = QueryResult(
            result=None,
            query_name="get_by_dob_and_email",
            query_type="test",
            error="No matching record",
        )

        failed_result2 = QueryResult(
            result=None,
            query_name="get_all_by_name_and_date_of_birth",
            query_type="test",
            error="No matching record",
        )

        # Patch _execute_single_query to return failed results for both queries
        with patch.object(
            executor,
            "_execute_single_query",
            side_effect=[failed_result1, failed_result2],
        ) as mock_execute:
            # Call the method
            result = await executor._execute_sequentially(
                executable_queries=executable_queries,
                method=EligibilityMethod.EMPLOYER,
                params={"some": "params"},
            )

            # Verify results
            assert isinstance(result, QueryResult)
            assert result.is_success is False
            assert result.query_name == "all_queries_failed"
            assert result.query_type == "execution_error"
            assert result.error == "All queries failed to execute"

            # Verify that _execute_single_query was called for both queries
            assert mock_execute.call_count == 2
            mock_execute.assert_has_calls(
                [
                    mock.call(mock_query1, mock_params1, EligibilityMethod.EMPLOYER),
                    mock.call(mock_query2, mock_params2, EligibilityMethod.EMPLOYER),
                ]
            )

    @pytest.mark.asyncio
    async def test_execute_sequentially_empty_queries_list(self, executor):
        """Test sequential execution with an empty list of queries."""
        # Call the method with an empty list of queries
        result = await executor._execute_sequentially(
            executable_queries=[],
            method=EligibilityMethod.EMPLOYER,
            params={"some": "params"},
        )

        # Verify results
        assert isinstance(result, QueryResult)
        assert result.is_success is False
        assert result.query_name == "all_queries_failed"
        assert result.query_type == "execution_error"
        assert result.error == "All queries failed to execute"

    @pytest.mark.asyncio
    async def test_execute_sequentially_with_actual_calls(self, executor):
        """Test sequential execution with actual calls to _execute_single_query."""
        # Create mock queries
        mock_query1 = MagicMock()
        mock_query1.method_name = "get_by_dob_and_email"
        mock_params1 = {"date_of_birth": "1990-01-01", "email": "john@example.com"}

        mock_query2 = MagicMock()
        mock_query2.method_name = "get_all_by_name_and_date_of_birth"
        mock_params2 = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

        executable_queries = [(mock_query1, mock_params1), (mock_query2, mock_params2)]

        # Set up repository methods
        mock_member = MagicMock(spec=model.MemberVersioned)
        mock_member.id = 12345
        mock_member.organization_id = 1

        # First method returns None (failure), second returns a member (success)
        executor.repository.get_by_dob_and_email = AsyncMock(return_value=None)
        executor.repository.get_all_by_name_and_date_of_birth = AsyncMock(
            return_value=[mock_member]
        )

        # Call the method directly without patching _execute_single_query
        with patch(
            "app.eligibility.query_framework.executor.logger"
        ):  # Just patch logger to avoid actual logging
            result = await executor._execute_sequentially(
                executable_queries=executable_queries,
                method=EligibilityMethod.EMPLOYER,
                params={"some": "params"},
            )

            # Verify results
            assert isinstance(result, QueryResult)
            assert result.is_success is True
            assert result.query_name == "get_all_by_name_and_date_of_birth"
            assert result.result == [mock_member]

    @pytest.mark.asyncio
    @patch("app.eligibility.query_framework.executor.QueryRegistry")
    async def test_no_queries_configured(self, mock_registry, executor):
        """
        Test for line: if not v1_queries: raise errors.ValidationError(f"No queries configured for method {method}")
        """
        # Setup mock to return empty list of queries
        mock_registry.get_v1_queries.return_value = []

        # Call method and expect ValidationError
        with pytest.raises(errors.ValidationError) as exc_info:
            await executor.perform_eligibility_check(
                method=EligibilityMethod.BASIC,
                params={"some": "params"},
                expected_type=MultipleRecordType,
            )

        # Verify the error message
        assert f"No queries configured for method {EligibilityMethod.BASIC}" in str(
            exc_info.value
        )

        # Verify the method was called correctly
        mock_registry.get_v1_queries.assert_called_once_with(EligibilityMethod.BASIC)

    @pytest.mark.asyncio
    @patch("app.eligibility.query_framework.executor.QueryRegistry")
    async def test_no_filtered_records(self, mock_registry, executor):
        """
        Test for line: if not filtered_records: raise errors.MemberSearchError(...)
        """
        # Create mock V1 result
        mock_member = MagicMock(spec=model.MemberVersioned)
        mock_member.id = 12345
        mock_member.organization_id = 1

        v1_result = QueryResult(
            result=mock_member, query_name="test_query", query_type="test", error=None
        )

        # Setup mocks
        mock_query = MagicMock()
        mock_registry.get_v1_queries.return_value = [mock_query]

        # Mock _execute_queries to return our V1 result
        with patch.object(
            executor, "_execute_queries", return_value=v1_result
        ), patch.object(
            executor, "_determine_query_version", return_value=(True, "test reason")
        ), patch.object(
            executor, "_log_result_usage"
        ), patch.object(
            executor, "_filter_active_records", return_value=None
        ):
            # Call method and expect MemberSearchError
            with pytest.raises(errors.MemberSearchError) as exc_info:
                await executor.perform_eligibility_check(
                    method=EligibilityMethod.BASIC,
                    params={"some": "params"},
                    expected_type=SingleRecordType,
                )

            # Verify the error message and method
            assert "No member records found matching the provided criteria" in str(
                exc_info.value
            )
            assert exc_info.value.method == EligibilityMethod.BASIC

    @pytest.mark.asyncio
    async def test_unsupported_return_type_error(self, executor):
        """
        Test for line: raise errors.UnsupportedReturnTypeError(method=method)
        """
        # Create a mock member
        mock_member = MagicMock(spec=model.MemberVersioned)

        # Setup mocks to trigger the error
        with patch("typing.get_origin", return_value=None), patch(
            "app.eligibility.query_framework.executor.MemberResultType", (object,)
        ):
            # Call method and expect UnsupportedReturnTypeError
            with pytest.raises(errors.UnsupportedReturnTypeError) as exc_info:
                await executor._filter_active_records(
                    member_records=mock_member,
                    method=EligibilityMethod.BASIC,
                    expected_type=str,  # Not a valid expected_type
                )

            # Verify the error has the correct method
            assert exc_info.value.method == EligibilityMethod.BASIC

    # New test for the QueryVersioningResult dataclass
    def test_query_versioning_result(self):
        """Test the QueryVersioningResult dataclass."""
        # Create a mock member and result
        mock_member = MagicMock(spec=model.MemberVersioned)
        v1_id = 12345
        org_id = "1"
        reason = "test reason"

        # Create a QueryVersioningResult instance
        result = QueryVersioningResult(
            member_result=mock_member,
            v1_id=v1_id,
            reason=reason,
            organization_id=org_id,
        )

        # Verify properties
        assert result.member_result == mock_member
        assert result.v1_id == v1_id
        assert result.reason == reason
        assert result.organization_id == org_id

    # Updated tests for _try_v1_queries method
    @pytest.mark.asyncio
    async def test_try_v1_queries(self, executor):
        """Test _try_v1_queries with the new tuple return structure."""
        # Create mock query and result
        mock_query = MagicMock()
        mock_registry = MagicMock()
        mock_registry.get_v1_queries.return_value = [mock_query]

        # Create mock member
        mock_member = MagicMock(spec=model.MemberVersioned)
        mock_member.id = 12345
        mock_member.organization_id = 1
        mock_member.first_name = "John"
        mock_member.last_name = "Doe"

        # Create QueryResult
        query_result = QueryResult(
            result=mock_member, query_name="test_query", query_type="test", error=None
        )

        # Mock execute_queries to return the query_result
        with patch(
            "app.eligibility.query_framework.executor.QueryRegistry", mock_registry
        ), patch.object(executor, "_execute_queries", return_value=query_result):
            # Call the method
            result, v1_id, org_id = await executor._try_v1_queries(
                method=EligibilityMethod.BASIC,
                params={"first_name": "John", "last_name": "Doe"},
            )

            # Verify the results
            assert result == query_result
            assert v1_id == 12345
            assert org_id == "1"

    @pytest.mark.asyncio
    async def test_try_v1_queries_error(self, executor):
        """Test _try_v1_queries when execute_queries returns an error."""
        # Create mock query and registry
        mock_query = MagicMock()
        mock_registry = MagicMock()
        mock_registry.get_v1_queries.return_value = [mock_query]

        # Create QueryResult with error
        error_result = QueryResult(
            result=None, query_name="test_query", query_type="test", error="Test error"
        )

        # Mock execute_queries to return the error_result
        with patch(
            "app.eligibility.query_framework.executor.QueryRegistry", mock_registry
        ), patch.object(executor, "_execute_queries", return_value=error_result):
            # Call the method and expect a MemberSearchError
            with pytest.raises(errors.MemberSearchError) as exc_info:
                await executor._try_v1_queries(
                    method=EligibilityMethod.BASIC,
                    params={"first_name": "John", "last_name": "Doe"},
                )

            # Verify the error message
            assert "No matching records found for" in str(exc_info.value)
            assert "details: Test error" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_try_v1_queries_not_success(self, executor):
        """Test _try_v1_queries when execute_queries returns a non-successful result."""
        # Create mock query and registry
        mock_query = MagicMock()
        mock_registry = MagicMock()
        mock_registry.get_v1_queries.return_value = [mock_query]

        # Create QueryResult that's not successful (empty result list)
        # We don't need to set is_success directly as it's a property
        # An empty list as result will make is_success return False
        not_success_result = QueryResult(
            result=[],  # Empty list will make is_success return False
            query_name="test_query",
            query_type="test",
            error=None,
        )

        # Verify the result is not successful
        assert not_success_result.is_success is False

        # Mock execute_queries to return the not_success_result
        with patch(
            "app.eligibility.query_framework.executor.QueryRegistry", mock_registry
        ), patch.object(executor, "_execute_queries", return_value=not_success_result):
            # Call the method and expect a MemberSearchError
            with pytest.raises(errors.MemberSearchError) as exc_info:
                await executor._try_v1_queries(
                    method=EligibilityMethod.BASIC,
                    params={"first_name": "John", "last_name": "Doe"},
                )

            # Verify the error message
            assert "No valid results found for" in str(exc_info.value)

    # Tests for _execute_eligibility_queries method
    @pytest.mark.asyncio
    async def test_execute_eligibility_queries_v1_only(self, executor):
        """Test _execute_eligibility_queries when only V1 should be used."""
        # Mock V1 query execution
        mock_member = MagicMock(spec=model.MemberVersioned)
        mock_member.id = 12345
        mock_member.organization_id = 1

        # Create V1 result
        v1_result = QueryResult(
            result=mock_member, query_name="test_query", query_type="test", error=None
        )

        # Mock the necessary methods
        with patch.object(
            executor, "_try_v1_queries", return_value=(v1_result, 12345, "1")
        ), patch.object(
            executor, "_determine_query_version", return_value=(True, "test reason")
        ), patch.object(
            executor, "_log_result_usage"
        ) as mock_log:
            # Call the method
            result = await executor._execute_eligibility_queries(
                method=EligibilityMethod.BASIC,
                params={"first_name": "John", "last_name": "Doe"},
            )

            # Verify the result
            assert isinstance(result, QueryVersioningResult)
            assert result.member_result == mock_member
            assert result.v1_id == 12345
            assert result.reason == "test reason"
            assert result.organization_id == "1"

            # Verify log_result_usage was called
            mock_log.assert_called_once_with(
                True, "test reason", EligibilityMethod.BASIC, v1_result, "1"
            )

    @pytest.mark.asyncio
    async def test_execute_eligibility_queries_v2_success(self, executor):
        """Test _execute_eligibility_queries when V2 should be used and succeeds."""
        # Mock V1 query execution
        mock_v1_member = MagicMock(spec=model.MemberVersioned)
        mock_v1_member.id = 12345
        mock_v1_member.organization_id = 1

        # Create V1 result
        v1_result = QueryResult(
            result=mock_v1_member,
            query_name="test_query",
            query_type="test",
            error=None,
        )

        # Mock V2 result
        mock_v2_member = MagicMock(spec=model.Member2)

        # Mock the necessary methods
        with patch.object(
            executor, "_try_v1_queries", return_value=(v1_result, 12345, "1")
        ), patch.object(
            executor, "_determine_query_version", return_value=(False, None)
        ), patch.object(
            executor, "_try_v2_queries", return_value=mock_v2_member
        ), patch.object(
            executor, "_log_result_usage"
        ) as mock_log:
            # Call the method
            result = await executor._execute_eligibility_queries(
                method=EligibilityMethod.BASIC,
                params={"first_name": "John", "last_name": "Doe"},
            )

            # Verify the result
            assert isinstance(result, QueryVersioningResult)
            assert result.member_result == mock_v2_member
            assert result.v1_id == 12345
            assert result.reason == "V2 validation successful"
            assert result.organization_id == "1"

            # Verify log_result_usage was called
            mock_log.assert_called_once_with(
                False,
                "V2 validation successful",
                EligibilityMethod.BASIC,
                v1_result,
                "1",
            )

    @pytest.mark.asyncio
    async def test_execute_eligibility_queries_v2_failure(self, executor):
        """Test _execute_eligibility_queries when V2 should be used but fails."""
        # Mock V1 query execution
        mock_member = MagicMock(spec=model.MemberVersioned)
        mock_member.id = 12345
        mock_member.organization_id = 1

        # Create V1 result
        v1_result = QueryResult(
            result=mock_member, query_name="test_query", query_type="test", error=None
        )

        # Mock the necessary methods
        with patch.object(
            executor, "_try_v1_queries", return_value=(v1_result, 12345, "1")
        ), patch.object(
            executor, "_determine_query_version", return_value=(False, None)
        ), patch.object(
            executor, "_try_v2_queries", return_value=None
        ), patch.object(
            executor, "_log_result_usage"
        ) as mock_log, patch(
            "app.eligibility.query_framework.executor.logger"
        ) as mock_logger:
            # Call the method
            result = await executor._execute_eligibility_queries(
                method=EligibilityMethod.BASIC,
                params={"first_name": "John", "last_name": "Doe"},
            )

            # Verify the result
            assert isinstance(result, QueryVersioningResult)
            assert result.member_result == mock_member
            assert result.v1_id == 12345
            assert result.reason == "V2 query failed or validation failed"
            assert result.organization_id == "1"

            # Verify log_result_usage was called
            mock_log.assert_called_once_with(
                True,
                "V2 query failed or validation failed",
                EligibilityMethod.BASIC,
                v1_result,
                "1",
            )

            # Verify warning was logged for V2 failure
            mock_logger.warning.assert_called_once_with(
                "V2 query failed, falling back to V1 results",
                extra={
                    "method": EligibilityMethod.BASIC.value,
                    "organization_id": "1",
                    "v1_id": 12345,
                },
            )

    # Test for perform_eligibility_check with the refactored code
    @pytest.mark.asyncio
    @patch("app.eligibility.query_framework.executor.QueryRegistry")
    async def test_perform_eligibility_check_with_query_versioning_result(
        self, mock_registry, executor
    ):
        """Test perform_eligibility_check with the new QueryVersioningResult return value."""
        # Mock query definition
        mock_query = MagicMock()
        mock_registry.get_v1_queries.return_value = [mock_query]

        # Mock member
        mock_member = MagicMock(spec=model.MemberVersioned)
        mock_member.id = 12345
        mock_member.organization_id = 1

        # Create a QueryVersioningResult
        versioning_result = QueryVersioningResult(
            member_result=mock_member,
            v1_id=12345,
            reason="test reason",
            organization_id="1",
        )

        # Mock the methods
        with patch.object(
            executor, "_execute_eligibility_queries", return_value=versioning_result
        ), patch.object(executor, "_filter_active_records", return_value=mock_member):
            # Call the method
            result = await executor.perform_eligibility_check(
                method=EligibilityMethod.BASIC,
                params={"first_name": "John", "last_name": "Doe"},
                expected_type=SingleRecordType,
            )

            # Verify the result
            assert isinstance(result, EligibilityResult)
            assert result.records == mock_member
            assert result.v1_id == 12345

            # Verify the correct method calls
            executor._execute_eligibility_queries.assert_called_once_with(
                EligibilityMethod.BASIC, {"first_name": "John", "last_name": "Doe"}
            )
            executor._filter_active_records.assert_called_once_with(
                mock_member, EligibilityMethod.BASIC, SingleRecordType
            )

    # Integration test that checks the entire flow
    @pytest.mark.asyncio
    @patch("app.eligibility.query_framework.executor.QueryRegistry")
    async def test_full_eligibility_flow_v2_fallback_with_warning(
        self, mock_registry, executor
    ):
        """Test the full eligibility flow with V2 fallback and warning."""
        # Mock queries
        mock_v1_query = MagicMock()
        mock_v1_query.method_name = "get_all_by_name_and_date_of_birth"
        mock_v1_query.validate_params.return_value.is_valid = True
        mock_v1_query.filter_params.return_value = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

        mock_v2_query = MagicMock()
        mock_v2_query.method_name = "get_all_by_name_and_date_of_birth_v2"
        mock_v2_query.validate_params.return_value.is_valid = True
        mock_v2_query.filter_params.return_value = {
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": "1990-01-01",
        }

        mock_registry.get_v1_queries.return_value = [mock_v1_query]
        mock_registry.get_v2_queries.return_value = [mock_v2_query]

        # Mock member
        mock_v1_member = MagicMock(spec=model.MemberVersioned)
        mock_v1_member.id = 12345
        mock_v1_member.organization_id = 1
        mock_v1_member.first_name = "John"
        mock_v1_member.last_name = "Doe"
        mock_v1_member.date_of_birth = "1990-01-01"
        mock_v1_member.unique_corp_id = "abc123"
        mock_v1_member.dependent_id = None

        # Repository setup
        executor.repository.get_all_by_name_and_date_of_birth.return_value = [
            mock_v1_member
        ]
        executor.repository.get_all_by_name_and_date_of_birth_v2.side_effect = (
            ValueError("Test error")
        )

        # Mock logger
        with patch(
            "app.eligibility.query_framework.executor.logger"
        ) as mock_logger, patch.object(
            executor, "_is_v2_enabled", return_value=True
        ), patch.object(
            executor, "_filter_active_records", return_value=[mock_v1_member]
        ):
            # Call the method
            result = await executor.perform_eligibility_check(
                method=EligibilityMethod.BASIC,
                params={
                    "first_name": "John",
                    "last_name": "Doe",
                    "date_of_birth": "1990-01-01",
                },
                expected_type=MultipleRecordType,
            )

            # Verify the result
            assert isinstance(result, EligibilityResult)
            assert result.records == [mock_v1_member]
            assert result.v1_id == 12345

            # Verify warning was logged for V2 failure
            mock_logger.warning.assert_any_call(
                "V2 query failed, falling back to V1 results",
                extra={
                    "method": EligibilityMethod.BASIC.value,
                    "organization_id": "1",
                    "v1_id": 12345,
                },
            )

    # Test for the execution_time_ms parameter in various log calls
    @pytest.mark.asyncio
    @patch("app.eligibility.query_framework.executor.time")
    async def test_execute_single_query_execution_time(self, mock_time, executor):
        """Test that execution_time_ms is correctly calculated and logged."""
        # Mock the time
        start_time = 1000.0
        end_time = 1001.5  # 1.5 seconds later
        mock_time.time.side_effect = [start_time, end_time]

        # Create a mock query
        mock_query = MagicMock()
        mock_query.method_name = "get_by_dob_and_email"
        mock_query.query_type = "dob_email"

        # Create a mock member
        mock_member = MagicMock(spec=model.MemberVersioned)
        mock_member.id = 12345
        mock_member.organization_id = 1

        # Mock repository method
        executor.repository.get_by_dob_and_email = AsyncMock(return_value=mock_member)

        # Mock logger
        with patch("app.eligibility.query_framework.executor.logger") as mock_logger:
            # Call the method
            await executor._execute_single_query(
                query=mock_query,
                params={"date_of_birth": "1990-01-01", "email": "test@example.com"},
                method=EligibilityMethod.BASIC,
            )

            # Verify execution_time_ms in log calls
            for call_args in mock_logger.info.call_args_list:
                if "Query successful" in call_args[0][0]:
                    assert call_args[1]["extra"]["execution_time_ms"] == 1500

    # Test for _try_v1_queries with no queries configured
    @pytest.mark.asyncio
    async def test_try_v1_queries_no_queries_configured(self, executor):
        """Test _try_v1_queries when no queries are configured."""
        # Mock the registry to return empty list
        with patch(
            "app.eligibility.query_framework.executor.QueryRegistry"
        ) as mock_registry:
            mock_registry.get_v1_queries.return_value = []

            # Call the method and expect ValidationError
            with pytest.raises(errors.ValidationError) as exc_info:
                await executor._try_v1_queries(
                    method=EligibilityMethod.BASIC,
                    params={"first_name": "John", "last_name": "Doe"},
                )

            # Verify the error message
            assert f"No queries configured for method {EligibilityMethod.BASIC}" in str(
                exc_info.value
            )


class TestUnsupportedReturnTypeError:
    """Tests specifically for the UnsupportedReturnTypeError class."""

    def test_unsupported_return_type_error_default_message(self):
        """Test UnsupportedReturnTypeError with default error message."""

        # Create an instance with the default message
        error = UnsupportedReturnTypeError(method=EligibilityMethod.BASIC)

        # Verify properties
        assert error.method == EligibilityMethod.BASIC
        assert f"Unsupported return type for {EligibilityMethod.BASIC.value}" in str(
            error
        )

    def test_unsupported_return_type_error_custom_message(self):
        """Test UnsupportedReturnTypeError with a custom message."""

        # Create an instance with a custom message
        custom_message = "This is a custom error message"
        error = UnsupportedReturnTypeError(
            method=EligibilityMethod.EMPLOYER, message=custom_message
        )

        # Verify properties
        assert error.method == EligibilityMethod.EMPLOYER
        assert str(error) == custom_message

    def test_unsupported_return_type_error_inheritance(self):
        """Test that UnsupportedReturnTypeError properly inherits from MatchError."""
        from app.eligibility.constants import EligibilityMethod

        # Create an instance
        error = UnsupportedReturnTypeError(method=EligibilityMethod.HEALTH_PLAN)

        # Verify inheritance
        assert isinstance(error, UnsupportedReturnTypeError)
        assert isinstance(error, MatchError)
