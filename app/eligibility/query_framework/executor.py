from __future__ import annotations

import time
from asyncio import gather
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Type, Union

from mmlib.ops import log

from app.eligibility.constants import EligibilityMethod
from app.eligibility.query_framework import errors
from app.eligibility.query_framework.registry import QueryDefinition, QueryRegistry
from app.eligibility.query_framework.result import EligibilityResult, QueryResult
from app.eligibility.query_framework.types import MemberResult, MemberResultType
from app.utils import eligibility_validation, feature_flag
from db import model
from db.clients import configuration_client

logger = log.getLogger(__name__)


@dataclass
class QueryVersioningResult:
    """Result of selecting between V1/V2 eligibility query versions."""

    member_result: MemberResult
    v1_id: int
    reason: str
    organization_id: str


class EligibilityQueryExecutor:
    """Handles execution of eligibility queries using the query registry."""

    def __init__(self, query_repository: Any):
        self.repository = query_repository
        self.configurations = configuration_client.Configurations()

    async def perform_eligibility_check(
        self,
        method: EligibilityMethod,
        params: Dict[str, Any],
        expected_type: MemberResultType,
    ) -> EligibilityResult:
        """
        Performs eligibility check by running appropriate queries.

        Executes V1 queries first, then attempts V2 if applicable.
        Filters results to include only active organizations.

        Returns:
            EligibilityCheckResult with filtered records and V1 reference ID

        Raises:
            ValidationError: If no valid queries can be executed
            MemberSearchError: If no matching records are found
            InactiveOrganizationError: If organization is inactive
        """
        # Execute queries with V1/V2 strategy
        versioning_result = await self._execute_eligibility_queries(method, params)

        # Filter and return results
        filtered_records = await self._filter_active_records(
            versioning_result.member_result, method, expected_type
        )

        if not filtered_records:
            raise errors.MemberSearchError(
                "No member records found matching the provided criteria.", method=method
            )

        return EligibilityResult(
            records=filtered_records, v1_id=versioning_result.v1_id
        )

    async def _execute_eligibility_queries(
        self, method: EligibilityMethod, params: Dict[str, Any]
    ) -> QueryVersioningResult:
        """
        Execute eligibility queries with V1/V2 strategy.
        Returns a QueryVersioningResult with the result to use and metadata.
        """
        # Execute V1 queries first
        v1_result, v1_id, org_id = await self._try_v1_queries(method, params)

        # Determine if we should try V2 queries
        use_v1_result, reason = self._determine_query_version(v1_result, org_id, method)
        result_to_use = v1_result.result  # Default to V1

        # Try V2 if needed
        if not use_v1_result:
            v2_result = await self._try_v2_queries(
                v1_result.first_result, method, params
            )
            if v2_result:
                result_to_use = v2_result
                reason = "V2 validation successful"
            else:
                reason = "V2 query failed or validation failed"
                use_v1_result = True  # Fall back to V1
                logger.warning(
                    "V2 query failed, falling back to V1 results",
                    extra={
                        "method": method.value,
                        "organization_id": org_id,
                        "v1_id": v1_id,
                    },
                )

        # Log which result we're using
        self._log_result_usage(use_v1_result, reason, method, v1_result, org_id)

        return QueryVersioningResult(
            member_result=result_to_use,
            v1_id=v1_id,
            reason=reason,
            organization_id=org_id,
        )

    # No longer needed - using primitive types

    async def _try_v1_queries(
        self, method: EligibilityMethod, params: Dict[str, Any]
    ) -> Tuple[QueryResult, int, str]:
        """Execute V1 queries and return result, record ID, and organization ID."""
        v1_queries = QueryRegistry.get_v1_queries(method)
        if not v1_queries:
            raise errors.ValidationError(f"No queries configured for method {method}")

        v1_result = await self._execute_queries(
            queries=v1_queries, method=method, params=params
        )
        if v1_result.error:
            error_msg = f"No matching records found for {method.value}"
            error_msg += f" (details: {v1_result.error})"
            raise errors.MemberSearchError(error_msg, method=method)

        if not v1_result.is_success or v1_result.first_result is None:
            raise errors.MemberSearchError(
                f"No valid results found for {method.value}", method=method
            )

        v1_first = v1_result.first_result
        v1_id = v1_first.id
        org_id = str(v1_first.organization_id)

        return v1_result, v1_id, org_id

    async def _try_v2_queries(
        self, v1_record, method: EligibilityMethod, params: Dict[str, Any]
    ) -> Optional[MemberResult]:
        """Attempt V2 queries and return result if valid, None otherwise."""
        v2_queries = QueryRegistry.get_v2_queries(method)
        v2_result = await self._execute_queries(
            queries=v2_queries, method=method, params=params
        )

        if v2_result.is_success and self._validate_results(
            v1_record, v2_result.first_result
        ):
            logger.info(
                "Using V2 results",
                extra={"v1_id": v1_record.id, "v2_id": v2_result.first_result.id},
            )
            return v2_result.result
        return None

    def _determine_query_version(
        self, v1_result: QueryResult, org_id: str, method: EligibilityMethod
    ) -> Tuple[bool, Optional[str]]:
        """
        Determine if V1 results should be used or V2 should be attempted.

        Args:
            v1_result: Result from V1 query execution
            org_id: Organization ID to check for V2 eligibility
            method: Eligibility method being used

        Returns:
            Tuple of (use_v1_result: bool, reason: str) indicating whether to use V1 results
            and the reason for the decision
        """
        if v1_result.is_multiple_results:
            return True, "multiple V1 records (overeligibility)"
        elif not self._is_v2_enabled(org_id):
            return True, "organization not V2 enabled"
        else:
            logger.info(
                "Running V2 queries for V2-enabled organization",
                extra={"organization_id": org_id, "method": method.value},
            )
            return False, None

    def _log_result_usage(self, use_v1_results, reason, method, v1_result, org_id):
        """Log which query result is being used and why."""
        if reason:
            version = "V1" if use_v1_results else "V2"
            logger.info(
                f"Using {version} results: {reason}",
                extra={
                    "method": method.value,
                    "record_count": len(v1_result.result)
                    if isinstance(v1_result.result, list)
                    else 1,
                    "organization_id": org_id,
                },
            )

    async def _execute_queries(
        self,
        queries: List[QueryDefinition],
        method: EligibilityMethod,
        params: Dict[str, Any],
    ) -> QueryResult:
        """Execute queries with improved error handling."""
        executable_queries = self._prepare_queries(queries, params, method)

        if not executable_queries:
            missing_params_str = "No valid queries available due to missing parameters"

            return QueryResult(
                result=None,
                query_name="no_executable_queries",
                query_type="validation_error",
                error=missing_params_str,
            )

        run_in_parallel = self._get_run_in_parallel_flag()

        if run_in_parallel:
            return await self._execute_in_parallel(executable_queries, method, params)
        else:
            return await self._execute_sequentially(executable_queries, method, params)

    def _prepare_queries(
        self,
        queries: List[QueryDefinition],
        params: Dict[str, Any],
        method: EligibilityMethod,
    ) -> List[Tuple[QueryDefinition, Dict[str, Any]]]:
        """Prepare and validate queries for execution."""
        executable_queries = []
        validation_failures = {}

        for query in queries:
            validation = query.validate_params(params)
            if validation.is_valid:
                filtered_params = query.filter_params(params)
                executable_queries.append((query, filtered_params))
            else:
                validation_failures[query.method_name] = validation.missing_params
                logger.warning(
                    "Query validation failed",
                    extra={
                        "method": method.value,
                        "query": query.method_name,
                        "query_type": query.query_type,
                        "missing_params": sorted(validation.missing_params),
                        "user_id": params.get("user_id"),
                    },
                )

        if not executable_queries:
            # Format error details
            error_details = []
            for query_name, missing_params in validation_failures.items():
                missing_params_str = ", ".join(sorted(missing_params))
                error_details.append(
                    f"Query '{query_name}' missing parameters: {missing_params_str}"
                )

            raise errors.ValidationError(" ".join(error_details))

        return executable_queries

    async def _execute_in_parallel(
        self,
        executable_queries: List[Tuple[QueryDefinition, Dict[str, Any]]],
        method: EligibilityMethod,
        params: Dict[str, Any],
    ) -> QueryResult:
        """Execute queries in parallel."""
        tasks = [
            self._execute_single_query(query, query_params, method)
            for query, query_params in executable_queries
        ]

        results = await gather(*tasks)

        # Return first successful result
        for result in results:
            if result.is_success:
                return result

        # If no successful results, return the last result
        return (
            results[-1]
            if results
            else QueryResult(
                result=None,
                query_name="all_queries_failed",
                query_type="execution_error",
                error="All queries failed to execute",
            )
        )

    async def _execute_sequentially(
        self,
        executable_queries: List[Tuple[QueryDefinition, Dict[str, Any]]],
        method: EligibilityMethod,
        params: Dict[str, Any],
    ) -> QueryResult:
        """Execute queries sequentially until one succeeds."""
        for query, query_params in executable_queries:
            result = await self._execute_single_query(query, query_params, method)

            if result.is_success:
                return result

        # If all queries failed, return empty result
        return QueryResult(
            result=None,
            query_name="all_queries_failed",
            query_type="execution_error",
            error="All queries failed to execute",
        )

    async def _execute_single_query(
        self,
        query: QueryDefinition,
        params: Dict[str, Any],
        method: EligibilityMethod,
    ) -> QueryResult:
        """
        Execute a single query with standardized logging and metrics.
        """
        start_time = time.time()
        method_name = query.method_name

        # safe parameter filtering to redact sensitive fields
        safe_params = {
            k: (
                v
                if k
                not in [
                    "first_name",
                    "last_name",
                    "employee_first_name",
                    "employee_last_name",
                    "email",
                    "date_of_birth",
                    "dependent_date_of_birth",
                    "unique_corp_id",
                ]
                else "***"
            )
            for k, v in params.items()
        }

        logger.info(
            f"Executing query: {method_name}",
            extra={
                "user_id": params.get("user_id"),
                "method": method.value,
                "query_type": query.query_type,
                "parameters": safe_params,
            },
        )

        try:
            method_func = getattr(self.repository, method_name)
            result = await method_func(**params)
            execution_time = time.time() - start_time

            # Check if result is empty or None
            is_empty_list = isinstance(result, list) and not result
            result_count = (
                len(result) if isinstance(result, list) else (1 if result else 0)
            )

            # If there are no results, log a warning and return an empty result
            if not result or is_empty_list:
                logger.warning(
                    f"Query returned no results: {method_name}",
                    extra={
                        "execution_time_ms": int(execution_time * 1000),
                        "query_type": query.query_type,
                        "method": method.value,
                        "parameters": safe_params,
                        "is_empty_list": is_empty_list,
                        "result_is_none": result is None,
                    },
                )
                return QueryResult(
                    result=result,
                    query_name=method_name,
                    query_type=query.query_type,
                    error=None,
                )
            else:
                # We have a successful result, capture IDs and log success
                first_record = result[0] if isinstance(result, list) else result
                org_id = str(first_record.organization_id)
                v1_id = getattr(first_record, "id", None)

                logger.info(
                    f"Query successful: {method_name}",
                    extra={
                        "execution_time_ms": int(execution_time * 1000),
                        "result_count": result_count,
                        "query_type": query.query_type,
                        "organization_id": org_id,
                        "method": method.value,
                        "v1_id": v1_id,
                    },
                )

                return QueryResult(
                    result=result,
                    query_name=method_name,
                    query_type=query.query_type,
                    organization_id=org_id,
                    v_id=v1_id,
                    error=None,
                )

        except Exception as e:
            execution_time = time.time() - start_time
            logger.warning(
                f"Query {method_name} failed with error: {str(e)}",
                extra={
                    "execution_time_ms": int(execution_time * 1000),
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "query_type": query.query_type,
                    "method": method.value,
                    "parameters": safe_params,
                },
            )
            return QueryResult(
                result=None,
                query_name=method_name,
                query_type=query.query_type,
                error=str(e),
            )

    def _is_v2_enabled(self, organization_id: str) -> bool:
        """Check feature flag if V2 queries are enabled for the organization."""
        return feature_flag.organization_enabled_for_e9y_2_write(int(organization_id))

    def _validate_results(
        self,
        v1_record: model.MemberVersioned | model.Member2,
        v2_record: model.MemberVersioned | model.Member2,
    ) -> bool:
        """Validate that V1 and V2 results match on key identifiers."""
        try:
            field_comparisons = {
                "unique_corp_id": v1_record.unique_corp_id == v2_record.unique_corp_id,
                "organization_id": v1_record.organization_id
                == v2_record.organization_id,
                "first_name": v1_record.first_name == v2_record.first_name,
                "last_name": v1_record.last_name == v2_record.last_name,
                "date_of_birth": v1_record.date_of_birth == v2_record.date_of_birth,
                "dependent_id": v1_record.dependent_id == v2_record.dependent_id,
            }

            for field, matches in field_comparisons.items():
                if not matches:
                    logger.warning(
                        "V1/V2 field mismatch",
                        extra={
                            "field": field,
                            "v1_value": getattr(v1_record, field),
                            "v2_value": getattr(v2_record, field),
                        },
                    )

            return all(field_comparisons.values())
        except Exception as e:
            logger.error(
                "Validation failed due to missing attribute", extra={"error": str(e)}
            )
            return False

    @staticmethod
    def _get_run_in_parallel_flag() -> bool:
        """Get configuration for parallel execution."""
        # TODO: read value from launchdarkly feature flag
        return False

    async def _filter_active_records(
        self,
        member_records: MemberResult,
        method: EligibilityMethod,
        expected_type: Type,
    ) -> MemberResult:
        """Filter member records to include only active organizations."""
        from typing import get_args, get_origin

        def is_list_type(t):
            origin = get_origin(t)
            if origin is Union:
                # If it's a Union, check if any of its args are List types
                args = get_args(t)
                return any(get_origin(arg) is list for arg in args)
            return origin is list

        if not (
            get_origin(expected_type) is Union or expected_type in MemberResultType
        ):
            raise errors.UnsupportedReturnTypeError(method=method)

        if isinstance(member_records, list):
            # Check if the expected_type is a List type or Union containing List types
            if is_list_type(expected_type):
                active_records = await eligibility_validation.check_member_org_active_and_overeligibility(
                    configuration_client=self.configurations,
                    member_list=member_records,
                )
            else:
                active_records = (
                    await eligibility_validation.check_member_org_active_and_single_org(
                        configuration_client=self.configurations,
                        member_list=member_records,
                    )
                )
        else:
            active_records = await eligibility_validation.check_member_org_active(
                configuration_client=self.configurations,
                member=member_records,
            )

        if not active_records:
            raise errors.InactiveOrganizationError(method=method)

        return active_records
