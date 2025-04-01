"""
Query execution and result handling for eligibility checks.
"""

from app.eligibility.query_framework.error_handler import eligibility_error_handler
from app.eligibility.query_framework.errors import (
    InactiveOrganizationError,
    MatchMultipleError,
    MemberSearchError,
    UnsupportedReturnTypeError,
    ValidationError,
)
from app.eligibility.query_framework.executor import EligibilityQueryExecutor
from app.eligibility.query_framework.registry import (
    QueryDefinition,
    QueryRegistry,
    ValidationResult,
)
from app.eligibility.query_framework.repository import EligibilityQueryRepository
from app.eligibility.query_framework.result import EligibilityResult, QueryResult
from app.eligibility.query_framework.types import (
    Member2Result,
    MemberResponseType,
    MemberResult,
    MemberResultType,
    MemberType,
    MemberVersionedResult,
)

__all__ = [
    "EligibilityQueryExecutor",
    "QueryDefinition",
    "QueryRegistry",
    "ValidationResult",
    "QueryResult",
    "MemberType",
    "MemberResult",
    "MemberResultType",
    "MemberResponseType",
    "MemberVersionedResult",
    "Member2Result",
    "MemberSearchError",
    "InactiveOrganizationError",
    "UnsupportedReturnTypeError",
    "MatchMultipleError",
    "ValidationError",
    "QueryDefinition",
    "QueryRegistry",
    "ValidationResult",
    "EligibilityQueryRepository",
    "eligibility_error_handler",
    "EligibilityResult",
]
