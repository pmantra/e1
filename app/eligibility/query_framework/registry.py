from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from mmlib.ops import log

from app.eligibility import convert
from app.eligibility.constants import EligibilityMethod
from app.eligibility.query_framework.repository import EligibilityQueryRepository

logger = log.getLogger(__name__)


@dataclass
class QueryDefinition:
    """
    Definition of a query with its method name and parameter requirements.

    Attributes:
        method_name: Name of the method to execute
        required_params: Set of required parameters (auto-detected if None)
        optional_params: Set of optional parameters (auto-detected if None)
    """

    method_name: str
    required_params: Optional[Set[str]] = None
    optional_params: Optional[Set[str]] = None

    def __post_init__(self):
        """Auto-detect required and optional parameters if not provided."""
        if self.required_params is None or self.optional_params is None:
            # Get repository class to inspect
            repo_class = EligibilityQueryRepository

            # Get method reference
            if not hasattr(repo_class, self.method_name):
                raise ValueError(f"Method '{self.method_name}' not found in repository")

            method = getattr(repo_class, self.method_name)
            sig = inspect.signature(method)

            # Extract parameters
            required = set()
            optional = set()

            for name, param in sig.parameters.items():
                if name == "self":
                    continue

                if param.default == param.empty and param.kind == param.KEYWORD_ONLY:
                    required.add(name)
                elif param.kind == param.KEYWORD_ONLY:
                    optional.add(name)

            # Set detected parameters
            if self.required_params is None:
                self.required_params = required

            if self.optional_params is None:
                self.optional_params = optional

    @property
    def query_type(self) -> str:
        """Get normalized query type from method name."""
        name = self.method_name
        # Remove prefixes
        for prefix in ["get_", "get_by_", "get_all_by_"]:
            if name.startswith(prefix):
                name = name[len(prefix) :]
                break
        # Remove version suffix
        return name[:-3] if name.endswith("_v2") else name

    def validate_params(self, params: Dict[str, Any]) -> ValidationResult:
        """Validate that all required parameters are present and valid."""
        missing_params = set()
        for param in self.required_params:
            if (
                param not in params
                or params[param] is None
                or (isinstance(params[param], str) and not params[param].strip())
                # Validate date format without converting
                or (
                    (
                        param.endswith("_date")
                        or param == "date_of_birth"
                        or param.endswith("_date_of_birth")
                    )
                    and convert.to_date(params[param]) is convert.DATE_UNKNOWN
                )
            ):
                missing_params.add(param)

        return ValidationResult(
            is_valid=len(missing_params) == 0, missing_params=missing_params
        )

    def filter_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and transform parameters as needed. Assumes required params are valid."""
        from app.eligibility import convert
        from app.eligibility.service import ValidationError

        valid_params = self.required_params | self.optional_params
        filtered_params = {}

        for param in valid_params:
            # Skip empty optional params (required params already validated)
            if param not in params or params[param] is None:
                continue

            value = params[param]
            # Convert date parameters
            if (
                param.endswith("_date")
                or param == "date_of_birth"
                or param.endswith("_date_of_birth")
            ):
                try:
                    value = convert.to_date(value)
                    # No need to skip DATE_UNKNOWN for required params - already validated
                    if (
                        value is convert.DATE_UNKNOWN
                        and param not in self.required_params
                    ):
                        continue
                except ValidationError:
                    if param not in self.required_params:
                        continue
                    else:
                        # For required params, use the original value
                        # (validation already passed, so this should be rare)
                        logger.warning(
                            "Required param validation inconsistency detected!",
                            extra={"param": param},
                        )

            filtered_params[param] = value
        return filtered_params


@dataclass
class ValidationResult:
    """Result of parameter validation."""

    is_valid: bool
    missing_params: Set[str] = field(default_factory=set)


class QueryRegistry:
    """
    Centralized registry of all eligibility queries.
    Maintains V1 and V2 query definitions separately to preserve execution order.
    """

    # Basic eligibility queries
    BASIC_V1_QUERIES = [
        QueryDefinition(method_name="get_all_by_name_and_date_of_birth"),
    ]

    BASIC_V2_QUERIES = [
        QueryDefinition(method_name="get_all_by_name_and_date_of_birth_v2"),
    ]

    # Employer eligibility queries
    EMPLOYER_V1_QUERIES = [
        QueryDefinition(method_name="get_by_dob_and_email"),
        QueryDefinition(method_name="get_by_dependent_dob_and_email"),
        QueryDefinition(method_name="get_by_dob_name_and_work_state"),
        QueryDefinition(method_name="get_by_email_and_name"),
        QueryDefinition(method_name="get_by_email_and_employee_name"),
        QueryDefinition(method_name="get_all_by_name_and_date_of_birth"),
    ]

    EMPLOYER_V2_QUERIES = [
        QueryDefinition(method_name="get_by_dob_and_email_v2"),
        QueryDefinition(method_name="get_by_dependent_dob_and_email_v2"),
        QueryDefinition(method_name="get_by_dob_name_and_work_state_v2"),
        QueryDefinition(method_name="get_by_email_and_name_v2"),
        QueryDefinition(method_name="get_by_email_and_employee_name_v2"),
        QueryDefinition(method_name="get_all_by_name_and_date_of_birth_v2"),
    ]

    # Health plan eligibility queries
    HEALTH_PLAN_V1_QUERIES = [
        QueryDefinition(method_name="get_by_name_and_unique_corp_id"),
        QueryDefinition(method_name="get_by_employee_name_and_unique_corp_id"),
        QueryDefinition(method_name="get_by_date_of_birth_and_unique_corp_id"),
        QueryDefinition(
            method_name="get_by_dependent_date_of_birth_and_unique_corp_id"
        ),
        QueryDefinition(method_name="get_all_by_name_and_date_of_birth"),
        QueryDefinition(method_name="get_all_by_employee_name_and_date_of_birth"),
    ]

    HEALTH_PLAN_V2_QUERIES = [
        QueryDefinition(method_name="get_by_name_and_unique_corp_id_v2"),
        QueryDefinition(method_name="get_by_employee_name_and_unique_corp_id_v2"),
        QueryDefinition(method_name="get_by_date_of_birth_and_unique_corp_id_v2"),
        QueryDefinition(
            method_name="get_by_dependent_date_of_birth_and_unique_corp_id_v2"
        ),
        QueryDefinition(method_name="get_all_by_name_and_date_of_birth_v2"),
        QueryDefinition(method_name="get_all_by_employee_name_and_date_of_birth_v2"),
    ]

    @classmethod
    def get_v1_queries(self, method: EligibilityMethod) -> List[QueryDefinition]:
        """Get V1 query definitions for given eligibility method."""
        if method == EligibilityMethod.BASIC:
            return self.BASIC_V1_QUERIES
        elif method == EligibilityMethod.EMPLOYER:
            return self.EMPLOYER_V1_QUERIES
        elif method == EligibilityMethod.HEALTH_PLAN:
            return self.HEALTH_PLAN_V1_QUERIES
        else:
            raise ValueError(f"Unknown eligibility method: {method}")

    @classmethod
    def get_v2_queries(self, method: EligibilityMethod) -> List[QueryDefinition]:
        """Get V2 query definitions for given eligibility method."""
        if method == EligibilityMethod.BASIC:
            return self.BASIC_V2_QUERIES
        elif method == EligibilityMethod.EMPLOYER:
            return self.EMPLOYER_V2_QUERIES
        elif method == EligibilityMethod.HEALTH_PLAN:
            return self.HEALTH_PLAN_V2_QUERIES
        else:
            raise ValueError(f"Unknown eligibility method: {method}")
