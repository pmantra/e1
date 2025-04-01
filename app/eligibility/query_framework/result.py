from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from mmlib.ops import log

from app.eligibility.constants import EligibilityMethod
from app.eligibility.query_framework.types import MemberResult, MemberType

logger = log.getLogger(__name__)


@dataclass
class QueryResult:
    """Result of a query execution with enhanced error handling and logging."""

    result: Optional[MemberResult]
    query_name: str
    query_type: Optional[str] = None
    v_id: Optional[int] = None
    organization_id: Optional[str] = None
    error: Optional[str] = None

    @property
    def is_success(self) -> bool:
        """Check if query execution was successful."""
        if self.error is not None:
            return False

        if self.result is None:
            return False

        if isinstance(self.result, list) and not self.result:
            return False

        return True

    @property
    def is_multiple_results(self) -> bool:
        """Check if query returned multiple results (overeligibility)."""
        return isinstance(self.result, list) and len(self.result) > 1

    @property
    def first_result(self) -> Optional[MemberType]:
        """Get first result safely, handling both single and list results."""
        if not self.result:
            return None
        return self.result[0] if isinstance(self.result, list) else self.result

    def log_execution(self, method: EligibilityMethod, params: Dict[str, Any]) -> None:
        """Log query execution with consistent format."""
        if self.is_success:
            logger.info(
                "Query succeeded",
                extra={
                    "query": self.query_name,
                    "query_type": self.query_type,
                    "method": method.value,
                    "user_id": params.get("user_id"),
                    "organization_id": self.organization_id,
                },
            )
        else:
            failure_reason = self._get_failure_reason()
            logger.warning(
                "Query failed",
                extra={
                    "query": self.query_name,
                    "query_type": self.query_type,
                    "error": self.error,
                    "failure_reason": failure_reason,
                    "result_type": type(self.result).__name__
                    if self.result is not None
                    else None,
                    "user_id": params.get("user_id"),
                    "method": method.value,
                },
            )

    def _get_failure_reason(self) -> str:
        """Determine detailed failure reason for logging."""
        if self.error:
            return f"Error: {self.error}"
        elif self.result is None:
            return "Result is None"
        elif isinstance(self.result, list) and not self.result:
            return "Empty result list"
        else:
            return f"Unexpected state: result={self.result}, error={self.error}"


@dataclass
class EligibilityResult:
    """Final result of eligibility check containing records and reference ID."""

    records: MemberResult
    v1_id: int

    @property
    def first_record(self) -> Optional[MemberType]:
        """Get first record safely."""
        if not self.records:
            return None
        return self.records[0] if isinstance(self.records, list) else self.records
