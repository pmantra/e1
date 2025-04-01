from __future__ import annotations

import contextvars
from typing import List, Optional

from mmlib.ops import log

from app.eligibility.constants import EligibilityMethod
from app.eligibility.query_framework.error_handler import eligibility_error_handler
from app.eligibility.query_framework.executor import EligibilityQueryExecutor
from app.eligibility.query_framework.repository import EligibilityQueryRepository
from app.eligibility.query_framework.types import (
    MemberResponseType,
    MemberResult,
    MultipleRecordType,
    SingleRecordType,
)
from db import model

logger = log.getLogger(__name__)


class EligibilityQueryService:
    def __init__(self, query_executor: EligibilityQueryExecutor):
        self.query_executor = query_executor

    @eligibility_error_handler(method=EligibilityMethod.BASIC)
    async def check_basic_eligibility(
        self,
        *,
        date_of_birth: str,
        first_name: str,
        last_name: str,
        user_id: Optional[int] = None,
    ) -> List[model.MemberResponse]:
        """
        Check basic eligibility.

        Args:
            date_of_birth: Member's date of birth
            first_name: Member's first name
            last_name: Member's last name
            user_id: Optional user ID for logging

        Returns:
            List of MemberResponse objects matching the criteria

        Raises:
            ValidationError: If required parameters are missing or invalid
            MemberSearchError: If no matching records are found or other search error occurs
        """
        all_params = locals()
        params = {key: all_params[key] for key in all_params if key != "self"}

        query_result = await self.query_executor.perform_eligibility_check(
            method=EligibilityMethod.BASIC,
            params=params,
            expected_type=MultipleRecordType,
        )

        return self._convert_to_member_response(
            data=query_result.records,
            v_id=query_result.v1_id,
        )

    @eligibility_error_handler(method=EligibilityMethod.EMPLOYER)
    async def check_employer_eligibility(
        self,
        email: Optional[str] = None,
        date_of_birth: Optional[str] = None,
        dependent_date_of_birth: Optional[str] = None,
        employee_first_name: Optional[str] = None,
        employee_last_name: Optional[str] = None,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
        work_state: Optional[str] = None,
        user_id: Optional[int] = None,
    ) -> model.MemberResponse:
        """
        Check employer eligibility.

        Args:
            email: Member's email
            date_of_birth: Member's date of birth
            dependent_date_of_birth: Dependent's date of birth
            employee_first_name: Employee's first name
            employee_last_name: Employee's last
            first_name: Member's or Dependent's first name
            last_name: Member's or Dependent's last name
            work_state: Member's work state
            user_id: Optional user ID for logging

        Returns:
            A single MemberResponse object matching the criteria

        Raises:
            ValidationError: If required parameters are missing or invalid
            MemberSearchError: If no matching records are found or other search error occurs
        """

        all_params = locals()
        params = {key: all_params[key] for key in all_params if key != "self"}

        query_result = await self.query_executor.perform_eligibility_check(
            method=EligibilityMethod.EMPLOYER,
            params=params,
            expected_type=SingleRecordType,
        )

        return self._convert_to_member_response(
            data=query_result.records,
            v_id=query_result.v1_id,
        )

    @eligibility_error_handler(method=EligibilityMethod.HEALTH_PLAN)
    async def check_healthplan_eligibility(
        self,
        *,
        unique_corp_id: Optional[str] = None,
        date_of_birth: Optional[str] = None,
        dependent_date_of_birth: Optional[str] = None,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
        employee_first_name: Optional[str] = None,
        employee_last_name: Optional[str] = None,
        user_id: Optional[int] = None,
    ) -> model.MemberResponse:
        """
        Check health-plan eligibility.

        Args:
            unique_corp_id: Member's employer or subscriber ID
            date_of_birth: Member's date of birth
            dependent_date_of_birth: Dependent's date of birth
            employee_first_name: Employee's first name
            employee_last_name: Employee's last
            first_name: Member's or Dependent's first name
            last_name: Member's or Dependent's last name
            user_id: Optional user ID for logging

        Returns:
            A single MemberResponse object matching the criteria

        Raises:
            ValidationError: If required parameters are missing or invalid
            MemberSearchError: If no matching records are found or other search error occurs
        """

        all_params = locals()
        params = {key: all_params[key] for key in all_params if key != "self"}

        query_result = await self.query_executor.perform_eligibility_check(
            method=EligibilityMethod.HEALTH_PLAN,
            params=params,
            expected_type=SingleRecordType,
        )

        return self._convert_to_member_response(
            data=query_result.records,
            v_id=query_result.v1_id,
        )

    def _convert_to_member_response(
        self,
        data: MemberResult,
        v_id: int,
    ) -> MemberResponseType:
        """
        Converts MemberVersioned or Member2 records to MemberResponse records.
        Args:
            data: Single MemberVersioned or Member2 record or list of records
        Returns:
            Single MemberResponse or list of MemberResponse records
        """
        if isinstance(data, list):
            return [self._convert_single_member(member, v_id) for member in data]
        return self._convert_single_member(data, v_id)

    @staticmethod
    def _convert_single_member(
        member: SingleRecordType,
        v_id: int,
    ) -> model.MemberResponse:
        """Converts a single MemberVersioned record to MemberResponse."""
        is_v2 = isinstance(member, model.Member2)
        version = 0 if not isinstance(member, model.Member2) else member.version
        file_id = member.file_id if not isinstance(member, model.Member2) else 0
        member_1_id = v_id
        member_2_id = member.id if isinstance(member, model.Member2) else None

        try:
            return model.MemberResponse(
                id=member.id,
                version=version,
                organization_id=member.organization_id,
                first_name=member.first_name,
                last_name=member.last_name,
                date_of_birth=member.date_of_birth,
                file_id=file_id,
                work_state=member.work_state,
                work_country=member.work_country,
                email=member.email,
                unique_corp_id=member.unique_corp_id,
                employer_assigned_id=member.employer_assigned_id,
                dependent_id=member.dependent_id,
                effective_range=member.effective_range,
                record=member.record,
                custom_attributes=member.custom_attributes,
                do_not_contact=member.do_not_contact,
                gender_code=member.gender_code,
                created_at=member.created_at,
                updated_at=member.updated_at,
                is_v2=is_v2,
                member_1_id=member_1_id,
                member_2_id=member_2_id,
            )
        except Exception as e:
            logger.error(
                "Error converting member to response",
                extra={
                    "error": str(e),
                    "member_id": getattr(member, "id", None),
                    "error_type": type(e).__name__,
                },
            )
            raise ValueError(f"Failed to convert member to response: {str(e)}")


_QUERY_SERVICE: contextvars.ContextVar[
    Optional[EligibilityQueryService]
] = contextvars.ContextVar("e9y_query_service", default=None)


def query_service() -> EligibilityQueryService:
    """Provides a singleton instance of EligibilityQueryService."""
    if (svc := _QUERY_SERVICE.get()) is None:
        query_repository = EligibilityQueryRepository()
        query_executor = EligibilityQueryExecutor(query_repository)
        svc = EligibilityQueryService(query_executor)
        _QUERY_SERVICE.set(svc)
    return svc
