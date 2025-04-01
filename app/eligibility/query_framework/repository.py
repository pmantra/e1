from __future__ import annotations

import datetime
from typing import List

from mmlib.ops import log

from db import model
from db.clients import member_2_client, member_versioned_client

logger = log.getLogger(__name__)


class EligibilityQueryRepository:
    def __init__(self):
        self.members_versioned = member_versioned_client.MembersVersioned()
        self.members_2 = member_2_client.Member2Client()

    async def get_all_by_name_and_date_of_birth(
        self,
        *,
        date_of_birth: datetime.date | str,
        first_name: str,
        last_name: str,
    ) -> List[model.MemberVersioned]:
        return await self.members_versioned.get_all_by_name_and_date_of_birth(
            date_of_birth=date_of_birth, first_name=first_name, last_name=last_name
        )

    async def get_all_by_employee_name_and_date_of_birth(
        self,
        *,
        date_of_birth: datetime.date | str,
        employee_first_name: str,
        employee_last_name: str,
    ) -> List[model.MemberVersioned]:
        return await self.members_versioned.get_all_by_name_and_date_of_birth(
            date_of_birth=date_of_birth,
            first_name=employee_first_name,
            last_name=employee_last_name,
        )

    async def get_by_name_and_unique_corp_id(
        self,
        *,
        first_name: str,
        last_name: str,
        unique_corp_id: str,
    ) -> model.MemberVersioned:
        return await self.members_versioned.get_by_name_and_unique_corp_id(
            first_name=first_name, last_name=last_name, unique_corp_id=unique_corp_id
        )

    async def get_by_employee_name_and_unique_corp_id(
        self,
        *,
        employee_first_name: str,
        employee_last_name: str,
        unique_corp_id: str,
        user_id: int,
    ) -> model.MemberVersioned:
        logger.info(
            "Executing query: get_by_employee_name_and_unique_corp_id",
            extra={
                "user_id": user_id,
            },
        )
        return await self.members_versioned.get_by_name_and_unique_corp_id(
            first_name=employee_first_name,
            last_name=employee_last_name,
            unique_corp_id=unique_corp_id,
        )

    async def get_by_date_of_birth_and_unique_corp_id(
        self,
        *,
        date_of_birth: datetime.date | str,
        unique_corp_id: str,
    ) -> model.MemberVersioned:
        return await self.members_versioned.get_by_date_of_birth_and_unique_corp_id(
            date_of_birth=date_of_birth, unique_corp_id=unique_corp_id
        )

    async def get_by_dependent_date_of_birth_and_unique_corp_id(
        self,
        *,
        dependent_date_of_birth: datetime.date | str,
        unique_corp_id: str,
    ) -> model.MemberVersioned:
        return await self.members_versioned.get_by_date_of_birth_and_unique_corp_id(
            date_of_birth=dependent_date_of_birth, unique_corp_id=unique_corp_id
        )

    async def get_by_dob_and_email(
        self,
        *,
        date_of_birth: datetime.date,
        email: str,
    ) -> List[model.MemberVersioned]:
        return await self.members_versioned.get_by_dob_and_email(
            date_of_birth=date_of_birth, email=email
        )

    async def get_by_dependent_dob_and_email(
        self,
        *,
        dependent_date_of_birth: datetime.date,
        email: str,
    ) -> List[model.MemberVersioned]:
        """
        A wrapper function for get_by_dob_and_email. Explicit indicate dependent_date_of_birth
        """
        return await self.members_versioned.get_by_dob_and_email(
            date_of_birth=dependent_date_of_birth, email=email
        )

    async def get_by_dob_name_and_work_state(
        self,
        *,
        date_of_birth: datetime.date,
        first_name: str,
        last_name: str,
        work_state: str,
    ) -> List[model.MemberVersioned]:
        return await self.members_versioned.get_by_dob_name_and_work_state(
            date_of_birth=date_of_birth,
            first_name=first_name,
            last_name=last_name,
            work_state=work_state,
        )

    async def get_by_email_and_name(
        self,
        *,
        email: str,
        first_name: str,
        last_name: str,
    ) -> List[model.MemberVersioned]:
        return await self.members_versioned.get_by_email_and_name(
            email=email, first_name=first_name, last_name=last_name
        )

    async def get_by_email_and_employee_name(
        self,
        *,
        email: str,
        employee_first_name: str,
        employee_last_name: str,
    ) -> List[model.MemberVersioned]:
        return await self.members_versioned.get_by_email_and_name(
            email=email, first_name=employee_first_name, last_name=employee_last_name
        )

    async def get_all_by_name_and_date_of_birth_v2(
        self,
        *,
        date_of_birth: datetime.date | str,
        first_name: str,
        last_name: str,
    ) -> List[model.Member2]:
        return await self.members_2.get_all_by_name_and_date_of_birth(
            date_of_birth=date_of_birth, first_name=first_name, last_name=last_name
        )

    async def get_all_by_employee_name_and_date_of_birth_v2(
        self,
        *,
        date_of_birth: datetime.date | str,
        employee_first_name: str,
        employee_last_name: str,
    ) -> List[model.Member2]:
        return await self.members_2.get_all_by_name_and_date_of_birth(
            date_of_birth=date_of_birth,
            first_name=employee_first_name,
            last_name=employee_last_name,
        )

    async def get_by_name_and_unique_corp_id_v2(
        self,
        *,
        first_name: str,
        last_name: str,
        unique_corp_id: str,
    ) -> model.Member2:
        return await self.members_2.get_by_name_and_unique_corp_id(
            first_name=first_name, last_name=last_name, unique_corp_id=unique_corp_id
        )

    async def get_by_employee_name_and_unique_corp_id_v2(
        self,
        *,
        employee_first_name: str,
        employee_last_name: str,
        unique_corp_id: str,
    ) -> model.Member2:
        return await self.members_2.get_by_name_and_unique_corp_id(
            first_name=employee_first_name,
            last_name=employee_last_name,
            unique_corp_id=unique_corp_id,
        )

    async def get_by_date_of_birth_and_unique_corp_id_v2(
        self,
        *,
        date_of_birth: datetime.date | str,
        unique_corp_id: str,
    ) -> model.Member2:
        return await self.members_2.get_by_date_of_birth_and_unique_corp_id(
            date_of_birth=date_of_birth, unique_corp_id=unique_corp_id
        )

    async def get_by_dependent_date_of_birth_and_unique_corp_id_v2(
        self,
        *,
        dependent_date_of_birth: datetime.date | str,
        unique_corp_id: str,
    ) -> model.Member2:
        return await self.members_2.get_by_date_of_birth_and_unique_corp_id(
            date_of_birth=dependent_date_of_birth, unique_corp_id=unique_corp_id
        )

    async def get_by_dob_and_email_v2(
        self,
        *,
        date_of_birth: datetime.date,
        email: str,
    ) -> List[model.Member2]:
        return await self.members_2.get_by_dob_and_email(
            date_of_birth=date_of_birth, email=email
        )

    async def get_by_dependent_dob_and_email_v2(
        self,
        *,
        dependent_date_of_birth: datetime.date,
        email: str,
    ) -> List[model.Member2]:
        return await self.members_2.get_by_dob_and_email(
            date_of_birth=dependent_date_of_birth, email=email
        )

    async def get_by_dob_name_and_work_state_v2(
        self,
        *,
        date_of_birth: datetime.date,
        first_name: str,
        last_name: str,
        work_state: str,
    ) -> List[model.Member2]:
        return await self.members_2.get_by_dob_name_and_work_state(
            date_of_birth=date_of_birth,
            first_name=first_name,
            last_name=last_name,
            work_state=work_state,
        )

    async def get_by_email_and_name_v2(
        self,
        *,
        email: str,
        first_name: str,
        last_name: str,
    ) -> List[model.Member2]:
        return await self.members_2.get_by_email_and_name(
            email=email, first_name=first_name, last_name=last_name
        )

    async def get_by_email_and_employee_name_v2(
        self,
        *,
        email: str,
        employee_first_name: str,
        employee_last_name: str,
    ) -> List[model.Member2]:
        return await self.members_2.get_by_email_and_name(
            email=email, first_name=employee_first_name, last_name=employee_last_name
        )
