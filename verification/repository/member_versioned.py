from __future__ import annotations

import datetime
from typing import List, Tuple

from db import model as db_model
from db.clients import member_versioned_client as m_client
from db.model import MemberVersioned


class MemberVersionedRepository:
    def __init__(
        self, member_versioned_client: m_client.MembersVersioned | None = None
    ):
        self._member_versioned_client: m_client.MembersVersioned = (
            member_versioned_client or m_client.MembersVersioned()
        )

    async def persist_members_and_address_records(
        self, records: List[db_model.ExternalRecordAndAddress]
    ) -> Tuple[list]:
        """Save a list of external records, consisting of member and address information"""
        return await self._member_versioned_client.bulk_persist_external_records(
            external_records=records
        )

    async def get_members_for_org(self, organization_id: int) -> [MemberVersioned]:
        """Return any member records for a given organization"""
        return await self._member_versioned_client.get_for_org(
            organization_id=organization_id
        )

    async def get_members_for_file(self, file_id: int) -> List[MemberVersioned]:
        """Return any member records for a given file"""

        return await self._member_versioned_client.get_for_file(file_id=file_id)

    async def get_by_dob_and_email(
        self, date_of_birth: datetime.date, email: str
    ) -> MemberVersioned | None:

        """Return the most recent effective record for a user, matching on date of birth and email"""
        return await self._member_versioned_client.get_by_dob_and_email(
            date_of_birth=date_of_birth, email=email
        )

    async def get_by_secondary_verification(
        self,
        date_of_birth: datetime.date,
        first_name: str,
        last_name: str,
        work_state: str,
    ) -> MemberVersioned | None:

        """Return the most recent effective record for a user, matching on dob, name, and email"""
        return await self._member_versioned_client.get_by_secondary_verification(
            date_of_birth=date_of_birth,
            first_name=first_name,
            last_name=last_name,
            work_state=work_state,
        )

    async def get_by_tertiary_verification(
        self, date_of_birth: datetime.date, unique_corp_id: str
    ) -> MemberVersioned | None:
        """Return the most recent effective record for a user, matching on dob and unique_corp_id"""

        return await self._member_versioned_client.get_by_tertiary_verification(
            date_of_birth=date_of_birth, unique_corp_id=unique_corp_id
        )

    async def get_by_any_verification(
        self,
        date_of_birth: datetime.date,
        first_name: str,
        last_name: str,
        work_state: str,
        email: str,
    ) -> MemberVersioned | None:

        """Return the most recent effective record for a user, using either primary or secondary validation"""
        return await self._member_versioned_client.get_by_any_verification(
            date_of_birth=date_of_birth,
            first_name=first_name,
            last_name=last_name,
            work_state=work_state,
            email=email,
        )
