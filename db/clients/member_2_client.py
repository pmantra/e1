from __future__ import annotations

import datetime
from datetime import date
from typing import Iterable, Iterator, List, Mapping, Tuple

import asyncpg
import typic

from db import model
from db.clients.client import BoundClient, ServiceProtocol, T, _coerceable
from db.clients.postgres_connector import PostgresConnector, retry

MemberIDtoRangeT = Tuple[int, asyncpg.Range]


class Member2ServiceProtocol(ServiceProtocol[model.Member2]):

    model = model.Member2
    __override_exclude_fields__ = frozenset(("created_at", "updated_at"))

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__()
        cls.__exclude_fields__ = (
            cls.__override_exclude_fields__ or cls.__exclude_fields__
        )


class Member2Client(Member2ServiceProtocol):
    """A service for querying the `eligibility.member_2` table."""

    def __init__(self, *, connector: PostgresConnector = None):
        super().__init__()
        self.client = BoundClient("member_2", connector=connector)

    # region fetch operations

    @_coerceable(bulk=True)
    @retry
    async def get_by_dob_and_email(
        self,
        date_of_birth: date,
        email: str,
        *,
        connection: asyncpg.Connection = None,
    ) -> List[model.Member2]:
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_by_dob_and_email(
                c,
                date_of_birth=date_of_birth,
                email=email,
            )

    @_coerceable(bulk=True)
    @retry
    async def get_by_secondary_verification(
        self,
        date_of_birth: date,
        first_name: str,
        last_name: str,
        work_state: str,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> List[model.Member2]:
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_by_secondary_verification(
                c,
                date_of_birth=date_of_birth,
                first_name=first_name,
                last_name=last_name,
                work_state=work_state,
            )

    @_coerceable(bulk=True)
    @retry
    async def get_by_tertiary_verification(
        self,
        date_of_birth: date,
        unique_corp_id: str,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> List[model.Member2]:
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_by_tertiary_verification(
                c,
                date_of_birth=date_of_birth,
                unique_corp_id=unique_corp_id,
            )

    @_coerceable
    @retry
    async def get_by_client_specific_verification(
        self,
        organization_id: int,
        unique_corp_id: str,
        date_of_birth: date,
        *,
        connection: asyncpg.Connection = None,
    ):
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_by_client_specific_verification(
                c,
                organization_id=organization_id,
                unique_corp_id=unique_corp_id,
                date_of_birth=date_of_birth,
            )

    @_coerceable
    @retry
    async def get_by_org_identity(
        self,
        identity: model.OrgIdentity,
        *,
        connection: asyncpg.Connection = None,
    ):
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_by_org_identity(
                c,
                organization_id=identity.organization_id,
                unique_corp_id=identity.unique_corp_id,
                dependent_id=identity.dependent_id,
            )

    @retry
    async def get_wallet_enablement_by_identity(
        self,
        identity: model.OrgIdentity,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> model.WalletEnablement | None:
        async with self.client.read_connector.connection(c=connection) as c:
            wallet = await self.client.queries.get_wallet_enablement_by_identity(
                c,
                organization_id=identity.organization_id,
                unique_corp_id=identity.unique_corp_id,
                dependent_id=identity.dependent_id,
            )
        if wallet and coerce:
            return typic.transmute(model.WalletEnablement, wallet)
        return wallet

    @retry
    async def get_wallet_enablement(
        self,
        member_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> model.WalletEnablement | None:
        async with self.client.read_connector.connection(c=connection) as c:
            wallet = await self.client.queries.get_wallet_enablement(
                c, member_id=member_id
            )
            if wallet and coerce:
                return typic.transmute(model.WalletEnablement, wallet)
            return wallet

    @retry
    async def get_other_user_ids_in_family(
        self, *, user_id: int, connection: asyncpg.Connection = None
    ) -> List[int]:
        """Given a user_id, return all user_id's for that family, as grouped by unique_corp_id and organization_id"""
        async with self.client.read_connector.connection(c=connection) as c:
            records: List[
                asyncpg.Record
            ] = await self.client.queries.get_other_user_ids_in_family(
                c, user_id=user_id
            )
            return [r["user_id"] for r in records]

    @_coerceable(bulk=True)
    @retry
    async def get_by_email_and_name(
        self,
        email: str,
        first_name: str,
        last_name: str,
        *,
        connection: asyncpg.Connection = None,
    ) -> List[model.Member2]:
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_by_email_and_name(
                c,
                email=email,
                first_name=first_name,
                last_name=last_name,
            )

    @_coerceable(bulk=True)
    @retry
    async def get_by_overeligibility(
        self,
        date_of_birth: date,
        first_name: str,
        last_name: str,
        *,
        connection: asyncpg.Connection = None,
    ) -> List[model.Member2]:
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_by_overeligibility(
                c,
                date_of_birth=date_of_birth,
                first_name=first_name,
                last_name=last_name,
            )

    @retry
    async def set_updated_at(
        self,
        id: int,
        updated_at: datetime.datetime,
        *,
        connection: asyncpg.Connection = None,
    ):
        """
        Do not use this outside of tests- this should be used to help us mock up records that represent 'updated' records
        Our test fixtures result in all records having the same created_at/updated_at date, which prevent us from sorting
        records by the date/time they were created.
        """
        async with self.client.connector.transaction(connection=connection) as c:
            updated = await self.client.queries.set_updated_at(
                c, id=id, updated_at=updated_at
            )
        return updated

    @_coerceable
    @retry
    async def get_by_member_versioned(
        self,
        member_versioned: model.MemberVersioned,
        *,
        connection: asyncpg.Connection = None,
    ) -> model.Member2:
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_by_member_versioned(
                c,
                organization_id=member_versioned.organization_id,
                first_name=member_versioned.first_name,
                last_name=member_versioned.last_name,
                email=member_versioned.email,
                date_of_birth=member_versioned.date_of_birth,
                work_state=member_versioned.work_state,
                unique_corp_id=member_versioned.unique_corp_id,
            )

    @_coerceable(bulk=True)
    @retry
    async def get_all_by_name_and_date_of_birth(
        self,
        date_of_birth: date,
        first_name: str,
        last_name: str,
        *,
        connection: asyncpg.Connection = None,
    ) -> List[model.Member2]:
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_all_by_name_and_date_of_birth(
                c,
                date_of_birth=date_of_birth,
                first_name=first_name,
                last_name=last_name,
            )

    @_coerceable(bulk=True)
    @retry
    async def get_by_dob_name_and_work_state(
        self,
        date_of_birth: date,
        first_name: str,
        last_name: str,
        work_state: str,
        *,
        connection: asyncpg.Connection = None,
    ) -> List[model.Member2]:
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_by_dob_name_and_work_state(
                c,
                date_of_birth=date_of_birth,
                first_name=first_name,
                last_name=last_name,
                work_state=work_state,
            )

    @_coerceable
    @retry
    async def get_by_name_and_unique_corp_id(
        self,
        unique_corp_id: str,
        first_name: str,
        last_name: str,
        *,
        connection: asyncpg.Connection = None,
    ) -> model.Member2:
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_by_name_and_unique_corp_id(
                c,
                unique_corp_id=unique_corp_id,
                first_name=first_name,
                last_name=last_name,
            )

    @_coerceable
    @retry
    async def get_by_date_of_birth_and_unique_corp_id(
        self,
        date_of_birth: date,
        unique_corp_id: str,
        *,
        connection: asyncpg.Connection = None,
    ) -> model.Member2:
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_by_date_of_birth_and_unique_corp_id(
                c,
                date_of_birth=date_of_birth,
                unique_corp_id=unique_corp_id,
            )

    # endregion

    @retry
    @_coerceable(bulk=True)
    async def bulk_persist(
        self,
        *,
        models: Iterable[T] = (),
        data: Iterable[Mapping] = (),
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        if models:
            data = [d for d in self._iterdump(models)]
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.bulk_persist(c, records=data)

    def _iterdump(self, models: Iterable[T]) -> Iterator[T]:
        kvs = self._get_kvs
        yield from (kvs(m) for m in models)
