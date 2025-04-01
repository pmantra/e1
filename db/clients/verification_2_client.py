from __future__ import annotations

from typing import Iterable, Iterator, List, Mapping, Tuple

import asyncpg

from db import model
from db.clients.client import BoundClient, ServiceProtocol, T, _coerceable
from db.clients.postgres_connector import PostgresConnector, retry

MemberIDtoRangeT = Tuple[int, asyncpg.Range]


class Verification2Client(ServiceProtocol[model.Verification2]):
    """A service for querying the `eligibility.verification_2` table."""

    model = model.Verification2

    def __init__(self, *, connector: PostgresConnector = None):
        super().__init__()
        self.client = BoundClient("verification_2", connector=connector)

    # region fetch operations

    @_coerceable(bulk=False)
    @retry
    async def get_for_member_id(
        self,
        member_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> model.Verification2:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_for_member_id(c, member_id=member_id)

    @retry
    async def get_verification_key_for_id(
        self,
        id: int,
        *,
        connection: asyncpg.Connection = None,
    ) -> model.VerificationKey | None:
        async with self.client.connector.connection(c=connection) as c:
            v2_record = await self.client.queries.get_verification_key_for_id(c, id=id)

        if not v2_record:
            return None

        return model.VerificationKey(
            member_id=None,
            organization_id=v2_record["organization_id"],
            is_v2=True,
            created_at=v2_record["created_at"],
            verification_1_id=None,
            verification_2_id=v2_record["verification_2_id"],
            member_2_id=v2_record["member_id"],
            member_2_version=v2_record["member_version"],
        )

    @retry
    async def get_verification_key_for_user_and_org(
        self,
        user_id: int,
        organization_id: int,
        *,
        connection: asyncpg.Connection = None,
    ) -> model.VerificationKey | None:
        async with self.client.connector.connection(c=connection) as c:
            record = await self.client.queries.get_verification_key_for_user_and_org(
                c,
                user_id=user_id,
                organization_id=organization_id,
            )

        # SQL limits records length to 1
        if not record:
            return None

        return model.VerificationKey(
            member_id=None,
            organization_id=record["organization_id"],
            is_v2=True,
            created_at=record["created_at"],
            verification_1_id=None,
            verification_2_id=record["verification_2_id"],
            member_2_id=record["member_id"],
            member_2_version=record["member_version"],
        )

    @_coerceable(bulk=False)
    @retry
    async def deactivate_verification_record_for_user(
        self,
        *,
        verification_id: int,
        user_id: int,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> model.Verification2:
        async with self.client.connector.transaction(connection=connection) as c:
            verification_2 = (
                await self.client.queries.deactivate_verification_2_record_for_user(
                    c,
                    verification_id=verification_id,
                    user_id=user_id,
                )
            )
            return verification_2

    @_coerceable(bulk=True)
    @retry
    async def bulk_persist(
        self,
        *,
        models: Iterable[T] = (),
        data: Iterable[Mapping] = (),
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> List[model.Verification2]:
        if models:
            data = [d for d in self._iterdump(models)]
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.bulk_persist(c, records=data)

    def _iterdump(self, models: Iterable[T]) -> Iterator[T]:
        kvs = self._get_kvs
        yield from (kvs(m) for m in models)
