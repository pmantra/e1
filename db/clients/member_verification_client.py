from __future__ import annotations

from typing import Iterable, Iterator, List, Mapping, Tuple

import asyncpg

from db.clients.client import BoundClient, ServiceProtocol, T, _coerceable
from db.clients.postgres_connector import PostgresConnector, retry
from db.model import MemberVerification

MemberIDtoRangeT = Tuple[int, asyncpg.Range]


class MemberVerifications(ServiceProtocol[MemberVerification]):
    """A service for querying & mutating the `eligibility.member_verification` table.

    Usage:
        >>> member_verification = MemberVerifications()
        >>> member_verification = MemberVerification(member_id=1)
        >>> member_verification = await member_verifications.persist(model=member_verification)
        >>> await member_verification.get(member_verification.id)
    """

    model = MemberVerification

    def __init__(self, *, connector: PostgresConnector = None):
        super().__init__()
        self.client = BoundClient("member_verification", connector=connector)

    # region fetch operations

    @_coerceable(bulk=False)
    @retry
    async def get_for_member_id(
        self,
        member_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> MemberVerification:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_for_member_id(c, member_id=member_id)

    @_coerceable(bulk=True)
    @retry
    async def get_all_for_member_id(
        self,
        member_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> List[MemberVerification]:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_all_for_member_id(
                c, member_id=member_id
            )

    @_coerceable(bulk=True)
    @retry
    async def get_for_verification_id(
        self,
        verification_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> List[MemberVerification]:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_for_verification_id(
                c, verification_id=verification_id
            )

    @_coerceable(bulk=True)
    @retry
    async def get_for_verification_attempt_id(
        self,
        verification_attempt_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> MemberVerification:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_for_verification_attempt_id(
                c, verification_attempt_id=verification_attempt_id
            )

    # endregion

    # region mutate operations
    @_coerceable(bulk=True)
    @retry
    async def bulk_persist(
        self,
        *,
        models: Iterable[T] = (),
        data: Iterable[Mapping] = (),
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> [MemberVerification]:
        if models:
            data = [d for d in self._iterdump(models)]
        if connection is not None:
            return await self.client.queries.bulk_persist(connection, records=data)
        else:
            async with self.client.connector.transaction(connection=connection) as c:
                return await self.client.queries.bulk_persist(c, records=data)

    def _iterdump(self, models: Iterable[T]) -> Iterator[T]:
        kvs = self._get_kvs
        yield from (kvs(m) for m in models)

    # endregion
