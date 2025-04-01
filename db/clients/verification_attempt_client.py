from __future__ import annotations

from typing import Iterable, Iterator, List, Mapping, Tuple

import asyncpg

from db.clients.client import BoundClient, ServiceProtocol, T, _coerceable
from db.clients.postgres_connector import PostgresConnector, retry
from db.model import Verification, VerificationAttempt

MemberIDtoRangeT = Tuple[int, asyncpg.Range]


class VerificationAttempts(ServiceProtocol[Verification]):
    """A service for querying & mutating the `eligibility.verification_attempt` table.

    Usage:
        >>> verification_attempt = VerificationAttempts()
        >>> verification_attempt = VerificationAttempt(organization_id=1, user_id=123, verification_type='standard')
        >>> verification_attempt = await verification_attempts.persist(model=verification_attempt)
        >>> await verification_attempts.get(verification_attempt.id)
    """

    model = VerificationAttempt

    def __init__(self, *, connector: PostgresConnector = None):
        super().__init__()
        self.client = BoundClient("verification_attempt", connector=connector)

    # region fetch operations

    @_coerceable(bulk=True)
    @retry
    async def get_for_org(
        self,
        organization_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> VerificationAttempt:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_for_org(
                c, organization_id=organization_id
            )

    @retry
    async def get_count_for_org(
        self, organization_id: int, *, connection: asyncpg.Connection = None
    ) -> int:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_count_for_org(
                c, organization_id=organization_id
            )

    @retry
    async def get_counts_for_orgs(
        self,
        *organization_ids: int,
        connection: asyncpg.Connection = None,
    ) -> List:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_counts_for_orgs(
                c, organization_ids=organization_ids
            )

    @retry
    async def get_for_ids(
        self,
        *verification_attempt_ids: int,
        connection: asyncpg.Connection = None,
    ) -> List:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_for_ids(
                c, verification_attempt_ids=verification_attempt_ids
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
    ) -> [VerificationAttempt]:
        if models:
            data = [d for d in self._iterdump(models)]
        if connection is not None:
            return await self.client.queries.bulk_persist(connection, records=data)
        else:
            async with self.client.connector.transaction(connection=connection) as c:
                return await self.client.queries.bulk_persist(c, records=data)

    @retry
    async def delete_all_for_org(
        self,
        organization_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> None:
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.delete_all_for_org(
                c,
                organization_id=organization_id,
            )

    def _iterdump(self, models: Iterable[T]) -> Iterator[T]:
        kvs = self._get_kvs
        yield from (kvs(m) for m in models)

    # endregion
