from __future__ import annotations

from typing import Iterable, Iterator, List, Mapping

import asyncpg
import ddtrace

from app.eligibility.populations import model as pop_model
from db.clients import client as db_client
from db.clients import postgres_connector
from db.clients.client import _coerceable
from db.clients.postgres_connector import retry


class MemberSubPopulations(db_client.ServiceProtocol[pop_model.MemberSubPopulation]):
    """A service for querying & mutating the `eligibility.member_sub_population` table.

    Usage:
        >>> member_sub_populations = MemberSubPopulations()
        >>> member_sub_population = pop_model.MemberSubPopulation(
                member_id=1,
                sub_population_id=1,
            )
        >>> await member_sub_populations.persist(model=member_sub_population)
        >>> await member_sub_populations.get_for_member_id(member_id=1)
        >>> await member_sub_populations.get_for_sub_population_id(sub_population_id=1)
    """

    model = pop_model.MemberSubPopulation

    def __init__(self, *, connector: postgres_connector.PostgresConnector = None):
        super().__init__()
        self.client = db_client.BoundClient(
            "member_sub_population", connector=connector
        )

    # region fetch operations

    @ddtrace.tracer.wrap()
    @retry
    async def get_sub_population_id_for_member_id(
        self,
        member_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> int:
        """
        Gets all populations for the specified organization ID. If no populations
        have ever been configured, it will return an empty list.
        """
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_sub_population_id_for_member_id(
                c, member_id=member_id
            )

    @ddtrace.tracer.wrap()
    @retry
    async def get_all_member_ids_for_sub_population_id(
        self,
        sub_population_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> List[int]:
        """
        Gets all populations for the specified organization ID. If no populations
        have ever been configured, it will return an empty list.
        """
        async with self.client.connector.connection(c=connection) as c:
            records = (
                await self.client.queries.get_all_member_ids_for_sub_population_id(
                    c, sub_population_id=sub_population_id
                )
            )
            return [record["member_id"] for record in records]

    @ddtrace.tracer.wrap()
    @retry
    async def get_all_active_member_ids_for_sub_population_id(
        self,
        sub_population_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> List[int]:
        """
        Gets all populations for the specified organization ID. If no populations
        have ever been configured, it will return an empty list.
        """
        async with self.client.connector.connection(c=connection) as c:
            records = await self.client.queries.get_all_active_member_ids_for_sub_population_id(
                c, sub_population_id=sub_population_id
            )
            return [record["member_id"] for record in records]

    # endregion fetch operations

    # region mutate operations

    @_coerceable(bulk=True)
    @retry
    async def bulk_persist(
        self,
        *,
        models: Iterable[db_client.T] = (),
        data: Iterable[Mapping] = (),
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> List[pop_model.MemberSubPopulation]:
        if models:
            data = [d for d in self._iterdump(models)]
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.bulk_persist(c, records=data)

    def _iterdump(self, models: Iterable[db_client.T]) -> Iterator[db_client.T]:
        kvs = self._get_kvs
        yield from (kvs(m) for m in models)

    # endregion mutate operations
