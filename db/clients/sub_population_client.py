from __future__ import annotations

from typing import Iterable, Iterator, List, Mapping

import asyncpg
import ddtrace

from app.eligibility.populations import model as pop_model
from db.clients import client as db_client
from db.clients import postgres_connector
from db.clients.client import _coerceable
from db.clients.postgres_connector import retry


class SubPopulations(db_client.ServiceProtocol[pop_model.SubPopulation]):
    """A service for querying & mutating the `eligibility.sub_population` table.

    Usage:
        >>> sub_populations = SubPopulations()
        >>> sub_population = pop_model.SubPopulation(
                population_id=1,
                feature_set_name="Sample",
                feature_set_details={
                    f"{FeatureTypes.TRACK_FEATURE}": [1, 2, 3],
                    f"{FeatureTypes.WALLET_FEATURE}": [2, 4, 6],
                },
            )
        >>> sub_population = await sub_populations.persist(model=sub_population)
        >>> await sub_populations.get(sub_population.id)
    """

    model = pop_model.SubPopulation

    def __init__(self, *, connector: postgres_connector.PostgresConnector = None):
        super().__init__()
        self.client = db_client.BoundClient("sub_population", connector=connector)

    # region fetch operations

    @ddtrace.tracer.wrap()
    @_coerceable(bulk=True)
    @retry
    async def get_for_population_id(
        self,
        population_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> List[pop_model.SubPopulation]:
        """
        Gets the sub_populations for the specified population ID. If no
        sub_populations exist or if the population doesn't exist, it returns
        an empty list.
        """
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_for_population_id(
                c, population_id=population_id
            )

    @ddtrace.tracer.wrap()
    @_coerceable(bulk=True)
    @retry
    async def get_for_active_population_for_organization_id(
        self,
        organization_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> List[pop_model.SubPopulation]:
        """
        Gets the sub_populations for the active population. If no sub_populations
        exist or if there is no active population, it returns an empty list.
        """
        async with self.client.connector.connection(c=connection) as c:
            return (
                await self.client.queries.get_for_active_population_for_organization_id(
                    c, organization_id=organization_id
                )
            )

    @ddtrace.tracer.wrap()
    @retry
    async def get_feature_list_of_type_for_id(
        self,
        id: int,
        feature_type: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> List[int]:
        """
        Gets the list of feature IDs of the specified feature type for
        the specified sub_population's ID. Once a user's sub_population has
        been determined, this function is used to get the features for which
        the user is eligible.

        As the call will be made from within specific
        feature domains, it is never necessary to return all IDs for all
        feature types. The wallet code, for example, will only ever be asking
        about wallet IDs and will not need information on eligible tracks.
        """
        async with self.client.connector.connection(c=connection) as c:
            record = await self.client.queries.get_feature_list_of_type_for_id(
                c, id=id, feature_type=str(feature_type)
            )

            # None return means that the sub_population couldn't be found
            if record is None:
                return None

            # [None] return means that the feature type couldn't be found
            if record[0] is None:
                return []

            record = [
                int(split_record)
                for split_record in record[0].split(",")
                if len(split_record) > 0
            ]
            return record

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
    ) -> List[pop_model.SubPopulation]:
        if models:
            data = [d for d in self._iterdump(models)]
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.bulk_persist(c, records=data)

    def _iterdump(self, models: Iterable[db_client.T]) -> Iterator[db_client.T]:
        kvs = self._get_kvs
        yield from (kvs(m) for m in models)

    # endregion mutate operations
