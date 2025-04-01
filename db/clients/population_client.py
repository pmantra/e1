from __future__ import annotations

import datetime
from typing import Iterable, Iterator, List, Mapping

import aiosql
import asyncpg
import ddtrace

from app.eligibility.populations import model as pop_model
from app.utils import eligibility_member as e9y_member_utils
from db import model as db_model
from db.clients import client as db_client
from db.clients import postgres_connector
from db.clients.client import _coerceable
from db.clients.postgres_connector import retry


class Populations(db_client.ServiceProtocol[pop_model.Population]):
    """A service for querying & mutating the `eligibility.population` table.

    Usage:
        >>> populations = Populations()
        >>> population = pop_model.Population(
                population_id=1,
                feature_set_name="Sample",
                feature_set_details={
                    f"{FeatureTypes.TRACK_FEATURE}": [1, 2, 3],
                    f"{FeatureTypes.WALLET_FEATURE}": [2, 4, 6],
                },
            )
        >>> population = await populations.persist(model=population)
        >>> await populations.get(population.id)
    """

    model = pop_model.Population

    def __init__(self, *, connector: postgres_connector.PostgresConnector = None):
        super().__init__()
        self.client = db_client.BoundClient("population", connector=connector)

    # region fetch operations

    @ddtrace.tracer.wrap()
    @_coerceable(bulk=True)
    @retry
    async def get_all_active_populations(
        self,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> List[pop_model.Population]:
        """
        Gets all populations for the specified organization ID. If no populations
        have ever been configured, it will return an empty list.
        """
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_all_active_populations(c)

    @ddtrace.tracer.wrap()
    @_coerceable(bulk=True)
    @retry
    async def get_all_for_organization_id(
        self,
        organization_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> List[pop_model.Population]:
        """
        Gets all populations for the specified organization ID. If no populations
        have ever been configured, it will return an empty list.
        """
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_all_for_organization_id(
                c, organization_id=organization_id
            )

    @ddtrace.tracer.wrap()
    @_coerceable(bulk=False)
    @retry
    async def get_active_population_for_organization_id(
        self,
        organization_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> pop_model.Population | None:
        """
        Gets the active population for the specified organization ID. If there
        is no active population, it will return None.
        """
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_active_population_for_organization_id(
                c, organization_id=organization_id
            )

    @ddtrace.tracer.wrap()
    @retry
    async def get_the_population_information_for_user_id(
        self,
        user_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> pop_model.PopulationInformation | None:
        """
        Gets the sub_pop_lookup_keys_csv of the active population for the specified
        user_id. If there is no active population, it will return None.
        """
        async with self.client.connector.connection(c=connection) as c:
            record = (
                await self.client.queries.get_the_population_information_for_user_id(
                    c, user_id=user_id
                )
            )
            if record is None:
                return None
            return pop_model.PopulationInformation(
                population_id=record["id"],
                sub_pop_lookup_keys_csv=record["sub_pop_lookup_keys_csv"],
                advanced=record["advanced"],
                organization_id=record["organization_id"],
            )

    @ddtrace.tracer.wrap()
    @retry
    async def get_the_population_information_for_user_and_org(
        self,
        user_id: int,
        organization_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> pop_model.PopulationInformation | None:
        """
        Gets the sub_pop_lookup_keys_csv of the active population for the specified
        user_id and org. If there is no active population, it will return None.
        """
        async with self.client.connector.connection(c=connection) as c:
            record = await self.client.queries.get_the_population_information_for_user_and_org(
                c, user_id=user_id, organization_id=organization_id
            )
            if record is None:
                return None
            return pop_model.PopulationInformation(
                population_id=record["id"],
                sub_pop_lookup_keys_csv=record["sub_pop_lookup_keys_csv"],
                advanced=record["advanced"],
            )

    @ddtrace.tracer.wrap()
    @retry
    async def get_the_sub_pop_id_using_lookup_keys_for_member(
        self,
        lookup_keys_csv: str,
        member: db_model.MemberVersioned | db_model.Member2,
        population_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> int | None:
        """
        Dynamically generates the query to get the sub_population ID for
        the member using the criteria specified in the lookup_keys_csv.

        Example:
            Given the following input:
                lookup_keys_csv: "work_state,custom_attributes.employment_status,custom_attributes.group_number"
                member: {
                    "id": 1,
                    ...
                    "work_state": "NY",
                    "custom_attributes": {
                        "employment_status": "Full",
                        "group_number": "2"
                    }
                }
                population_id: 101
            The sql string generated would look like:
                SELECT pop.sub_pop_lookup_map_json->'NY'->'Full'->'2'
                FROM eligibility.population pop
                WHERE pop.id = 101;

        If the member, the lookup keys, or the final value doesn't exist,
        this function will return a None.
        """
        lookup_keys = lookup_keys_csv.split(",")
        if len(lookup_keys) == 0:
            return None

        # Construct the SQL
        sql_str = (
            "-- name: get_sub_pop_id_for_criteria^\n"
            "SELECT pop.sub_pop_lookup_map_json"
        )
        # Add more to the query string
        for key in lookup_keys:
            member_attribute = e9y_member_utils.get_member_attribute(member, key)
            if member_attribute is None:
                return None
            sql_str += f"->'{member_attribute}'"
        sql_str += (
            " \nFROM eligibility.population pop \n" f"WHERE pop.id = {population_id};\n"
        )

        # Create the query from the SQL
        queries = aiosql.from_str(sql_str, "asyncpg")

        # Use the query to get the sub_population ID
        async with self.client.connector.connection(c=connection) as c:
            record = await queries.get_sub_pop_id_for_criteria(c)

        if record is not None:
            record = record[0]
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
    ) -> List[pop_model.Population]:
        if models:
            data = [d for d in self._iterdump(models)]
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.bulk_persist(c, records=data)

    def _iterdump(self, models: Iterable[db_client.T]) -> Iterator[db_client.T]:
        kvs = self._get_kvs
        yield from (kvs(m) for m in models)

    @retry
    async def set_sub_pop_lookup_info(
        self,
        population_id: int,
        sub_pop_lookup_keys_csv: str,
        sub_pop_lookup_map: dict,
        *,
        connection: asyncpg.Connection = None,
    ) -> None:
        """
        Sets the sub_population lookup info, which includes the keys csv list and the
        mapping dictionary.

        This function allows us to create the population first and add the mapping to the
        sub_populations later since their IDs will not be available until after they are
        added to the database.
        """
        async with self.client.connector.connection(c=connection) as c:
            await self.client.queries.set_sub_pop_lookup_info(
                c,
                id=population_id,
                sub_pop_lookup_keys_csv=sub_pop_lookup_keys_csv,
                sub_pop_lookup_map_json=sub_pop_lookup_map,
            )

    @retry
    async def activate_population(
        self,
        population_id: int,
        activated_at: datetime.datetime | None = None,
        *,
        connection: asyncpg.Connection = None,
    ):
        """
        Activates the population specified by the ID by setting the
        activated_at. If a datetime is provided, it will be used as the
        activated_at value. If not, the sql will use the current timestamp.
        """
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.activate_population(
                c,
                id=population_id,
                activated_at=activated_at,
            )

    @retry
    async def deactivate_population(
        self,
        population_id: int,
        deactivated_at: datetime.datetime | None = None,
        *,
        connection: asyncpg.Connection = None,
    ):
        """
        Deactivates the population specified by the ID by setting the
        deactivated_at. If a datetime is provided, it will be used as the
        deactivated_at value. If not, the sql will use the current timestamp.
        """
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.deactivate_population(
                c,
                id=population_id,
                deactivated_at=deactivated_at,
            )

    @retry
    async def deactivate_populations_for_organization_id(
        self,
        organization_id: int,
        deactivated_at: datetime.datetime | None = None,
        *,
        connection: asyncpg.Connection = None,
    ):
        """
        Deactivates all active populations for the organization specified
        by the ID by setting the deactivated_at. If a datetime is provided,
        it will be used as the deactivated_at value. If not, the sql will
        use the current timestamp

        This function allows the caller to not need to know what the current
        active population is. This call can be used to precede a call to
        create a new active population to ensure that all current populations
        are marked as deactivated.
        """
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.deactivate_populations_for_organization_id(
                c,
                organization_id=organization_id,
                deactivated_at=deactivated_at,
            )

    # endregion mutate operations
