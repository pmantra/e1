from __future__ import annotations

from typing import List, Optional, TypedDict

import asyncpg
import typic
from mmlib.ops import log

from db.clients.client import BoundClient, ServiceProtocol, _coerceable
from db.clients.postgres_connector import PostgresConnector, retry
from db.model import Configuration, ExternalMavenOrgInfo
from db.mono.client import MavenOrgExternalID


class _ExternalIdentity(TypedDict):
    source: str
    data_provider_organization_id: str
    external_id: str
    organization_id: int


logger = log.getLogger(__name__)


class Configurations(ServiceProtocol[Configuration]):
    """A service for querying & mutating the `eligibility.configuration` table.

    Usage:
    >>> configs = Configurations()
    >>> config = Configuration(1, "email")
    >>> config = await configs.persist(model=config)
    >>> await configs.get(config.organization_id)
    """

    model = Configuration

    def __init__(self, *, connector: PostgresConnector = None):
        super().__init__()
        self.client = BoundClient(
            name="configuration", pk="organization_id", connector=connector
        )

    # region fetch operations

    @_coerceable
    @retry
    async def get_by_directory_name(
        self,
        directory_name: str,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> Optional[Configuration]:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_by_directory_name(
                c, directory_name=directory_name
            )

    @retry
    async def get_sub_orgs_by_data_provider(
        self,
        data_provider_org_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> List[Configuration]:
        async with self.client.connector.connection(c=connection) as c:
            orgs = await self.client.queries.get_sub_orgs_by_data_provider(
                c, data_provider_org_id=data_provider_org_id
            )
            return typic.transmute(List[Configuration], orgs)

    @_coerceable
    @retry
    async def get_by_external_id(
        self,
        source: str,
        external_id: str,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> Optional[Configuration]:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_by_external_id(
                c,
                source=source,
                external_id=external_id,
            )

    @_coerceable(bulk=True)
    @retry
    async def get_for_orgs(
        self,
        *organization_ids: int,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_for_orgs(
                c, organization_ids=organization_ids
            )

    @_coerceable
    @retry
    async def get_for_file(
        self,
        file_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> List[Configuration]:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_for_file(c, file_id=file_id)

    @_coerceable(bulk=True)
    @retry
    async def get_for_files(
        self,
        *file_ids: int,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> List[Configuration]:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_for_files(c, file_ids=file_ids)

    @retry
    async def get_external_ids(
        self,
        organization_id: int,
        *,
        connection: asyncpg.Connection = None,
    ) -> List[MavenOrgExternalID]:
        async with self.client.connector.connection(c=connection) as c:
            external_ids = await self.client.queries.get_external_ids(
                c, organization_id=organization_id
            )
            return typic.transmute(List[MavenOrgExternalID], external_ids)

    @retry
    async def get_all_external_ids(
        self,
        *,
        connection: asyncpg.Connection = None,
    ) -> List[MavenOrgExternalID]:
        async with self.client.connector.connection(c=connection) as c:
            external_ids = await self.client.queries.get_all_external_ids(c)
            return typic.transmute(List[MavenOrgExternalID], external_ids)

    @retry
    async def get_external_ids_by_data_provider(
        self,
        data_provider_organization_id: int,
        *,
        connection: asyncpg.Connection = None,
    ) -> List[MavenOrgExternalID]:
        async with self.client.connector.connection(c=connection) as c:
            external_ids = (
                await self.client.queries.get_external_ids_by_data_provider_id(
                    c, organization_id=data_provider_organization_id
                )
            )

        return typic.transmute(List[MavenOrgExternalID], external_ids)

    @retry
    # TODO: We should be able to modify this to only use dataprovider ID AFTER we migrate our optum records in organization_external_id to have a dataprovider ID rather than live as an enum in the 'source' column
    # Once the migration is done, we should deprecate this function with get_external_org_infos_by_value_and_data_provider
    async def get_external_org_infos_by_value_and_source(
        self,
        external_id: str,
        source: str,
        *,
        connection: asyncpg.Connection = None,
    ) -> List[ExternalMavenOrgInfo]:
        async with self.client.connector.connection(c=connection) as c:
            external_infos = (
                await self.client.queries.get_external_org_infos_by_value_and_source(
                    c,
                    external_id=external_id,
                    source=source,
                )
            )
            return typic.transmute(List[ExternalMavenOrgInfo], external_infos)

    @retry
    # TODO: We should be able to modify this to only use dataprovider ID AFTER we migrate our optum records in organization_external_id to have a dataprovider ID rather than live as an enum in the 'source' column
    # Once the migration is done, we should use this function
    async def get_external_org_infos_by_value_and_data_provider(
        self,
        external_id: str,
        data_provider_organization_id: int,
        *,
        connection: asyncpg.Connection = None,
    ):
        async with self.client.connector.connection(c=connection) as c:
            external_infos = await self.client.queries.get_external_org_infos_by_value_and_data_provider(
                c,
                external_id=external_id,
                data_provider_organization_id=data_provider_organization_id,
            )
            return typic.transmute(List[ExternalMavenOrgInfo], external_infos)

    @_coerceable(bulk=True)
    @retry
    async def get_configs_for_optum(
        self,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_configs_for_optum(c)

    # endregion

    # region mutate operations

    @retry
    async def add_external_id(
        self,
        organization_id: int,
        external_id: str,
        *,
        source: str | None = None,
        data_provider_organization_id: int | None = None,
        connection: asyncpg.Connection | None = None,
    ) -> MavenOrgExternalID:
        async with self.client.connector.transaction(connection=connection) as c:
            eid = await self.client.queries.add_external_id(
                c,
                organization_id=organization_id,
                source=source,
                external_id=external_id,
                data_provider_organization_id=data_provider_organization_id,
            )
            return typic.transmute(MavenOrgExternalID, eid)

    @retry
    async def bulk_add_external_id(
        self,
        identities: List[MavenOrgExternalID],
        *,
        connection: asyncpg.Connection = None,
    ):
        data = typic.primitive(identities)
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.bulk_add_external_id(c, data)

    @retry
    async def delete_external_ids_for_org(
        self, organization_id: int, *, connection: asyncpg.Connection = None
    ):
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.delete_external_ids_for_org(
                c, organization_id=organization_id
            )

    @retry
    async def delete_external_ids_for_data_provider_org(
        self,
        data_provider_organization_id: int,
        *,
        connection: asyncpg.Connection = None,
    ) -> List[MavenOrgExternalID]:
        async with self.client.connector.transaction(connection=connection) as c:
            deleted = (
                await self.client.queries.delete_external_ids_for_data_provider_org(
                    c, data_provider_organization_id=data_provider_organization_id
                )
            )
        return typic.transmute(List[MavenOrgExternalID], deleted)

    @retry
    async def delete_and_recreate_all_external_ids(
        self,
        identities: List[MavenOrgExternalID],
        *,
        connection: asyncpg.Connection = None,
    ):
        async with self.client.connector.transaction(connection=connection) as c:
            # First, delete all our existing external IDs

            logger.info("Deleting all external IDs for later recreation")
            await self.client.queries.delete_all_external_ids(c)

            # Now, recreate them based on fresh values
            logger.info("Recreating all external IDs")

            data = typic.primitive(identities)
            return await self.client.queries.bulk_add_external_id(c, data)

    # endregion
