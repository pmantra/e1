from __future__ import annotations

from itertools import chain
from typing import Iterable, Mapping, Optional, Tuple

import asyncpg

from db.clients.client import BoundClient, ServiceProtocol, _coerceable
from db.clients.postgres_connector import PostgresConnector, retry
from db.model import HeaderAlias, HeaderMapping


class HeaderAliases(ServiceProtocol[HeaderAlias]):
    model = HeaderAlias

    def __init__(self, *, connector: PostgresConnector = None):
        super().__init__()
        self.client = BoundClient("header_alias", connector=connector)

    # region fetch operations

    @_coerceable
    @retry
    async def get_org_header_alias(
        self,
        organization_id: int,
        header: str,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_org_header_alias(
                c, organization_id=organization_id, header=header
            )

    @_coerceable(bulk=True)
    @retry
    async def get_for_org(
        self,
        organization_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_for_org(
                c, organization_id=organization_id
            )

    @_coerceable(bulk=True)
    @retry
    async def get_for_file(
        self,
        file_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_for_file(c, file_id=file_id)

    @_coerceable(bulk=True)
    @retry
    async def get_for_files(
        self,
        *file_ids: int,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_for_files(c, file_ids=file_ids)

    @retry
    async def get_header_mapping(
        self,
        organization_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        async with self.client.connector.connection(c=connection) as c:
            mapping = await self.client.queries.get_header_mapping(
                c, organization_id=organization_id
            )
            return HeaderMapping(mapping) if coerce else mapping

    @staticmethod
    def _extract_headers(
        org_id: int, mapping: Mapping[str, str]
    ) -> Optional[Iterable[Mapping]]:
        if mapping:
            values = (
                {"organization_id": org_id, "header": h, "alias": a}
                for h, a in mapping.items()
            )
            return values
        return None

    @_coerceable(bulk=True)
    @retry
    async def get_affiliations_header_for_org(
        self,
        organization_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_affiliations_header_for_org(
                c, organization_id=organization_id
            )

    # endregion

    # region mutate operations

    @retry
    async def persist_header_mapping(
        self,
        organization_id: int,
        mapping: Mapping[str, str],
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        aliases = self._extract_headers(organization_id, mapping)
        async with self.client.connector.connection(c=connection) as c:
            if aliases is not None:
                await self.bulk_persist(data=aliases, connection=c)
                await self.delete_missing(
                    organization_id=organization_id,
                    headers=mapping.keys(),
                    connection=c,
                )
            return await self.get_header_mapping(
                organization_id, connection=c, coerce=coerce
            )

    @retry
    async def persist_header_mappings(
        self,
        org_mappings: Iterable[Tuple[int, Mapping[str, str]]],
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        if org_mappings:
            aliases = chain(
                *(
                    aliases
                    for org_id, mapping in org_mappings
                    if (aliases := self._extract_headers(org_id, mapping)) is not None
                )
            )
            await self.bulk_persist(data=aliases, connection=connection)

    @_coerceable(bulk=True)
    @retry
    async def bulk_refresh(
        self,
        org_mappings: Iterable[Tuple[int, Mapping[str, str]]],
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        if org_mappings:
            aliases = []
            for org_id, mapping in org_mappings:
                batch = self._extract_headers(org_id, mapping)
                if not batch:
                    continue
                aliases.extend(batch)
            async with self.client.connector.transaction(connection=connection) as c:
                return await self.client.queries.bulk_refresh(c, aliases)

    @retry
    async def delete_missing(
        self,
        organization_id: int,
        headers: Iterable[str],
        *,
        connection: asyncpg.Connection = None,
    ):
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.delete_missing(
                c,
                organization_id=organization_id,
                headers=headers,
            )

    # endregion
