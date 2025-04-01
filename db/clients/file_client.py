from __future__ import annotations

import enum
from datetime import datetime
from typing import Any, Collection, Optional, Sequence, Tuple, TypeVar, Union

import asyncpg

from db.clients.client import BoundClient, ServiceProtocol, _coerceable
from db.clients.postgres_connector import PostgresConnector, retry
from db.model import Configuration, File, FileError

__all__ = ("Files", "FileStatus")


class FileStatus(str, enum.Enum):
    pending = "pending"
    incomplete = "incomplete"
    completed = "completed"


T = TypeVar("T")


class Files(ServiceProtocol[File]):
    """A service for querying & mutating the `eligibility.file` table.

    Usage:
        >>> files = Files()
        >>> file = File(1, "foo")
        >>> file = await files.persist(model=file)
        >>> await files.get(file.id)
    """

    model = File

    __exclude_fields__ = frozenset(("started_at", "completed_at"))

    def __init__(self, *, connector: PostgresConnector = None):
        super().__init__()
        self.client = BoundClient("file", connector=connector)

    # TMP methods used for validation
    @_coerceable
    async def tmp_persist(
        self,
        *,
        model: T = None,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
        **data,
    ):
        """Used for persistence to eligibility.tmp_file"""
        if model:
            data = self._get_kvs(model)
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.tmp_persist(c, **data)

    @retry
    async def tmp_set_started_at(
        self, id: int, *, connection: asyncpg.Connection = None
    ) -> Optional[datetime]:
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.tmp_set_started_at(c, id=id)

    @retry
    async def tmp_set_encoding(
        self,
        id: int,
        encoding: str,
        *,
        connection: asyncpg.Connection = None,
    ) -> Optional[str]:
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.tmp_set_encoding(
                c, id=id, encoding=encoding
            )

    @retry
    async def tmp_set_error(
        self, id: int, error: FileError, *, connection: asyncpg.Connection = None
    ) -> Optional[str]:
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.tmp_set_error(c, id=id, error=error)

    @retry
    async def tmp_set_completed_at(
        self, id: int, *, connection: asyncpg.Connection = None
    ) -> Optional[datetime]:
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.tmp_set_completed_at(c, id=id)

    # endregion

    # region fetch operations
    @_coerceable
    @retry
    async def get_latest_for_org(
        self,
        organization_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> Collection[Union[Configuration, asyncpg.Record]]:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_latest_for_org(
                c, organization_id=organization_id
            )

    @_coerceable
    @retry
    async def get_one_before_latest_for_org(
        self,
        organization_id: int,
        *,
        connection: Optional[asyncpg.Connection] = None,
        coerce: bool = True,
    ) -> Collection[asyncpg.Record]:
        """
        Retrieves the file record representing the one before latest for the given organization.

        Args:
            organization_id (int): The ID of the organization.
            connection (asyncpg.Connection, optional): The database connection to use.
            coerce (bool, optional): Whether to coerce the result.

        Returns:
            Collection[asyncpg.Record]: The file record one before latest.
        """
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_one_before_latest_for_org(
                c, organization_id=organization_id
            )

    @_coerceable(bulk=True)
    @retry
    async def get_all_for_org(
        self,
        organization_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> Collection[Union[Configuration, asyncpg.Record]]:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_all_for_org(
                c, organization_id=organization_id
            )

    @_coerceable(bulk=True)
    @retry
    async def for_ids(
        self,
        *ids: int,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> Collection[Union[Configuration, asyncpg.Record]]:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.for_ids(c, ids=ids)

    @retry
    async def get_names(
        self,
        *ids: int,
        connection: asyncpg.Connection = None,
    ) -> Sequence[asyncpg.Record]:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_names(c, ids=ids)

    @_coerceable(bulk=True)
    @retry
    async def get_by_name(
        self,
        file_name: str,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> Collection[Union[Configuration, asyncpg.Record]]:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_by_name(c, name=file_name)

    @_coerceable(bulk=True)
    @retry
    async def get_by_name_for_org(
        self,
        file_name: str,
        organization_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> Collection[Union[Configuration, asyncpg.Record]]:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_by_name_for_org(
                c, name=file_name, organization_id=organization_id
            )

    @_coerceable(bulk=True)
    @retry
    async def get_completed_for_org(
        self,
        organization_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> Collection[Union[Configuration, asyncpg.Record]]:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_completed_for_org(
                c, organization_id=organization_id
            )

    @_coerceable(bulk=True)
    @retry
    async def get_completed(
        self,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> Collection[Union[Configuration, asyncpg.Record]]:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_completed(c)

    @_coerceable(bulk=True)
    @retry
    async def get_incomplete_for_org(
        self,
        organization_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> Collection[Union[Configuration, asyncpg.Record]]:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_incomplete_for_org(
                c, organization_id=organization_id
            )

    @_coerceable(bulk=True)
    @retry
    async def get_incomplete(
        self,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> Collection[Union[Configuration, asyncpg.Record]]:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_incomplete(c)

    @retry
    async def get_incomplete_org_ids_file_ids(
        self,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> Collection[Union[Configuration, asyncpg.Record]]:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_incomplete_org_ids_file_ids(c)

    @retry
    async def get_incomplete_by_org(
        self,
        *,
        connection: asyncpg.Connection = None,
    ) -> Collection[Union[Configuration, asyncpg.Record]]:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_incomplete_by_org(c)

    @_coerceable(bulk=True)
    @retry
    async def get_pending_for_org(
        self,
        organization_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> Collection[Union[Configuration, asyncpg.Record]]:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_pending_for_org(
                c, organization_id=organization_id
            )

    @_coerceable(bulk=True)
    @retry
    async def get_pending(
        self,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> Collection[Union[Configuration, asyncpg.Record]]:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_pending(c)

    def _build_select(self, *fields: str, **where: Any) -> Tuple[str, Tuple[Any, ...]]:
        fields = (*(f for f in fields if f != "status"),)
        status = where.pop("status", None)
        stmt, args = super()._build_select(*fields, **where)
        status_filter = ""
        if status:
            prefix = "AND " if "WHERE" in stmt else "WHERE"
            if status == FileStatus.completed:
                status_filter = f"{prefix} completed_at IS NOT NULL"
            elif status == FileStatus.pending:
                status_filter = f"{prefix} started_at IS NULL"
            elif status == FileStatus.incomplete:
                status_filter = f"{prefix} completed_at IS NULL"
        stmt += status_filter
        return stmt, args

    # endregion

    # region mutate operations
    @retry
    async def set_started_at(
        self, id: int, *, connection: asyncpg.Connection = None
    ) -> Optional[datetime]:
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.set_started_at(c, id=id)

    @retry
    async def set_completed_at(
        self, id: int, *, connection: asyncpg.Connection = None
    ) -> Optional[datetime]:
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.set_completed_at(c, id=id)

    @retry
    async def set_file_count(
        self,
        *,
        id: int,
        raw_count: int,
        success_count: int,
        failure_count: int,
        connection: asyncpg.Connection = None,
    ) -> None:
        async with self.client.connector.transaction(connection=connection) as c:
            await self.client.queries.set_file_count(
                c,
                id=id,
                raw_count=raw_count,
                success_count=success_count,
                failure_count=failure_count,
            )

    @retry
    async def get_success_count(
        self, id: int, *, connection: asyncpg.Connection = None
    ) -> Optional[int]:
        async with self.client.connector.transaction(connection=connection) as c:
            a = await self.client.queries.get_success_count(c, id=id)
            return a["success_count"]

    @retry
    async def get_failure_count(
        self, id: int, *, connection: asyncpg.Connection = None
    ) -> Optional[int]:
        async with self.client.connector.transaction(connection=connection) as c:
            a = await self.client.queries.get_failure_count(c, id=id)
            return a["failure_count"]

    @retry
    async def get_raw_count(
        self, id: int, *, connection: asyncpg.Connection = None
    ) -> Optional[int]:
        async with self.client.connector.transaction(connection=connection) as c:
            a = await self.client.queries.get_raw_count(c, id=id)
            return a["raw_count"]

    @retry
    async def set_encoding(
        self,
        id: int,
        encoding: str,
        *,
        connection: asyncpg.Connection = None,
    ) -> Optional[str]:
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.set_encoding(c, id=id, encoding=encoding)

    @retry
    async def set_error(
        self, id: int, error: FileError, *, connection: asyncpg.Connection = None
    ) -> Optional[str]:
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.set_error(c, id=id, error=error)

    # endregion
