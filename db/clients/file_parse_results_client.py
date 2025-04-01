from __future__ import annotations

from datetime import datetime
from typing import Iterable, Iterator, List

import asyncpg
import typic

from db.clients.client import BoundClient, ServiceProtocol, T, _coerceable
from db.clients.postgres_connector import PostgresConnector, retry
from db.model import FileParseError, FileParseResult, IncompleteFilesByOrg


class FileParseResults(ServiceProtocol[FileParseResult]):
    """A service for querying & mutating the `eligibility.file_parse_results` table.

    Usage:
        >>> file_parse_results = FileParseResults()
        >>> file_parse_results = FileParseResults(1, "foo")
        >>> file_parse_results = await file_parse_results.persist(model=file_parse_results)
        >>> await file_parse_results.get(file_parse_results.id)
    """

    model = FileParseResult

    def __init__(self, *, connector: PostgresConnector = None):
        super().__init__()
        self.client = BoundClient("file_parse_results", connector=connector)

    # We choose to do this query via postgres sql rather than SQL alchemy because of this -> for bulk inserts/upserts,
    # raw SQL is *much* faster than using an ORM. Since this is called for all of our records, speed is essential
    # https://docs.sqlalchemy.org/en/14/faq/performance.html#i-m-inserting-400-000-rows-with-the-orm-and-it-s-really-slow

    # TMP methods used for validation
    @retry
    async def tmp_bulk_persist_file_parse_results(
        self,
        data: Iterable[T] = (),
        *,
        results: Iterable[FileParseResult] = (),
        connection: asyncpg.Connection = None,
    ) -> int:
        if results:
            data = [d for d in self._iterdump(results)]
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.tmp_bulk_persist_file_parse_results(
                c, results=data
            )

    @retry
    async def tmp_bulk_persist_file_parse_errors(
        self,
        data: Iterable[T] = (),
        *,
        errors: Iterable[FileParseError],
        connection: asyncpg.Connection = None,
    ) -> int:
        if errors:
            data = typic.primitive(errors)
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.tmp_bulk_persist_file_parse_errors(
                c, errors=data
            )

    @retry
    async def tmp_expire_missing_records_for_file(
        self, file_id: int, organization_id: int, connection: asyncpg.Connection = None
    ) -> int:
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.tmp_expire_missing_records_for_file(
                c, file_id=file_id, organization_id=organization_id
            )

    @retry
    async def tmp_bulk_persist_parsed_records_for_files(
        self, *files: int, connection: asyncpg.Connection = None
    ) -> int:
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.tmp_bulk_persist_parsed_records_for_files(
                c, files=files
            )

    @retry
    async def tmp_delete_file_parse_results_for_files(
        self, *files: int, connection: asyncpg.Connection = None
    ) -> int:
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.tmp_delete_file_parse_results_for_files(
                c, files=files
            )

    @retry
    async def tmp_delete_file_parse_errors_for_files(
        self, *files: int, connection: asyncpg.Connection = None
    ) -> int:
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.tmp_delete_file_parse_errors_for_files(
                c, files=files
            )

    # endregion

    # region create/update
    @retry
    async def bulk_persist_file_parse_results(
        self,
        data: Iterable[T] = (),
        *,
        results: Iterable[FileParseResult] = (),
        connection: asyncpg.Connection = None,
    ) -> int:
        if results:
            data = [d for d in self._iterdump(results)]
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.bulk_persist_file_parse_results(
                c, results=data
            )

    @retry
    async def bulk_persist_file_parse_errors(
        self,
        data: Iterable[T] = (),
        *,
        errors: Iterable[FileParseError],
        connection: asyncpg.Connection = None,
    ) -> int:
        if errors:
            data = typic.primitive(errors)
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.bulk_persist_file_parse_errors(
                c, errors=data
            )

    @retry
    async def expire_missing_records_for_file(
        self, file_id: int, organization_id: int, connection: asyncpg.Connection = None
    ) -> int:
        async with self.client.connector.transaction(connection=connection) as c:
            await self.client.queries.set_work_mem(c)
            return await self.client.queries.expire_missing_records_for_file(
                c, file_id=file_id, organization_id=organization_id
            )

    @retry
    async def expire_missing_records_for_file_versioned(
        self, file_id: int, organization_id: int, connection: asyncpg.Connection = None
    ) -> int:
        async with self.client.connector.transaction(connection=connection) as c:
            await self.client.queries.set_work_mem(c)
            return await self.client.queries.expire_missing_records_for_file_versioned(
                c, file_id=file_id, organization_id=organization_id
            )

    @retry
    async def bulk_persist_parsed_records_for_files(
        self, *files: int, connection: asyncpg.Connection = None
    ) -> int:
        """Copy records from file_parse_results to member table"""
        async with self.client.connector.transaction(connection=connection) as c:
            await self.client.queries.set_work_mem(c)
            return await self.client.queries.bulk_persist_parsed_records_for_files(
                c, files=files
            )

    @retry
    async def bulk_persist_parsed_records_for_files_dual_write(
        self, *files: int, connection: asyncpg.Connection = None
    ) -> int:
        """Copy records from file_parse_results to both member and member_versioned tables"""
        async with self.client.connector.transaction(connection=connection) as c:
            await self.client.queries.set_work_mem(c)
            return await self.client.queries.bulk_persist_parsed_records_for_files_dual_write(
                c, files=files
            )

    @retry
    async def bulk_persist_parsed_records_for_files_dual_write_hash(
        self, *files: int, connection: asyncpg.Connection = None
    ) -> List:
        """Copy records from file_parse_results to the member and member_versioned tables -
        Utilize hash column to prevent duplicate inserts to member_versioned
        Returns the list of IDs of the records copied to member versioned"""
        async with self.client.connector.transaction(connection=connection) as c:
            await self.client.queries.set_work_mem(c)
            return await self.client.queries.bulk_persist_parsed_records_for_files_dual_write_hash(
                c, files=files
            )

    @retry
    async def bulk_persist_parsed_records_for_file_and_org_dual_write_hash(
        self, file_id: int, organization_id: int, connection: asyncpg.Connection = None
    ) -> None:
        """Same function with bulk_persist_parsed_records_for_files_dual_write_hash,
        just in smaller size with certain organization
        """
        async with self.client.connector.transaction(connection=connection) as c:
            await self.client.queries.set_work_mem(c)
            return await self.client.queries.bulk_persist_parsed_records_for_file_and_org_dual_write_hash(
                c, file_id=file_id, organization_id=organization_id
            )

    # endregion

    # region fetch
    @retry
    async def get_all_file_parse_results(
        self,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> list[FileParseResult]:
        async with self.client.connector.connection(c=connection) as c:
            records = await self.client.queries.get_all_file_parse_results(c)
            return typic.transmute(List[FileParseResult], records)

    @retry
    async def get_all_file_parse_errors(
        self,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> list[FileParseError]:
        async with self.client.connector.connection(c=connection) as c:
            records = await self.client.queries.get_all_file_parse_errors(c)
            return typic.transmute(List[FileParseError], records)

    @retry
    async def get_file_parse_results_for_file(
        self,
        file_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> list[FileParseResult]:
        async with self.client.connector.connection(c=connection) as c:
            records = await self.client.queries.get_file_parse_results_for_file(
                c, file_id=file_id
            )
            return typic.transmute(List[FileParseResult], records)

    @retry
    async def get_file_parse_errors_for_file(
        self,
        file_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> list[FileParseError]:
        async with self.client.connector.connection(c=connection) as c:
            records = await self.client.queries.get_file_parse_errors_for_file(
                c, file_id=file_id
            )
            return typic.transmute(List[FileParseError], records)

    @_coerceable(bulk=True)
    @retry
    async def get_file_parse_results_for_org(
        self,
        org_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> list[asyncpg.Record]:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_file_parse_results_for_org(
                c, org_id=org_id
            )

    @retry
    async def get_file_parse_errors_for_org(
        self,
        org_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> list[FileParseError]:

        async with self.client.connector.connection(c=connection) as c:
            records = await self.client.queries.get_file_parse_errors_for_org(
                c, org_id=org_id
            )
            return typic.transmute(List[FileParseError], records)

    @retry
    async def get_incomplete_files_by_org(
        self,
        *,
        connection: asyncpg.Connection = None,
    ) -> list[IncompleteFilesByOrg]:
        async with self.client.connector.connection(c=connection) as c:
            records = await self.client.queries.get_incomplete_files_by_org(c)
            return typic.transmute(List[IncompleteFilesByOrg], records)

    @retry
    async def get_count_hashed_inserted_for_file(
        self,
        file_id: int,
        file_created_at: datetime,
        connection: asyncpg.Connection = None,
    ):

        async with self.client.connector.connection(c=connection) as c:
            results = await self.client.queries.get_count_hashed_and_new_records(
                c, file_id=file_id, created_at=file_created_at
            )
            return {
                "hashed_count": results[0]["hashed_count"],
                "new_count": results[0]["new_count"],
            }

    # endregion

    # region delete

    @retry
    async def delete_file_parse_results_for_files(
        self, *files: int, connection: asyncpg.Connection = None
    ) -> int:
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.delete_file_parse_results_for_files(
                c, files=files
            )

    @retry
    async def delete_file_parse_errors_for_files(
        self, *files: int, connection: asyncpg.Connection = None
    ) -> int:
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.delete_file_parse_errors_for_files(
                c, files=files
            )

    # endregion

    def _iterdump(self, models: Iterable[T]) -> Iterator[T]:
        kvs = self._get_kvs
        yield from (kvs(m) for m in models)
