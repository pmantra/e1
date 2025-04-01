from __future__ import annotations

import asyncio
import contextlib
import functools
import pathlib
import threading
from typing import (
    Any,
    Awaitable,
    Callable,
    Collection,
    Dict,
    Iterable,
    Iterator,
    Literal,
    Mapping,
    Optional,
    Protocol,
    Tuple,
    TypeVar,
    Union,
    overload,
)

import aiosql
import asyncpg
import typic
from aiosql.queries import Queries
from aiosql.types import QueryFn
from inflection import underscore
from mmlib.ops import log

from db.clients.postgres_connector import PostgresConnector, cached_connectors, retry
from db.clients.utils import singleton

QUERY_PATH = pathlib.Path(__file__).parent.parent / "queries"
LOG = log.getLogger(__name__)

__all__ = (
    "BoundClient",
    "ClientProtocol",
    "ServiceProtocol",
)

ExplainFormatT = Literal[
    "json",
    "yaml",
    "xml",
]

# region coercable

Coerceable = Callable[..., Awaitable[Union[asyncpg.Record, Iterable[asyncpg.Record]]]]
_FuncT = TypeVar("_FuncT", bound=Coerceable)


class CoercingCursor:
    def __init__(
        self, service: ServiceProtocol, cursor: asyncpg.connection.cursor.Cursor
    ):
        self.service = service
        self.cursor = cursor

    async def forward(self, n: int, *, timeout: float = None):
        return await self.cursor.forward(n, timeout=timeout)

    async def fetch(self, n: int, *, timeout: float = None):
        page = await self.cursor.fetch(n, timeout=timeout)
        return page and self.service.bulk_protocol(({**r} for r in page))

    async def fetchrow(self, *, timeout: float = None):
        row = await self.cursor.fetchrow(timeout=timeout)
        return row and self.service.protocol({**row})


@overload
def _coerceable(func: _FuncT) -> _FuncT:
    ...


@overload
def _coerceable(*, bulk: bool = False) -> Callable[[_FuncT], _FuncT]:
    ...


def _coerceable(func: _FuncT = None, *, bulk: bool = False):
    """A helper which will automatically coerce an asyncpg.Record to a model."""

    def _maybe_coerce_result(func_: _FuncT):
        if bulk:

            @functools.wraps(func_)
            async def _maybe_coerce_bulk_result_wrapper(
                self: ServiceProtocol, *args, **kwargs
            ):
                coerce = kwargs.get("coerce", True)
                res: Iterable[asyncpg.Record] = await func_(self, *args, **kwargs)
                return (
                    self.bulk_protocol.transmute(({**r} for r in res))
                    if coerce and res
                    else res
                )

            return _maybe_coerce_bulk_result_wrapper

        @functools.wraps(func_)
        async def _maybe_coerce_result_wrapper(self: ServiceProtocol, *args, **kwargs):
            coerce = kwargs.get("coerce", True)
            res: Optional[asyncpg.Record] = await func_(self, *args, **kwargs)
            return self.protocol.transmute({**res}) if coerce and res else res

        return _maybe_coerce_result_wrapper

    return _maybe_coerce_result(func) if func else _maybe_coerce_result


# endregion

# region db_client


T = TypeVar("T")


class ClientProtocol(Protocol[T]):
    async def get(self, pk: int, *, connection: asyncpg.Connection = None):
        ...

    async def all(self, *, connection: asyncpg.Connection = None):
        ...

    async def persist(self, *, connection: asyncpg.Connection = None, **data):
        ...

    async def bulk_persist(
        self,
        datum: Iterable,
        *,
        connection: asyncpg.Connection = None,
    ):
        ...

    async def delete(self, pk: int, *, connection: asyncpg.Connection = None):
        ...

    async def bulk_delete(self, *pks: int, connection: asyncpg.Connection = None):
        ...

    async def count(
        self, query, *args, connection: asyncpg.Connection = None, **kwargs
    ) -> int:
        ...

    async def explain(
        self, query, *args, connection: asyncpg.Connection = None, **kwargs
    ) -> asyncpg.Record:
        ...


@singleton
class QueryLoader:
    def __init__(self):
        self._lock = threading.Lock()
        self._queries: Dict[str, aiosql.queries.Queries] = {}

    def load(self, name: str) -> aiosql.queries.Queries:
        with self._lock:
            if name not in self._queries:
                self._queries[name] = aiosql.from_path(QUERY_PATH / name, "asyncpg")
            return self._queries[name]


class BoundClient(ClientProtocol):
    """A "bound" client performs queries directly on the table specified by `name`.

    Usage:
        >>> files = BoundClient(name="file")
        >>> await files.get(1)
    """

    __slots__ = "name", "pk", "connectors", "queries"

    def __init__(
        self, name: str, *, pk: str = "id", connector: PostgresConnector = None
    ):
        self.name = name
        self.pk = pk
        self.connectors = {"main": connector} if connector else cached_connectors()
        self.queries = QueryLoader().load(name)

    def __repr__(self):
        name, client = self.name, self.connectors
        return f"<{self.__class__.__name__} {name=} {client=}>"

    def _get_connector(self, read_only: bool = False):
        if read_only and "read" in self.connectors:
            return self.connectors["read"]
        return self.connectors["main"]

    @property
    def connector(self):
        return self._get_connector(read_only=False)

    @property
    def read_connector(self):
        return self._get_connector(read_only=True)

    @retry
    async def get(self, pk: int, *, connection: asyncpg.Connection = None):
        """Get a single row with the given primary key, or None."""
        async with self.connector.connection(c=connection) as c:
            return await self.queries.get(c, **{self.pk: pk})

    @retry
    async def all(self, *, connection: asyncpg.Connection = None):
        """Get all records for this table."""
        async with self.connector.connection(c=connection) as c:
            return await self.queries.all(c)

    @contextlib.asynccontextmanager
    async def iterall(self, *, connection: asyncpg.Connection = None):
        """Get all records for this table."""
        async with self.connector.connection(c=connection) as c:
            async with self.queries.all_cursor(c) as cursor:
                yield await cursor

    @retry
    async def persist(self, *, connection: asyncpg.Connection = None, **data):
        """Persist the **data to this table."""
        async with self.connector.transaction(connection=connection) as c:
            return await self.queries.persist(c, **data)

    @retry
    async def bulk_persist(
        self,
        datum: Iterable,
        connection: asyncpg.Connection = None,
    ):
        """Bulk-persist an iterable of datum to this table."""
        async with self.connector.transaction(connection=connection) as c:
            return await self.queries.bulk_persist(c, datum)

    @retry
    async def delete(self, pk: int, *, connection: asyncpg.Connection = None):
        """Delete the record with the given primary key."""
        async with self.connector.transaction(connection=connection) as c:
            return await self.queries.delete(c, **{self.pk: pk})

    @retry
    async def bulk_delete(self, *pks: int, connection: asyncpg.Connection = None):
        """Delete the records with the given primary keys in bulk."""
        async with self.connector.transaction(connection=connection) as c:
            return await self.queries.bulk_delete(c, **{self.pk + "s": pks})

    @retry
    async def count(
        self, query: QueryFn, *args, connection: asyncpg.Connection = None, **kwargs
    ) -> int:
        sql = f"SELECT count(*) FROM ({query.sql.rstrip(';')}) AS q;"
        async with self.connector.connection(c=connection) as c:
            return await self.queries.driver_adapter.select_value(
                c,
                query_name=query.__name__,
                sql=sql,
                parameters=kwargs or args,
            )

    @retry
    async def explain(
        self,
        query: QueryFn,
        *args,
        connection: asyncpg.Connection = None,
        format: Optional[ExplainFormatT] = "json",
        **kwargs,
    ) -> Union[asyncpg.Record, str]:
        c: asyncpg.Connection
        async with self.connector.connection(c=connection) as c:
            transaction = c.transaction()
            await transaction.start()
            try:
                selector, sql = (
                    self.queries.driver_adapter.select_one,
                    f"EXPLAIN ANALYZE {query.sql}",
                )
                if format:
                    selector, sql = (
                        self.queries.driver_adapter.select_value,
                        f"EXPLAIN (FORMAT {format}) {query.sql}",
                    )
                return await selector(
                    c,
                    query_name=query.__name__,
                    sql=sql,
                    parameters=kwargs or args,
                )
            finally:
                await transaction.rollback()


class ServiceProtocol(ClientProtocol[T]):
    """The abstract protocol for a 'service'.

    A 'service' is responsible for querying a specific table.
    It also is semi-aware of in-memory dataclass representing the table data.

    By default, a service will coerce the query result to the correct model.
    """

    model: T
    client: BoundClient
    protocol: typic.SerdeProtocol
    bulk_protocol: typic.SerdeProtocol
    iterator: Callable[[T], Iterator[Tuple[str, Any]]]
    __exclude_fields__ = frozenset(("id", "created_at", "updated_at"))

    __slots__ = ("client",)

    def __init__(self, *, connector: PostgresConnector = None):
        self.client = ...

    def __init_subclass__(cls, **kwargs):
        cls.__exclude_fields__ = (
            ServiceProtocol.__exclude_fields__ | cls.__exclude_fields__
        )
        cls.protocol = typic.protocol(cls.model, is_optional=True)
        cls.bulk_protocol = typic.protocol(Iterable[cls.model])
        cls.iterator = typic.resolver.translator.iterator(cls.model)
        super().__init_subclass__()

    @_coerceable
    async def get(
        self, pk: int, *, connection: asyncpg.Connection = None, coerce: bool = True
    ):
        return await self.client.get(pk, connection=connection)

    @_coerceable(bulk=True)
    async def all(self, *, connection: asyncpg.Connection = None, coerce: bool = True):
        return await self.client.all(connection=connection)

    @contextlib.asynccontextmanager
    async def iterall(
        self, *, connection: asyncpg.Connection = None, coerce: bool = True
    ):
        async with self.client.iterall(connection=connection) as cursor:
            yield CoercingCursor(self, cursor) if coerce else cursor

    @_coerceable
    async def delete(
        self, pk: int, *, connection: asyncpg.Connection = None, coerce: bool = True
    ):
        return await self.client.delete(pk, connection=connection)

    async def bulk_delete(
        self, *pks: int, connection: asyncpg.Connection = None, coerce: bool = True
    ):
        return await self.client.bulk_delete(*pks, connection=connection)

    @classmethod
    def _get_kvs(cls, model: T) -> Mapping:
        return {
            field: value
            for field, value in cls.iterator(model)
            if field not in cls.__exclude_fields__
        }

    @_coerceable
    async def persist(
        self,
        *,
        model: T = None,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
        **data,
    ):
        if model:
            data = self._get_kvs(model)
        return await self.client.persist(connection=connection, **data)

    @_coerceable(bulk=True)
    async def bulk_persist(
        self,
        *,
        models: Iterable[T] = (),
        data: Iterable[Mapping] = (),
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        if models:
            data = (self._get_kvs(m) for m in models)
        return await self.client.bulk_persist(data, connection=connection)

    async def select(
        self,
        *fields,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
        count: bool = True,
        **where: Any,
    ) -> Tuple[Optional[int], Collection[Union[T, asyncpg.Record]]]:
        query, args = self._build_select(*fields, **where)
        if count:
            explain_query = f"EXPLAIN (FORMAT JSON) {query}"
            await self.client.connector.initialize()
            explain_result, result = await asyncio.gather(
                self._select_sql(explain_query, *args, coerce=False, value=True),
                self._select_sql(query, *args, connection=connection, coerce=coerce),
            )
            return explain_result[0]["Plan"]["Plan Rows"], result
        return None, await self._select_sql(
            query, *args, connection=connection, coerce=coerce
        )

    @contextlib.asynccontextmanager
    async def select_cursor(
        self,
        *fields,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
        count: bool = True,
        sort_field: str = None,
        sort_desc: bool = False,
        **where,
    ) -> Tuple[Optional[int], Union[CoercingCursor, asyncpg.connection.cursor.Cursor]]:
        query, args = self._build_select(*fields, **where)

        if sort_field:
            query = (
                f"{query} order by {sort_field} {'ASC' if not sort_desc else 'DESC'}"
            )
        if count:
            explain_query = f"EXPLAIN (FORMAT JSON) {query}"
            await self.client.connector.initialize()
            explain_task = asyncio.create_task(
                self._select_sql(explain_query, *args, coerce=False, value=True)
            )
            async with self._select_sql_cursor(
                query, *args, connection=connection, coerce=coerce
            ) as cursor:
                count = (await explain_task)[0]["Plan"]["Plan Rows"]
                yield count, cursor
        else:
            async with self._select_sql_cursor(
                query, *args, connection=connection, coerce=coerce
            ) as cursor:
                yield None, cursor

    @_coerceable(bulk=True)
    @retry
    async def _select_sql(
        self,
        query: str,
        *args,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
        value: bool = False,
    ) -> Collection[Union[T, asyncpg.Record]]:
        async with self.client.connector.connection(c=connection) as c:
            selector = (
                self.client.queries.driver_adapter.select_value
                if value
                else self.client.queries.driver_adapter.select
            )
            return await selector(c, "all", sql=query, parameters=args)

    @contextlib.asynccontextmanager
    async def _select_sql_cursor(
        self,
        query: str,
        *args,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> Union[CoercingCursor, asyncpg.connection.cursor.Cursor]:
        async with self.client.connector.connection(c=connection) as c:
            async with self.client.queries.driver_adapter.select_cursor(
                c, "all", sql=query, parameters=args
            ) as factory:
                cursor = await factory
                yield CoercingCursor(self, cursor) if coerce else cursor

    @property
    def _tablename(self) -> str:
        return underscore(self.model.__name__)

    def _get_select_clause(self, *fields) -> str:
        sqlfields = ", ".join(fields) or "*"
        return f"SELECT {sqlfields} FROM {self._tablename}"

    def _build_select(self, *fields: str, **where: Any) -> Tuple[str, Tuple[Any, ...]]:
        select = self._get_select_clause(*fields)
        args = tuple(where.values())
        params = tuple(where.keys())
        ands = " AND ".join(f"{p} = ${i}" for i, p in enumerate(params, start=1))
        filters = ands and f"WHERE {ands}"
        return f"{select}\n{filters}", args

    async def count(
        self, query: Callable, *args, connection: asyncpg.Connection = None, **kwargs
    ) -> int:
        query_fn = getattr(self.client.queries, query.__name__)
        return await self.client.count(query_fn, *args, connection=connection, **kwargs)

    async def explain(
        self, query, *args, connection: asyncpg.Connection = None, **kwargs
    ) -> asyncpg.Record:
        query_fn = getattr(self.client.queries, query.__name__)
        return await self.client.explain(
            query_fn, *args, connection=connection, **kwargs
        )

    def load_child_queries(self, name, queries: Queries):
        self.client.queries.add_child_queries(child_name=name, child_queries=queries)


# endregion
