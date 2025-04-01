from __future__ import annotations

import asyncio
import contextlib
import contextvars
import functools
from typing import Awaitable, Callable, Mapping, Optional, Type, TypeVar, overload

import asyncpg
import orjson
import typic
from asyncpg.transaction import Transaction
from maven import feature_flags
from mmlib.config import apply_app_environment_namespace

import constants
from app.eligibility import constants as e9y_constants
from config import settings
from db.clients.utils import (
    _TRANSIENT,
    LOG,
    dump_json,
    dump_jsonb,
    load_jsonb,
    singleton,
)

# Event-loop-local state for connection
CONNECTORS: contextvars.ContextVar[
    Optional[Mapping[str, PostgresConnector]]
] = contextvars.ContextVar("pg_connector", default=None)

# region connector object


class PostgresConnector:
    """A simple connector for asyncpg."""

    __slots__ = "dsn", "pool", "initialized", "schema", "_loop", "__dict__"

    def __init__(self, dsn, pool: asyncpg.pool.Pool = None):
        self.dsn = dsn
        self.pool: asyncpg.pool.Pool = pool or create_pool(dsn)
        self.initialized = False
        self._loop = asyncio.get_event_loop()

    def __repr__(self):
        dsn, initialized, open = self.dsn, self.initialized, self.open
        return f"<{self.__class__.__name__} {dsn=} {initialized=} {open=}>"

    async def initialize(self):
        if not self.initialized:
            await self.pool
            self.initialized = True

    @contextlib.asynccontextmanager
    async def connection(
        self, *, timeout: int = 20, c: asyncpg.Connection = None
    ) -> asyncpg.Connection:
        await self.initialize()
        if c:
            yield c
        else:
            async with self.pool.acquire(timeout=timeout) as conn:
                yield conn

    @contextlib.asynccontextmanager
    async def transaction(
        self, *, connection: asyncpg.Connection = None
    ) -> Transaction:
        async with self.connection(c=connection) as conn:
            async with conn.transaction():
                yield conn

    async def close(self, timeout: int = 10):
        try:
            await asyncio.wait_for(self.pool.close(), timeout=timeout)
        finally:
            self.initialized = False

    @property
    def open(self) -> bool:
        return not self.pool._closed


# endregion


# region connector methods


@functools.lru_cache()
def compose_dsn(
    scheme: str,
    user: str,
    password: str,
    host: str,
    port: int,
    db: str,
    *schemas: str,
    ssl: bool = True,
    app: str = None,
) -> typic.DSN:
    sslmode = "prefer" if ssl else "disable"
    dsn = f"{scheme}://{user}:{password}@{host}:{str(port)}/{apply_app_environment_namespace(db)}?sslmode={sslmode}"
    if schemas:
        search_path = ",".join(schemas)
        dsn += f"&search_path={search_path}"
    if app:
        dsn += f"&application_name={app}"
    return typic.DSN(dsn)


def get_dsn(read_only: bool = False) -> typic.DSN:
    app_name = "-".join(n for n in (constants.APP_NAME, constants.APP_FACET) if n)
    db_settings = settings.DB()
    the_db_host = db_settings.host
    the_db_port = db_settings.read_port if read_only else db_settings.main_port

    use_new_db = feature_flags.bool_variation(
        e9y_constants.E9yFeatureFlag.RELEASE_ELIGIBILITY_DATABASE_INSTANCE_SWITCH,
        default=False,
    )
    if use_new_db:
        the_db_host = db_settings.host
        the_db_port = the_db_port + 4

    return compose_dsn(
        db_settings.scheme,
        db_settings.user,
        db_settings.password,
        the_db_host,
        the_db_port,
        db_settings.db,
        "eligibility",
        "public",
        app=app_name,
    )


async def initialize(*, sighandlers: bool = False):
    for conn_type, conn in cached_connectors().items():
        await conn.initialize()


async def _init_connection(connection: asyncpg.Connection):
    await connection.set_type_codec(
        "json",
        # orjson encodes to binary, but libpq (the c bindings for postgres)
        # can't write binary data to JSONB columns.
        # https://github.com/lib/pq/issues/528
        # This is still orders of magnitude faster than any other lib.
        encoder=dump_json,
        decoder=orjson.loads,
        schema="pg_catalog",
    )

    await connection.set_type_codec(
        "jsonb",
        # orjson encodes to binary, but libpq (the c bindings for postgres)
        # can't natively write binary data to JSONB columns.
        # https://github.com/lib/pq/issues/528
        # https://github.com/MagicStack/asyncpg/issues/783#issuecomment-883158242
        encoder=dump_jsonb,
        decoder=load_jsonb,
        schema="pg_catalog",
        format="binary",
    )


def create_pool(dsn: str, *, loop: asyncio.AbstractEventLoop = None, **kwargs):
    kwargs.setdefault("init", _init_connection)
    kwargs.setdefault("loop", loop)
    kwargs.setdefault("min_size", 10)
    kwargs.setdefault("max_size", 10)
    return asyncpg.create_pool(dsn, **kwargs)


@overload
def retry(
    func: _FuncT,
) -> _FuncT:
    ...


@overload
def retry(
    *errors: Type[BaseException],
    retries: int = 10,
    delay: float = 0.1,
) -> Callable[[_FuncT], _FuncT]:
    ...


def retry(
    func: _FuncT = None,
    *errors: Type[BaseException],
    retries: int = 10,
    delay: float = 0.1,
):
    """Automatically retry a database operation on a transient error.

    Default errors are:
        - asyncpg.DeadlockDetectedError
        - asyncpg.TooManyConnectionsError
        - asyncpg.PostgresConnectionError
    """
    errors = (*{*_TRANSIENT, *errors},)
    logger = LOG.bind(
        watching_errors=[e.__name__ for e in errors],
        retry_delay=delay,
        max_retries=retries,
    )

    def _retry(
        func_: _FuncT,
        *,
        _retries=retries,
        _errors=errors,
        _logger=logger,
    ) -> _FuncT:
        @functools.wraps(func_)
        async def _retry(*args, **kwargs):
            try:
                return await func_(*args, **kwargs)
            except _errors as e:
                _logger.info(
                    "Got a watched error. Entering retry loop.",
                    error=e.__class__.__name__,
                    exception=str(e),
                )
                tries = 0
                while tries < _retries:
                    tries += 1
                    await asyncio.sleep(delay)
                    try:
                        return await func_(*args, **kwargs)
                    except _errors:
                        _logger.warning("Failed on retry.", retry=tries)
                _logger.error("Couldn't recover on retries. Re-raising original error.")
                raise e

        return _retry

    return _retry(func) if func else _retry


_FuncT = TypeVar("_FuncT", bound=Callable[[...], Awaitable])


async def teardown(*, signal: int = 0):
    if (connectors := CONNECTORS.get()) is not None:
        for conn_type, conn in connectors.items():
            try:
                await conn.close()
            finally:
                continue
        CONNECTORS.set(None)


# endregion


def cached_connectors(*, loop: asyncio.AbstractEventLoop = None, **kwargs):
    if (connector := CONNECTORS.get()) is None:
        main_dsn: str = get_dsn(read_only=False)
        read_dsn: str = get_dsn(read_only=True)

        main_pool = create_pool(main_dsn, loop=loop, **kwargs)
        read_pool = create_pool(read_dsn, loop=loop, **kwargs)

        main_connector = PostgresConnector(main_dsn, pool=main_pool)
        read_connector = PostgresConnector(read_dsn, pool=read_pool)

        CONNECTORS.set({"main": main_connector, "read": read_connector})

        connector = CONNECTORS.get()

    return connector


@singleton
class ApplicationConnectors:
    def __init__(self):
        self.connectors = {}


def application_connectors():
    global_connector = ApplicationConnectors()
    if not global_connector.connectors:
        main_dsn: str = get_dsn(read_only=False)
        read_dsn: str = get_dsn(read_only=True)

        main_pool = create_pool(main_dsn)
        read_pool = create_pool(read_dsn)

        main_connector = PostgresConnector(main_dsn, pool=main_pool)
        read_connector = PostgresConnector(read_dsn, pool=read_pool)

        global_connector.connectors = {"main": main_connector, "read": read_connector}

    return global_connector.connectors
