from __future__ import annotations

import contextvars
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Collection,
    Iterable,
    Iterator,
    Optional,
    Tuple,
    Union,
)

import aioredis
import orjson
import typic
from mmlib.redis import RedisConnector
from mmlib.redis.client import make_dsn

from db.clients.postgres_connector import retry

CONNECTOR: contextvars.ContextVar[Optional[RedisConnector]] = contextvars.ContextVar(
    "redis_connector", default=None
)


def cached_connector(dsn: str):
    if (connector := CONNECTOR.get()) is None:
        connector = RedisConnector(dsn)
        CONNECTOR.set(connector)
    return connector


async def initialize(*, sighandlers: bool = False):
    from config import settings

    redis_settings = settings.Redis()
    dsn = make_dsn(redis_settings.host, password=redis_settings.password)
    redis = cached_connector(dsn)
    await redis.initialize()


async def teardown(*, signal: int = 0):
    if (connector := CONNECTOR.get()) is not None:
        await connector.teardown()


class Scanner:
    """An async iterator which yields successive 'pages' of keys matching a pattern.

    Notes:
        You probably want to initialize this via RedisKeyStore.scanner(...).
    """

    __slots__ = (
        "tmp",
        "pattern",
        "pagesize",
        "start",
        "stop",
        "cursor",
        "loc",
        "exhausted",
        "values",
        "op",
        "opargs",
        "indices",
        "_state",
    )

    def __repr__(self) -> str:
        op, pattern, start, stop, cursor = (
            self.op,
            self.pattern,
            self.start,
            self.stop,
            self.cursor,
        )
        return (
            f"<{self.__class__.__name__} {op=} {pattern=} {start=} {stop=} {cursor=}>"
        )

    def __init__(
        self,
        tmp: RedisKeyStore,
        *opargs,
        op: str = "scan",
        indices: Iterable[str] = (),
        pattern: str = None,
        pagesize: int = 10,
        start: int = 0,
        stop: int = None,
        values: bool = True,
    ):
        self.tmp = tmp
        self.pattern = pattern
        self.pagesize = pagesize
        self.start = start
        self.stop = stop
        self.cursor = start
        self.loc = start
        self.exhausted = False
        self.values = values
        self.op = op
        self.opargs = opargs
        self.indices = indices
        self._state = None

    async def _init_intersection(self):
        if self._state or not self.indices:
            return
        key = "|".join(self.indices)
        async with self.tmp.redis.connection() as c:
            await c.sinterstore(key, *self.indices)
            self._state = self.op, self.opargs
            self.op, self.opargs = "sscan", (key,)

    async def _del_intersection(self):
        if not self._state or self.indices:
            return
        try:
            await self.tmp.delete("|".join(self.indices))
        finally:
            self.op, self.opargs = self._state

    async def next(
        self, *, c: aioredis.Redis = None, once: bool = True
    ) -> Collection[Any]:
        """Get the next page of keys which match the defined pattern."""
        try:
            await self._init_intersection()
            async with self.tmp.redis.connection(c=c) as c:
                scan = getattr(c, self.op)
                ix = 0
                page = []
                while ix < self.pagesize and not self.exhausted:
                    self.cursor, _page = await scan(
                        *self.opargs,
                        cursor=self.cursor,
                        match=self.pattern,
                        count=self.pagesize,
                    )
                    ix += len(_page)
                    self.loc += ix
                    self.exhausted = self.cursor == 0 or (
                        self.stop and self.loc >= self.stop
                    )
                    page.extend(_page)
                if self.values:
                    page = [*(await self.tmp.imget(*page, c=c))]

                return page
        finally:
            if once:
                await self._del_intersection()

    async def count(self) -> int:
        """Get the size of the full result-set.

        This means we have to do a full iteration of all results, starting at 0.
        """
        await self._init_intersection()
        if self.indices:
            async with self.tmp.redis.connection() as c:
                return await c.scard(*self.opargs)

        counter = Scanner(
            self.tmp,
            *self.opargs,
            op=self.op,
            pattern=self.pattern,
            pagesize=100_000,
            values=False,
        )
        size = 0
        async for page in counter:
            size += len(page)
        return size

    def reset(self):
        """Reset the cursor to the defined start."""
        self.cursor = self.loc = self.start

    def seek(self, loc: int):
        """Set the cursor to the desired location."""
        self.cursor = self.loc = loc

    async def __aiter__(self) -> AsyncIterator[Collection[Any]]:
        try:
            await self._init_intersection()
            page = await self.next(once=False)
            yield page
            while not self.exhausted:
                yield await self.next(once=False)
        finally:
            await self._del_intersection()


def _retry(
    func: Callable[..., Awaitable] = None,
    retries: int = 10,
    delay: float = 0.1,
):
    return retry(
        func,
        aioredis.exceptions.ConnectionError,
        aioredis.exceptions.TimeoutError,
        retries=retries,
        delay=delay,
    )


class RedisKeyStore:
    """A Redis client for working with simple key/value pairs."""

    __slots__ = ("redis",)

    def __init__(self, dsn: str = "redis://localhost:6379/0"):
        self.redis = cached_connector(dsn)

    def __repr__(self):
        redis = self.redis.redis
        return f"<{self.__class__.__name__} {redis=}>"

    @staticmethod
    def key(*args: Union[str, int]) -> str:
        """Generate a key from a series of ints or strings."""
        return ":".join(str(a) for a in args)

    @_retry
    async def set(
        self,
        key: str,
        value: Any,
        *,
        c: aioredis.Redis = None,
        t: aioredis.Redis = None,
    ):
        """Save a value as a simple Redis string at the specified key."""
        c: aioredis.Redis
        if t:
            t.set(key, orjson.dumps(value, default=typic.primitive))
        else:
            async with self.redis.connection(c=c) as c:
                return await c.set(key, orjson.dumps(value, default=typic.primitive))

    @_retry
    async def mset(
        self, *, c: aioredis.Redis = None, t: aioredis.Redis = None, **pairs: Any
    ):
        """Set a series of key-value pairs."""
        if pairs:
            dumped = {
                k: orjson.dumps(v, default=typic.primitive) for k, v in pairs.items()
            }
            if t:
                return t.mset(dumped)
            else:
                c: aioredis.Redis
                async with self.redis.connection(c=c) as c:
                    return await c.mset(dumped)

    @staticmethod
    def _iter_key_values(
        keys, values, filter_null: bool = True
    ) -> Iterator[Tuple[str, Any]]:
        if filter_null:
            for i, v in enumerate(values):
                if not v:
                    continue
                data = orjson.loads(v)
                key = keys[i]
                data["key"] = keys[i]
                yield key, data
        else:
            for i, v in enumerate(values):
                key = keys[i]
                if not v:
                    yield key, None
                else:
                    data = orjson.loads(v)
                    data["key"] = keys[i]
                    yield key, data

    @_retry
    async def imget(
        self,
        *keys: str,
        filter_null: bool = True,
        with_keys: bool = False,
        c: aioredis.Redis = None,
    ) -> Iterator[Any]:
        if not keys:
            return iter([])
        c: aioredis.Redis
        async with self.redis.connection(c=c) as c:
            values = await c.mget(*keys)
        ikvs = self._iter_key_values(keys, values, filter_null=filter_null)
        return ikvs if with_keys else (v for k, v in ikvs)

    @_retry
    async def get(
        self, key: str, *, default: Any = None, c: aioredis.Redis = None
    ) -> Any:
        """Fetch the value located at the specified key."""
        c: aioredis.Redis
        async with self.redis.connection(c=c) as c:
            value = await c.get(key)
            if value:
                value = (
                    orjson.loads(value)
                    if value and isinstance(value, (str, bytes))
                    else value
                )
                if isinstance(value, dict):
                    value["key"] = key
                return value
            return default

    @_retry
    async def delete(self, *keys: str, c: aioredis.Redis = None):
        c: aioredis.Redis
        async with self.redis.connection(c=c) as c:
            return await c.delete(*keys)

    @_retry
    async def delete_pattern(self, pattern: str, c: aioredis.Redis = None):
        """Delete all keys matching a pattern"""
        c: aioredis.Redis
        async with self.redis.connection(c=c) as c:
            async for key in c.scan_iter(match=pattern):
                await c.delete(key)

    @_retry
    async def incr(self, key: str, *, amount: int = 1, c: aioredis.Redis = None) -> Any:
        """Increment the value located at the specified key by 1"""
        c: aioredis.Redis
        async with self.redis.connection(c=c) as c:
            return await c.incr(key, amount=amount)

    def scanner(
        self,
        *opargs,
        op: str = "scan",
        indices: Iterable[str] = (),
        pattern: str = None,
        pagesize: int = 10,
        start: int = 0,
        stop: int = None,
        values: bool = True,
    ) -> Scanner:
        """Get a `Scanner` instance for querying a specific key-pattern."""
        return Scanner(
            self,
            *opargs,
            op=op,
            indices=indices,
            pattern=pattern,
            pagesize=pagesize,
            start=start,
            stop=stop,
            values=values,
        )

    query = scanner
