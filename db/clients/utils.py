from __future__ import annotations

from typing import Any

import asyncpg
import orjson
import typic
from mmlib.ops import log

LOG = log.getLogger(__name__)

_TRANSIENT = (
    asyncpg.DeadlockDetectedError,
    asyncpg.TooManyConnectionsError,
    asyncpg.PostgresConnectionError,
)


def dump_json(o) -> str:
    return orjson.dumps(o, default=typic.tojson).decode()


def dump_jsonb(o: Any) -> bytes:
    dumped = typic.tojson(o)
    return b"\x01" + dumped


def load_jsonb(o: bytes) -> Any:
    return orjson.loads(o[1:])


def singleton(cls):
    """
    decorator to make a class singleton
    usage:
    @singleton
    class MyClass:
        pass

    See QueryLoader as example.
    """
    instances = {}

    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return get_instance
