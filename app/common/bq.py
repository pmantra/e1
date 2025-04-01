from __future__ import annotations

import asyncio
import contextvars
import datetime
from typing import Any, Iterable, Mapping, Optional, Sequence, TypedDict

import orjson
import typic
from ddtrace import tracer
from mmlib.ops.log import getLogger
from mmstream import pubsub

import constants
from config import settings

log = getLogger(__name__)

PUBLISHER: contextvars.ContextVar[
    Optional[pubsub.PubSubPublisher]
] = contextvars.ContextVar("bq_publisher", default=None)
LOCK: contextvars.ContextVar[Optional[asyncio.Lock]] = contextvars.ContextVar(
    "bq_lock", default=None
)


def lock() -> asyncio.Lock:
    if (lck := LOCK.get()) is None:
        lck = asyncio.Lock()
        LOCK.set(lck)
    return lck


async def bq() -> pubsub.PubSubPublisher:
    if (publisher := PUBLISHER.get()) is None:
        async with lock():
            gcp_settings = settings.GCP()
            publisher = pubsub.PubSubPublisher(
                project=gcp_settings.project,
                topic=gcp_settings.data_export_topic,
                name=f"{constants.APP_NAME}-worker",
            )
            await publisher.initialize()
            PUBLISHER.set(publisher)
    return publisher


def chunks(lst: Sequence, n: int) -> Iterable[Sequence]:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def _serialize_nested(r: dict) -> dict:
    return {
        k: orjson.dumps(v).decode("utf-8") if k in _SERIALIZE else v
        for k, v in r.items()
    }


_SERIALIZE = frozenset({"record"})


def _payload(table: str, rows: Iterable[dict], exported_at: datetime) -> BQPayload:
    return {
        "table": table,
        "rows": [
            {
                **_serialize_nested(typic.primitive(r)),
                "exported_at": exported_at,
            }
            for r in rows
        ],
    }


class BQPayload(TypedDict):
    table: str
    rows: Sequence[Mapping[str, Any]]


@tracer.wrap()
async def export_rows_to_table(
    table: str, rows: Sequence, batch_size: int = 2_000
) -> Sequence[Optional[str]]:
    if not rows:
        return []

    try:
        publisher = await bq()
        exported_at = datetime.datetime.utcnow()
        results = await asyncio.gather(
            *(
                publisher.publish(
                    pubsub.PublisherMessage(message=_payload(table, batch, exported_at))
                )
                for batch in chunks(rows, batch_size)
            )
        )
    except Exception as exc:
        log.exception(f"Got an exception when publishing message: {exc}")
        return []

    return results
