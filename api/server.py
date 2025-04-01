from __future__ import annotations

import contextlib
import contextvars
import functools
import logging
from typing import Collection

import aiodebug.log_slow_callbacks
import ddtrace
import ddtrace.ext
import inflection
import structlog
from ddtrace.contrib.grpc import constants
from ddtrace.contrib.grpc import utils as ddutils
from ddtrace.propagation import http
from google.rpc.error_details_pb2 import BadRequest, ErrorInfo
from grpclib import events, server, utils
from grpclib.reflection import service

from app.eligibility.client_specific import service as client_specific
from app.utils.status_code_mapping import grpc_to_http_status_code
from db.clients import postgres_connector
from db.mono import client as mono

logger = logging.getLogger(__name__)
HTTP_STATUS_CODE = "http.status_code"
GRPC_STATUS_MESSAGE = "grpc.status.message"
SERVICE_NS_TAG = "service_ns"
TEAM_NS_TAG = "team_ns"
SERVICE_NS = "eligibility-api"
TEAM_NS = "eligibility"


def factory(
    services: Collection[server.IServable],
    reflection: bool = True,
):
    # reflection lets grpc clients explore the api methods and data types through the
    # api itself
    if reflection:
        services = service.ServerReflection.extend(services)

    srv = server.Server(services)
    events.listen(srv, events.RecvRequest, listen_start_span)
    events.listen(srv, events.SendMessage, listen_finish_span)
    events.listen(srv, events.SendTrailingMetadata, listen_set_error_status)
    return srv


async def serve(
    services: Collection[server.IServable],
    host: str = "0.0.0.0",
    port: int = 50051,
    reflection: bool = True,
) -> None:
    srv = factory(services)
    aiodebug.log_slow_callbacks.enable(1)
    async with app_context(host=host, port=port, reflection=reflection):
        with utils.graceful_exit([srv]):
            await srv.start(host, port)
            logger.info("Serving GRPC.")
            await srv.wait_closed()
            logger.info("Done serving GRPC.")


async def listen_start_span(event: events.RecvRequest):
    meta = dict(event.metadata or {})
    context = http.HTTPPropagator.extract(meta)
    if context:
        ddtrace.tracer.context_provider.activate(context)
    tags = _extract_internal_tags(meta)
    tags[TEAM_NS_TAG] = TEAM_NS
    tags[SERVICE_NS_TAG] = SERVICE_NS
    ddtrace.tracer.set_tags(tags)
    span = ddtrace.tracer.trace(
        name="grpc",
        resource=event.method_name,
        span_type=ddtrace.ext.SpanTypes.GRPC,
    )
    ddutils.set_grpc_method_meta(
        span=span,
        method=event.method_name,
        method_kind=constants.GRPC_METHOD_KIND_UNARY,
    )


def _extract_internal_tags(meta: dict[str, str]) -> dict[str, str]:
    return {
        span_tag: v for k, v in meta.items() if (span_tag := _gettag(k)) is not None
    }


@functools.cache
def _gettag(k: str) -> str | None:
    if (lk := k.lower()).startswith(_HEADER_PREFIX):
        return (
            f"{_SPAN_PREFIX}.{inflection.underscore(lk.removeprefix(_HEADER_PREFIX))}"
        )
    return None


_HEADER_PREFIX = "x-maven-"
_SPAN_PREFIX = "maven"


async def listen_set_error_status(event: events.SendTrailingMetadata):
    span = ddtrace.tracer.current_span()
    if span and not span.finished:
        tags = {
            constants.GRPC_STATUS_CODE_KEY: str(event.status.name),
            GRPC_STATUS_MESSAGE: event.status_message,
            HTTP_STATUS_CODE: grpc_to_http_status_code(event.status),
            **_get_error_tags(event),
        }
        span.set_tags(tags)
    span.finish()


def _get_error_tags(event: events.SendTrailingMetadata) -> dict[str, str]:
    errinfo: ErrorInfo | BadRequest | None = next(
        iter(event.status_details or ()), None
    )
    if errinfo is None:
        return {}
    if isinstance(errinfo, ErrorInfo):
        return {
            f"grpc.status.metadata.{k}": str(v) for k, v in errinfo.metadata.items()
        }
    if isinstance(errinfo, BadRequest):
        return {
            f"grpc.status.violation.{fv.field}": str(fv.description)
            for fv in errinfo.field_violations
        }


async def listen_finish_span(event: events.SendMessage):
    span = ddtrace.tracer.current_span()
    if span and not span.finished:
        span.set_tag(constants.GRPC_STATUS_CODE_KEY, str(server.Status))
        span.set_tag(HTTP_STATUS_CODE, grpc_to_http_status_code(server.Status))


@contextlib.asynccontextmanager
async def app_context(**kwargs):
    structlog.contextvars.bind_contextvars(**kwargs)
    await postgres_connector.initialize()
    await mono.initialize()
    await client_specific.initialize()
    ctx = contextvars.copy_context()
    yield ctx
    await client_specific.teardown()
    await mono.teardown()
    await postgres_connector.teardown()
