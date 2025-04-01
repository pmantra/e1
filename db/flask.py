import asyncio
import functools
from typing import Awaitable, Callable, TypeVar

import ddtrace
from flask import Flask
from mmlib.ops import log

from db.clients import postgres_connector

__all__ = ("init_app", "sync", "synchronize")

logger = log.getLogger(__name__)

SERVICE_NS_TAG = "service_ns"
TEAM_NS_TAG = "team_ns"
SERVICE_NS = "eligibility-api"
TEAM_NS = "eligibility"


def _init_thread():
    # Check for an event loop and set one if it's not available.
    # asyncio event loops are thread-safe, and are only created automatically within the
    #   main thread of an application.
    # So we check for one, and if it's not there, we create it.
    # We're not using `get_running_loop()` because we don't care if it's running,
    #   just that it exists.
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    # Eager-init the connection to the DB.
    connections = postgres_connector.application_connectors()
    connection = connections["main"]
    loop.run_until_complete(connection.initialize())


def _cancel_all_tasks(loop):
    to_cancel = asyncio.all_tasks(loop)
    if not to_cancel:
        return

    for task in to_cancel:
        task.cancel()

    loop.run_until_complete(
        asyncio.gather(*to_cancel, loop=loop, return_exceptions=True)
    )

    for task in to_cancel:
        if task.cancelled():
            continue
        if task.exception() is not None:
            loop.call_exception_handler(
                {
                    "message": "unhandled exception during asyncio.run() shutdown",
                    "exception": task.exception(),
                    "task": task,
                }
            )


def _clear_tasks(exc=None):
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.stop()
        loop.run_until_complete(loop.shutdown_asyncgens())
        _cancel_all_tasks(loop)
        # Teardown the connection pool.
        # loop.run_until_complete(postgres_connector.teardown())
    except Exception as e:
        logger.warning("Got exception on request teardown.", exception=str(e))


def init_app(app: Flask):
    ddtrace.tracer.on_start_span(_inject_service_ns_info)
    app.before_request(_init_thread)
    app.teardown_request(_clear_tasks)


def _inject_service_ns_info(span):
    span = span or ddtrace.tracer.current_root_span()
    span.set_tag(SERVICE_NS_TAG, str(SERVICE_NS))
    span.set_tag(TEAM_NS_TAG, str(TEAM_NS))


T = TypeVar("T")


def sync(coro: Awaitable[T]) -> T:
    """Run a coroutine within the app-context's event loop and return the result.

    Usage:
        >>> from flask import Flask
        >>> from db.flask import init_app, sync
        >>>
        >>> app = Flask("foo")
        >>> init_app(app)
        >>>
        >>> async def foo():
        ...     return "bar"
        ...
        >>> with app.app_context():
        ...     print(sync(foo()))
        ...
        bar
    """
    loop = asyncio.get_event_loop()
    # Schedule the coroutine as a task, which will cause it
    # to inherit all registered contextvars.
    task = loop.create_task(coro)
    # Run the loop until the task is complete.
    return loop.run_until_complete(task)


def synchronize(fn: Callable[..., Awaitable[T]]) -> Callable[..., T]:
    """A decorator which allows you to write async code which will execute sync.

    Usage:
        >>> import flask
        ... from db.flask import synchronize, init_app
        ... from db.clients.member_client import Members
        ... app = flask.Flask("foo")
        ... init_app(app)
        ...
        >>> @synchronize
        ... async def get_member(id):
        ...     members = Members()
        ...     return await members.get(id)
        ...
        >>> with app.app_context():
        ...     print(get_member(1))
        ...
        None
    """

    @functools.wraps(fn)
    def _synchronize(*args, **kwargs):
        return sync(fn(*args, **kwargs))

    return _synchronize
