import logging
import logging.config
from typing import Callable, List, Tuple

import orjson
import structlog
import typic
from mmlib.ops.log import (
    add_severity,
    make_set_message_key,
    resolve_level,
    trace_processor,
    use_colors,
)


def json_renderer(logger, name: str, event_dict: dict):
    return orjson.dumps(event_dict, default=typic.primitive).decode()


def configure(
    project: str,
    service: str,
    version: str,
    json: bool,
    level: str,
):
    """Set up structlog with formatting and context providers for your app."""
    shared, structured, renderer = _get_processors(service, project, version, json)
    formatting = {
        "()": structlog.stdlib.ProcessorFormatter,
        "processor": renderer,
        "foreign_pre_chain": shared,
    }
    level = resolve_level(level)
    # we specify force because ddtrace messses with the root handler in a way
    # that screws up our logging, so we're force-resetting the root handler to
    # be what we want
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {"default": formatting},
            "handlers": {
                "default": {
                    "level": level,
                    "class": "logging.StreamHandler",
                    "formatter": "default",
                    "stream": "ext://sys.stdout",
                }
            },
            "loggers": {
                "": {
                    "handlers": ["default"],
                    "level": level,
                    "propagate": True,
                    "force": True,
                }
            },
        }
    )
    structlog.configure(
        processors=shared + structured,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def _get_processors(
    service: str, project: str, version: str, json: bool
) -> Tuple[List, List, Callable]:
    def add_project_and_app_name(logger, method_name, event_dict):
        event_dict.update(app=service, project=project)
        return event_dict

    shared = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
        add_project_and_app_name,
        add_severity,
    ]
    structured = [structlog.stdlib.PositionalArgumentsFormatter()]

    if json:
        # `json=True` is sort of a proxy for whether we're in a context that will
        # be reporting to GCP. In those cases, we want these processors so that our
        # logging plays nicely with our monitoring.
        shared.extend(
            (
                trace_processor,
                make_set_message_key(service, version),
            )
        )
        renderer = json_renderer
    else:
        # Otherwise, we're probably testing or running in a dev environment.
        # Make it easy on the eyes.
        renderer = structlog.dev.ConsoleRenderer(colors=use_colors())
    # Add the formatter wrapper as the last callee in the processors for structlog.
    structured.append(structlog.stdlib.ProcessorFormatter.wrap_for_formatter)
    return shared, structured, renderer
