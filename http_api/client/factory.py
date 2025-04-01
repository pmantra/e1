from __future__ import annotations

import logging

from aiohttp import web
from http_api.client.healthcheck import init_healthchecks
from http_api.client.views import init_views

logger = logging.getLogger("e9y_api.client.http")


def create_app() -> web.Application:
    app = web.Application(logger=logger)
    init_healthchecks(app)
    init_views(app)

    return app
