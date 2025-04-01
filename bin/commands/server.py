import asyncio

from grpclib.health.service import OVERALL, Health
from mmlib.grpc.server import harvest_health_checks, harvest_services

from api import handlers as app_handlers
from api import server
from bin.commands.base import BaseAppCommand

SUBTITLE = """
╔═╗┌─┐┬─┐┬  ┬┌─┐┬─┐
╚═╗├┤ ├┬┘└┐┌┘├┤ ├┬┘
╚═╝└─┘┴└─ └┘ └─┘┴└─
"""


class APICommand(BaseAppCommand):
    """Run the GRPC API server.

    api
    """

    name = "api"
    subtitle = SUBTITLE

    def handle(self) -> int:
        services = list(harvest_services(app_handlers))
        health_checks = list(harvest_health_checks(app_handlers))

        # https://grpclib.readthedocs.io/en/latest/health.html
        health = Health({OVERALL: health_checks})

        services.append(health)

        asyncio.run(server.serve(services))
        return 0
