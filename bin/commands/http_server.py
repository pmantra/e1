from http_api.bin.http import run

from bin.commands.base import BaseAppCommand

SUBTITLE = "HTTP API"


class HTTPAPICommand(BaseAppCommand):
    """Run the REST API server.

    http-api
    """

    name = "http-api"
    subtitle = SUBTITLE

    def handle(self) -> int:
        run()
        return 0
