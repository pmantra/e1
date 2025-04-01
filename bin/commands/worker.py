import logging

import constants
from bin.commands.base import BaseAppCommand
from db.clients import postgres_connector

SUBTITLE = """
╦ ╦┌─┐┬─┐┬┌─┌─┐┬─┐
║║║│ │├┬┘├┴┐├┤ ├┬┘
╚╩╝└─┘┴└─┴ ┴└─┘┴└─
"""


class WorkerCommand(BaseAppCommand):
    """Run the worker process for handling asynchronous tasks.

    worker
    """

    name = "worker"
    subtitle = SUBTITLE

    def handle(self) -> int:
        from app.worker.pubsub import subscriptions
        from app.worker.redis import stream_supervisor as redis_streams
        from config import settings
        from db import redis
        from db.mono import client as mclient

        project = settings.GCP().project
        redis_streams.project = subscriptions.project = project
        subscriptions.name = redis_streams.name = f"{constants.APP_NAME}-{self.name}"
        subscriptions.pod = redis_streams.pod = "core_services"
        redis_streams.attach_supervisor(subscriptions)
        redis_streams.propagate_metadata()
        subscriptions.propagate_metadata()
        redis_streams.configured = subscriptions.configured = True
        redis_streams.on_startup.extend(
            (
                redis.initialize,
                postgres_connector.initialize,
                mclient.initialize,
            )
        )
        redis_streams.on_shutdown.extend(
            (redis.teardown, postgres_connector.teardown, mclient.teardown)
        )

        dev_local = self.option("dev-local")

        if dev_local:
            # Mute datadog logs at local dev env
            logging.getLogger("datadog.dogstatsd").setLevel(logging.CRITICAL)

        redis_streams.run(debug=settings.Asyncio().debug)
        return 0
