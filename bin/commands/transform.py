import constants
from bin.commands.base import BaseAppCommand
from config import settings
from db.clients import postgres_connector

GCP_SETTINGS = settings.GCP()
SUBTITLE = "Transform Module"


class TransforCommand(BaseAppCommand):
    """Run the process for transformation of member records"""

    name = "transform"
    subtitle = SUBTITLE

    def handle(self) -> int:
        from ingestion.service.transform.transformer import subscriptions

        from db import redis
        from db.mono import client as mclient

        # Configure subscription
        subscriptions.project = GCP_SETTINGS.project
        subscriptions.name = f"{constants.APP_NAME}-{self.name}"
        subscriptions.pod = constants.POD
        subscriptions.propagate_metadata()
        subscriptions.configured = True

        # Add client to event loop
        subscriptions.on_startup.extend(
            (
                redis.initialize,
                mclient.initialize,
                postgres_connector.initialize,
            )
        )

        subscriptions.on_shutdown.extend(
            (
                redis.teardown,
                mclient.teardown,
                postgres_connector.teardown,
            )
        )

        subscriptions.run(debug=settings.Asyncio().debug)
        return 0
