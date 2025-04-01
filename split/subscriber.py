from __future__ import annotations

import structlog
from ingestion import model
from mmstream import pubsub

import constants
from config import settings

__all__ = ("handle_file_notification",)

TIMEOUT_MINUTES = 10
APP_SETTINGS = settings.App()
GCP_SETTINGS = settings.GCP()
PUBSUB_SETTINGS = settings.Pubsub()
DEFAULT_ENCODING = "utf-8"

MODULE = __name__
logger = structlog.getLogger(MODULE)

subscriptions = pubsub.PubSubStreams(constants.APP_NAME)


@subscriptions.consumer(
    GCP_SETTINGS.census_file_topic,
    group=GCP_SETTINGS.census_file_group_split,
    model=model.FileUploadNotification,
    timeoutms=1_000 * 60 * TIMEOUT_MINUTES,
    auto_create=APP_SETTINGS.dev_enabled,
)
async def handle_file_notification(
    stream: pubsub.SubscriptionStream[model.FileUploadNotification],
):
    message: pubsub.PubSubEntry[model.FileUploadNotification]

    async for message in stream:
        logger.info(
            "Received a large file requiring splitting", file_name=message.data.name
        )
