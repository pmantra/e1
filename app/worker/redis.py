from __future__ import annotations

import asyncio
from typing import AsyncIterator

import pendulum
import structlog
from mmlib.ops import stats
from mmlib.redis.client import make_dsn
from mmstream import redis
from structlog.contextvars import bind_contextvars, unbind_contextvars

import constants
from app.eligibility import process
from app.eligibility.constants import ProcessingResult
from app.eligibility.domain.model.parsed_records import ProcessedRecords
from app.worker import types
from config import settings
from constants import APP_NAME
from db import model

redis_settings = settings.Redis()
REDIS_DSN = make_dsn(redis_settings.host, password=redis_settings.password)
stream_supervisor = redis.RedisStreamSupervisor(APP_NAME, dsn=REDIS_DSN)

SLA_MS = int(pendulum.duration(hours=8).total_seconds() * 1_000)

logger = structlog.getLogger(__name__)


@stream_supervisor.consumer(
    "pending-file",
    group=APP_NAME,
    model=types.PendingFileNotification,
    timeoutms=SLA_MS,
)
async def process_file(
    stream: redis.RedisStream[model.File],
) -> AsyncIterator[process.ProcessedRecords]:
    """Read in a file and attempt to normalize its contents before shipping off for storage"""
    # General globals for processing
    bucket = settings.GCP().census_file_bucket

    loop = asyncio.get_event_loop()
    processor = process.EligibilityFileProcessor(
        bucket=bucket,
        project=stream_supervisor.project,
        project_supervisor=stream_supervisor.name,
        loop=loop,
    )
    logger.info("Listening for new files.")
    async for key, messageid, notification in stream:
        file = await processor.files.get(notification.file_id)
        if not file:
            logger.warning(
                "Couldn't locate File with ID.", file_id=notification.file_id
            )
            continue

        config = await processor.configs.get(file.organization_id)

        bind_contextvars(
            file_id=file.id,
            filename=file.name,
            organization_id=config.organization_id,
            stream=key,
            message_id=messageid,
        )

        logger.info("File parsing starting")

        result: ProcessingResult
        processed: ProcessedRecords
        result, processed = await processor.process(key, messageid, file, config)

        if result == ProcessingResult.NO_RECORDS_FOUND:
            logger.warning("No data processed for file.")
            unbind_contextvars(
                "file_id", "filename", "organization_id", "stream", "message_id"
            )
            continue
        elif result == ProcessingResult.ERROR_DURING_PROCESSING:
            logger.warning("Error processing file")
            unbind_contextvars(
                "file_id", "filename", "organization_id", "stream", "message_id"
            )
            continue
        elif result == ProcessingResult.FILE_MISSING:
            logger.warning("File not found")
            unbind_contextvars(
                "file_id", "filename", "organization_id", "stream", "message_id"
            )
            continue
        elif result == ProcessingResult.BAD_FILE_ENCODING:
            logger.warning("Error detecting file encoding")
            unbind_contextvars(
                "file_id", "filename", "organization_id", "stream", "message_id"
            )
            continue

        stats.increment(
            metric_name="eligibility.process.file_parse.valid_rows_encountered",
            metric_value=processed.valid,
            pod_name=constants.POD,
            tags=[
                "eligibility:info",
                f"organization_id:{file.organization_id}",
                f"file_id:{file.id}",
            ],
        )
        stats.increment(
            metric_name="eligibility.process.file_parse.error_rows_encountered",
            metric_value=processed.errors,
            pod_name=constants.POD,
            tags=[
                "eligibility:info",
                f"organization_id:{file.organization_id}",
                f"file_id:{file.id}",
            ],
        )

        logger.info(
            "File parsing complete",
            errors=processed.errors,
            valid=processed.valid,
        )

        unbind_contextvars(
            "file_id", "filename", "organization_id", "stream", "message_id"
        )

        yield processed
