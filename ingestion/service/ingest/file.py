from __future__ import annotations

import dataclasses
import datetime
from typing import List, Tuple

import cchardet
import ddtrace
import structlog
from datadog import statsd
from ingestion import model, repository
from mmstream import pubsub

import constants
from config import settings
from db import model as db_model

__all__ = ("subscriptions", "FileIngestionService", "consume_file", "FileParserConfig")

USE_TMP = True

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
    group=GCP_SETTINGS.census_file_group_tmp,
    model=model.FileUploadNotification,
    timeoutms=1_000 * 60 * TIMEOUT_MINUTES,
    auto_create=APP_SETTINGS.dev_enabled,
)
async def consume_file(stream: pubsub.SubscriptionStream[model.FileUploadNotification]):
    """Pubsub consumer which reads from the file notification topic and calls the ingestion service to process a file"""
    async with pubsub.PubSubPublisher(
        project=GCP_SETTINGS.project,
        topic=GCP_SETTINGS.unprocessed_topic,
        name=MODULE,
        emulator_host=PUBSUB_SETTINGS.emulator_host,
    ) as publisher:
        file_manager = repository.EligibilityFileManager(project=GCP_SETTINGS.project)
        ingestion_service = FileIngestionService(gcs=file_manager)

        message: pubsub.PubSubEntry[model.FileUploadNotification]

        async for message in stream:
            file: db_model.File = await ingestion_service.process_file(
                filename=message.data.name, publisher=publisher
            )
            yield file


class FileIngestionService:
    FILE_CACHE_NAMESPACE = "file"
    FILE_COUNT_CACHE_KEY = "num_rows"
    BATCH_SIZE = 1_000

    def __init__(
        self,
        ingest_config: repository.IngestConfigurationRepository | None = None,
        gcs: repository.EligibilityFileManager | None = None,
    ):
        self._ingest_config = ingest_config or repository.IngestConfigurationRepository(
            use_tmp=USE_TMP
        )
        self._gcs = gcs or repository.EligibilityFileManager(
            project=GCP_SETTINGS.project
        )

    @ddtrace.tracer.wrap()
    async def process_file(
        self, *, filename: str, publisher: pubsub.PubSubPublisher
    ) -> db_model.File | None:
        """Public method for processing a file, this method catches any unhandled exceptions"""
        parser_config: FileParserConfig | None = None

        try:
            parser_config = await self.initialize_reader(filename=filename)

            if not parser_config:
                return None

            await self._process_file(parser_config=parser_config, publisher=publisher)
        except Exception as e:
            if parser_config:
                await self._ingest_config.set_error(
                    file_id=parser_config.file.id, error=db_model.FileError.UNKNOWN
                )
            logger.error(
                "File level error encountered",
                error=db_model.FileError.UNKNOWN,
                filename=filename,
                organization_id=parser_config.file.organization_id
                if parser_config
                else None,
                module=MODULE,
            )
            logger.exception(e)

        return parser_config.file if parser_config else None

    @ddtrace.tracer.wrap()
    async def _process_file(
        self,
        *,
        parser_config: FileParserConfig,
        publisher: pubsub.PubSubPublisher,
    ):
        """Private process file method which handles all the business logic for processing a file"""
        # This counter is used instead of calling
        # len() so we don't iterate through the reader twice
        num_rows: int = 0

        for batch_num, batch in enumerate(
            parser_config.reader.parse(batch_size=self.BATCH_SIZE)
        ):
            messages: List[pubsub.PublisherMessage] = [
                pubsub.PublisherMessage(
                    message=model.UnprocessedNotification(
                        metadata=model.Metadata(
                            file_id=parser_config.file.id,
                            organization_id=parser_config.file.organization_id,
                            identifier=parser_config.file.name,
                            index=self.BATCH_SIZE * batch_num + i,
                            type=repository.IngestionType.FILE,
                            ingestion_ts=datetime.datetime.utcnow(),
                            data_provider=parser_config.org_config.data_provider,
                        ),
                        record=r,
                    )
                )
                for i, r in enumerate(batch)
            ]
            await publisher.publish(*messages)
            batch_size = len(batch)
            num_rows += batch_size
            logger.info(
                "Published batch",
                count=batch_size,
                filename=parser_config.file.name,
                organization_id=parser_config.file.organization_id,
                file_id=parser_config.file.id,
                module=MODULE,
            )
            statsd.increment(
                value=batch_size,
                metric="eligibility.ingest.count",
                tags=[
                    f"source:{MODULE}",
                    f"type:{repository.IngestionType.FILE}",
                ],
            )

        await self.set_row_count(file_id=parser_config.file.id, row_count=num_rows)

    @ddtrace.tracer.wrap()
    async def initialize_reader(self, *, filename: str) -> FileParserConfig | None:
        """Attempts to initialize a file and orchestrate the necessary steps needed to parse the file"""
        file: db_model.File | None
        org_config: db_model.Configuration | None

        file, org_config = await self._initialize_file(filename=filename)

        if (file, org_config) == (None, None):
            # org configuration not found for file based on directory name
            return None

        # Pull the file data from GCS
        file_data: bytes | None = await self._gcs.get(
            name=file.name, bucket_name=GCP_SETTINGS.census_file_bucket
        )

        if not file_data:
            await self._ingest_config.set_error(
                file_id=file.id, error=db_model.FileError.MISSING
            )
            logger.error(
                "File level error encountered",
                error=db_model.FileError.MISSING,
                filename=file.name,
                organization_id=file.organization_id,
                module=MODULE,
            )
            return None

        encoding: str = FileIngestionService.detect_encoding(file_data)
        await self._ingest_config.set_encoding(file_id=file.id, encoding=encoding)

        reader = repository.EligibilityCSVReader(data=file_data, encoding=encoding)

        if reader.set_dialect() is False:
            await self._ingest_config.set_error(
                file_id=file.id, error=db_model.FileError.DELIMITER
            )
            logger.error(
                "File level error encountered",
                error=db_model.FileError.DELIMITER,
                filename=file.name,
                organization_id=file.organization_id,
                module=MODULE,
            )
            return None

        return FileParserConfig(reader=reader, file=file, org_config=org_config)

    @ddtrace.tracer.wrap()
    async def _initialize_file(
        self, *, filename: str
    ) -> Tuple[db_model.File, db_model.Configuration] | Tuple[None, None]:
        """
        Initialize the file in our DB and prepare for parsing

        If we fail to initialize the file, since configuration is missing for this file directory,
        we will return a tuple of (None, None), otherwise, we will return a File instance
        and a Configuration instance
        """
        # attempt to sync the config data for this org based on filename
        org_config: db_model.Configuration | None = await self._ingest_config.sync(
            filename=filename
        )

        # org not found based on configured file paths
        if not org_config:
            # TODO: Send metrics about how this org is not configured
            logger.warn(
                "Could not find organization associated with file",
                filename=filename,
            )
            return None, None

        # create an instance of it in our DB
        file: db_model.File = await self._ingest_config.create_file(
            organization_id=org_config.organization_id, filename=filename
        )

        # set the `started_at` time for file
        await self._ingest_config.set_started_at(file_id=file.id)

        return file, org_config

    @ddtrace.tracer.wrap()
    async def set_row_count(self, *, file_id: int, row_count: int):
        """Update the cache for the number of rows that have been parsed for this file"""
        await self._ingest_config.set_cache(
            namespace=self.FILE_CACHE_NAMESPACE,
            id=file_id,
            key=self.FILE_COUNT_CACHE_KEY,
            value=row_count,
        )

    @staticmethod
    @ddtrace.tracer.wrap()
    def detect_encoding(data: bytes) -> str:
        """Detect the encoding of a byte array"""
        detected: dict = cchardet.detect(data)
        return detected.get("encoding", DEFAULT_ENCODING)


@dataclasses.dataclass
class FileParserConfig:
    reader: repository.EligibilityCSVReader
    file: db_model.File
    org_config: db_model.Configuration
