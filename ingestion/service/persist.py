from __future__ import annotations

from typing import List

import ddtrace
import structlog
from datadog import statsd
from ingestion import model, repository, service
from mmstream import pubsub

import constants
from app.eligibility.domain.repository import parsed_records_db
from app.utils import async_ttl_cache
from config import settings
from db import model as db_model

__all__ = (
    "subscriptions",
    "PersistenceService",
    "consume_processed",
)

USE_TMP = True

APP_SETTINGS = settings.App()
GCP_SETTINGS = settings.GCP()
PUBSUB_SETTINGS = settings.Pubsub()
BATCH_SIZE = 1_000
REVIEW_THRESHOLD = 0.95
TIMEOUT_SECONDS = 30

MODULE = __name__
logger = structlog.getLogger(MODULE)

subscriptions = pubsub.PubSubStreams(constants.APP_NAME)


@subscriptions.consumer(
    GCP_SETTINGS.processed_topic,
    group=GCP_SETTINGS.processed_group,
    model=model.ProcessedNotification,
    timeoutms=TIMEOUT_SECONDS * 1_000,
    auto_create=APP_SETTINGS.dev_enabled,
)
async def consume_processed(
    stream: pubsub.SubscriptionStream[model.ProcessedNotification],
):
    persistence_service = PersistenceService()

    messages: List[pubsub.PubSubEntry[model.ProcessedNotification]]

    async for messages in stream.next(count=BATCH_SIZE):
        await persistence_service.persist_batch(messages=messages)
        yield messages


class PersistenceService:
    FILE_COUNT_SUCCESS_CACHE_KEY = "num_success"
    FILE_COUNT_ERROR_CACHE_KEY = "num_error"

    def __init__(
        self,
        ingest_config: repository.IngestConfigurationRepository | None = None,
        file_parse_repo: parsed_records_db.ParsedRecordsDatabaseRepository
        | None = None,
        member_repo: repository.MemberRepository | None = None,
    ):
        self._ingest_config = ingest_config or repository.IngestConfigurationRepository(
            use_tmp=USE_TMP
        )
        self._file_parse_repo = (
            file_parse_repo
            or parsed_records_db.ParsedRecordsDatabaseRepository(use_tmp=USE_TMP)
        )
        self._member_repo = member_repo or repository.MemberRepository(use_tmp=USE_TMP)

    @ddtrace.tracer.wrap()
    async def persist_batch(
        self, *, messages: List[pubsub.PubSubEntry[model.ProcessedNotification]]
    ):
        """Loop through a batch and persist each record"""
        for message in messages:
            try:
                await self.persist_record(processed=message.data)
            except Exception as e:
                logger.exception(
                    "Message persist error encountered",
                    metadata=message.data.metadata,
                    error=e,
                )
                continue

    @ddtrace.tracer.wrap()
    async def persist_record(self, *, processed: model.ProcessedNotification):
        """Persist a single record"""
        if processed.metadata.type == repository.IngestionType.FILE:
            await self.persist_file(processed=processed)
        elif processed.metadata.type == repository.IngestionType.STREAM:
            await self.persist_optum(processed=processed)
        else:
            raise NotImplementedError

        statsd.increment(
            metric="eligibility.persist.count",
            tags=[f"source:{MODULE}", f"type:{processed.metadata.type}"],
        )

    @ddtrace.tracer.wrap()
    async def persist_file(self, *, processed: model.ProcessedNotification):
        """Persist a single file record, if we have processed the whole file, flush or leave for review"""
        num_error: int
        num_success: int
        # get the record count for file (from a cached service function)
        num_row: int = await self.get_row_count(file_id=processed.metadata.file_id)

        if processed.record.errors:
            # convert to FileParseError
            error = db_model.FileParseError(
                file_id=processed.record.file_id,
                organization_id=processed.record.organization_id,
                record=processed.record.record,
                errors=processed.record.errors,
                warnings=processed.record.warnings,
            )
            await self._file_parse_repo.persist_errors(errors=[error])
            # increment the count for errors by 1
            num_error = await self._ingest_config.incr_cache(
                namespace=service.FileIngestionService.FILE_CACHE_NAMESPACE,
                key=self.FILE_COUNT_ERROR_CACHE_KEY,
                id=processed.metadata.file_id,
            )
            # get the number of success
            num_success: int = await self._ingest_config.get_cache(
                namespace=service.FileIngestionService.FILE_CACHE_NAMESPACE,
                key=self.FILE_COUNT_SUCCESS_CACHE_KEY,
                id=processed.metadata.file_id,
            )
        else:
            # convert to FileParseResult
            result = db_model.FileParseResult(**processed.record.__dict__)
            await self._file_parse_repo.persist_valid(valid=[result])
            # increment the count for success by 1
            num_success = await self._ingest_config.incr_cache(
                namespace=service.FileIngestionService.FILE_CACHE_NAMESPACE,
                key=self.FILE_COUNT_SUCCESS_CACHE_KEY,
                id=processed.metadata.file_id,
            )
            # get the number of error
            num_error = await self._ingest_config.get_cache(
                namespace=service.FileIngestionService.FILE_CACHE_NAMESPACE,
                key=self.FILE_COUNT_ERROR_CACHE_KEY,
                id=processed.metadata.file_id,
            )

        if num_error + num_success == num_row:
            if PersistenceService.should_review(total=num_row, success=num_success):
                logger.info(
                    "Record processing complete for file",
                    should_review=True,
                    num_row=num_row,
                    num_success=num_success,
                    num_error=num_error,
                )
            else:
                await self._file_parse_repo.flush(
                    file=db_model.File(
                        organization_id=processed.record.organization_id,
                        id=processed.metadata.file_id,
                        name=processed.metadata.identifier,
                    )
                )
                logger.info(
                    "Record processing complete for file",
                    should_review=False,
                    num_row=num_row,
                    num_success=num_success,
                    num_error=num_error,
                )

            await self._ingest_config.delete_cache(
                namespace=service.FileIngestionService.FILE_CACHE_NAMESPACE,
                id=processed.metadata.file_id,
            )

    @ddtrace.tracer.wrap()
    async def persist_optum(self, *, processed: model.ProcessedNotification):
        """Persist a single Optum record"""
        external_record = db_model.ExternalRecord(
            organization_id=processed.record.organization_id,
            first_name=processed.record.first_name,
            last_name=processed.record.last_name,
            date_of_birth=processed.record.date_of_birth,
            email=processed.record.email,
            unique_corp_id=processed.record.unique_corp_id,
            dependent_id=processed.record.dependent_id,
            work_state=processed.record.work_state,
            employer_assigned_id=processed.record.employer_assigned_id,
            effective_range=db_model.DateRange(**processed.record.effective_range),
            record=processed.record.record,
            gender_code=processed.record.gender_code,
            do_not_contact=processed.record.do_not_contact,
            external_id=processed.record.record["external_id"],
        )
        record_and_address = db_model.ExternalRecordAndAddress(
            external_record=external_record, record_address=processed.address
        )
        await self._member_repo.persist_optum_members(records=[record_and_address])

    @staticmethod
    @ddtrace.tracer.wrap()
    def should_review(
        *, total: int, success: int, threshold: float = REVIEW_THRESHOLD
    ) -> bool:
        """Determine if a set of processed records should be reviewed"""
        if total <= 0:
            raise ValueError("The total rows must be greater than 0")

        persist_rate = success / total

        return persist_rate <= threshold

    @ddtrace.tracer.wrap()
    @async_ttl_cache.AsyncTTLCache(time_to_live=30 * 60, max_size=5_000)
    async def get_row_count(self, *, file_id: int) -> int:
        """Get the total number of rows for this file"""
        return await self._ingest_config.get_cache(
            namespace=service.FileIngestionService.FILE_CACHE_NAMESPACE,
            key=service.FileIngestionService.FILE_COUNT_CACHE_KEY,
            id=file_id,
        )
