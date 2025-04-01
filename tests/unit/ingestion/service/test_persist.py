from typing import List
from unittest import mock

import pytest
from ingestion import model, repository, service
from mmstream import pubsub
from tests.factories.workers.pubsub import pubsub_factory

from db import model as db_model

pytestmark = pytest.mark.asyncio


class TestPersistFile:
    @staticmethod
    async def test_persist_file_get_row_count_called(
        persist_service: service.PersistenceService,
    ):
        # Given
        processed: model.ProcessedNotification = (
            pubsub_factory.ProcessedNotificationFactory()
        )

        # When
        with mock.patch(
            "ingestion.service.PersistenceService.get_row_count"
        ) as mock_get_row_count:
            await persist_service.persist_file(processed=processed)

        # Then
        mock_get_row_count.assert_called_with(file_id=processed.metadata.file_id)

    @staticmethod
    async def test_persist_file_persist_errors_called(
        persist_service: service.PersistenceService,
    ):
        # Given
        errors: List[str] = ["error"]
        members: model.ProcessedMember = pubsub_factory.ProcessedMemberFactory(
            errors=errors
        )
        processed: model.ProcessedNotification = (
            pubsub_factory.ProcessedNotificationFactory(record=members)
        )

        # When
        await persist_service.persist_file(processed=processed)

        # Then
        persist_service._file_parse_repo.persist_errors.assert_called()

    @staticmethod
    async def test_persist_file_error_incr_cache_called(
        persist_service: service.PersistenceService,
    ):
        # Given
        errors: List[str] = ["error"]
        members: model.ProcessedMember = pubsub_factory.ProcessedMemberFactory(
            errors=errors
        )
        processed: model.ProcessedNotification = (
            pubsub_factory.ProcessedNotificationFactory(record=members)
        )

        # When
        await persist_service.persist_file(processed=processed)

        # Then
        persist_service._ingest_config.incr_cache.assert_called_with(
            namespace=service.FileIngestionService.FILE_CACHE_NAMESPACE,
            key=persist_service.FILE_COUNT_ERROR_CACHE_KEY,
            id=processed.metadata.file_id,
        )

    @staticmethod
    async def test_persist_file_error_get_cache_called(
        persist_service: service.PersistenceService,
    ):
        # Given
        errors: List[str] = ["error"]
        members: model.ProcessedMember = pubsub_factory.ProcessedMemberFactory(
            errors=errors
        )
        processed: model.ProcessedNotification = (
            pubsub_factory.ProcessedNotificationFactory(record=members)
        )

        # When
        await persist_service.persist_file(processed=processed)

        # Then
        persist_service._ingest_config.get_cache.assert_called_with(
            namespace=service.FileIngestionService.FILE_CACHE_NAMESPACE,
            key=persist_service.FILE_COUNT_SUCCESS_CACHE_KEY,
            id=processed.metadata.file_id,
        )

    @staticmethod
    async def test_persist_file_persist_valid_called(
        persist_service: service.PersistenceService,
    ):
        # Given
        errors: List[str] = []
        members: model.ProcessedMember = pubsub_factory.ProcessedMemberFactory(
            errors=errors
        )
        processed: model.ProcessedNotification = (
            pubsub_factory.ProcessedNotificationFactory(record=members)
        )

        # When
        await persist_service.persist_file(processed=processed)

        # Then
        persist_service._file_parse_repo.persist_valid.assert_called()

    @staticmethod
    async def test_persist_file_valid_incr_cache_called(
        persist_service: service.PersistenceService,
    ):
        # Given
        errors: List[str] = []
        members: model.ProcessedMember = pubsub_factory.ProcessedMemberFactory(
            errors=errors
        )
        processed: model.ProcessedNotification = (
            pubsub_factory.ProcessedNotificationFactory(record=members)
        )

        # When
        await persist_service.persist_file(processed=processed)

        # Then
        persist_service._ingest_config.incr_cache.assert_called_with(
            namespace=service.FileIngestionService.FILE_CACHE_NAMESPACE,
            key=persist_service.FILE_COUNT_SUCCESS_CACHE_KEY,
            id=processed.metadata.file_id,
        )

    @staticmethod
    async def test_persist_file_valid_get_cache_called(
        persist_service: service.PersistenceService,
    ):
        # Given
        errors: List[str] = []
        members: model.ProcessedMember = pubsub_factory.ProcessedMemberFactory(
            errors=errors
        )
        processed: model.ProcessedNotification = (
            pubsub_factory.ProcessedNotificationFactory(record=members)
        )

        # When
        await persist_service.persist_file(processed=processed)

        # Then
        persist_service._ingest_config.get_cache.assert_called_with(
            namespace=service.FileIngestionService.FILE_CACHE_NAMESPACE,
            key=persist_service.FILE_COUNT_ERROR_CACHE_KEY,
            id=processed.metadata.file_id,
        )

    @staticmethod
    async def test_all_rows_processed_should_review_called(
        persist_service: service.PersistenceService,
    ):
        # Given
        errors: List[str] = []
        members: model.ProcessedMember = pubsub_factory.ProcessedMemberFactory(
            errors=errors
        )
        processed: model.ProcessedNotification = (
            pubsub_factory.ProcessedNotificationFactory(record=members)
        )
        total_rows: int = 100
        total_success: int = 90
        total_errors: int = 10
        persist_service._ingest_config.incr_cache.return_value = total_success
        persist_service._ingest_config.get_cache.return_value = total_errors

        with mock.patch(
            "ingestion.service.PersistenceService.get_row_count"
        ) as mock_get_row_count:
            mock_get_row_count.return_value = total_rows
            with mock.patch(
                "ingestion.service.PersistenceService.should_review"
            ) as mock_should_review:

                # When
                await persist_service.persist_file(processed=processed)

                # Then
                mock_should_review.assert_called_with(
                    total=total_rows, success=total_success
                )

    @staticmethod
    async def test_not_all_rows_processed_should_review_not_called(
        persist_service: service.PersistenceService,
    ):
        # Given
        errors: List[str] = []
        members: model.ProcessedMember = pubsub_factory.ProcessedMemberFactory(
            errors=errors
        )
        processed: model.ProcessedNotification = (
            pubsub_factory.ProcessedNotificationFactory(record=members)
        )
        total_rows: int = 200
        total_success: int = 90
        total_errors: int = 10
        persist_service._ingest_config.incr_cache.return_value = total_success
        persist_service._ingest_config.get_cache.return_value = total_errors

        with mock.patch(
            "ingestion.service.PersistenceService.get_row_count"
        ) as mock_get_row_count:
            mock_get_row_count.return_value = total_rows
            with mock.patch(
                "ingestion.service.PersistenceService.should_review"
            ) as mock_should_review:

                # When
                await persist_service.persist_file(processed=processed)

                # Then
                mock_should_review.assert_not_called()

    @staticmethod
    async def test_all_rows_processed_flush_is_called(
        persist_service: service.PersistenceService,
    ):
        # Given
        errors: List[str] = []
        members: model.ProcessedMember = pubsub_factory.ProcessedMemberFactory(
            errors=errors
        )
        processed: model.ProcessedNotification = (
            pubsub_factory.ProcessedNotificationFactory(record=members)
        )
        total_rows: int = 100
        total_success: int = 90
        total_errors: int = 10
        persist_service._ingest_config.incr_cache.return_value = total_success
        persist_service._ingest_config.get_cache.return_value = total_errors

        with mock.patch(
            "ingestion.service.PersistenceService.get_row_count"
        ) as mock_get_row_count:
            mock_get_row_count.return_value = total_rows
            with mock.patch(
                "ingestion.service.PersistenceService.should_review"
            ) as mock_should_review:
                mock_should_review.return_value = False
                # When

                await persist_service.persist_file(processed=processed)

        # Then
        persist_service._file_parse_repo.flush.assert_called()

    @staticmethod
    async def test_all_rows_processed_flush_is_not_called(
        persist_service: service.PersistenceService,
    ):
        # Given
        errors: List[str] = []
        members: model.ProcessedMember = pubsub_factory.ProcessedMemberFactory(
            errors=errors
        )
        processed: model.ProcessedNotification = (
            pubsub_factory.ProcessedNotificationFactory(record=members)
        )
        total_rows: int = 100
        total_success: int = 90
        total_errors: int = 10
        persist_service._ingest_config.incr_cache.return_value = total_success
        persist_service._ingest_config.get_cache.return_value = total_errors

        with mock.patch(
            "ingestion.service.PersistenceService.get_row_count"
        ) as mock_get_row_count:
            mock_get_row_count.return_value = total_rows
            with mock.patch(
                "ingestion.service.PersistenceService.should_review"
            ) as mock_should_review:
                mock_should_review.return_value = True
                # When

                await persist_service.persist_file(processed=processed)

        # Then
        persist_service._file_parse_repo.flush.assert_not_called()

    @staticmethod
    async def test_all_rows_processed_delete_cache_is_called(
        persist_service: service.PersistenceService,
    ):
        # Given
        errors: List[str] = []
        members: model.ProcessedMember = pubsub_factory.ProcessedMemberFactory(
            errors=errors
        )
        processed: model.ProcessedNotification = (
            pubsub_factory.ProcessedNotificationFactory(record=members)
        )
        total_rows: int = 100
        total_success: int = 90
        total_errors: int = 10
        persist_service._ingest_config.incr_cache.return_value = total_success
        persist_service._ingest_config.get_cache.return_value = total_errors

        with mock.patch(
            "ingestion.service.PersistenceService.get_row_count"
        ) as mock_get_row_count:
            mock_get_row_count.return_value = total_rows
            with mock.patch(
                "ingestion.service.PersistenceService.should_review"
            ) as mock_should_review:
                mock_should_review.return_value = True

                # When
                await persist_service.persist_file(processed=processed)

        # Then
        persist_service._ingest_config.delete_cache.assert_called_with(
            namespace=service.FileIngestionService.FILE_CACHE_NAMESPACE,
            id=processed.metadata.file_id,
        )

    @staticmethod
    async def test_not_rows_processed_delete_cache_is_not_called(
        persist_service: service.PersistenceService,
    ):
        # Given
        errors: List[str] = []
        members: model.ProcessedMember = pubsub_factory.ProcessedMemberFactory(
            errors=errors
        )
        processed: model.ProcessedNotification = (
            pubsub_factory.ProcessedNotificationFactory(record=members)
        )
        total_rows: int = 200
        total_success: int = 90
        total_errors: int = 10
        persist_service._ingest_config.incr_cache.return_value = total_success
        persist_service._ingest_config.get_cache.return_value = total_errors

        # When
        with mock.patch(
            "ingestion.service.PersistenceService.get_row_count"
        ) as mock_get_row_count:
            mock_get_row_count.return_value = total_rows

            await persist_service.persist_file(processed=processed)

        # Then
        persist_service._ingest_config.delete_cache.assert_not_called()


class TestPersistOptum:
    @staticmethod
    async def test_persist_optum_correct_fields_are_mapped(
        persist_service: service.PersistenceService,
    ):
        # Given
        processed: model.ProcessedNotification = (
            pubsub_factory.ProcessedNotificationFactory()
        )
        # When
        await persist_service.persist_optum(processed=processed)
        # Then
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
        persist_service._member_repo.persist_optum_members.assert_called_with(
            records=[record_and_address]
        )


class TestShouldReview:
    @staticmethod
    def test_above_threshold():
        # Given
        success: int = 99
        total: int = 100
        threshold: float = 0.9

        # When
        should_review: bool = service.PersistenceService.should_review(
            total=total, success=success, threshold=threshold
        )

        # Then
        assert should_review is False

    @staticmethod
    def test_at_threshold():
        # Given
        success: int = 90
        total: int = 100
        threshold: float = 0.9

        # When
        should_review: bool = service.PersistenceService.should_review(
            total=total, success=success, threshold=threshold
        )

        # Then
        assert should_review is True

    @staticmethod
    def test_below_threshold():
        # Given
        success: int = 80
        total: int = 100
        threshold: float = 0.9

        # When
        should_review: bool = service.PersistenceService.should_review(
            total=total, success=success, threshold=threshold
        )

        # Then
        assert should_review is True

    @staticmethod
    def test_total_row_is_zero():
        # Given
        success: int = 80
        total: int = 0
        threshold: float = 0.9

        # When Then
        with pytest.raises(ValueError):
            service.PersistenceService.should_review(
                total=total, success=success, threshold=threshold
            )


class TestGetRowCount:
    @staticmethod
    async def test_get_cache_called(persist_service: service.PersistenceService):
        # Given
        file_id: int = 1

        # When
        await persist_service.get_row_count(file_id=file_id)

        # Then
        persist_service._ingest_config.get_cache.assert_called_with(
            namespace=service.FileIngestionService.FILE_CACHE_NAMESPACE,
            key=service.FileIngestionService.FILE_COUNT_CACHE_KEY,
            id=file_id,
        )


class TestPersistRecord:
    @staticmethod
    async def test_persist_record_calls_persist_file(
        persist_service: service.PersistenceService,
    ):
        # Given
        metadata: model.Metadata = pubsub_factory.MetadataFactory(
            type=repository.IngestionType.FILE
        )
        processed: model.ProcessedNotification = (
            pubsub_factory.ProcessedNotificationFactory(metadata=metadata)
        )

        # When
        with mock.patch(
            "ingestion.service.PersistenceService.persist_file"
        ) as mock_persist_file:
            await persist_service.persist_record(processed=processed)

            # Then
            mock_persist_file.assert_called_with(processed=processed)

    @staticmethod
    async def test_persist_record_calls_persist_optum(
        persist_service: service.PersistenceService,
    ):
        # Given
        metadata: model.Metadata = pubsub_factory.MetadataFactory(
            type=repository.IngestionType.STREAM
        )
        processed: model.ProcessedNotification = (
            pubsub_factory.ProcessedNotificationFactory(metadata=metadata)
        )

        # When
        with mock.patch(
            "ingestion.service.PersistenceService.persist_optum"
        ) as mock_persist_optum:
            await persist_service.persist_record(processed=processed)

            # Then
            mock_persist_optum.assert_called_with(processed=processed)


class TestPersistBatch:
    @staticmethod
    async def test_persist_batch_calls_persist_record(
        persist_service: service.PersistenceService,
    ):
        # Given
        batch_size: int = 10
        metadata: model.Metadata = pubsub_factory.MetadataFactory(
            type=repository.IngestionType.FILE
        )
        processed: model.ProcessedNotification = (
            pubsub_factory.ProcessedNotificationFactory.create(metadata=metadata)
        )
        batch: List[
            pubsub.PubSubEntry[model.ProcessedNotification]
        ] = pubsub_factory.PubSubMessageFactory.create_batch(
            size=batch_size, data=processed
        )

        # When
        with mock.patch(
            "ingestion.service.PersistenceService.persist_record"
        ) as mock_persist_record:
            await persist_service.persist_batch(messages=batch)

            # Then
            assert mock_persist_record.call_count == batch_size

    @staticmethod
    async def test_persist_record_continues_on_exception(
        persist_service: service.PersistenceService,
    ):
        # Given
        batch_size: int = 10
        metadata: model.Metadata = pubsub_factory.MetadataFactory(
            type=repository.IngestionType.FILE
        )
        processed: model.ProcessedNotification = (
            pubsub_factory.ProcessedNotificationFactory.create(metadata=metadata)
        )
        batch: List[
            pubsub.PubSubEntry[model.ProcessedNotification]
        ] = pubsub_factory.PubSubMessageFactory.create_batch(
            size=batch_size, data=processed
        )
        _persist_record_side_effects = [Exception] + [
            None for _ in range(batch_size - 1)
        ]
        persist_service.persist_record = mock.AsyncMock(
            side_effect=_persist_record_side_effects
        )

        # When
        await persist_service.persist_batch(messages=batch)

        # Then
        assert persist_service.persist_record.call_count == batch_size


class TestConsumeProcessed:
    @staticmethod
    async def test_consume_processed_called_once(
        subscription: pubsub.SubscriptionStream,
    ):
        # Given
        processed_notification: model.ProcessedNotification = (
            pubsub_factory.ProcessedNotificationFactory()
        )
        pubsub_message: pubsub.PubSubEntry = pubsub_factory.PubSubMessageFactory.create(
            data=processed_notification
        )
        subscription.next.return_value.__aiter__.return_value = [[pubsub_message]]

        # When
        with mock.patch(
            "ingestion.service.PersistenceService.persist_batch"
        ) as mock_persist_batch:
            async for _ in service.consume_processed(stream=subscription):
                continue

            # Then
            mock_persist_batch.assert_called_once()

    @staticmethod
    async def test_consume_yields_messages(subscription: pubsub.SubscriptionStream):
        # Given
        processed_notification: model.ProcessedNotification = (
            pubsub_factory.ProcessedNotificationFactory()
        )
        pubsub_message: pubsub.PubSubEntry = pubsub_factory.PubSubMessageFactory.create(
            data=processed_notification
        )
        subscription.next.return_value.__aiter__.return_value = [[pubsub_message]]

        # When
        with mock.patch("ingestion.service.PersistenceService.persist_batch"):
            assert [
                f async for f in service.consume_processed(stream=subscription)
            ] == [[pubsub_message]]
