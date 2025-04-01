import datetime
from typing import Dict, List
from unittest import mock

import pytest
from ingestion import model, repository, service
from mmstream import pubsub
from tests.factories.workers.pubsub import pubsub_factory

from db import model as db_model

pytestmark = pytest.mark.asyncio


class TestMapFields:
    @staticmethod
    def test_map_fields():
        # given
        record: Dict = {"fn": "Ted", "ln": "Lasso", "birthday": "05/21/1993"}

        mapping: Dict = {
            "fn": "first_name",
            "ln": "last_name",
            "birthday": "date_of_birth",
        }

        # When
        mapped: Dict = service.TransformationService._map_fields(
            record=record, mapping=mapping
        )

        # Then
        assert mapped == {
            "first_name": "Ted",
            "last_name": "Lasso",
            "date_of_birth": "05/21/1993",
        }

    @staticmethod
    def test_map_fields_missing_mappings():
        # given
        record: Dict = {"fn": "Ted", "ln": "Lasso", "birthday": "05/21/1993"}

        mapping: Dict = {"fn": "first_name", "birthday": "date_of_birth"}

        # When
        mapped: Dict = service.TransformationService._map_fields(
            record=record, mapping=mapping
        )

        # Then
        assert mapped == {"first_name": "Ted", "date_of_birth": "05/21/1993"}

    @staticmethod
    def test_map_fields_key_already_internal(
        transform_service: service.TransformationService,
    ):
        # given
        record: Dict = {
            "fn": "Ted",
            "ln": "Lasso",
            "birthday": "05/21/1993",
            "unique_corp_id": "1",
        }

        mapping: Dict = {
            "fn": "first_name",
            "birthday": "date_of_birth",
            "employee_id": "unique_corp_id",
        }

        # When
        mapped: Dict = transform_service._map_fields(record=record, mapping=mapping)

        # Then
        assert mapped == {
            "first_name": "Ted",
            "date_of_birth": "05/21/1993",
            "unique_corp_id": "1",
        }


class TestMapAndParse:
    @staticmethod
    async def test_map_and_parse_get_header_mapping_called_with(
        transform_service: service.TransformationService,
    ):
        # Given
        transform_service._ingest_config.get_header_mapping.return_value = {
            "fn": "first_name",
            "ln": "last_name",
        }
        metadata = model.Metadata(
            type=repository.IngestionType.FILE,
            identifier="dir/file.csv",
            index=0,
            ingestion_ts=datetime.datetime.utcnow(),
            organization_id=1,
        )

        record = {
            "fn": "Ted",
            "ln": "Lasso",
            "date_of_birth": "05-21-1993",
            "unique_corp_id": "12",
        }

        # When
        await transform_service._map_and_parse(record=record, metadata=metadata)

        # Then
        transform_service._ingest_config.get_header_mapping.assert_called_with(
            source=metadata.type, organization_id=metadata.organization_id
        )

    @staticmethod
    async def test_map_and_parse_map_fields_called_with(
        transform_service: service.TransformationService,
    ):
        # Given
        header_mapping: Dict = {"fn": "first_name", "ln": "last_name"}
        transform_service._ingest_config.get_header_mapping.return_value = (
            header_mapping
        )
        metadata = model.Metadata(
            type=repository.IngestionType.FILE,
            identifier="dir/file.csv",
            index=0,
            ingestion_ts=datetime.datetime.utcnow(),
            organization_id=1,
        )

        record = {
            "fn": "Ted",
            "ln": "Lasso",
            "date_of_birth": "05-21-1993",
            "unique_corp_id": "12",
        }

        # When
        with mock.patch(
            "ingestion.service.TransformationService._map_fields"
        ) as mock_map_fields:
            await transform_service._map_and_parse(record=record, metadata=metadata)

            # Then
            mock_map_fields.assert_called_with(record=record, mapping=header_mapping)

    @staticmethod
    async def test_map_and_parse_parse_called_with(
        transform_service: service.TransformationService,
    ):
        # Given
        record = {
            "first_name": "Ted",
            "last_name": "Lasso",
            "date_of_birth": "05-21-1993",
            "unique_corp_id": "12",
        }
        metadata = model.Metadata(
            type=repository.IngestionType.FILE,
            identifier="dir/file.csv",
            index=0,
            ingestion_ts=datetime.datetime.utcnow(),
            organization_id=1,
        )

        # When
        with mock.patch(
            "ingestion.service.TransformationService._map_fields"
        ) as mock_map_fields:
            mock_map_fields.return_value = record

            await transform_service._map_and_parse(record={}, metadata=metadata)

            # Then
            transform_service._file_parser.parse.assert_called_with(row=record)


class TestTransformFile:
    @staticmethod
    async def test_transform_file_map_and_parse_called_with(
        transform_service: service.TransformationService,
    ):
        # Given
        record = {
            "fn": "Ted",
            "ln": "Lasso",
            "date_of_birth": "05-21-1993",
            "unique_corp_id": "12",
        }
        metadata = model.Metadata(
            type=repository.IngestionType.FILE,
            identifier="dir/file.csv",
            index=0,
            ingestion_ts=datetime.datetime.utcnow(),
        )
        transform_service._map_and_parse = mock.AsyncMock()
        transform_service._map_and_parse.return_value = model.ParsedRecord(
            record={"date_of_birth": "05-21-1993", "unique_corp_id": "12"}
        )

        # When
        await transform_service._transform_file(record=record, metadata=metadata)

        # Then
        transform_service._map_and_parse.assert_called_with(
            record=record, metadata=metadata
        )

    @staticmethod
    async def test_transform_file_get_mapped_org_id_called_with(
        transform_service: service.TransformationService,
    ):
        # Given
        record = {
            "fn": "Ted",
            "ln": "Lasso",
            "date_of_birth": "05-21-1993",
            "unique_corp_id": "12",
        }
        metadata = model.Metadata(
            type=repository.IngestionType.FILE,
            identifier="dir/file.csv",
            index=0,
            ingestion_ts=datetime.datetime.utcnow(),
            organization_id=1,
        )
        external_id: str = "id"
        transform_service._map_and_parse = mock.AsyncMock()
        transform_service._map_and_parse.return_value = model.ParsedRecord(
            record={
                "date_of_birth": "05-21-1993",
                "unique_corp_id": "12",
                "client_id": external_id,
            }
        )

        # When
        await transform_service._transform_file(record=record, metadata=metadata)

        # Then
        transform_service._ingest_config.get_external_org_info.assert_called_with(
            source=metadata.type,
            client_id=external_id,
            organization_id=metadata.organization_id,
        )

    @staticmethod
    async def test_transform_file_get_mapped_org_missing_client_id(
        transform_service: service.TransformationService,
    ):
        # Given
        record = {
            "fn": "Ted",
            "ln": "Lasso",
            "date_of_birth": "05-21-1993",
            "unique_corp_id": "12",
        }
        metadata = model.Metadata(
            type=repository.IngestionType.FILE,
            identifier="dir/file.csv",
            index=0,
            ingestion_ts=datetime.datetime.utcnow(),
            organization_id=1,
        )
        external_id: str = "id"
        transform_service._map_and_parse = mock.AsyncMock()
        transform_service._map_and_parse.return_value = model.ParsedRecord(
            record={
                "date_of_birth": "05-21-1993",
                "unique_corp_id": "12",
                "client_id": external_id,
            }
        )
        transform_service._ingest_config.get_external_org_info.side_effect = (
            service.UnmappedOrganizationError
        )

        # When
        processed: model.ProcessedMember = await transform_service._transform_file(
            record=record, metadata=metadata
        )

        # Then
        assert processed.errors == [service.ParseErrorMessage.CLIENT_ID_NO_MAPPING]

    @staticmethod
    async def test_transform_file_get_mapped_org_returns_org(
        transform_service: service.TransformationService,
    ):
        # Given
        record = {
            "fn": "Ted",
            "ln": "Lasso",
            "date_of_birth": "05-21-1993",
            "unique_corp_id": "12",
        }
        metadata = model.Metadata(
            type=repository.IngestionType.FILE,
            identifier="dir/file.csv",
            index=0,
            ingestion_ts=datetime.datetime.utcnow(),
            organization_id=1,
        )
        external_id: str = "id"
        organization_id: int = 2
        transform_service._map_and_parse = mock.AsyncMock()
        transform_service._map_and_parse.return_value = model.ParsedRecord(
            record={
                "date_of_birth": "05-21-1993",
                "unique_corp_id": "12",
                "client_id": external_id,
            }
        )
        transform_service._ingest_config.get_external_org_info.return_value = (
            db_model.ExternalMavenOrgInfo(organization_id=organization_id)
        )

        # When
        processed: model.ProcessedMember = await transform_service._transform_file(
            record=record, metadata=metadata
        )

        # Then
        assert processed.organization_id == organization_id

    @staticmethod
    async def test_transform_file_record_contains_correct_keys(
        transform_service: service.TransformationService,
    ):
        # Given
        record = {
            "fn": "Ted",
            "ln": "Lasso",
            "date_of_birth": "05/21/1993",
            "unique_corp_id": "12",
        }
        parsed_record = {
            "date_of_birth": "05-21-1993",
            "unique_corp_id": "12",
        }
        metadata = model.Metadata(
            type=repository.IngestionType.FILE,
            identifier="dir/file.csv",
            index=0,
            ingestion_ts=datetime.datetime.utcnow(),
            organization_id=1,
        )
        organization_id: int = 2
        transform_service._map_and_parse = mock.AsyncMock()
        transform_service._map_and_parse.return_value = model.ParsedRecord(
            record=parsed_record
        )
        transform_service._ingest_config.get_external_org_info.return_value = (
            db_model.ExternalMavenOrgInfo(organization_id=organization_id)
        )

        # When
        processed: model.ProcessedMember = await transform_service._transform_file(
            record=record, metadata=metadata
        )

        # Then
        assert processed.record == {**record, **parsed_record}


class TestTransform:
    @staticmethod
    async def test_transform_file_record_calls_correct_method(
        transform_service: service.TransformationService,
    ):
        # Given
        message = model.UnprocessedNotification(
            record={},
            metadata=model.Metadata(
                type=repository.IngestionType.FILE,
                identifier="dir/file.csv",
                index=0,
                ingestion_ts=datetime.datetime.utcnow(),
                organization_id=1,
            ),
        )
        transform_service._transform_file = mock.AsyncMock()

        # When
        await transform_service._transform(message=message)

        # Then
        transform_service._transform_file.assert_called_with(
            record=message.record, metadata=message.metadata
        )

    @staticmethod
    async def test_transform_stream_record_calls_correct_method(
        transform_service: service.TransformationService,
    ):
        # Given
        message = model.UnprocessedNotification(
            record={},
            metadata=model.Metadata(
                type=repository.IngestionType.STREAM,
                identifier="dir/file.csv",
                index=0,
                ingestion_ts=datetime.datetime.utcnow(),
                organization_id=1,
            ),
        )
        transform_service._transform_file = mock.AsyncMock()

        # When
        with mock.patch(
            "ingestion.service.TransformationService._transform_optum"
        ) as mock_transform_optum:
            mock_transform_optum.return_value = None, None
            await transform_service._transform(message=message)

        # Then
        mock_transform_optum.assert_called_with(
            record=message.record, metadata=message.metadata
        )

    @staticmethod
    async def test_transform_file_record_returns_value(
        transform_service: service.TransformationService,
    ):
        # Given
        message = model.UnprocessedNotification(
            record={},
            metadata=model.Metadata(
                type=repository.IngestionType.FILE,
                identifier="dir/file.csv",
                index=0,
                ingestion_ts=datetime.datetime.utcnow(),
                organization_id=1,
            ),
        )
        transform_service._transform_file = mock.AsyncMock()
        transform_service._transform_file.return_value = model.ProcessedMember(
            date_of_birth="05-01-2002", unique_corp_id="1234"
        )

        # When
        processed: model.ProcessedNotification = await transform_service._transform(
            message=message
        )

        # Then
        assert processed

    @staticmethod
    async def test_transform_stream_record_returns_none(
        transform_service: service.TransformationService,
    ):
        # Given
        message = model.UnprocessedNotification(
            record={},
            metadata=model.Metadata(
                type=repository.IngestionType.STREAM,
                identifier="optum",
                index=0,
                ingestion_ts=datetime.datetime.utcnow(),
            ),
        )
        transform_service._transform_optum = mock.AsyncMock()
        transform_service._transform_optum.return_value = None, None

        # When
        processed: model.ProcessedNotification | None = (
            await transform_service._transform(message=message)
        )

        # Then
        assert not processed

    @staticmethod
    async def test_transform_stream_record_returns_value(
        transform_service: service.TransformationService,
    ):
        # Given
        message = model.UnprocessedNotification(
            record={},
            metadata=model.Metadata(
                type=repository.IngestionType.STREAM,
                identifier="optum",
                index=0,
                ingestion_ts=datetime.datetime.utcnow(),
            ),
        )
        transform_service._transform_optum = mock.AsyncMock()
        transform_service._transform_optum.return_value = (
            model.ProcessedMember(date_of_birth="05-01-2002", unique_corp_id="1234"),
            model.Address(),
        )

        # When
        processed: model.ProcessedNotification | None = (
            await transform_service._transform(message=message)
        )

        # Then
        assert processed


class TestTransformBatch:
    @staticmethod
    async def test_transform_batch_called_correct_num_times(
        transform_service: service.TransformationService,
        mock_publisher: pubsub.PubSubPublisher,
    ):
        # Given
        unprocessed_notification: model.UnprocessedNotification = (
            pubsub_factory.UnprocessedNotificationFactory()
        )
        batch_size = 5
        pubsub_message: List[
            pubsub.PubSubEntry
        ] = pubsub_factory.PubSubMessageFactory.create_batch(
            size=batch_size, data=unprocessed_notification
        )
        transform_service._transform = mock.AsyncMock()

        # When
        await transform_service.transform_batch(
            messages=pubsub_message, publisher=mock_publisher
        )

        # Then
        assert transform_service._transform.call_count == batch_size

    @staticmethod
    async def test_transform_batch_continues_on_exception(
        transform_service: service.TransformationService,
        mock_publisher: pubsub.PubSubPublisher,
    ):
        # Given
        unprocessed_notification: model.UnprocessedNotification = (
            pubsub_factory.UnprocessedNotificationFactory()
        )
        batch_size = 5
        pubsub_message: List[
            pubsub.PubSubEntry
        ] = pubsub_factory.PubSubMessageFactory.create_batch(
            size=batch_size, data=unprocessed_notification
        )
        processed_notifications: List[
            model.ProcessedNotification
        ] = pubsub_factory.ProcessedNotificationFactory.create_batch(size=4)
        _transform_side_effects = processed_notifications + [Exception]
        transform_service._transform = mock.AsyncMock(
            side_effect=_transform_side_effects
        )
        expected_calls = [
            pubsub.PublisherMessage(message=p) for p in processed_notifications
        ]

        # When
        await transform_service.transform_batch(
            messages=pubsub_message, publisher=mock_publisher
        )

        # Then
        mock_publisher.publish.assert_called_with(*expected_calls)

    @staticmethod
    async def test_transform_batch_transformation_ts_set(
        transform_service: service.TransformationService,
        mock_publisher: pubsub.PubSubPublisher,
    ):
        # Given
        unprocessed_notification: model.UnprocessedNotification = (
            pubsub_factory.UnprocessedNotificationFactory()
        )
        pubsub_message: pubsub.PubSubEntry = pubsub_factory.PubSubMessageFactory.create(
            data=unprocessed_notification
        )
        processed_notification: model.ProcessedNotification = (
            pubsub_factory.ProcessedNotificationFactory.create()
        )
        transform_service._transform = mock.AsyncMock(
            return_value=processed_notification
        )

        # When
        published: List[
            pubsub.PublisherMessage[model.ProcessedNotification]
        ] = await transform_service.transform_batch(
            messages=[pubsub_message], publisher=mock_publisher
        )

        # Then
        assert published[0].message.metadata.transformation_ts


class TestConsume:
    @staticmethod
    async def test_consume_unprocessed_called_once(
        subscription: pubsub.SubscriptionStream,
    ):
        # Given
        unprocessed_notification: model.UnprocessedNotification = (
            pubsub_factory.UnprocessedNotificationFactory()
        )
        pubsub_message: pubsub.PubSubEntry = pubsub_factory.PubSubMessageFactory.create(
            data=unprocessed_notification
        )
        subscription.next.return_value.__aiter__.return_value = [[pubsub_message]]

        # When
        with mock.patch(
            "ingestion.service.TransformationService.transform_batch"
        ) as mock_transform_batch:
            async for _ in service.consume_unprocessed(stream=subscription):
                continue

            # Then
            mock_transform_batch.assert_called_once()

    @staticmethod
    async def test_consume_yields_messages(subscription: pubsub.SubscriptionStream):
        # Given
        unprocessed_notification: model.UnprocessedNotification = (
            pubsub_factory.UnprocessedNotificationFactory()
        )
        pubsub_message: pubsub.PubSubEntry = pubsub_factory.PubSubMessageFactory.create(
            data=unprocessed_notification
        )
        subscription.next.return_value.__aiter__.return_value = [[pubsub_message]]

        # When
        with mock.patch("ingestion.service.TransformationService.transform_batch"):

            assert [
                f async for f in service.consume_unprocessed(stream=subscription)
            ] == [[pubsub_message]]


class TestTransformOptum:
    @staticmethod
    async def test_transform_optum_external_identifiers_unmapped(
        transform_service: service.TransformationService,
    ):
        # Given
        transform_service._ingest_config.get_external_org_info.return_value = None
        record = (
            model.OptumEligibilityRecord
        ) = pubsub_factory.OptumStreamValueFactory.create()
        metadata = model.Metadata = pubsub_factory.MetadataFactory.create()

        # When
        member: model.ProcessedMember | None
        address: model.Address | None
        member, address = await transform_service._transform_optum(
            record=record, metadata=metadata
        )

        # Then
        assert (member, address) == (None, None)

    @staticmethod
    async def test_transform_optum_effective_range_not_activated(
        transform_service: service.TransformationService,
    ):
        # Given
        record = (
            model.OptumEligibilityRecord
        ) = pubsub_factory.OptumStreamValueFactory.create()
        metadata = model.Metadata = pubsub_factory.MetadataFactory.create()
        transform_service._ingest_config.get_external_org_info.return_value = (
            db_model.ExternalMavenOrgInfo(
                organization_id=1, activated_at=datetime.datetime.today()
            )
        )

        # When
        member: model.ProcessedMember | None
        address: model.Address | None

        with mock.patch(
            "ingestion.service.transform.validate.is_effective_range_activated"
        ) as mock_is_effective_range_activated:
            mock_is_effective_range_activated.return_value = False
            member, address = await transform_service._transform_optum(
                record=record, metadata=metadata
            )

        # Then
        assert (member, address) == (None, None)

    @staticmethod
    async def test_transform_optum_returns_value(
        transform_service: service.TransformationService,
    ):
        # Given
        record = (
            model.OptumEligibilityRecord
        ) = pubsub_factory.OptumStreamValueFactory.create()
        metadata = model.Metadata = pubsub_factory.MetadataFactory.create()
        transform_service._ingest_config.get_external_org_info.return_value = (
            db_model.ExternalMavenOrgInfo(
                organization_id=1, activated_at=datetime.datetime.today()
            )
        )

        # When
        member: model.ProcessedMember | None
        address: model.Address | None

        member, address = await transform_service._transform_optum(
            record=record, metadata=metadata
        )

        # Then
        assert member and address

    @staticmethod
    async def test_transform_optum_record_extract_custom_attributes(
        transform_service: service.TransformationService,
    ):
        # Given
        record = (
            model.OptumEligibilityRecord
        ) = pubsub_factory.OptumStreamValueFactory.create()
        metadata = model.Metadata = pubsub_factory.MetadataFactory.create()

        # When
        member: model.ProcessedMember | None
        address: model.Address | None

        with mock.patch(
            "ingestion.service.transform.validate.is_effective_range_activated"
        ) as mock_is_effective_range_activated:
            mock_is_effective_range_activated.return_value = True
            member, address = await transform_service._transform_optum(
                record=record, metadata=metadata
            )
            assert len(member.custom_attributes) == len(record["attributes"])
