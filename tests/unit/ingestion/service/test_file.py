from unittest import mock

import pytest
from ingestion import model, repository, service
from mmstream import pubsub
from tests.factories import data_models as factories
from tests.factories.workers.pubsub import pubsub_factory

from db import model as db_model

pytestmark = pytest.mark.asyncio


class TestInitializeReader:
    @staticmethod
    async def test_initialize_reader_no_org_config(
        file_service: service.FileIngestionService,
    ):
        # Given
        with mock.patch(
            "ingestion.service.FileIngestionService._initialize_file"
        ) as mock_initialize_file:
            # No org configuration is found for file
            mock_initialize_file.return_value = (None, None)

            # When
            parser_config = await file_service.initialize_reader(filename="file")

        # Then
        assert not parser_config

    @staticmethod
    async def test_initialize_reader_missing_file_data(
        file_service: service.FileIngestionService,
    ):
        # Given
        file = factories.FileFactory.create()
        org_config = factories.ConfigurationFactory.create()

        with mock.patch(
            "ingestion.service.FileIngestionService._initialize_file"
        ) as mock_initialize_file:
            # create a file
            mock_initialize_file.return_value = (file, org_config)
            # nothing found in GCS for file
            file_service._gcs.get.return_value = None

            # When
            parser_config = await file_service.initialize_reader(filename="file")

        # Then
        assert not parser_config

    @staticmethod
    async def test_initialize_reader_missing_file_data_set_error_called(
        file_service: service.FileIngestionService,
    ):
        # Given
        file = factories.FileFactory.create()
        org_config = factories.ConfigurationFactory.create()

        with mock.patch(
            "ingestion.service.FileIngestionService._initialize_file"
        ) as mock_initialize_file:
            # create a file
            mock_initialize_file.return_value = (file, org_config)
            # nothing found in GCS for file
            file_service._gcs.get.return_value = None

            # When
            await file_service.initialize_reader(filename="file")

        # Then
        file_service._ingest_config.set_error.assert_called_with(
            file_id=file.id, error=db_model.FileError.MISSING
        )

    @staticmethod
    async def test_initialize_reader_encoding_is_set(
        file_service: service.FileIngestionService,
    ):
        # Given
        file: db_model.File = factories.FileFactory.create()
        org_config = factories.ConfigurationFactory.create()
        encoding: str = "ASCII"
        file_data: bytes = "some,csv,header".encode(encoding=encoding, errors="strict")

        with mock.patch(
            "ingestion.service.FileIngestionService._initialize_file"
        ) as mock_initialize_file:
            # create a file
            mock_initialize_file.return_value = (file, org_config)
            # data found in GCS for file
            file_service._gcs.get.return_value = file_data

            # When
            await file_service.initialize_reader(filename="file")

        # Then
        file_service._ingest_config.set_encoding.assert_called_with(
            file_id=file.id, encoding=encoding
        )

    @staticmethod
    async def test_initialize_reader_invalid_delimiter(
        file_service: service.FileIngestionService,
    ):
        # Given
        file: db_model.File = factories.FileFactory.create()
        org_config = factories.ConfigurationFactory.create()

        encoding: str = "ASCII"
        file_data: bytes = "some,csv,header".encode(encoding=encoding, errors="strict")

        with mock.patch(
            "ingestion.service.FileIngestionService._initialize_file"
        ) as mock_initialize_file, mock.patch(
            "ingestion.repository.EligibilityCSVReader.set_dialect"
        ) as mock_set_dialect:
            # create a file
            mock_initialize_file.return_value = (file, org_config)
            # data found in GCS for file
            file_service._gcs.get.return_value = file_data
            # set_dialect returns False
            mock_set_dialect.return_value = False

            # When
            parser_config = await file_service.initialize_reader(filename="file")

        # Then
        assert not parser_config

    @staticmethod
    async def test_initialize_reader_invalid_delimiter_set_error_called(
        file_service: service.FileIngestionService,
    ):
        # Given
        file: db_model.File = factories.FileFactory.create()
        org_config = factories.ConfigurationFactory.create()
        encoding: str = "ASCII"
        file_data: bytes = "some,csv,header".encode(encoding=encoding, errors="strict")

        with mock.patch(
            "ingestion.service.FileIngestionService._initialize_file"
        ) as mock_initialize_file, mock.patch(
            "ingestion.repository.EligibilityCSVReader.set_dialect"
        ) as mock_set_dialect:
            # create a file
            mock_initialize_file.return_value = (file, org_config)
            # data found in GCS for file
            file_service._gcs.get.return_value = file_data
            # set_dialect returns False
            mock_set_dialect.return_value = False

            # When
            await file_service.initialize_reader(filename="file")

        # Then
        file_service._ingest_config.set_error.assert_called_with(
            file_id=file.id, error=db_model.FileError.DELIMITER
        )

    @staticmethod
    async def test_initialize_reader_happy_path(
        file_service: service.FileIngestionService,
    ):
        # Given
        file: db_model.File = factories.FileFactory.create()
        org_config = factories.ConfigurationFactory.create()
        encoding: str = "ASCII"
        file_data: bytes = "some,csv,header".encode(encoding=encoding, errors="strict")

        with mock.patch(
            "ingestion.service.FileIngestionService._initialize_file"
        ) as mock_initialize_file, mock.patch(
            "ingestion.repository.EligibilityCSVReader.set_dialect"
        ) as mock_set_dialect:
            # create a file
            mock_initialize_file.return_value = (file, org_config)
            # data found in GCS for file
            file_service._gcs.get.return_value = file_data
            # set_dialect returns True
            mock_set_dialect.return_value = True

            # When
            parser_config = await file_service.initialize_reader(filename="file")

        # Then
        assert parser_config


class TestInitializeFile:
    @staticmethod
    async def test_initialize_file_missing_org_config(
        file_service: service.FileIngestionService,
    ):
        # Given
        filename: str = "mario_racing/file.csv"
        # This file is not setup based on the file directory
        file_service._ingest_config.sync.return_value = None

        # When
        file, org_config = await file_service._initialize_file(filename=filename)

        # Then
        assert not file and not org_config

    @staticmethod
    async def test_initialize_file_create_file_called(
        file_service: service.FileIngestionService,
    ):
        # Given
        filename: str = "mario_racing/file.csv"
        org_config: db_model.Configuration = factories.ConfigurationFactory.create()

        # This file is correctly setup based on the file directory
        file_service._ingest_config.sync.return_value = org_config

        # When
        await file_service._initialize_file(filename=filename)

        # Then
        file_service._ingest_config.create_file.assert_called_with(
            organization_id=org_config.organization_id, filename=filename
        )

    @staticmethod
    async def test_initialize_file_set_started_at_called(
        file_service: service.FileIngestionService,
    ):
        # Given
        filename: str = "mario_racing/file.csv"
        org_config: db_model.Configuration = factories.ConfigurationFactory.create()
        file: db_model.File = factories.FileFactory.create()

        # This file is correctly setup based on the file directory
        file_service._ingest_config.sync.return_value = org_config
        file_service._ingest_config.create_file.return_value = file

        # When
        await file_service._initialize_file(filename=filename)

        # Then
        file_service._ingest_config.set_started_at.assert_called_with(file_id=file.id)

    @staticmethod
    async def test_initialize_file_happy_path(
        file_service: service.FileIngestionService,
    ):
        # Given
        filename: str = "mario_racing/file.csv"
        expected_org_config: db_model.Configuration = (
            factories.ConfigurationFactory.create()
        )
        expected_file: db_model.File = factories.FileFactory.create()

        # This file is correctly setup based on the file directory
        file_service._ingest_config.sync.return_value = expected_org_config
        file_service._ingest_config.create_file.return_value = expected_file

        # When
        file, org_config = await file_service._initialize_file(filename=filename)

        # Then
        assert (file, org_config) == (expected_file, expected_org_config)


class TestDetectEncoding:
    @staticmethod
    @pytest.mark.parametrize(
        argnames="string_data,expected_encoding",
        argvalues=[("hello", "ASCII"), ("编程", "UTF-8")],
        ids=["ascii-encoded", "utf-8-encoded"],
    )
    def test_detect_encoding(
        file_service: service.FileIngestionService, string_data, expected_encoding
    ):
        # Given
        encoded: bytes = string_data.encode(expected_encoding)

        # When
        detected_encoding: str = file_service.detect_encoding(data=encoded)

        # Then
        assert detected_encoding == expected_encoding


class TestUpdateMetadata:
    @staticmethod
    async def test_update_metadata(file_service: service.FileIngestionService):
        # Given
        file_id: int = 1
        row_count: int = 100
        # When
        await file_service.set_row_count(file_id=file_id, row_count=row_count)
        # Then
        file_service._ingest_config.set_cache.assert_called_with(
            namespace=file_service.FILE_CACHE_NAMESPACE,
            id=file_id,
            key=file_service.FILE_COUNT_CACHE_KEY,
            value=row_count,
        )


class TestConsume:
    @staticmethod
    async def test_consume_process_file_called_once(
        subscription: pubsub.SubscriptionStream,
    ):
        # Given
        file_notification: model.FileUploadNotification = (
            pubsub_factory.FileUploadNotificationFactory()
        )
        pubsub_message: pubsub.PubSubEntry = pubsub_factory.PubSubMessageFactory.create(
            data=file_notification
        )
        subscription.__aiter__.return_value = [pubsub_message]

        # When
        with mock.patch(
            "ingestion.service.FileIngestionService.process_file"
        ) as mock_process_file:
            async for _ in service.consume_file(stream=subscription):
                continue

            # Then
            mock_process_file.assert_called_once()

    @staticmethod
    async def test_consume_yields_none(subscription: pubsub.SubscriptionStream):
        # Given
        file_notification: model.FileUploadNotification = (
            pubsub_factory.FileUploadNotificationFactory()
        )
        pubsub_message: pubsub.PubSubEntry = pubsub_factory.PubSubMessageFactory.create(
            data=file_notification
        )
        subscription.__aiter__.return_value = [pubsub_message]

        # When
        with mock.patch(
            "ingestion.service.FileIngestionService.process_file"
        ) as mock_process_file:
            mock_process_file.return_value = None

            assert [f async for f in service.consume_file(stream=subscription)] == [
                None
            ]

    @staticmethod
    async def test_consume_yields_file(subscription: pubsub.SubscriptionStream):
        # Given
        file: db_model.File = factories.FileFactory.create()
        file_notification: model.FileUploadNotification = (
            pubsub_factory.FileUploadNotificationFactory()
        )
        pubsub_message: pubsub.PubSubEntry = pubsub_factory.PubSubMessageFactory.create(
            data=file_notification
        )
        subscription.__aiter__.return_value = [pubsub_message]

        # When
        with mock.patch(
            "ingestion.service.FileIngestionService.process_file"
        ) as mock_process_file:
            mock_process_file.return_value = file

            assert [f async for f in service.consume_file(stream=subscription)] == [
                file
            ]


class TestPrivateProcessFile:
    @staticmethod
    async def test_process_file_parse_called_once(
        mock_publisher: pubsub.PubSubPublisher,
        file_service: service.FileIngestionService,
    ):
        # Given
        parser_config = service.FileParserConfig(
            reader=repository.EligibilityCSVReader(data="hello".encode("ASCII")),
            file=factories.FileFactory.create(),
            org_config=factories.ConfigurationFactory.create(),
        )

        # When
        with mock.patch(
            "ingestion.repository.EligibilityCSVReader.parse"
        ) as mock_parse:
            mock_parse.return_value = [[{"hey": "there"}]]

            await file_service._process_file(
                parser_config=parser_config, publisher=mock_publisher
            )

        # Then
        mock_parse.assert_called_once()

    @staticmethod
    async def test_process_file_publish_called_twice(
        mock_publisher: pubsub.PubSubPublisher,
        file_service: service.FileIngestionService,
    ):
        parser_config = service.FileParserConfig(
            reader=repository.EligibilityCSVReader(data="hello".encode("ASCII")),
            file=factories.FileFactory.create(),
            org_config=factories.ConfigurationFactory.create(),
        )

        # When
        with mock.patch(
            "ingestion.repository.EligibilityCSVReader.parse"
        ) as mock_parse:
            mock_parse.return_value = [[{"hey": "there"}], [{"hey": "there"}]]

            await file_service._process_file(
                parser_config=parser_config, publisher=mock_publisher
            )

        # Then
        assert mock_publisher.publish.call_count == 2

    @staticmethod
    async def test_process_file_set_row_count_called(
        mock_publisher: pubsub.PubSubPublisher,
        file_service: service.FileIngestionService,
    ):
        parser_config = service.FileParserConfig(
            reader=repository.EligibilityCSVReader(data="hello".encode("ASCII")),
            file=factories.FileFactory.create(),
            org_config=factories.ConfigurationFactory.create(),
        )

        # When
        with mock.patch(
            "ingestion.service.FileIngestionService.set_row_count"
        ) as mock_set_row_count, mock.patch(
            "ingestion.repository.EligibilityCSVReader.parse"
        ) as mock_parse:
            mock_parse.return_value = [[{"hey": "there"}]]

            await file_service._process_file(
                parser_config=parser_config, publisher=mock_publisher
            )

        # Then
        mock_set_row_count.assert_called()


class TestPublicProcessFile:
    @staticmethod
    async def test_process_file_unable_to_initialize(
        mock_publisher: pubsub.PubSubPublisher,
        file_service: service.FileIngestionService,
    ):
        # Given
        filename: str = "krustykrab/employees.csv"

        # When
        with mock.patch(
            "ingestion.service.FileIngestionService.initialize_reader"
        ) as mock_init_reader, mock.patch(
            "ingestion.service.FileIngestionService._process_file"
        ) as mock_process_file:
            mock_init_reader.return_value = None

            await file_service.process_file(filename=filename, publisher=mock_publisher)

            mock_process_file.assert_not_called()

    @staticmethod
    async def test_process_unable_to_initialize_returns_none(
        mock_publisher: pubsub.PubSubPublisher,
        file_service: service.FileIngestionService,
    ):
        # Given
        filename: str = "krustykrab/employees.csv"

        # When
        with mock.patch(
            "ingestion.service.FileIngestionService.initialize_reader"
        ) as mock_init_reader:
            mock_init_reader.return_value = None

            file = await file_service.process_file(
                filename=filename, publisher=mock_publisher
            )

        # Then
        assert not file

    @staticmethod
    async def test_process_file_called(
        mock_publisher: pubsub.PubSubPublisher,
        file_service: service.FileIngestionService,
    ):
        # Given
        parser_config = service.FileParserConfig(
            reader=repository.EligibilityCSVReader(data="hello".encode("ASCII")),
            file=factories.FileFactory.create(),
            org_config=factories.ConfigurationFactory.create(),
        )

        with mock.patch(
            "ingestion.service.FileIngestionService.initialize_reader"
        ) as mock_init_reader, mock.patch(
            "ingestion.service.FileIngestionService._process_file"
        ) as mock_process_file:
            mock_init_reader.return_value = parser_config

            # When
            await file_service.process_file(
                filename=parser_config.file.name, publisher=mock_publisher
            )

            # Then
            mock_process_file.assert_called_once()

    @staticmethod
    async def test_process_file_raises_exception(
        mock_publisher: pubsub.PubSubPublisher,
        file_service: service.FileIngestionService,
    ):
        # Given
        parser_config = service.FileParserConfig(
            reader=repository.EligibilityCSVReader(data="hello".encode("ASCII")),
            file=factories.FileFactory.create(),
            org_config=factories.ConfigurationFactory.create(),
        )

        with mock.patch(
            "ingestion.service.FileIngestionService.initialize_reader"
        ) as mock_init_reader, mock.patch(
            "ingestion.service.FileIngestionService._process_file"
        ) as mock_process_file:
            mock_init_reader.return_value = parser_config
            mock_process_file.side_effect = Exception("Some file level exception")

            # When
            file = await file_service.process_file(
                filename=parser_config.file.name, publisher=mock_publisher
            )

        # Then
        assert file == parser_config.file

    @staticmethod
    async def test_process_file_set_error_called_on_exception(
        mock_publisher: pubsub.PubSubPublisher,
        file_service: service.FileIngestionService,
    ):
        # Given
        parser_config = service.FileParserConfig(
            reader=repository.EligibilityCSVReader(data="hello".encode("ASCII")),
            file=factories.FileFactory.create(),
            org_config=factories.ConfigurationFactory.create(),
        )

        with mock.patch(
            "ingestion.service.FileIngestionService.initialize_reader"
        ) as mock_init_reader, mock.patch(
            "ingestion.service.FileIngestionService._process_file"
        ) as mock_process_file:
            mock_init_reader.return_value = parser_config
            mock_process_file.side_effect = Exception("Some file level exception")

            # When
            file = await file_service.process_file(
                filename=parser_config.file.name, publisher=mock_publisher
            )

        # Then
        file_service._ingest_config.set_error.assert_called_with(
            file_id=file.id, error=db_model.FileError.UNKNOWN
        )

    @staticmethod
    async def test_process_file_set_error_not_called_on_exception(
        mock_publisher: pubsub.PubSubPublisher,
        file_service: service.FileIngestionService,
    ):
        # Given
        expected_file: db_model.File = factories.FileFactory.create()

        with mock.patch(
            "ingestion.service.FileIngestionService.initialize_reader"
        ) as mock_init_reader:
            mock_init_reader.side_effect = Exception(
                "Some exception encountered while initializing reader"
            )

            # When
            await file_service.process_file(
                filename=expected_file.name, publisher=mock_publisher
            )

        # Then
        file_service._ingest_config.set_error.assert_not_called()
