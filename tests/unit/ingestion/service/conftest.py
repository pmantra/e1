from unittest import mock

import pytest
from ingestion import repository, service
from mmstream import pubsub

from app.eligibility.domain.repository import parsed_records_db


@pytest.fixture
def file_service():
    return service.FileIngestionService(
        ingest_config=mock.create_autospec(repository.IngestConfigurationRepository),
        gcs=mock.create_autospec(repository.EligibilityFileManager),
    )


@pytest.fixture
def transform_service():
    return service.TransformationService(
        ingest_config=mock.create_autospec(repository.IngestConfigurationRepository),
        file_parser=mock.create_autospec(service.EligibilityFileParser),
    )


@pytest.fixture
def persist_service():
    return service.PersistenceService(
        ingest_config=mock.create_autospec(repository.IngestConfigurationRepository),
        file_parse_repo=mock.create_autospec(
            parsed_records_db.ParsedRecordsDatabaseRepository
        ),
        member_repo=mock.create_autospec(repository.MemberRepository),
    )


@pytest.fixture
def parser_service():
    return service.EligibilityFileParser()


@pytest.fixture
def mock_publisher():
    return mock.create_autospec(pubsub.PubSubPublisher)
