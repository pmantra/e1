from unittest import mock

import pytest
from ingestion import repository
from split.service import split

# from mmstream import pubsub

# from app.eligibility.domain.repository import parsed_records_db


@pytest.fixture
def file_split_service():
    return split.FileSplitService(
        ingest_config_repo=mock.create_autospec(
            repository.IngestConfigurationRepository
        ),
        file_manager=mock.create_autospec(repository.EligibilityFileManager),
    )
