from unittest import mock

import pytest
from ingestion import repository

from app.common import crypto, gcs


@pytest.fixture
def ingest_config(MockFiles, MockConfigurations, MockHeaderAliases, MockMonoClient):
    return repository.IngestConfigurationRepository(
        file_client=MockFiles,
        config_client=MockConfigurations,
        header_client=MockHeaderAliases,
        mono_client=MockMonoClient,
    )


@pytest.fixture
def file_manager():
    # Given
    file_manager = repository.EligibilityFileManager(project="project")
    file_manager.storage = mock.create_autospec(gcs.Storage)
    file_manager.crypto = mock.create_autospec(crypto.Cryptographer)
    return file_manager
