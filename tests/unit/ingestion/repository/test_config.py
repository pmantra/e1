from unittest import mock

import pytest
from ingestion import repository

pytestmark = pytest.mark.asyncio


class TestSync:
    @staticmethod
    async def test_sync_awaited(
        ingest_config: repository.IngestConfigurationRepository,
    ):
        # Given
        filename: str = "primary/file.csv"
        # When
        with mock.patch(
            "ingestion.repository.config.sync.sync_single_mono_org_for_directory"
        ) as mock_sync:
            await ingest_config.sync(filename=filename)

            mock_sync.assert_awaited()

    @staticmethod
    async def test_sync_awaited_with_directory(
        ingest_config: repository.IngestConfigurationRepository,
    ):
        # Given
        directory: str = "primary"
        filename: str = "file.csv"
        # When
        with mock.patch(
            "ingestion.repository.config.sync.sync_single_mono_org_for_directory"
        ) as mock_sync:
            await ingest_config.sync(filename=f"{directory}/{filename}")

            mock_sync.assert_awaited_with(
                configuration_client=ingest_config._config_client,
                header_client=ingest_config._header_client,
                mono_client=ingest_config._mono_client,
                directory=directory,
            )


class TestSplitFileName:
    @staticmethod
    async def test_split_filename(
        ingest_config: repository.IngestConfigurationRepository,
    ):
        # Given
        expected_directory: str = "primary"
        expected_filename: str = "file.csv"
        full_filename: str = f"{expected_directory}/{expected_filename}"

        # When
        directory, file = ingest_config._split_filename(filename=full_filename)

        # Then
        assert (directory, file) == (expected_directory, expected_filename)
