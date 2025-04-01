from unittest import mock

import pytest
from tests.factories import data_models as factory

from app.eligibility.constants import ProcessingResult
from app.worker import redis

from .factories import (
    PendingFileStreamEntryFactory,
    ProcessedRecordsFactory,
    processed_data,
)

pytestmark = pytest.mark.asyncio


async def test_process_file(stream, processor):
    # Given
    config = factory.ConfigurationFactory.create()
    file = factory.FileFactory.create(organization_id=config.organization_id)
    notification = PendingFileStreamEntryFactory.create(message__file_id=file.id)
    processed = ProcessedRecordsFactory.create(valid=processed_data(["foo", "bar"]))
    processor.configs.get.side_effect = mock.AsyncMock(return_value=config)
    processor.files.get.side_effect = mock.AsyncMock(return_value=file)
    processor.process.side_effect = mock.AsyncMock(
        return_value=(ProcessingResult.PROCESSING_SUCCESSFUL, processed)
    )
    stream.__aiter__.return_value = [notification]
    # When
    results = [p async for p in redis.process_file(stream)]
    # Then
    assert results == [processed]


async def test_process_file_no_file(stream, processor):
    # Given
    notification = PendingFileStreamEntryFactory.create()
    processor.files.get.side_effect = mock.AsyncMock(return_value=None)
    stream.__aiter__.return_value = [notification]
    # When
    results = [p async for p in redis.process_file(stream)]
    # Then
    assert results == []


async def test_process_file_no_data(stream, processor):
    # Given
    config = factory.ConfigurationFactory.create()
    file = factory.FileFactory.create(organization_id=config.organization_id)
    notification = PendingFileStreamEntryFactory.create(message__file_id=file.id)
    processor.configs.get.side_effect = mock.AsyncMock(return_value=config)
    processor.files.get.side_effect = mock.AsyncMock(return_value=file)
    processor.process.side_effect = mock.AsyncMock(
        return_value=(ProcessingResult.NO_RECORDS_FOUND, None)
    )
    stream.__aiter__.return_value = [notification]
    # When
    results = [p async for p in redis.process_file(stream)]
    # Then
    assert results == []


async def test_process_file_bad_data(stream, processor):
    # Given
    config = factory.ConfigurationFactory.create()
    file = factory.FileFactory.create(organization_id=config.organization_id)
    notification = PendingFileStreamEntryFactory.create(message__file_id=file.id)
    processor.configs.get.side_effect = mock.AsyncMock(return_value=config)
    processor.files.get.side_effect = mock.AsyncMock(return_value=file)
    processor.process.side_effect = mock.AsyncMock(
        return_value=(ProcessingResult.ERROR_DURING_PROCESSING, None)
    )
    stream.__aiter__.return_value = [notification]
    # When
    results = [p async for p in redis.process_file(stream)]
    # Then
    assert results == []
