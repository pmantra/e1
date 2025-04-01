from unittest import mock

import pytest
from tests.factories.data_models import ConfigurationFactory, MavenOrgExternalIDFactory

from app.eligibility import process
from app.eligibility.constants import ProcessingResult
from app.eligibility.domain import model
from db import model as db_model
from db.model import Configuration
from db.mono.client import MavenOrgExternalID

pytestmark = pytest.mark.asyncio


@pytest.fixture
def processor_bad_data(
    mock_manager, records, files, members, configs, header_aliases, file_data_bad_format
):
    mock_manager.get.return_value = file_data_bad_format.encode()
    processor = process.EligibilityFileProcessor("test")
    processor.manager = mock_manager
    processor.store = records
    processor.files = files
    processor.members = members
    processor.configs = configs
    processor.headers = header_aliases
    return processor


async def test_processor_process(mock_manager, config, file, file_data, header_aliases):
    # Given
    mock_manager.get.return_value = file_data.encode()
    processor = process.EligibilityFileProcessor("test")
    processor.manager = mock_manager
    processor.headers = header_aliases

    # When
    with mock.patch(
        "app.eligibility.domain.service.persist"
    ) as mocked_persist, mock.patch(
        "app.eligibility.domain.service.parsed_records.has_enough_valid_records"
    ) as mocked_does_file_have_enough_valid_records, mock.patch(
        "db.clients.verification_client.Verifications.batch_pre_verify_records_by_org",
        return_value=0,
    ):
        mocked_does_file_have_enough_valid_records.return_value = True
        mocked_persist.return_value = model.ProcessedRecords(valid=10_000)
        result, parsed = await processor.process("key", 1, file=file, config=config)

    assert result == ProcessingResult.PROCESSING_SUCCESSFUL and parsed.valid == 10_000


async def test_processor_process_custom_attributes(
    mock_manager, config, file, file_data, MockHeaderAliases
):
    # Given
    mock_manager.get.return_value = file_data.encode()
    processor = process.EligibilityFileProcessor("test")
    processor.manager = mock_manager
    custom_attributes = {
        "custom_attributes.sub_population_identifier": "sub_population_identifier"
    }
    header_aliases = MockHeaderAliases.return_value
    header_aliases.get_header_mapping.return_value = db_model.HeaderMapping(
        custom_attributes
    )
    processor.headers = header_aliases

    # When
    with mock.patch("app.eligibility.parse.EligibilityFileParser") as mocked_parser:
        await processor.process("key", 1, file=file, config=config)

    # Then
    # Asserted this way instead of using assert_called_with because the parser is called with many
    # args/kwargs that we don't care about
    mocked_parser.assert_called()
    assert (
        mocked_parser.call_args.kwargs.get("custom_attributes", None)
        == custom_attributes
    )


async def test_processor_process_data_provider(
    mock_manager, file, file_data, header_aliases
):
    # Given
    config_data_provider: Configuration = ConfigurationFactory.create(
        data_provider=True
    )
    config_sub_org: Configuration = ConfigurationFactory.create()
    external_id: MavenOrgExternalID = MavenOrgExternalIDFactory.create(
        organization_id=config_sub_org.organization_id,
        data_provider_organization_id=config_data_provider.organization_id,
    )

    mock_manager.get.return_value = file_data.encode()
    processor = process.EligibilityFileProcessor("test")
    processor.configs.get_external_ids_by_data_provider.return_value = [external_id]
    processor.manager = mock_manager
    processor.headers = header_aliases

    # When
    with mock.patch(
        "app.eligibility.domain.service.persist"
    ) as mocked_persist, mock.patch(
        "app.eligibility.domain.service.parsed_records.has_enough_valid_records"
    ) as mocked_does_file_have_enough_valid_records, mock.patch(
        "db.clients.verification_client.Verifications.batch_pre_verify_records_by_org",
        return_value=0,
    ):
        mocked_does_file_have_enough_valid_records.return_value = True
        mocked_persist.return_value = model.ProcessedRecords(valid=10_000)
        result, parsed = await processor.process(
            "key", 1, file=file, config=config_data_provider
        )

    assert result == ProcessingResult.PROCESSING_SUCCESSFUL and parsed.valid == 10_000


async def test_processor_process_data_provider_with_composite_key(
    mock_manager, file, file_data, header_aliases
):
    # Given
    config_data_provider: Configuration = ConfigurationFactory.create(
        data_provider=True
    )
    config_sub_org: Configuration = ConfigurationFactory.create()
    external_id: MavenOrgExternalID = MavenOrgExternalIDFactory.create(
        organization_id=config_sub_org.organization_id,
        data_provider_organization_id=config_data_provider.organization_id,
    )
    # Fake a random composite key
    external_id.external_id = "NIFDNESI:CFNDIN"
    mock_manager.get.return_value = file_data.encode()
    processor = process.EligibilityFileProcessor("test")
    processor.configs.get_external_ids_by_data_provider.return_value = [external_id]
    processor.manager = mock_manager
    processor.headers = header_aliases

    # When
    with mock.patch(
        "app.eligibility.domain.service.persist"
    ) as mocked_persist, mock.patch(
        "app.eligibility.domain.service.parsed_records.has_enough_valid_records"
    ) as mocked_does_file_have_enough_valid_records, mock.patch(
        "db.clients.verification_client.Verifications.batch_pre_verify_records_by_org",
        return_value=0,
    ):
        mocked_does_file_have_enough_valid_records.return_value = True
        mocked_persist.return_value = model.ProcessedRecords(valid=10_000)
        result, parsed = await processor.process(
            "key", 1, file=file, config=config_data_provider
        )

    assert result == ProcessingResult.PROCESSING_SUCCESSFUL and parsed.valid == 10_000


async def test_processor_bad_row_data(
    mock_manager, config, file, file_data_bad_format, header_aliases
):
    # Given
    mock_manager.get.return_value = file_data_bad_format.encode()
    processor = process.EligibilityFileProcessor("test")
    processor.manager = mock_manager
    processor.headers = header_aliases

    # When
    with mock.patch("app.eligibility.domain.service.persist") as mocked_persist:
        mocked_persist.return_value = model.ProcessedRecords(valid=8, errors=2)
        result, parsed = await processor.process("key", 1, file=file, config=config)

    # Then
    assert result == ProcessingResult.PROCESSING_SUCCESSFUL
    assert parsed.errors == 2


async def test_processor_no_file_found(mock_manager, config, file):
    # Given
    mock_manager.get.return_value = None
    processor = process.EligibilityFileProcessor("test")
    processor.manager = mock_manager

    # When
    result, parsed = await processor.process("key", 1, file=file, config=config)

    # Then
    assert result == ProcessingResult.FILE_MISSING


async def test_processor_cant_detect_encoding(
    mock_manager, file_data_bad_format, config, file
):
    # Given
    mock_manager.get.return_value = "I'm not encoded"
    processor = process.EligibilityFileProcessor("test")
    processor.manager = mock_manager

    # When
    result, parsed = await processor.process("key", 1, file=file, config=config)

    # Then
    assert result == ProcessingResult.BAD_FILE_ENCODING


async def test_processor_processing_error(
    mock_manager, file_data_bad_format, config, file
):
    # Given
    e = "РїРѕРј"
    mock_manager.get.return_value = e.encode("utf-8", errors="backslashreplace")
    processor = process.EligibilityFileProcessor("test")
    processor.manager = mock_manager

    # When
    result, parsed = await processor.process("key", 1, file=file, config=config)

    # Then
    assert result == ProcessingResult.ERROR_DURING_PROCESSING


async def test_processor_set_file_count_called(
    mock_manager, config, file, file_data, header_aliases
):
    # Given
    mock_manager.get.return_value = file_data.encode()
    processor = process.EligibilityFileProcessor("test")
    processor.manager = mock_manager
    processor.headers = header_aliases
    batch_size = 1
    num_valid = 10
    num_failure = 0

    # When
    with mock.patch(
        "app.eligibility.domain.service.persist"
    ) as mocked_persist, mock.patch(
        "app.eligibility.domain.service.persist_file_counts"
    ) as mock_persist_file_counts:
        mocked_persist.return_value = model.ProcessedRecords(valid=batch_size)
        await processor.process(
            "key", 1, file=file, config=config, batch_size=batch_size
        )

    mock_persist_file_counts.assert_called_with(
        db_repository=mock.ANY,
        file=file,
        success_count=num_valid,
        failure_count=num_failure,
    )


# endregion
