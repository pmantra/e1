from unittest import mock

import pytest

from app.eligibility.domain import repository
from db import model as db_model

pytestmark = pytest.mark.asyncio


class TestParsedRecordsDatabaseRepository:

    # region persist_as_members

    @staticmethod
    async def test_persist_as_members_calls_dual_write_hash(
        parsed_records_repo: repository.ParsedRecordsDatabaseRepository,
    ):
        # Given
        file = db_model.File(organization_id=1, name="file", success_count=10)

        # When
        await parsed_records_repo.persist_as_members(file=file)

        # Then
        parsed_records_repo.fpr_client.bulk_persist_parsed_records_for_files_dual_write_hash.assert_called_once()

    @staticmethod
    async def test_persist_as_members_calls_dual_write_hash_with_large_records(
        parsed_records_repo: repository.ParsedRecordsDatabaseRepository,
    ):
        # Given
        file = db_model.File(organization_id=1, name="file", success_count=1000_001)

        # When
        with mock.patch(
            "app.eligibility.domain.repository.parsed_records_db.ParsedRecordsDatabaseRepository.get_organization_ids_for_file",
            return_value={2, 3, 4},
        ):
            await parsed_records_repo.persist_as_members(file=file)

        # Then
        assert (
            parsed_records_repo.fpr_client.bulk_persist_parsed_records_for_file_and_org_dual_write_hash.call_count
            == 3
        )
        parsed_records_repo.fpr_client.bulk_persist_parsed_records_for_files_dual_write_hash.assert_called_once()

    # endregion persist_as_members

    # region persist_missing
    @staticmethod
    async def test_persist_missing_write_to_member(
        parsed_records_repo: repository.ParsedRecordsDatabaseRepository,
    ):
        # Given
        file = db_model.File(organization_id=1, name="file")
        # feature flag not configured should default to expiring of only member table records

        # When
        await parsed_records_repo.persist_missing(file=file)

        # Then
        assert (
            parsed_records_repo.fpr_client.expire_missing_records_for_file_versioned.called
        )

    @staticmethod
    async def test_persist_missing_write_to_member_versioned(
        parsed_records_repo: repository.ParsedRecordsDatabaseRepository,
    ):
        # Given
        file = db_model.File(organization_id=1, name="file")

        # When
        await parsed_records_repo.persist_missing(file=file)

        # Then
        assert (
            parsed_records_repo.fpr_client.expire_missing_records_for_file_versioned.called
            and parsed_records_repo.fpr_client.expire_missing_records_for_file.called
        )

    @staticmethod
    async def test_persist_missing_dual_write(
        parsed_records_repo: repository.ParsedRecordsDatabaseRepository,
    ):
        # Given
        file = db_model.File(organization_id=1, name="file")

        await parsed_records_repo.persist_missing(file=file)

        # Then
        assert (
            parsed_records_repo.fpr_client.expire_missing_records_for_file_versioned.called
            and parsed_records_repo.fpr_client.expire_missing_records_for_file.called
        )

    # endregion persist_missing

    @staticmethod
    async def test_persist_file_counts(
        parsed_records_repo: repository.ParsedRecordsDatabaseRepository,
    ):
        # Given
        file = db_model.File(organization_id=1, name="file")
        success_count: int = 150
        failure_count: int = 20

        # When
        await parsed_records_repo.persist_file_counts(
            file=file, success_count=success_count, failure_count=failure_count
        )

        # Then
        parsed_records_repo.file_client.set_file_count.assert_called_with(
            id=file.id, raw_count=170, success_count=150, failure_count=20
        )
