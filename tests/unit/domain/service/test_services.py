from __future__ import annotations

import pytest

from app.eligibility.domain import model, repository, service
from db.model import File, FileParseError, FileParseResult

pytestmark = pytest.mark.asyncio


class FakeParsedRecordsDatabaseRepository(repository.ParsedRecordsAbstractRepository):
    def __init__(
        self,
        file_parse_results: list = None,
        file_parse_errors: list = None,
        members: list = None,
    ):
        """
        Initialize a fake repo with sets to represent our file_parse_results, file_parse_errors and members
        unique members are represented by an integer

        Args:
            file_parse_results:
            file_parse_errors:
            members:
        """
        if not file_parse_results:
            file_parse_results = []
        if not file_parse_errors:
            file_parse_errors = []
        if not members:
            members = []

        self.file_parse_results = set(file_parse_results)
        self.file_parse_errors = set(file_parse_errors)
        self.members = set(members)

    async def persist(
        self, parsed_records: model.ParsedFileRecords, file: File
    ) -> model.ProcessedRecords:
        """
        Persist a ParsedRecords instance

        Args:
            parsed_records:
            file:

        Returns:

        """
        if parsed_records.valid:
            await self.persist_valid(valid=set(parsed_records.valid))
        if parsed_records.errors:
            await self.persist_errors(errors=set(parsed_records.errors))

        return model.ProcessedRecords(
            valid=len(self.file_parse_results),
            errors=len(self.file_parse_errors),
            missing=len(self.members - self.file_parse_results),
        )

    async def flush(self, file: File):
        """
        Clear the staging "tables".

        Args:
            file:

        Returns:

        """
        await self.delete_errors(file=file)
        await self.persist_missing(file=file)
        await self.persist_as_members(file=file)
        await self.set_file_completed(file=file)

    async def delete_errors(self, file: File) -> int:
        """
        Clear the errors from staging tables

        Args:
            file:

        Returns:

        """
        n_errors = len(self.file_parse_errors)
        self.file_parse_errors.clear()
        return n_errors

    async def delete_results(self, file: File) -> int:
        """
        Clear the results from staging tables

        Args:
            file:

        Returns:

        """
        n_results = len(self.file_parse_results)
        self.file_parse_results.clear()
        return n_results

    async def persist_as_members(self, file: File) -> int:
        """
        Move the valid records from staging table to members table

        Args:
            file:

        Returns:

        """
        n_persisted = len(self.file_parse_results)
        self.file_parse_results.clear()
        return n_persisted

    async def persist_missing(self, file: File) -> int:
        """
        In the non-mock implementation, this should expire records, so this mock
        should just return number of records that we expire

        Args:
            file:
        Returns:

        """
        missing = self.members.difference(self.file_parse_results)
        return len(missing)

    async def persist_valid(
        self, valid: list[FileParseResult] | set, file: File | None = None
    ) -> int:
        """
        Save valid records to our staging table

        Args:
            valid:
            file:

        Returns:

        """
        self.file_parse_results = valid.copy()
        return len(valid)

    async def persist_errors(
        self, errors: list[FileParseError] | set, file: File | None = None
    ) -> int:
        """
        Save errors to our staging table

        Args:
            errors:
            file:

        Returns:

        """
        self.file_parse_errors = errors.copy()
        return len(errors)

    async def set_file_completed(self, file: File | None = None):
        pass

    async def check_file_completed(self, file: File | None = None) -> bool:
        """
        File is complete when there are no records and errors left

        Args:
            file:

        Returns:

        """
        return not self.file_parse_results and not self.file_parse_errors

    async def get_file(self, file_id: int) -> File:
        """
        This function will retrieve a File object based on id

        Args:
            file_id:

        Returns: File

        """
        return File(id=0, organization_id=0, name="coolfile.csv")

    async def get_raw_count_from_previous_file(self, organization_id: int) -> int:
        pass

    async def get_success_count_from_previous_file(self, organization_id: int) -> int:
        return 100


@pytest.mark.parametrize(
    argnames="valid,errors,members,action,expected",
    argvalues=[
        # Persist records, expire missing records, and delete errors
        (
            [1, 2, 3, 4, 5, 6],
            ["e1", "e2"],
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
            {"persist": True, "expire": True, "clear_errors": True, "purge_all": False},
            {
                "num_records_deleted": 0,
                "num_errors_deleted": 2,
                "num_members_expired": 8,
                "num_members_persisted": 6,
            },
        ),
        # Perform no actions in this test case
        (
            [1, 2, 3, 4, 5, 6],
            ["e1", "e2"],
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
            {
                "persist": False,
                "expire": False,
                "clear_errors": False,
                "purge_all": False,
            },
            {
                "num_records_deleted": 0,
                "num_errors_deleted": 0,
                "num_members_expired": 0,
                "num_members_persisted": 0,
            },
        ),
        # Only purge our staging tables, don't persist any records
        (
            [1, 2, 3, 4, 5, 6],
            ["e1", "e2"],
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
            {
                "persist": False,
                "expire": False,
                "clear_errors": False,
                "purge_all": True,
            },
            {
                "num_records_deleted": 6,
                "num_errors_deleted": 2,
                "num_members_expired": 0,
                "num_members_persisted": 0,
            },
        ),
        # Only persist valid records
        (
            [1, 2, 3, 4, 5, 6],
            ["e1", "e2"],
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
            {
                "persist": True,
                "expire": False,
                "clear_errors": False,
                "purge_all": False,
            },
            {
                "num_records_deleted": 0,
                "num_errors_deleted": 0,
                "num_members_expired": 0,
                "num_members_persisted": 6,
            },
        ),
        # Only expire missing records
        (
            [1, 2, 3, 4, 5, 6],
            ["e1", "e2"],
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
            {
                "persist": False,
                "expire": True,
                "clear_errors": False,
                "purge_all": False,
            },
            {
                "num_records_deleted": 0,
                "num_errors_deleted": 0,
                "num_members_expired": 8,
                "num_members_persisted": 0,
            },
        ),
        # Only clear errors from staging tables
        (
            [1, 2, 3, 4, 5, 6],
            ["e1", "e2"],
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
            {
                "persist": False,
                "expire": False,
                "clear_errors": True,
                "purge_all": False,
            },
            {
                "num_records_deleted": 0,
                "num_errors_deleted": 2,
                "num_members_expired": 0,
                "num_members_persisted": 0,
            },
        ),
    ],
)
async def test_process_actions_for_file(valid, errors, members, action, expected):
    db_repo = FakeParsedRecordsDatabaseRepository(
        file_parse_results=valid, file_parse_errors=errors, members=members
    )

    result = await service.process_actions_for_file(file_id=1, repo=db_repo, **action)

    assert result == expected


@pytest.mark.parametrize(
    argnames="valid,errors,expected",
    argvalues=[
        # Staging results should be reviewed and not flushed
        (
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            ["e1", "e2", "e3"],
            {"n_fpr": 10, "n_errors": 3},
        ),
        # Staging results should not be reviewed and records are flushed
        ([r for r in range(1, 100)], [], {"n_fpr": 0, "n_errors": 0}),
    ],
)
async def test_persist_and_flush(file, valid, errors, expected):
    db_repo = FakeParsedRecordsDatabaseRepository()

    parsed: model.ParsedFileRecords = model.ParsedFileRecords(
        valid=valid, errors=errors
    )

    processed: model.ProcessedRecords = await service.persist_and_flush(
        file=file, parsed_records=parsed, db_repository=db_repo
    )

    assert (
        processed.valid == len(valid)
        and processed.errors == len(errors)
        and expected["n_fpr"] == len(db_repo.file_parse_results)
        and expected["n_errors"] == len(db_repo.file_parse_errors)
    )


@pytest.mark.parametrize(
    argnames="valid,errors",
    argvalues=[
        (
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            ["e1", "e2", "e3"],
        ),
        (
            [],
            ["e1", "e2", "e3"],
        ),
        (
            [1, 2, 3],
            ["e1", "e2", "e3", "e4", "e5"],
        ),
    ],
)
async def test_persist(file, valid, errors):
    # Given
    db_repo = FakeParsedRecordsDatabaseRepository()
    parsed: model.ParsedFileRecords = model.ParsedFileRecords(
        valid=valid, errors=errors
    )

    # When
    processed: model.ProcessedRecords = await service.persist(
        file=file, parsed_records=parsed, db_repository=db_repo
    )

    # Then
    assert (processed.valid, processed.errors) == (len(valid), len(errors))


@pytest.mark.parametrize(
    argnames="valid,errors,flushed",
    argvalues=[
        ([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], ["e1", "e2", "e3"], False),
        ([i for i in range(100)], ["e1"], True),
        ([1, 2, 3], ["e1", "e2", "e3", "e4", "e5"], False),
    ],
)
async def test_flush(file, valid, errors, flushed):
    # Given
    db_repo = FakeParsedRecordsDatabaseRepository()
    db_repo.file_parse_results = valid
    db_repo.file_parse_errors = errors
    processed = model.ProcessedRecords(valid=len(valid), errors=len(errors))

    # When
    await service.flush(file=file, db_repository=db_repo, processed=processed)

    # Then
    assert (not db_repo.file_parse_results and not db_repo.file_parse_errors) == flushed


@pytest.mark.parametrize(
    argnames="result,should_review,threshold",
    argvalues=[
        (
            # 100%
            model.ProcessedRecords(errors=0, valid=1, missing=0),
            False,
            0.95,
        ),
        (
            # 90%
            model.ProcessedRecords(
                errors=10,
                valid=90,
                missing=0,
            ),
            True,
            0.95,
        ),
        (
            # 89%
            model.ProcessedRecords(
                errors=2,
                valid=100,
                missing=10,
            ),
            True,
            0.90,
        ),
    ],
)
def test_should_review(result, should_review, threshold):
    service.REVIEW_THRESHOLD = threshold
    assert service.should_review(result) == should_review


@pytest.mark.parametrize(
    argnames="result,repo,has_enough_valid_records,threshold",
    argvalues=[
        (
            # 100%
            model.ProcessedRecords(valid=100),
            FakeParsedRecordsDatabaseRepository(),
            True,
            0.90,
        ),
        (
            # 90%
            model.ProcessedRecords(valid=90),
            FakeParsedRecordsDatabaseRepository(),
            True,
            0.90,
        ),
        (
            # 85%
            model.ProcessedRecords(valid=85),
            FakeParsedRecordsDatabaseRepository(),
            False,
            0.90,
        ),
        (
            # 10%
            model.ProcessedRecords(valid=10),
            FakeParsedRecordsDatabaseRepository(),
            False,
            0.90,
        ),
    ],
    ids=["100%", "90%", "85%", "10%"],
)
async def test_has_enough_valid_records(
    file, result, repo, has_enough_valid_records, threshold
):
    service.REVIEW_THRESHOLD_PREVIOUS_FILE = threshold
    # When
    return_val = await service.has_enough_valid_records(file, result, repo)
    # Then
    assert return_val == has_enough_valid_records
