from __future__ import annotations

import ddtrace
import structlog
from mmlib.ops import stats

import constants
from app.eligibility.domain import model, repository
from db.model import File

__all__ = (
    "process_actions_for_file",
    "persist_and_flush",
    "persist",
    "persist_file_counts",
    "flush",
    "should_review",
    "has_enough_valid_records",
    "REVIEW_THRESHOLD",
)

logger = structlog.getLogger(__name__)

REVIEW_THRESHOLD = 0.95
REVIEW_THRESHOLD_PREVIOUS_FILE = 0.90


@ddtrace.tracer.wrap()
async def process_actions_for_file(
    file_id: int,
    repo: repository.ParsedRecordsDatabaseRepository,
    *,
    persist: bool = False,
    expire: bool = False,
    clear_errors: bool = False,
    purge_all: bool = False,
) -> dict:
    """
    This service function will take a ParsedRecordsDatabaseRepository object, and use it to
    orchestrate actions to take against file records

    Args:
        file_id:
        repo:
        persist:
        expire:
        clear_errors:
        purge_all:

    Returns:

    """
    logger.info(
        "Processing actions for file",
        file_id=file_id,
        persist=persist,
        expire=expire,
        clear_errors=clear_errors,
        purge_all=purge_all,
    )

    # This is used purely for the purpose of passing the file_id through
    # When we remove redis, we can modify the ABC to take a file_id instead
    # of a file obj as the parameter
    file: File = await repo.get_file(file_id=file_id)

    records_deleted: int = 0
    errors_deleted: int = 0
    members_expired: int = 0
    members_persisted: int = 0

    if clear_errors:
        errors_deleted = await repo.delete_errors(file=file)

    # We want to expire records first because once we clear out the records
    # from file_parse_results, we won't know what is missing
    if expire:
        members_expired = await repo.persist_missing(file=file)

    if persist:
        members_persisted = await repo.persist_as_members(file=file)

    if purge_all:
        # Delete file parse errors for this file
        errors_deleted = await repo.delete_errors(file=file)
        # Delete file parse results for this file
        records_deleted = await repo.delete_results(file=file)

    if await repo.check_file_completed(file=file):
        # Mark the file as completed
        await repo.set_file_completed(file=file)
        logger.info("File marked as complete", file_id=file.id)

    return {
        "num_records_deleted": records_deleted,
        "num_errors_deleted": errors_deleted,
        "num_members_expired": members_expired,
        "num_members_persisted": members_persisted,
    }


@ddtrace.tracer.wrap()
async def persist_and_flush(
    file: File,
    parsed_records: model.ParsedFileRecords,
    db_repository: repository.ParsedRecordsDatabaseRepository,
) -> model.ProcessedRecords:
    """
    Orchestrates the persistence for parsed records into temp storage and
    flushing the temp storage into permanent storage

    Args:
        file:
        parsed_records:
        db_repository:

    Returns:

    """
    db_processed: model.ProcessedRecords = await db_repository.persist(
        parsed_records=parsed_records, file=file
    )

    if should_review(processed=db_processed):
        logger.info(
            "File exceeded error threshold - records quarantined for review",
            file_id=file.id,
        )
        return db_processed

    file_has_enough_valid_records: bool = await has_enough_valid_records(
        file=file, processed=db_processed, db_repository=db_repository
    )

    if not file_has_enough_valid_records:
        logger.info(
            f"Count of valid member records in this file are below threshold (%{REVIEW_THRESHOLD_PREVIOUS_FILE * 100}%) - records quarantined for review",
            file_id=file.id,
            organization_id=file.organization_id,
        )
        return db_processed

    await db_repository.flush(file=file)

    logger.info(
        "File records flushed from staging tables",
        file_id=file.id,
        valid_count=db_processed.valid,
        error_count=db_processed.errors,
    )

    return db_processed


@ddtrace.tracer.wrap()
async def persist(
    file: File,
    parsed_records: model.ParsedFileRecords,
    db_repository: repository.ParsedRecordsDatabaseRepository,
) -> model.ProcessedRecords:
    """
    Orchestrates the persistence for parsed records into temp storage

    Args:
        file:
        parsed_records:
        db_repository:

    Returns:

    """
    db_processed: model.ProcessedRecords = await db_repository.persist(
        parsed_records=parsed_records, file=file
    )

    return db_processed


@ddtrace.tracer.wrap()
async def persist_file_counts(
    file: File,
    db_repository: repository.ParsedRecordsDatabaseRepository,
    success_count: int = 0,
    failure_count: int = 0,
):
    """
    Save the file counts for a file

    Args:
        file:
        db_repository:
        success_count:
        failure_count:

    Returns:

    """
    await db_repository.persist_file_counts(
        file=file, success_count=success_count, failure_count=failure_count
    )


@ddtrace.tracer.wrap()
async def flush(
    file: File,
    db_repository: repository.ParsedRecordsDatabaseRepository,
    processed: model.ProcessedRecords,
):
    """
    Orchestrates the flush of records from temp storage into perm tables

    Args:
        file:
        db_repository:
        processed:

    Returns:

    """
    if should_review(processed=processed):
        stats.increment(
            metric_name="eligibility.process.file_parse.above_error_threshold",
            pod_name=constants.POD,
            tags=[
                "eligibility:info",
                f"file_id:{file.id}",
                f"organization_id:{file.organization_id}",
            ],
        )
        logger.info(
            "File exceeded error threshold - records quarantined for review",
            file_id=file.id,
        )
        return processed

    file_has_enough_valid_records: bool = await has_enough_valid_records(
        file=file, processed=processed, db_repository=db_repository
    )

    if not file_has_enough_valid_records:
        logger.info(
            f"Count of valid member records in this file are below threshold (%{REVIEW_THRESHOLD_PREVIOUS_FILE * 100}%) - records quarantined for review",
            file_id=file.id,
            organization_id=file.organization_id,
        )
        return processed

    await db_repository.flush(file=file)

    stats.increment(
        metric_name="eligibility.process.file_parse.below_error_threshold",
        pod_name=constants.POD,
        tags=[
            "eligibility:info",
            f"file_id:{file.id}",
            f"organization_id:{file.organization_id}",
        ],
    )
    logger.info(
        "File records flushed from staging tables",
        file_id=file.id,
        valid_count=processed.valid,
        error_count=processed.errors,
    )

    return processed


@ddtrace.tracer.wrap()
def should_review(processed: model.ProcessedRecords) -> bool:
    """
    Determine if a set of processed records should be reviewed
    heuristics: https://app.clubhouse.io/maven-clinic/story/8727/determine-heuristics-for-when-to-automatically-persist-processed-records#activity-34848

    Args:
        processed:

    Returns:

    """
    if not processed:
        return True

    n_errors, n_missing, n_valid = (
        processed.errors,
        processed.missing,
        processed.valid,
    )

    persist_rate = n_valid / sum((n_valid, n_missing, n_errors))

    return persist_rate <= REVIEW_THRESHOLD


@ddtrace.tracer.wrap()
async def has_enough_valid_records(
    file: File,
    processed: model.ProcessedRecords,
    db_repository: repository.ParsedRecordsDatabaseRepository,
) -> bool:
    """
    Check if the file for a given organization has enough valid records compared to a previous version.

    Args:
        file (File): The file to check.
        processed (model.ProcessedRecords): Processed records information.
        db_repository (repository.ParsedRecordsDatabaseRepository): Database repository.

    Returns:
        bool: True if the file has enough valid records, False otherwise.
    """
    # Retrieve the number of valid records from the previous file
    n_valid_current = processed.valid
    n_valid_previous = await db_repository.get_success_count_from_previous_file(
        organization_id=file.organization_id
    )

    # Skip check if the previous file doesn't exist or the count is 0
    if n_valid_previous is None or n_valid_previous == 0:
        return True

    # Calculate the valid rate
    valid_rate = n_valid_current / n_valid_previous

    logger.info(
        "check if file has enough valid records compared to previous file",
        valid_records_from_current_file=n_valid_current,
        valid_records_from_previous_file=n_valid_previous,
        valid_rate=valid_rate,
    )

    # Check if the valid rate is above the threshold
    return valid_rate >= REVIEW_THRESHOLD_PREVIOUS_FILE
