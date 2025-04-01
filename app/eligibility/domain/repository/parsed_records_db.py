from __future__ import annotations

import datetime
from typing import List, Optional, Set

import ddtrace
import structlog

from app.eligibility.domain import model
from app.eligibility.domain.repository import ParsedRecordsAbstractRepository
from app.tasks import pre_verify
from db import model as db_model
from db.clients.configuration_client import Configurations
from db.clients.file_client import Files
from db.clients.file_parse_results_client import FileParseResults
from db.clients.member_client import Members
from db.clients.member_versioned_client import MembersVersioned
from db.clients.verification_client import Verifications
from db.mono.client import MavenOrgExternalID

__all__ = "ParsedRecordsDatabaseRepository"


logger = structlog.getLogger(__name__)

PERSIST_ALL_THRESHOLD = 1000_000


class ParsedRecordsDatabaseRepository(ParsedRecordsAbstractRepository):
    __slots__ = (
        "fpr_client",
        "member_client",
        "file_client",
        "member_versioned_client",
        "verification_client",
        "config_client",
    )

    def __init__(
        self,
        fpr_client: FileParseResults | None = None,
        member_client: Members | None = None,
        member_versioned_client: MembersVersioned | None = None,
        verification_client: Verifications | None = None,
        file_client: Files | None = None,
        config_client: Configurations | None = None,
        use_tmp: bool | None = False,
    ):
        self._use_tmp: bool = use_tmp
        self.fpr_client = fpr_client or FileParseResults()
        self.member_client = member_client or Members()
        self.member_versioned_client = member_versioned_client or MembersVersioned()
        self.verification_client = verification_client or Verifications()
        self.file_client = file_client or Files()
        self.config_client = config_client or Configurations()

    @ddtrace.tracer.wrap()
    async def persist(
        self, parsed_records: model.ParsedFileRecords, file: db_model.File
    ) -> model.ProcessedRecords:
        """
        Top-level function call to persist records and errors in postgres.

        Args:
            parsed_records:
            file:

        Returns: model.ProcessedRecords

        """
        processed = model.ProcessedRecords()

        if parsed_records.errors:
            logger.info(
                "Persisting errors to staging tables in DB",
                count=len(parsed_records.errors),
            )
            processed.errors = await self.persist_errors(
                errors=parsed_records.errors, file=file.id
            )

        if parsed_records.valid:
            logger.info(
                "Persisting records to staging tables in DB",
                count=len(parsed_records.valid),
            )
            processed.valid = await self.persist_valid(
                valid=parsed_records.valid, file=file.id
            )

        return processed

    @ddtrace.tracer.wrap()
    async def flush(self, file: db_model.File):
        """
        Clear the temp tables, persist records into permanent storage, and expire
        existing members that were missing from new file. With the option to do
        a no-op.

        Args:
            file:

        Returns:

        """
        # 1. Delete file_parse_errors
        logger.info(
            "Deleting file parsing errors...",
            organization_id=file.organization_id,
            file_id=file.id,
        )
        await self.delete_errors(file=file)
        # 2. Move the members to member_versioned table
        logger.info(
            "Persisting members...",
            organization_id=file.organization_id,
            file_id=file.id,
        )
        await self.persist_as_members(file=file)
        # 3. Pre-verify these new members
        logger.info(
            "Pre-verifying members...",
            organization_id=file.organization_id,
            file_id=file.id,
        )
        await self.pre_verify(file=file)
        # 4. Expire the old population
        logger.info(
            "Expiring missing members...",
            organization_id=file.organization_id,
            file_id=file.id,
        )
        expired_count = await self.persist_missing(file=file)

        # 5. Get count of records that were inserted/updated
        logger.info(
            "Getting record counts...",
            organization_id=file.organization_id,
            file_id=file.id,
        )
        hashed_inserted = await self.fpr_client.get_count_hashed_inserted_for_file(
            file_id=file.id, file_created_at=file.created_at
        )
        # 6. Done
        logger.info(
            "Marking file as complete...",
            organization_id=file.organization_id,
            file_id=file.id,
        )
        await self.set_file_completed(file=file)

        logger.info(
            "Completed file processing",
            organization_id=file.organization_id,
            file_id=file.id,
            expired_rows=expired_count,
            hashed_noop_rows=hashed_inserted["hashed_count"],
            inserted_rows=hashed_inserted["new_count"],
            total_rows=hashed_inserted["hashed_count"] + hashed_inserted["new_count"],
        )

    @ddtrace.tracer.wrap()
    async def persist_errors(
        self, errors: list[db_model.FileParseError], file: db_model.File = None
    ) -> int:
        """
        Persist the errors into the temp tables in Postgres

        Args:
            errors:
            file:

        Returns: list

        """
        if self._use_tmp:
            return await self.fpr_client.tmp_bulk_persist_file_parse_errors(
                errors=errors
            )
        else:
            return await self.fpr_client.bulk_persist_file_parse_errors(errors=errors)

    @ddtrace.tracer.wrap()
    async def persist_valid(
        self, valid: list[db_model.FileParseResult], file: db_model.File = None
    ) -> int:
        """
        Persist the valid records into the temp tables in postgres

        Args:
            valid:
            file:

        Returns: list

        """
        if self._use_tmp:
            return await self.fpr_client.tmp_bulk_persist_file_parse_results(
                results=valid
            )
        else:
            return await self.fpr_client.bulk_persist_file_parse_results(results=valid)

    @ddtrace.tracer.wrap()
    async def persist_missing(self, file: db_model.File) -> int:
        """
        Expire existing member records that were not found in the newly processed file.
        The no-op option will not expire any existing records.

        Args:
            file:

        Returns: int

        """
        if self._use_tmp:
            return await self.fpr_client.tmp_expire_missing_records_for_file(
                file_id=file.id, organization_id=file.organization_id
            )

        try:
            expired = await self.fpr_client.expire_missing_records_for_file_versioned(
                file_id=file.id, organization_id=file.organization_id
            )
        except Exception as e:
            logger.exception(
                "[dual-ingest] Exception encountered while expiring file records",
                error=e,
            )
        await self.fpr_client.expire_missing_records_for_file(
            file_id=file.id, organization_id=file.organization_id
        )
        return expired

    @ddtrace.tracer.wrap()
    async def persist_as_members(self, file: db_model.File) -> int:
        # in case file count > than threshold, we persist in smaller batch by file and org
        if file.success_count > PERSIST_ALL_THRESHOLD:
            organization_ids: Set[int] = await self.get_organization_ids_for_file(
                file=file
            )
            for organization_id in organization_ids:
                logger.info(
                    "Persisting members for file and org",
                    file_id=file.id,
                    organization_id=organization_id,
                )
                await self._persist_organization_members(file, organization_id)
        # in case there is missing org, we do a persist all at the end
        logger.info(
            "Persisting remaining members for file",
            file_id=file.id,
        )
        return await self._persist_all_as_members(file)

    async def _persist_organization_members(
        self, file: db_model.File, organization_id: int
    ) -> None:
        return await self.fpr_client.bulk_persist_parsed_records_for_file_and_org_dual_write_hash(
            file.id, organization_id
        )

    async def _persist_all_as_members(self, file: db_model.File) -> int:
        """
        Move the valid records from the temp tables into the member record table for
        permanent storage.

        Args:
            file:

        Returns: int

        """
        return (
            await self.fpr_client.bulk_persist_parsed_records_for_files_dual_write_hash(
                file.id
            )
        )

    @ddtrace.tracer.wrap()
    async def persist_file_counts(
        self, *, file: db_model.File, success_count: int = 0, failure_count: int = 0
    ):
        """Save the success and failure count of a file"""
        await self.file_client.set_file_count(
            id=file.id,
            raw_count=success_count + failure_count,
            success_count=success_count,
            failure_count=failure_count,
        )
        file.raw_count = success_count + failure_count
        file.success_count = success_count
        file.failure_count = failure_count

    @ddtrace.tracer.wrap()
    async def get_success_count_from_previous_file(
        self, *, organization_id: int
    ) -> Optional[int]:
        """
        Get the success record count from the file that precedes the latest file for a given organization.

        Args:
            organization_id (int): The ID of the organization.

        Returns:
            int: The raw record count from the previous file.
        """
        # current file will always be latest as we create a record for it as soon as receive it
        # previous file is the one before latest
        previous_file = await self.file_client.get_one_before_latest_for_org(
            organization_id=organization_id
        )
        if previous_file is not None:
            try:
                success_count = await self.file_client.get_success_count(
                    id=previous_file.id
                )
                return success_count
            except Exception as e:
                logger.error(f"Error retrieving success count for previous file: {e}")
                return 0
        else:
            logger.info("No previous file found.")
            return 0

    @ddtrace.tracer.wrap()
    async def get_organization_ids_for_file(self, file: db_model.File) -> Set[int]:
        """For a file, determine what organizations are part of that file"""
        config: db_model.Configuration = await self.config_client.get(
            file.organization_id
        )
        organization_ids: List[int]

        if config is not None and config.data_provider is True:
            mappings: List[
                MavenOrgExternalID
            ] = await self.config_client.get_external_ids_by_data_provider(
                data_provider_organization_id=config.organization_id
            )
            organization_ids = [mapping.organization_id for mapping in mappings]
        else:
            organization_ids = [file.organization_id]

        return set(organization_ids)

    @ddtrace.tracer.wrap()
    async def pre_verify(self, file: db_model.File):
        """
        Pre-verify this file

        Args:
            file:
        """
        organization_ids: Set[int] = await self.get_organization_ids_for_file(file=file)
        for org_id in organization_ids:
            await pre_verify.pre_verify_org(
                organization_id=org_id,
                file_id=file.id,
                members_versioned=self.member_versioned_client,
                verifications=self.verification_client,
            )

    @ddtrace.tracer.wrap()
    async def delete_errors(self, file: db_model.File) -> int:
        """
        Delete errors from the temp tables.

        Args:
            file:

        Returns: int

        """
        if self._use_tmp:
            return await self.fpr_client.tmp_delete_file_parse_errors_for_files(file.id)
        else:
            return await self.fpr_client.delete_file_parse_errors_for_files(file.id)

    @ddtrace.tracer.wrap()
    async def delete_results(self, file: db_model.File) -> int:
        """
        Delete results from the temp tables.

        Args:
            file:

        Returns: int

        """
        if self._use_tmp:
            return await self.fpr_client.tmp_delete_file_parse_results_for_files(
                file.id
            )
        else:
            return await self.fpr_client.delete_file_parse_results_for_files(file.id)

    @ddtrace.tracer.wrap()
    async def set_file_completed(self, file: db_model.File) -> datetime.datetime | None:
        """
        This function will set the completed_at date for a file.

        Args:
            file:

        Returns: datetime.datetime | None

        """
        if self._use_tmp:
            return await self.file_client.tmp_set_completed_at(file.id)
        else:
            return await self.file_client.set_completed_at(file.id)

    @ddtrace.tracer.wrap()
    async def check_file_completed(self, file: db_model.File) -> bool:
        """
        This function will check if we have any records left in
        file_parse_results and file_parse_errors

        Args:
            file:

        Returns: bool

        """
        results = await self.fpr_client.get_file_parse_results_for_file(file.id)
        errors = await self.fpr_client.get_file_parse_errors_for_file(file.id)

        return not results and not errors

    @ddtrace.tracer.wrap()
    async def get_file(self, file_id: int) -> db_model.File:
        """
        This function will retrieve a File object based on id

        Args:
            file_id:

        Returns: File

        """
        return await self.file_client.get(file_id)
