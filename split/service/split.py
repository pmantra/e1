from __future__ import annotations

from typing import List

import ddtrace
import structlog
from ingestion import repository
from ingestion.service import FileIngestionService
from split.constants import AffiliationColumns, FileSplitConstants
from split.model import (
    AffiliationsHeader,
    ChildFileInfo,
    ParentFileInfo,
    SplitFileResult,
)
from split.repository.csv import SplitFileCsvWriter
from split.utils import helper

from config import settings
from db import model as db_model

MODULE = __name__
logger = structlog.getLogger(MODULE)

GCP_SETTINGS = settings.GCP()


class FileSplitService:
    BATCH_SIZE = 1_000

    def __init__(
        self,
        *,
        ingest_config_repo: repository.IngestConfigurationRepository | None = None,
        file_manager: repository.EligibilityFileManager | None = None,
    ):
        self._ingest_config_repo = ingest_config_repo
        self._file_manager = file_manager

    @ddtrace.tracer.wrap()
    async def process_file(self, *, filename: str) -> db_model.File | None:
        parent_file_info = await self._initialize_parent_file(filename=filename)
        if not parent_file_info:
            # incase initialize parent file fail by configuration issue, we stop process
            # configuration issue includes:
            # - organization not found
            # - organization is not configured to split filename
            # - organization not configured with affiliation headers
            return None

        reader = await self._initialize_reader(file=parent_file_info.file)
        if not reader:
            # incase initialize reader fail (empty file or no delimiter found)  , we stop process
            return None

        split_file_result = await self._split_file(
            reader=reader, parent_file_info=parent_file_info
        )
        if not split_file_result:
            # incase invalid rows exceed threshold in parent file, None will be returned and error logged in _split_file
            # we stop file process
            return None

        return await self._write_split_files(split_file_result=split_file_result)

    @ddtrace.tracer.wrap()
    async def _write_split_files(
        self, *, split_file_result: SplitFileResult
    ) -> db_model.File | None:
        """
        this function will write the child file to GCS
        @param split_file_result:
        @return:
        """
        # Todo, will be implement in next MR

        pass

    @ddtrace.tracer.wrap()
    async def _initialize_parent_file(self, *, filename: str) -> ParentFileInfo | None:
        """
        Verify file with org configuration by searching directory in the configurations
        Sync organization from mono to e9y if needed
        Check if organization configured to split file
        Read affiliations header
        Create e9y.file record and returns the db model
        @param filename: GCS file name
        @return: ParentFileInfo (contains e9y.file db model and affiliations header) or
            None, if
                organization not found
                organization not configured to split file
                no affiliation headers configured
        """
        org_config: db_model.Configuration | None = await self._ingest_config_repo.sync(
            filename=filename
        )

        # org not found based on configured file paths
        if not org_config:
            logger.error(
                "Could not find organization associated with file",
                filename=filename,
            )
            return None

        if not helper.is_parent_org(org_config):
            logger.info("Got non parent file, ignore it")
            return None

        logger.info("Got a parent file")

        affiliations_header = await self._get_affiliations_header(
            organization_id=org_config.organization_id
        )
        if not affiliations_header:
            # in case affiliation header not correctly configured, return None
            return None

        file: db_model.File = await self._ingest_config_repo.create_file(
            organization_id=org_config.organization_id, filename=filename
        )

        # set the `started_at` time for file
        await self._ingest_config_repo.set_started_at(file_id=file.id)

        return ParentFileInfo(file=file, affiliations_header=affiliations_header)

    async def _get_affiliations_header(
        self, *, organization_id: int
    ) -> AffiliationsHeader | None:
        """
        Read affilications header from configuration
        find the source header which mapping to client_id and customer_id
        @param organization_id: organization_id
        @return: AffiliationsHeader(contains source_headers) or
            None when any source cannot be found
        """
        client_id_source, customer_id_source = None, None
        header_alias_list: List[
            db_model.HeaderAlias
        ] = await self._ingest_config_repo.get_affiliations_header_for_org(
            organization_id=organization_id
        )
        for head_alias in header_alias_list:
            if head_alias.alias == AffiliationColumns.COL_CLIENT_ID:
                client_id_source = head_alias.header
            elif head_alias.alias == AffiliationColumns.COL_CUSTOMER_ID:
                customer_id_source = head_alias.header
        # We require both at this time.
        if client_id_source and customer_id_source:
            return AffiliationsHeader(
                client_id_source=client_id_source, customer_id_source=customer_id_source
            )
        logger.error(
            "Parent organization with incorrect affiliations_header.",
            organization_id=organization_id,
        )
        return None

    @ddtrace.tracer.wrap()
    async def _initialize_reader(
        self, *, file: db_model.File
    ) -> repository.EligibilityCSVReader | None:
        """
        @param file: e9y.file model
        @return: EligibilityCSVReader with correctly read the GCS file content. or
            None if failed to read file as csv file. (either missing delimiter or not recognized encoding.
        """
        file_data: bytes | None = await self._file_manager.get(
            name=file.name, bucket_name=GCP_SETTINGS.census_file_bucket
        )

        if not file_data:
            await self._ingest_config_repo.set_error(
                file_id=file.id, error=db_model.FileError.MISSING
            )
            logger.error(
                "File level error encountered",
                error=db_model.FileError.MISSING,
                filename=file.name,
                organization_id=file.organization_id,
                module=MODULE,
            )
            return None

        encoding: str = FileIngestionService.detect_encoding(file_data)
        await self._ingest_config_repo.set_encoding(file_id=file.id, encoding=encoding)

        reader = repository.EligibilityCSVReader(data=file_data, encoding=encoding)

        # Trying to detect and set csv file dialect (delimiter, quote char and so on)
        # if failed, log error
        if reader.set_dialect() is False:
            await self._ingest_config_repo.set_error(
                file_id=file.id, error=db_model.FileError.DELIMITER
            )
            logger.error(
                "File level error encountered",
                error=db_model.FileError.DELIMITER,
                filename=file.name,
                organization_id=file.organization_id,
                module=MODULE,
            )
            return None
        return reader

    @ddtrace.tracer.wrap()
    async def _split_file(
        self,
        *,
        reader: repository.EligibilityCSVReader,
        parent_file_info: ParentFileInfo,
    ) -> SplitFileResult | None:
        """
        read parent file rows and split rows based on child organization id
        for each parent file row,
        1. find child organization by client_id, customer_id source column
        2. group rows by child organization id in the split file result
        @param reader: EligibilityCSVReader which hold the parent file content and information
        @param parent_file_info: SplitFileResult holding the split file result info,
          including row counts and child organization grouped rows. see definition for details
        @return:
        """
        split_file_result = SplitFileResult()

        for batch in reader.parse(batch_size=FileSplitConstants.READ_BATCH_SIZE):
            for r in batch:
                child_org: db_model.ExternalMavenOrgInfo | None = (
                    await self._ingest_config_repo.get_external_org_info(
                        source=repository.IngestionType.FILE,
                        client_id=r[
                            parent_file_info.affiliations_header.client_id_source
                        ],
                        customer_id=r.get(
                            parent_file_info.affiliations_header.customer_id_source, ""
                        ),
                        organization_id=parent_file_info.file.organization_id,
                    )
                )
                # cannot find child org based on client_id, customer_id, mark row invalid
                if not child_org:
                    split_file_result.invalid_rows += 1
                    continue
                # build the child_files, which is a child organization id -> ChildFileInfo dict.
                # ChildFileInfo holding necessary information to write a child file
                if child_org.organization_id not in split_file_result.child_files:
                    split_file_result.child_files[
                        child_org.organization_id
                    ] = ChildFileInfo(
                        organization=child_org,
                        writer=SplitFileCsvWriter(fieldnames=list(r.keys())),
                    )

                # write row to corresponding child file info
                split_file_result.child_files[
                    child_org.organization_id
                ].writer.write_row(r)

            batch_size = len(batch)
            split_file_result.total_rows += batch_size

        # Roughly persisted file counts here, may change in https://mavenclinic.atlassian.net/browse/ELIG-1565
        await self._ingest_config_repo.set_file_count(
            file_id=parent_file_info.file.id,
            raw_count=split_file_result.total_rows,
            success_count=split_file_result.total_rows - split_file_result.invalid_rows,
            failure_count=split_file_result.invalid_rows,
        )

        # in case invalid rows exceed threshold, log error
        if split_file_result.should_review():
            logger.error(
                "Too many invalid rows.",
                file_id=parent_file_info.file.id,
                filename=parent_file_info.file.name,
                organization_id=parent_file_info.file.organization_id,
                total_rows=split_file_result.total_rows,
                invalid_rows=split_file_result.invalid_rows,
            )
            return None
        return split_file_result
