from __future__ import annotations

import asyncio
from concurrent.futures.process import ProcessPoolExecutor
from typing import List, Tuple

import cchardet
import structlog
from ddtrace import tracer
from mmlib.ops import stats

import constants
from app.common import apm
from app.eligibility.domain import model, repository, service
from db import model as db_model
from db.clients import (
    configuration_client,
    file_client,
    file_parse_results_client,
    header_aliases_client,
    member_client,
    member_versioned_client,
    verification_client,
)
from db.mono.client import MavenOrgExternalID

from ..common.gcs import LocalStorage, Storage
from . import parse
from .constants import ProcessingResult
from .domain.model import ProcessedRecords
from .gcs import EligibilityFileManager

logger = structlog.getLogger(__name__)

RESOURCE = "process-file"


class EligibilityFileProcessor:
    """This service encapsulates the core logic for processing a client eligibility file.

    At a high level, it:

        1. Fetches the target file from cloud storage.
        2. Runs the parser on each entry.
        3. Finds all entries in the database which should be considered "expired".
        4. Check the change rate for a file to determine if a manual review is required.

    See Also:
        https://www.notion.so/mavenclinic/File-based-Eligibility-5c1a73c9aba144b58db1c4b7880f113f
    """

    __slots__ = (
        "bucket",
        "project",
        "project_supervisor",
        "pool",
        "files",
        "file_parse_results_client",
        "configs",
        "headers",
        "manager",
        "store",
        "members",
        "members_versioned",
        "verifications",
        "loop",
    )

    def __init__(
        self,
        bucket: str,
        *,
        project: str = None,
        project_supervisor: str = None,
        loop: asyncio.AbstractEventLoop = None,
        files: file_client.Files | None = None,
        fpr_client: file_parse_results_client.FileParseResult | None = None,
        configs: configuration_client.Configurations | None = None,
        headers: header_aliases_client.HeaderAliases | None = None,
        members: member_client.Members | None = None,
        members_versioned: member_versioned_client.MembersVersioned | None = None,
        verifications: verification_client.Verifications | None = None,
    ):
        if project and project != "local-dev":
            storage = Storage(project)
            encrypted = True
        else:
            encrypted = False
            storage = LocalStorage("local-dev")

        self.bucket = bucket
        self.project = project
        self.project_supervisor = project_supervisor
        self.pool = ProcessPoolExecutor(2)
        self.files = files or file_client.Files()
        self.file_parse_results_client = (
            fpr_client or file_parse_results_client.FileParseResults()
        )
        self.configs = configs or configuration_client.Configurations()
        self.headers = headers or header_aliases_client.HeaderAliases()
        self.manager = EligibilityFileManager(storage, encrypted)
        self.members = members or member_client.Members()
        self.members_versioned = (
            members_versioned or member_versioned_client.MembersVersioned()
        )
        self.verifications = verifications or verification_client.Verifications()

        self.loop = loop

    DEFAULT_ENCODING = "utf-8"

    @tracer.wrap(service=apm.ApmService.ELIGIBILITY_WORKER, resource=RESOURCE)
    async def process(
        self,
        key: str,
        messageid: int,
        file: db_model.File,
        config: db_model.Configuration,
        batch_size: int = 10_000,
    ) -> Tuple[ProcessingResult, model.ProcessedRecords | None]:
        """Given a file and configuration, download the data and normalize.

        Args:
            key: The source stream which sent in the file notification.
            messageid: The exact message ID of the file notification.
            file: The file reference as represented in our database.
            config: The parser configuration related to this file.
            batch_size: The size of each batch to process.

        Returns:
            The `ProcessedRecords`, if file data was located in the storage bucket.
        """
        if data_provider_file := config.data_provider:
            logger.info(
                "Got an organization configuration associated with a data_provider",
            )

        file.started_at = await self.files.set_started_at(file.id)
        logger.info("Fetching contents from GCS.")
        data = await self.manager.get(file.name, self.bucket)

        if not data:
            stats.increment(
                metric_name="eligibility.process.file_content_missing",
                pod_name=constants.POD,
                tags=["eligibility:error", f"organization_id:{config.organization_id}"],
            )
            logger.error("Couldn't locate file contents in GCS.")
            file.completed_at = await self.files.set_completed_at(file.id)
            return ProcessingResult.FILE_MISSING, None

        # Attempt to detect the encoding of our file
        try:
            result = cchardet.detect(data)
            logger.info("Detected encoding.", **result)
        except Exception as e:
            logger.exception("Could not detect encoding of file", error=e)
            stats.increment(
                metric_name="eligibility.process.ParseErrorMessage.InvalidEncoding",
                pod_name=constants.POD,
                tags=[
                    "eligibility:error",
                    f"organization_id:{config.organization_id}",
                    f"file_id:{file.id}",
                ],
            )
            return ProcessingResult.BAD_FILE_ENCODING, None

        # Set the file encoding
        file.encoding = (result["encoding"] or self.DEFAULT_ENCODING).lower()
        await self.files.set_encoding(file.id, encoding=file.encoding)

        headers = await self.headers.get_header_mapping(file.organization_id)
        custom_attributes = {
            k: v for k, v in headers.items() if k.startswith("custom_attributes.")
        }

        try:
            external_id_mappings = {}
            if data_provider_file:
                raw_external_id_mappings: List[
                    MavenOrgExternalID
                ] = await self.configs.get_external_ids_by_data_provider(
                    data_provider_organization_id=config.organization_id
                )
                # Massage our results into a nice key:value pair of external_id to org_id
                # Two cases for external_id
                # case 1: external_id is colon separated string such as 300:48416 (client_id : customer_id)
                # case 2: external_id is a string such as 300 (client_id)
                for r in raw_external_id_mappings:
                    external_id = r.external_id
                    if ":" in r.external_id:
                        client_id, customer_id = external_id.split(":")
                        external_id_mappings[
                            (client_id, customer_id)
                        ] = r.organization_id
                    else:
                        external_id_mappings[external_id] = r.organization_id

            parser: parse.EligibilityFileParser = parse.EligibilityFileParser(
                file=file,
                configuration=config,
                headers=headers,
                custom_attributes=custom_attributes,
                data=data,
                external_id_mappings=external_id_mappings,
            )

            db_repository: repository.ParsedRecordsDatabaseRepository = (
                repository.ParsedRecordsDatabaseRepository(
                    fpr_client=self.file_parse_results_client,
                    member_client=self.members,
                    member_versioned_client=self.members_versioned,
                    verification_client=self.verifications,
                    file_client=self.files,
                    config_client=self.configs,
                )
            )

            num_valid, num_error, batch_num, num_not_persisted = 0, 0, 0, 0
            for batch in parser.parse(batch_size=batch_size):
                try:
                    processed_batch: model.ProcessedRecords = await service.persist(
                        file=file,
                        parsed_records=batch,
                        db_repository=db_repository,
                    )
                except Exception as e:
                    logger.exception(
                        "Encountered an error in saving processed file results to temp tables",
                        error=e,
                    )
                    stats.increment(
                        metric_name="eligibility.process.file_process.temp_table_persist_error",
                        pod_name=constants.POD,
                        tags=[
                            "eligibility:error",
                            f"organization_id:{config.organization_id}",
                        ],
                    )
                    return ProcessingResult.ERROR_DURING_PROCESSING, None
                else:
                    num_valid += processed_batch.valid
                    num_error += processed_batch.errors
                    batch_num += 1
                    logger.info(
                        f"{num_valid} valid rows, {num_error} errors rows completed in {batch_num} batches"
                    )

                    num_not_persisted += (len(batch.valid) - processed_batch.valid) + (
                        len(batch.errors) - processed_batch.errors
                    )

            if num_not_persisted > 0:
                logger.warning(
                    "Some records were not persisted to the database.",
                    count=num_not_persisted,
                )

            await service.persist_file_counts(
                db_repository=db_repository,
                file=file,
                success_count=num_valid,
                failure_count=num_error,
            )

        except Exception as e:
            logger.exception("Unable to parse file- error encountered", error=e)
            stats.increment(
                metric_name="eligibility.process.file_process.encoding_or_delimiter_issues",
                pod_name=constants.POD,
                tags=[
                    "eligibility:error",
                    f"organization_id:{config.organization_id}",
                ],
            )
            return ProcessingResult.ERROR_DURING_PROCESSING, None

        try:
            logger.info("Flushing records from staging tables")
            processed: model.ProcessedRecords = await service.flush(
                file=file,
                db_repository=db_repository,
                processed=ProcessedRecords(valid=num_valid, errors=num_error),
            )
        except Exception as e:
            logger.exception(
                "Encountered an error in saving processed file results to temp tables",
                error=e,
            )
            stats.increment(
                metric_name="eligibility.process.file_process.temp_table_persist_error",
                pod_name=constants.POD,
                tags=[
                    "eligibility:error",
                    f"organization_id:{config.organization_id}",
                ],
            )
            return ProcessingResult.ERROR_DURING_PROCESSING, None

        stats.increment(
            metric_name="eligibility.process.file_parse.completed",
            pod_name=constants.POD,
            tags=[
                "eligibility:info",
                f"file_id:{file.id}",
                f"organization_id:{file.organization_id}",
            ],
        )

        return ProcessingResult.PROCESSING_SUCCESSFUL, processed

    def _detect_encoding(self, data: bytes) -> str:
        detected_encoding = cchardet.detect(data)
        return (detected_encoding["encoding"] or self.DEFAULT_ENCODING).lower()
