from __future__ import annotations

import asyncio
from typing import Dict

import structlog
from structlog.contextvars import unbind_contextvars

from app.dryrun import calculator
from app.dryrun import model as dry_run_model
from app.dryrun import report
from app.dryrun import repository as dry_run_repository
from app.eligibility import process
from app.eligibility.constants import ProcessingResult
from app.eligibility.domain.model.parsed_records import ProcessedRecords
from config import settings

RESOURCE = "dryrun"
logger = structlog.getLogger(__name__)


async def process_dryrun(
    file_name: str, *, override_sub_population: Dict[int, int] | None = None
) -> None:
    bucket = settings.GCP().census_file_bucket
    project = settings.GCP().project
    try:
        loop = asyncio.get_event_loop()

        files = dry_run_repository.Files()
        file = files.build_for_dry_run(file_name=file_name)

        configs = dry_run_repository.Configurations()
        config = await configs.get(file.organization_id)

        processor = process.EligibilityFileProcessor(
            bucket=bucket,
            project=project,
            loop=loop,
            files=files,
            fpr_client=dry_run_repository.FileParseResults(),
            configs=configs,
            headers=dry_run_repository.HeaderAliases(),
            members=dry_run_repository.Members(),
            members_versioned=dry_run_repository.MembersVersioned(),
            verifications=dry_run_repository.Verifications(),
        )
        logger.info(
            f"Dry run file_name={file_name}, organization_id={file.organization_id}."
        )
        logger.info("File parsing starting")

        result: ProcessingResult
        processed: ProcessedRecords

        # key and messageid actually not used in the processor.
        key = "dry-run"
        messageid = 1

        result, processed = await processor.process(key, messageid, file, config)

        if result == ProcessingResult.NO_RECORDS_FOUND:
            logger.warning("No data processed for file.")
            unbind_contextvars(
                "file_id", "filename", "organization_id", "stream", "message_id"
            )

        elif result == ProcessingResult.ERROR_DURING_PROCESSING:
            logger.warning("Error processing file")
            unbind_contextvars(
                "file_id", "filename", "organization_id", "stream", "message_id"
            )

        elif result == ProcessingResult.FILE_MISSING:
            logger.warning("File not found")
            unbind_contextvars(
                "file_id", "filename", "organization_id", "stream", "message_id"
            )

        elif result == ProcessingResult.BAD_FILE_ENCODING:
            logger.warning("Error detecting file encoding")
            unbind_contextvars(
                "file_id", "filename", "organization_id", "stream", "message_id"
            )

        logger.info(
            "File parsing complete",
            errors=processed.errors,
            valid=processed.valid,
        )

        unbind_contextvars(
            "file_id", "filename", "organization_id", "stream", "message_id"
        )

        additional_summary_lines = []
        parsed_members = processor.file_parse_results_client.parsed_members
        pop_calculator = calculator.PopulationCalculator(
            override_sub_population=override_sub_population
        )
        population_result = {}
        for organization_id in parsed_members:
            try:
                pop_data = await pop_calculator.calculate_sub_pops(
                    organization_id, parsed_members[organization_id]
                )
                population_result[organization_id] = pop_data
            except dry_run_model.NoEffectivePopulation as e:
                additional_summary_lines.append(e.message)

        dry_run_result = dry_run_model.DryRunResult(
            census_file_name=file_name,
            file=processor.files.file,
            parse_errors=processor.file_parse_results_client.parse_errors,
            population_result=population_result,
            additional_summary_lines=additional_summary_lines,
        )

        reporter = report.Reporter(file_manager=processor.manager, bucket=bucket)
        await reporter.build_report(dry_run_result=dry_run_result)

        return processed
    except dry_run_model.ParseOrganizationError as e:
        logger.error(e)
