from __future__ import annotations

import csv
import dataclasses
import datetime
import io
import os
from typing import DefaultDict, Dict, Iterable, List, Sequence

from app.dryrun import model as dry_run_model
from app.dryrun import repository as dry_run_repository

# from app.eligibility.parse import ParseErrorMessage
from app.utils import eligibility_member
from db import model as db_model


class DryRunCsvWriter:
    def __init__(self, *, fieldnames: Sequence[str]):
        self._buffer = io.StringIO()
        self._writer = csv.DictWriter(self._buffer, fieldnames=fieldnames)
        self._writer.writeheader()

    def write_row(self, row: Dict):
        self._writer.writerow(row)

    def get_value(self):
        return self._buffer.getvalue()


class Reporter:
    def __init__(self, file_manager: eligibility_member, bucket: str):
        self.file_manager = file_manager
        self.bucket = bucket

    async def build_report(self, dry_run_result: dry_run_model.DryRunResult):
        report_folder = self._get_report_folder(dry_run_result.census_file_name)
        summary = []
        summary.append(
            await self._write_file_records(report_folder, dry_run_result.file)
        )
        summary.append(
            await self._write_parse_errors(report_folder, dry_run_result.parse_errors)
        )

        for organization_id in dry_run_result.population_result:
            summary.extend(
                await self._write_population_summary(
                    report_folder=report_folder,
                    pop_data=dry_run_result.population_result[organization_id],
                    organization_id=organization_id,
                )
            )
        summary.extend(dry_run_result.additional_summary_lines)
        await self.file_manager.put(
            data="\r\n".join(summary),
            name=os.path.join(
                dry_run_repository.Files.DRY_RUN_FOLDER, f"{report_folder}/summary.txt"
            ),
            bucket_name=self.bucket,
        )
        pass

    def _get_report_folder(self, census_file_name: str) -> str:
        name, _ = os.path.splitext(census_file_name)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        folder_name = f"{name}/{timestamp}"
        return folder_name

    async def _write_file_records(self, report_folder: str, file: db_model.File) -> str:
        file_cols = [field.name for field in dataclasses.fields(db_model.File)]
        file_writer = DryRunCsvWriter(fieldnames=file_cols)
        file_writer.write_row(dataclasses.asdict(file))
        await self.file_manager.put(
            data=file_writer.get_value(),
            name=os.path.join(
                dry_run_repository.Files.DRY_RUN_FOLDER, f"{report_folder}/file.csv"
            ),
            bucket_name=self.bucket,
        )

        if file.completed_at:
            seconds = (file.completed_at - file.started_at).total_seconds()
            min, sec = divmod(seconds, 60)
            return f"Dry run for file {file.name} takes {min} mins, {sec} secs. {file.raw_count} rows processed. {file.success_count} success rows, {file.failure_count} error rows."
        else:
            return f"Dry run for file {file.name} not complete due to process errors. {file.raw_count} rows processed. {file.success_count} success rows, {file.failure_count} error rows."

    async def _write_parse_errors(
        self, report_folder: str, parse_errors: Iterable[db_model.FileParseError]
    ):
        parse_error_cols = [
            field.name for field in dataclasses.fields(dry_run_model.ReportParseError)
        ]
        error_writer = DryRunCsvWriter(fieldnames=parse_error_cols)
        orphan_count = 0
        invalid_count = 0
        for parse_error in parse_errors:
            report_error = dry_run_model.ReportParseError.from_file_parse_error(
                parse_error
            )
            if "ParseErrorMessage.CLIENT_ID_NO_MAPPING" in parse_error.errors:
                orphan_count += 1
            else:
                invalid_count += 1
            error_writer.write_row(dataclasses.asdict(report_error))
        await self.file_manager.put(
            data=error_writer.get_value(),
            name=os.path.join(
                dry_run_repository.Files.DRY_RUN_FOLDER,
                f"{report_folder}/errors.csv",
            ),
            bucket_name=self.bucket,
        )
        return (
            f"{invalid_count} parse errors found, {orphan_count} orphan records found. "
        )

    async def _write_population_summary(
        self,
        report_folder: str,
        pop_data: dry_run_model.PopulationData,
        organization_id: int,
    ) -> List[str]:
        population_count = DefaultDict(int)
        members_wo_pop = []

        for member in pop_data.populated_members:
            if member.sub_pop_id is None:
                members_wo_pop.append(
                    dry_run_model.ReportNoPopError.from_member_versioned_with_pop(
                        member
                    )
                )
            else:
                population_count[member.sub_pop_id] += 1

        record_id_cols = [
            field.name for field in dataclasses.fields(dry_run_model.ReportNoPopError)
        ]
        writer = DryRunCsvWriter(fieldnames=record_id_cols)
        for record in members_wo_pop:
            writer.write_row(dataclasses.asdict(record))
        await self.file_manager.put(
            data=writer.get_value(),
            name=os.path.join(
                dry_run_repository.Files.DRY_RUN_FOLDER,
                f"{report_folder}/{organization_id}_non_pop_member.csv",
            ),
            bucket_name=self.bucket,
        )

        summary_lines = []
        summary_lines.append(
            f"Organization {organization_id}: total {len(pop_data.populated_members)} members, sub population calculated based on populadtion_id={pop_data.population_id_used} - {len(members_wo_pop)} does not has population"
        )
        for sub_pop_id in population_count:
            summary_lines.append(
                f"population {sub_pop_id} has {population_count[sub_pop_id]} members"
            )
        return summary_lines
