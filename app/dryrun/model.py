from __future__ import annotations

import dataclasses
from typing import Dict, Iterable, List

from db import model as db_model


@dataclasses.dataclass
class MemberVersionedWithPopulation:
    member_versioned: db_model.MemberVersioned
    sub_pop_id: int | None


@dataclasses.dataclass
class PopulationData:
    population_id_used: int | None
    populated_members: List[MemberVersionedWithPopulation]


@dataclasses.dataclass
class DryRunResult:
    census_file_name: str
    file: db_model.File
    parse_errors: Iterable[db_model.FileParseError]
    population_result: Dict[int, PopulationData]
    additional_summary_lines: List[str]


@dataclasses.dataclass
class ReportNoPopError:
    parse_line_no: int | None = None
    client_id: str | None = None
    customer_id: str | None = None
    unique_corp_id: str | None = None
    dependent_id: str | None = None

    @staticmethod
    def from_member_versioned_with_pop(member: MemberVersionedWithPopulation):
        return ReportNoPopError(
            parse_line_no=member.member_versioned.record.get("parse_line_no", None),
            client_id=member.member_versioned.record.get("client_i,d", None),
            customer_id=member.member_versioned.record.get("customer_id", None),
            unique_corp_id=member.member_versioned.unique_corp_id,
            dependent_id=member.member_versioned.dependent_id,
        )


@dataclasses.dataclass
class ReportParseError:
    organization_id: int
    parse_line_no: int | None = None
    unique_corp_id: str | None = None
    dependent_id: str | None = None
    errors: list[str] = None
    warnings: list[str] = None

    @staticmethod
    def from_file_parse_error(file_parse_error: db_model.FileParseError):
        return ReportParseError(
            parse_line_no=file_parse_error.record.get("parse_line_no", None),
            organization_id=file_parse_error.organization_id,
            unique_corp_id=file_parse_error.record.get("unique_corp_id", None),
            dependent_id=file_parse_error.record.get("dependent_id", None),
            errors=file_parse_error.errors,
            warnings=file_parse_error.warnings,
        )


class DryRunError(Exception):
    ...


class NoEffectivePopulation(DryRunError):
    def __init__(self, organization_id: int, member_count: int) -> None:
        self.organization_id = organization_id
        self.message = f"Organization {organization_id}: total {member_count} members, No effective population found."
        super().__init__(self.message)


class ParseOrganizationError(DryRunError):
    def __init__(self, file_name: str = None):
        message = (
            f"Failed to parse organization_id from dry run file name: {file_name}."
        )
        super().__init__(message)
