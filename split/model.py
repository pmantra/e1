from __future__ import annotations

import dataclasses
from typing import Dict

from split.constants import FileSplitConstants
from split.repository.csv import SplitFileCsvWriter

from db import model as db_model
from db.model import ExternalMavenOrgInfo


@dataclasses.dataclass
class SplitFileResult:
    """
    Split file result:
    total_rows: total rows in parent file
    invalid_rows: rows cannot find child organization via client_id, customer_id
    child_files: a dict have child organization id -> ChildFileInfo
    """

    total_rows: int = 0
    invalid_rows: int = 0
    child_files: Dict[int, ChildFileInfo] = dataclasses.field(default_factory=dict)

    def should_review(self) -> bool:
        valid_rate = (self.total_rows - self.invalid_rows) / self.total_rows
        return valid_rate <= FileSplitConstants.PARENT_FILE_REVIEW_THRESHOLD


@dataclasses.dataclass
class AffiliationsHeader:
    client_id_source: str
    customer_id_source: str


@dataclasses.dataclass
class ParentFileInfo:
    file: db_model.File
    affiliations_header: AffiliationsHeader


@dataclasses.dataclass
class ChildFileInfo:
    """
    holding all information needed to write a child file
    organization: ExternalMavenOrgInfo which has child organization id and directory_name, will be used for write child file
    writer: SplitFileCsvWriter holding the columns info and buffer with child file content
    """

    organization: ExternalMavenOrgInfo
    writer: SplitFileCsvWriter
