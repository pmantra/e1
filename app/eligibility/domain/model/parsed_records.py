import dataclasses
from datetime import date
from typing import Dict, Mapping, Sequence, TypedDict, Union

import typic

from db import model

__all__ = (
    "ProcessedRecords",
    "ParsedFileRecords",
    "FileActions",
    "RowT",
)


RowT = Dict[str, Union[str, int, bool, date]]
RecordsT = Sequence[RowT]
ProcessedRecordsT = Mapping[str, RowT]


class FileActions(TypedDict, total=False):
    persist: bool
    expire: bool
    clear_errors: bool
    purge_all: bool


@dataclasses.dataclass
class ProcessedRecords:
    errors: int = 0
    valid: int = 0
    missing: int = 0


# This is the wrapper data model for the PG backed models
@typic.slotted(dict=False)
@dataclasses.dataclass
class ParsedFileRecords:
    errors: list[model.FileParseError] = dataclasses.field(default_factory=list)
    valid: list[model.FileParseResult] = dataclasses.field(default_factory=list)
    missing: list[model.Member] = dataclasses.field(default_factory=list)
