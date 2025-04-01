from __future__ import annotations

import csv
import io
from typing import Dict, Sequence

import structlog

logger = structlog.getLogger(__name__)

EXTRA_HEADER = "extra"


class SplitFileCsvWriter:
    def __init__(self, *, fieldnames: Sequence[str]):
        self._buffer = io.StringIO()
        self._writer = csv.DictWriter(self._buffer, fieldnames=fieldnames)
        self._writer.writeheader()

    def write_row(self, row: Dict):
        self._writer.writerow(row)

    def get_value(self):
        return self._buffer.getvalue()
