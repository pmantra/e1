from __future__ import annotations

import csv
import io
import itertools
from typing import AnyStr, Dict, Iterable, Iterator, List, TypeVar

import ddtrace
import structlog

__all__ = ("EligibilityCSVReader", "DelimiterError", "EXTRA_HEADER", "chunker")

T = TypeVar("T")

logger = structlog.getLogger(__name__)

EXTRA_HEADER = "extra"


class EligibilityCSVReader:
    """A module for ingesting CSV data"""

    __slots__ = "_data", "_encoding", "_buffer", "_dialect"

    def __init__(
        self,
        data: AnyStr,
        *,
        encoding: str = "utf-8",
    ):
        self._data: bytes | str = data
        self._encoding: str = encoding
        self._dialect: csv.Dialect | None = None
        self._buffer = (
            io.TextIOWrapper(io.BytesIO(self._data), encoding=self._encoding)
            if isinstance(self._data, bytes)
            else io.StringIO(self._data)
        )

    @ddtrace.tracer.wrap()
    def _get_reader(self) -> csv.DictReader:
        """Returns a reader which is used to read through the file"""
        if not self._dialect:
            valid_delimiter: bool = self.set_dialect()
            if not valid_delimiter:
                raise DelimiterError("Invalid delimiter encountered")

        self._buffer.seek(0)
        reader = csv.DictReader(
            self._buffer, restkey=EXTRA_HEADER, dialect=self._dialect
        )
        reader.fieldnames = EligibilityCSVReader._sanitize_headers(
            headers=reader.fieldnames
        )
        return reader

    @staticmethod
    @ddtrace.tracer.wrap()
    def _sanitize_headers(*, headers: Iterable[str]) -> Iterable[str]:
        """Clean the headers from the file, removing whitespace and line breaks"""
        sanitized: List[str] = []
        for h in headers:
            cleaned: str = (
                h.lower().strip().strip("'\"").replace("\r", " ").replace("\n", " ")
            )
            if cleaned:
                sanitized.append(cleaned)
        return sanitized

    @ddtrace.tracer.wrap()
    def set_dialect(self) -> bool:
        """Attempt to detect if the delimiter of the file is valid"""
        try:
            self._dialect = csv.Sniffer().sniff(
                self._buffer.readline(), delimiters=",\t"
            )
        except Exception:
            return False

        return True

    def __iter__(self) -> Iterator[dict]:
        yield from self._get_reader()

    @ddtrace.tracer.wrap()
    def parse(self, *, batch_size: int = 10_000) -> Iterator[List[Dict]]:
        """Returns an iterator which yields batches of size batch_size"""
        for batch in chunker(self, batch_size):
            yield batch


@ddtrace.tracer.wrap()
def chunker(iterable: Iterable[T], n: int) -> Iterator[list[T]]:
    """
    Takes in an iterable and returns iterator of lists of size n,
    last one is trimmed to size of remainder

    Args:
        iterable: an iterable
        n: size of chunks to return

    Returns:
        Iterator[list[T]]

    """
    it = iter(iterable)
    while batch := list(itertools.islice(it, n)):
        yield batch


class DelimiterError(Exception):
    """Raise when we encounter a non-standard CSV delimiter"""

    pass
