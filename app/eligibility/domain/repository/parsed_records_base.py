from __future__ import annotations

from abc import abstractmethod
from typing import Protocol

import structlog

from app.eligibility.domain import model
from db.model import File, FileParseError, FileParseResult

__all__ = "ParsedRecordsAbstractRepository"


logger = structlog.getLogger(__name__)


class ParsedRecordsAbstractRepository(Protocol):
    """
    Any children of this class should be drop-in replacements of each other.

    An implementation of this class and its methods will handle the storage of
    records into a temp store, and the flushing and persistence of the records
    into more permanent storage.
    """

    @abstractmethod
    async def persist(
        self, parsed_records: model.ParsedRecords, file: File
    ) -> model.ProcessedRecords:
        """
        Top-level function call to persist the records contained within a ParsedRecords instance
        into temp storage.

        Args:
            parsed_records:
            file:

        Returns:

        """
        raise NotImplementedError

    @abstractmethod
    async def flush(self, file: File):
        """
        Top-level function call to clear the records that were persisted into temp storage and
        store the records in permanent DB storage.

        Args:
            file:

        Returns:

        """
        raise NotImplementedError

    @abstractmethod
    async def persist_errors(self, errors: list[FileParseError], file: File) -> int:
        """
        The implementation of this function should store the errors from file parsing in
        temp storage.

        Args:
            errors:
            file:

        Returns:

        """
        raise NotImplementedError

    @abstractmethod
    async def persist_valid(self, valid: list[FileParseResult], file: File) -> int:
        """
        The implementation of this function should store the valid records from file parsing in
        temp storage.

        Args:
            valid:
            file:

        Returns:

        """
        raise NotImplementedError

    @abstractmethod
    async def persist_missing(self, file: File):
        """
        The implementation of this function should invalidate/delete the existing member records
        that were missing from the new file.

        Args:
            file:

        Returns:

        """
        raise NotImplementedError

    @abstractmethod
    async def persist_as_members(self, file: File):
        """
        The implementation of this function should persist all valid records as member records.

        Args:
            file:

        Returns:

        """
        raise NotImplementedError

    @abstractmethod
    async def delete_errors(self, file: File):
        """
        The implementation of this function should delete the errors from temp storage.

        Args:
            file:

        Returns:

        """
        raise NotImplementedError

    @abstractmethod
    async def set_file_completed(self, file: File):
        """
        The implementation of this function should set the completed_at date for a file.

        Args:
            file:

        Returns:

        """
        raise NotImplementedError

    @abstractmethod
    async def check_file_completed(self, file: File) -> bool:
        """
        The implementation of this function should check if there are any records remaining
        in the staging tables for this file_id

        Args:
            file:

        Returns: bool

        """
        raise NotImplementedError

    @abstractmethod
    async def get_file(self, file_id: int) -> File:
        """
        This function will retrieve a File object based on id

        Args:
            file_id:

        Returns: File

        """
        raise NotImplementedError
