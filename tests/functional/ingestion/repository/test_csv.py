from typing import Iterable

import pytest
from ingestion import repository


class TestEligibilityCSVReader:
    def test_valid_csv(self):
        # Given
        csv = "first_name,last_name\nstan,smith\nbob,ross\n"

        # When
        reader = repository.EligibilityCSVReader(data=csv)

        # Then
        row = next(iter(reader))

        assert {"first_name", "last_name"} == row.keys()

    def test_invalid_csv_bad_delimiter(self):
        # Given
        csv = "first_name&last_name\nstan,smith\nbob,ross\n"

        # When
        reader = repository.EligibilityCSVReader(data=csv)

        # Then
        with pytest.raises(repository.DelimiterError):
            next(iter(reader))

    def test_invalid_csv_bad_delimiter_set_dialect_not_called(self):
        # Given
        csv = "first_name&last_name\nstan,smith\nbob,ross\n"

        # When
        reader = repository.EligibilityCSVReader(data=csv)

        # Then

        assert reader.set_dialect() is False

    def test_valid_csv_good_delimiter(self):
        # Given
        csv = "first_name,last_name\nstan,smith\nbob,ross\n"

        # When
        reader = repository.EligibilityCSVReader(data=csv)

        # Then

        assert reader.set_dialect() is True

    def test_extra_header(self):
        # Given
        csv = "first_name,last_name\nstan,smith,extra_value\nbob,ross\n"

        # When
        reader = repository.EligibilityCSVReader(data=csv)
        record: dict = next(iter(reader))

        # Then
        assert repository.EXTRA_HEADER in record

    def test_space_in_header(self):
        # Given
        csv = "first_name     ,   last_name\nstan,smith,extra_value\nbob,ross\n"

        # When
        reader = repository.EligibilityCSVReader(data=csv)
        record: dict = next(iter(reader))

        # Then
        assert {"first_name", "last_name"} <= record.keys()

    def test_upper_case_lowered_in_headers(self):
        # Given
        csv = "FIRST_NAME,last_name\nstan,smith,extra_value\nbob,ross\n"

        # When
        reader = repository.EligibilityCSVReader(data=csv)
        record: dict = next(iter(reader))

        # Then
        assert {"first_name", "last_name"} <= record.keys()

    def test_sanitize_header(self):
        # Given
        headers: Iterable[str] = ["first_name     ", "   last_name\n"]

        # When
        sanitized: Iterable[str] = repository.EligibilityCSVReader._sanitize_headers(
            headers=headers
        )

        # Then
        assert sanitized == ["first_name", "last_name"]


class TestChunker:
    @staticmethod
    def test_chunker_no_remainder():
        # Given
        numbers = range(100)
        chunk_size = 10

        # When
        chunks = []
        for chunk in repository.chunker(numbers, chunk_size):
            chunks.append(chunk)

        # Then
        # All chunks are same size
        assert all(len(chunk) == chunk_size for chunk in chunks)
        # We get all the elements
        assert set(numbers) == set([num for chunk in chunks for num in chunk])

    @staticmethod
    def test_chunker_remainder():
        # Given
        size = 100
        numbers = range(size)
        chunk_size = 11

        # When
        chunks = []
        for chunk in repository.chunker(numbers, chunk_size):
            chunks.append(chunk)

        # Then
        # All but the last are of length chunk_size
        assert all(len(chunk) == chunk_size for chunk in chunks[:-1])
        # Last one is of length remainder
        assert len(chunks[-1]) == size % chunk_size
        # We get all the elements
        assert set(numbers) == set([num for chunk in chunks for num in chunk])

    @staticmethod
    def test_chunker_empty():
        # Given
        size = 0
        numbers = range(size)
        chunk_size = 10

        # When
        chunks = []
        for chunk in repository.chunker(numbers, chunk_size):
            chunks.append(chunk)

        # Then
        assert not chunks
