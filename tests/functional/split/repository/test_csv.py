from split.repository.csv import SplitFileCsvWriter

from app.eligibility.parse import EligibilityCSVReader as WorkerReader
from db import model


class TestSplitFileCsvWriter:
    @staticmethod
    def test_write_csv():
        # Given
        fieldnames = [
            "first",
            "middle",
            "last",
        ]
        writer = SplitFileCsvWriter(fieldnames=fieldnames)
        row = {"first": "最高", "middle": "james", "last": "サートした"}
        # When
        writer.write_row(row)

        # Then
        assert writer.get_value() == "first,middle,last\r\n最高,james,サートした\r\n"

        # Verify worker reader can read the csv correctly
        # Given
        reader = WorkerReader(
            headers=model.HeaderMapping(), data=writer.get_value().encode("utf-8")
        )
        # When
        row = next(iter(reader))
        # Then
        assert {"first", "middle", "last"} == row.keys()
        assert row["first"] == "最高"
        assert row["middle"] == "james"
        assert row["last"] == "サートした"
