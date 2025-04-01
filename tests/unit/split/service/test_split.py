from __future__ import annotations

from unittest import mock

import pytest
from ingestion import repository
from ingestion.repository import IngestionType
from split.model import AffiliationsHeader, ParentFileInfo, SplitFileResult
from split.service import split
from tests.factories import data_models as factories

from db import model as db_model

pytestmark = pytest.mark.asyncio

# region shared
filename: str = "some-directory/some-file.csv"


# endregion

# region test process file


class TestProcessFile:

    mock_parent_file_info = factories.ParentFileInfoFactory.create()

    @staticmethod
    async def test_no_split_file_when_not_parent_file(
        file_split_service: split.FileSplitService,
    ):
        # Given
        with mock.patch(
            "split.service.split.FileSplitService._initialize_parent_file"
        ) as mock_parent_file, mock.patch(
            "split.service.split.FileSplitService._initialize_reader"
        ) as mock_reader, mock.patch(
            "split.service.split.FileSplitService._split_file"
        ) as mock_split_file, mock.patch(
            "split.service.split.FileSplitService._write_split_files"
        ) as mock_write_files:
            mock_parent_file.return_value = None
            # When
            res = await file_split_service.process_file(filename=filename)
            # Then
            assert res is None
            mock_reader.assert_not_called()
            mock_split_file.assert_not_called()
            mock_write_files.assert_not_called()

    @staticmethod
    async def test_return_none_when_parent_file_no_reader(
        file_split_service: split.FileSplitService,
    ):
        # Given
        with mock.patch(
            "split.service.split.FileSplitService._initialize_parent_file"
        ) as mock_parent_file, mock.patch(
            "split.service.split.FileSplitService._initialize_reader"
        ) as mock_reader, mock.patch(
            "split.service.split.FileSplitService._split_file"
        ) as mock_split_file, mock.patch(
            "split.service.split.FileSplitService._write_split_files"
        ) as mock_write_files:
            mock_parent_file.return_value = TestProcessFile.mock_parent_file_info
            mock_reader.return_value = None

            # When
            res = await file_split_service.process_file(filename=filename)
            # Then
            assert res is None
            mock_split_file.assert_not_called()
            mock_write_files.assert_not_called()

    @staticmethod
    async def test_return_none_when_parent_file_and_reader_no_split_file(
        file_split_service: split.FileSplitService,
    ):
        # Given
        with mock.patch(
            "split.service.split.FileSplitService._initialize_parent_file"
        ) as mock_parent_file, mock.patch(
            "split.service.split.FileSplitService._initialize_reader"
        ) as mock_reader, mock.patch(
            "split.service.split.FileSplitService._split_file"
        ) as mock_split_file, mock.patch(
            "split.service.split.FileSplitService._write_split_files"
        ) as mock_write_files:
            mock_parent_file.return_value = TestProcessFile.mock_parent_file_info
            mock_reader.return_value = repository.EligibilityCSVReader(data="some-line")
            mock_split_file.return_value = None

            # When
            res = await file_split_service.process_file(filename=filename)
            # Then
            assert res is None
            mock_write_files.assert_not_called()

    @staticmethod
    async def test_return_file_when_parent_file_and_reader_split_file(
        file_split_service: split.FileSplitService,
    ):
        # Given
        with mock.patch(
            "split.service.split.FileSplitService._initialize_parent_file"
        ) as mock_parent_file, mock.patch(
            "split.service.split.FileSplitService._initialize_reader"
        ) as mock_reader, mock.patch(
            "split.service.split.FileSplitService._split_file"
        ) as mock_split_file, mock.patch(
            "split.service.split.FileSplitService._write_split_files"
        ) as mock_write_files:
            mock_parent_file.return_value = TestProcessFile.mock_parent_file_info
            mock_reader.return_value = repository.EligibilityCSVReader(data="some-line")
            mock_split_file.return_value = SplitFileResult(
                total_rows=100, invalid_rows=0
            )
            mock_write_files.return_value = factories.FileFactory.create()

            # When
            res = await file_split_service.process_file(filename=filename)
            # Then
            assert res is not None
            mock_write_files.assert_called()


# endregion

# region get_affiliations_header
class TestGetAffiliationsHeader:
    client_header_alias = db_model.HeaderAlias(
        organization_id=1, header="c", alias="client_id"
    )
    customer_header_alias = db_model.HeaderAlias(
        organization_id=1, header="d", alias="customer_id"
    )

    @staticmethod
    async def test_return_none_when_no_header_alias_found(
        file_split_service: split.FileSplitService,
    ):
        # Given
        file_split_service._ingest_config_repo.get_affiliations_header_for_org.return_value = (
            []
        )
        # When
        res = await file_split_service._get_affiliations_header(organization_id=1)
        # Then
        assert res is None

    @staticmethod
    async def test_return_none_when_only_client_header_alias_found(
        file_split_service: split.FileSplitService,
    ):
        # Given
        file_split_service._ingest_config_repo.get_affiliations_header_for_org.return_value = [
            TestGetAffiliationsHeader.client_header_alias
        ]
        # When
        res = await file_split_service._get_affiliations_header(organization_id=1)
        # Then
        assert res is None

    @staticmethod
    async def test_return_none_when_only_customer_header_alias_found(
        file_split_service: split.FileSplitService,
    ):
        # Given
        file_split_service._ingest_config_repo.get_affiliations_header_for_org.return_value = [
            TestGetAffiliationsHeader.customer_header_alias
        ]
        # When
        res = await file_split_service._get_affiliations_header(organization_id=1)
        # Then
        assert res is None

    @staticmethod
    async def test_return_header_when_found_both(
        file_split_service: split.FileSplitService,
    ):
        # Given
        file_split_service._ingest_config_repo.get_affiliations_header_for_org.return_value = [
            TestGetAffiliationsHeader.customer_header_alias,
            TestGetAffiliationsHeader.client_header_alias,
        ]
        # When
        res = await file_split_service._get_affiliations_header(organization_id=1)
        # Then
        assert res is not None
        assert res == AffiliationsHeader(client_id_source="c", customer_id_source="d")


# endregion

# region initialize_parent_file
class TestInitializeParentFile:
    @staticmethod
    async def test_return_none_when_org_info_not_found(
        file_split_service: split.FileSplitService,
    ):
        # Given
        file_split_service._ingest_config_repo.sync.return_value = None
        # When
        res = await file_split_service._initialize_parent_file(filename=filename)
        # Then
        assert res is None

    @staticmethod
    async def test_return_none_when_not_a_parent_file(
        file_split_service: split.FileSplitService,
    ):
        # Given
        file_split_service._ingest_config_repo.sync.return_value = (
            factories.ConfigurationFactory.create()
        )
        with mock.patch(
            "split.utils.helper.is_parent_org",
            return_value=False,
        ):
            # When
            res = await file_split_service._initialize_parent_file(filename=filename)
            # Then
            assert res is None

    @staticmethod
    async def test_return_none_when_no_affiliations_header(
        file_split_service: split.FileSplitService,
    ):
        # Given
        file_split_service._ingest_config_repo.sync.return_value = (
            factories.ConfigurationFactory.create()
        )

        with mock.patch(
            "split.utils.helper.is_parent_org",
            return_value=True,
        ), mock.patch(
            "split.service.split.FileSplitService._get_affiliations_header",
            return_value=None,
        ):
            # When
            res = await file_split_service._initialize_parent_file(filename=filename)
            # Then
            assert res is None

    @staticmethod
    async def test_return_file_when_a_parent_file(
        file_split_service: split.FileSplitService,
    ):
        # Given
        file_split_service._ingest_config_repo.sync.return_value = (
            factories.ConfigurationFactory.create()
        )

        with mock.patch(
            "split.utils.helper.is_parent_org",
            return_value=True,
        ), mock.patch(
            "split.service.split.FileSplitService._get_affiliations_header",
            return_value=AffiliationsHeader(
                client_id_source="c", customer_id_source="d"
            ),
        ):
            # When
            res = await file_split_service._initialize_parent_file(filename=filename)
            # Then
            assert res is not None


# endregion

# region test initialize reader


class TestInitializeReader:
    file = factories.FileFactory.create()

    @staticmethod
    async def test_return_none_when_empty_file(
        file_split_service: split.FileSplitService,
    ):
        # Given
        file_split_service._file_manager.get.return_value = None
        # When
        res = await file_split_service._initialize_reader(
            file=TestInitializeReader.file
        )
        # Then
        assert res is None

    @staticmethod
    async def test_return_none_when_set_dialect_false(
        file_split_service: split.FileSplitService,
    ):
        # Given
        file_split_service._file_manager.get.return_value = b"ok"
        # When
        res = await file_split_service._initialize_reader(
            file=TestInitializeReader.file
        )
        # Then
        assert res is None

    @staticmethod
    async def test_return_reader_when_set_dialect_true(
        file_split_service: split.FileSplitService,
    ):
        # Given
        file_split_service._file_manager.get.return_value = b"ok,good"
        # When
        res = await file_split_service._initialize_reader(
            file=TestInitializeReader.file
        )
        # Then
        assert res is not None


# endregion

# region test split_file
class TestSplitFile:
    file = factories.FileFactory.create()
    reader = repository.EligibilityCSVReader(
        """client_id,customer_id,id
a,b,001
c,d,002
a,b,003"""
    )

    reader_with_invalid = repository.EligibilityCSVReader(
        """client_id,customer_id,id
a,b,001
c,d,002
a,b,003
x,y,00z"""
    )
    affiliations_header = AffiliationsHeader(
        client_id_source="client_id", customer_id_source="customer_id"
    )
    parent_file_info = ParentFileInfo(
        file=file, affiliations_header=affiliations_header
    )

    @staticmethod
    async def mock_get_external_org_info(
        *,
        source: IngestionType,
        client_id: str | None = None,
        customer_id: str | None = None,
        organization_id: int | None = None,
    ):
        """
        A mock method to mock _ingest_config_repo.get_external_org_info
        """
        _, _ = source, organization_id
        mapping = {("a", "b"): 1, ("c", "d"): 2}
        key = (client_id, customer_id)
        if key not in mapping:
            return None
        external_id = mapping[key]
        return db_model.ExternalMavenOrgInfo(organization_id=external_id)

    @staticmethod
    async def test_return_result_when_happy_path(
        file_split_service: split.FileSplitService,
    ):
        # Given
        file_split_service._ingest_config_repo.get_external_org_info = (
            TestSplitFile.mock_get_external_org_info
        )
        file_split_service._ingest_config_repo.set_file_count.return_value = None

        file_split_service._ingest_config_repo.get_affiliations_header_for_org.return_value = (
            TestSplitFile.affiliations_header
        )

        # When
        res = await file_split_service._split_file(
            parent_file_info=TestSplitFile.parent_file_info, reader=TestSplitFile.reader
        )
        # Then
        assert res is not None
        # verify rows count are correct
        assert res.total_rows == 3
        assert res.invalid_rows == 0

        # verify child org id=1 and its rows
        assert 1 in res.child_files
        assert (
            res.child_files[1].writer.get_value()
            == "client_id,customer_id,id\r\na,b,001\r\na,b,003\r\n"
        )

        # verify child org id=2 and its rows
        assert 2 in res.child_files
        assert (
            res.child_files[2].writer.get_value()
            == "client_id,customer_id,id\r\nc,d,002\r\n"
        )

    @staticmethod
    async def test_return_none_when_too_many_invalid_rows(
        file_split_service: split.FileSplitService,
    ):
        # Given
        file_split_service._ingest_config_repo.get_external_org_info = (
            TestSplitFile.mock_get_external_org_info
        )
        file_split_service._ingest_config_repo.set_file_count.return_value = None
        file_split_service._ingest_config_repo.get_affiliations_header_for_org.return_value = (
            TestSplitFile.affiliations_header
        )
        # When
        # parent file with invalid rows
        res = await file_split_service._split_file(
            parent_file_info=TestSplitFile.parent_file_info,
            reader=TestSplitFile.reader_with_invalid,
        )
        # Then
        assert res is None

    @staticmethod
    async def test_return_none_when_no_external_id_match(
        file_split_service: split.FileSplitService,
    ):
        # Given
        # no matching child org find
        file_split_service._ingest_config_repo.get_external_org_info.return_value = None
        file_split_service._ingest_config_repo.set_file_count.return_value = None

        res = await file_split_service._split_file(
            parent_file_info=TestSplitFile.parent_file_info, reader=TestSplitFile.reader
        )
        # Then
        assert res is None


# endregion
