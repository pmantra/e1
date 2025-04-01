from unittest import mock

import pytest

from app.eligibility.domain import repository


@pytest.fixture
def parsed_records_repo():
    return repository.ParsedRecordsDatabaseRepository(
        fpr_client=mock.AsyncMock(),
        member_client=mock.AsyncMock(),
        file_client=mock.AsyncMock(),
    )
