from unittest import mock

import pytest

from app.common import bq

pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="module", autouse=True)
def MockPubSubPublisher():
    with mock.patch(
        "mmstream.pubsub.PubSubPublisher", autospec=True, spec_set=True
    ) as PubSubPublisher:
        PubSubPublisher.return_value.__aenter__.return_value = (
            PubSubPublisher.return_value
        )
        yield PubSubPublisher


@pytest.fixture
def publish(MockPubSubPublisher):
    pub = MockPubSubPublisher.return_value.publish
    yield pub
    pub.reset_mock()


async def test_export_rows_to_table_batching(publish):
    # Given
    table = "test"
    rows = [{"key": "value"} for i in range(2001)]
    # When
    await bq.export_rows_to_table(table, rows)
    # Then
    assert publish.call_count == 2
