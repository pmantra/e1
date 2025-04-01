from unittest import mock

import pytest


@pytest.fixture(scope="package", autouse=True)
def MockEligibilityFileProcessor():
    with mock.patch(
        "app.eligibility.process.EligibilityFileProcessor", autospec=True, spec_set=True
    ) as pm:
        yield pm


@pytest.fixture
def processor(MockEligibilityFileProcessor):
    processor = MockEligibilityFileProcessor.return_value
    yield processor
    processor.reset_mock()


@pytest.fixture(scope="package", autouse=True)
def MockRedisStream():
    with mock.patch("mmstream.redis.RedisStream", autospec=True, spec_set=True) as sm:
        yield sm


@pytest.fixture
def stream(MockRedisStream):
    stream = MockRedisStream.return_value
    yield stream
    stream.reset_mock()


@pytest.fixture(scope="package", autouse=True)
def MockRedisPublisher():
    with mock.patch(
        "mmstream.redis.RedisStreamPublisher", autospec=True, spec_set=True
    ) as sm:
        sm.return_value.__aenter__.return_value = sm.return_value
        with mock.patch(
            "app.worker.redis.stream_supervisor.publisher", autospec=True, spec_set=True
        ) as pm:
            pm.return_value = sm.return_value
            yield sm


@pytest.fixture
def publisher(MockRedisPublisher):
    publisher = MockRedisPublisher.return_value
    yield publisher
    publisher.reset_mock()
