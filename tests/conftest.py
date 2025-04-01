import asyncio
import logging
import os
import pathlib
import sys
from unittest import mock

import pytest
from aioresponses import aioresponses

from app.common import gcs
from app.eligibility.gcs import EligibilityFileManager
from app.eligibility.parse import EligibilityFileParser
from config import settings

PROJECT_DIR = pathlib.Path(__file__).parent.parent
PROTOS = PROJECT_DIR / "api" / "protobufs" / "generated" / "python"

sys.path.append(str(PROTOS))

_mock_namespace = "cool-namespace-my+fell0w_human"


@pytest.fixture(scope="session")
def event_loop():
    return asyncio.new_event_loop()


@pytest.fixture(scope="session", autouse=True)
def bootstrap():
    import ddtrace
    from mmlib.ops import error

    from app.common import log

    settings.load_env()

    log.configure(
        project="local-dev",
        service="eligibility",
        version="test",
        json=False,
        level="debug",
    )
    error.configure(
        project=None,
        service="eligibility",
        facet="test",
        version="test",
    )
    ddtrace.tracer.enabled = False
    for loggername in ("ddtrace", "faker", "factory"):
        logging.getLogger(loggername).setLevel(logging.WARNING)
    yield
    # https://github.com/pytest-dev/pytest/issues/5502#issuecomment-647157873
    loggers = [logging.getLogger()] + list(logging.Logger.manager.loggerDict.values())
    for logger in loggers:
        handlers = getattr(logger, "handlers", [])
        for handler in handlers:
            logger.removeHandler(handler)


@pytest.fixture(scope="session", autouse=True)
def patch_pubsub():
    with mock.patch("mmlib.pubsub.pub.publish") as m:
        yield m


@pytest.fixture(scope="session")
def manager() -> EligibilityFileManager:
    return EligibilityFileManager(gcs.LocalStorage("local-dev"), False)


@pytest.fixture(scope="session")
def mock_manager():
    return mock.MagicMock(EligibilityFileManager)


@pytest.fixture(scope="session")
def mock_parser():
    return mock.MagicMock(EligibilityFileParser)


@pytest.fixture()
def patch_namespace():
    with mock.patch.dict(os.environ, {"APP_ENVIRONMENT_NAMESPACE": _mock_namespace}):
        yield


@pytest.fixture(autouse=True)
def MockConfidentialClientApplication():
    with mock.patch("msal.ConfidentialClientApplication", autospec=True) as m:
        m.return_value.acquire_token_silent.return_value = {"access_token": "XXXXXX"}
        yield m


@pytest.fixture
def response_mock():
    with aioresponses() as m:
        yield m


@pytest.fixture(scope="package", autouse=True)
def MockSubscriptionStream():
    with mock.patch(
        "mmstream.pubsub.SubscriptionStream", autospec=True, spec_set=True
    ) as pm:
        yield pm


@pytest.fixture
def subscription(MockSubscriptionStream):
    sub = MockSubscriptionStream.return_value
    yield sub
    sub.reset_mock()


@pytest.fixture
def MockMonoClient():
    with mock.patch("db.mono.client.MavenMonoClient", autospec=True) as mmc:
        mmcs = mmc.return_value
        mmcs.connector.initialize.side_effect = mock.AsyncMock()
        yield mmc


@pytest.fixture
def maven(MockMonoClient):
    instance = MockMonoClient.return_value
    yield instance
    instance.reset_mock()
