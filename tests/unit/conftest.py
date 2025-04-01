from datetime import datetime
from unittest import mock

import pytest
import structlog
from mmlib import redis
from tests.factories import data_models

from db import model
from db.clients import (
    client,
    configuration_client,
    file_client,
    header_aliases_client,
    member_client,
    member_versioned_client,
)
from db.model import ClientSpecificImplementation


@pytest.fixture(scope="package", autouse=True)
def MockFiles():
    with mock.patch("db.clients.file_client.Files", autospec=True, spec_set=True) as fm:
        fs = fm.return_value
        fs.client.connector.initialize.side_effect = mock.AsyncMock()
        yield fm


@pytest.fixture
def files(MockFiles):
    files = MockFiles.return_value
    yield files
    files.reset_mock()


@pytest.fixture(scope="package", autouse=False)
def MockFileParseResults():
    with mock.patch(
        "db.clients.file_parse_results_client.FileParseResults",
        autospec=True,
        spec_set=True,
    ) as fpr:
        mfpr = fpr.return_value
        mfpr.client.connector.initialize.side_effect = mock.AsyncMock()
        yield fpr


@pytest.fixture
def file_parse_results_client(MockFileParseResults):
    file_parse_results_client = MockFileParseResults.return_value
    yield file_parse_results_client
    file_parse_results_client.reset_mock()


@pytest.fixture(scope="package", autouse=True)
def MockMembers():
    with mock.patch(
        "db.clients.member_client.Members", autospec=True, spec_set=True
    ) as mm:
        ms = mm.return_value
        ms.client.connector.initialize.side_effect = mock.AsyncMock()
        yield mm


@pytest.fixture(scope="package", autouse=True)
def MockMembersVersioned():
    with mock.patch(
        "db.clients.member_versioned_client.MembersVersioned",
        autospec=True,
        spec_set=True,
    ) as mm:
        ms = mm.return_value
        ms.client.connector.initialize.side_effect = mock.AsyncMock()
        yield mm


@pytest.fixture
def members(MockMembers):
    members = MockMembers.return_value
    yield members
    members.reset_mock()


@pytest.fixture
def members_versioned(MockMembersVersioned):
    members_versioned = MockMembersVersioned.return_value
    yield members_versioned
    members_versioned.reset_mock()


@pytest.fixture(scope="package", autouse=True)
def MockConfigurations():
    with mock.patch(
        "db.clients.configuration_client.Configurations", autospec=True, spec_set=True
    ) as cm:
        cs = cm.return_value
        cs.client.connector.initialize.side_effect = mock.AsyncMock()
        yield cm


@pytest.fixture
def configs(MockConfigurations):
    configs = MockConfigurations.return_value
    yield configs
    configs.reset_mock()


@pytest.fixture
def members_2(MockMember2Client):
    member_2_client = MockMember2Client.return_value
    yield member_2_client
    member_2_client.reset_mock()


@pytest.fixture(scope="package", autouse=True)
def MockMember2Client():
    with mock.patch(
        "db.clients.member_2_client.Member2Client",
        autospec=True,
        spec_set=True,
    ) as mm:
        ms = mm.return_value
        ms.client.connector.initialize.side_effect = mock.AsyncMock()
        yield mm


@pytest.fixture(scope="package", autouse=True)
def MockHeaderAliases():
    with mock.patch(
        "db.clients.header_aliases_client.HeaderAliases", autospec=True, spec_set=True
    ) as ha:
        has = ha.return_value
        has.client.connector.initialize.side_effect = mock.AsyncMock()
        yield ha


@pytest.fixture(scope="module")
def logger() -> structlog.stdlib.BoundLogger:
    return structlog.getLogger(__name__)


@pytest.fixture
def header_aliases(MockHeaderAliases):
    ha = MockHeaderAliases.return_value
    ha.get_header_mapping.return_value = model.HeaderMapping()
    yield ha
    ha.reset_mock()


@pytest.fixture
def config(configs) -> configuration_client.Configuration:
    return data_models.ConfigurationFactory.create()


@pytest.fixture
def config_client_specific(configs) -> configuration_client.Configuration:
    return data_models.ConfigurationFactory.create(
        implementation=ClientSpecificImplementation.MICROSOFT
    )


@pytest.fixture
def terminated_config(configs) -> configuration_client.Configuration:
    return data_models.ConfigurationFactory.create(terminated_at=datetime(2001, 1, 1))


@pytest.fixture
def terminated_config_client_specific(configs) -> configuration_client.Configuration:
    return data_models.ConfigurationFactory.create(
        terminated_at=datetime(2001, 1, 1),
        implementation=ClientSpecificImplementation.MICROSOFT,
    )


@pytest.fixture
def file(files, config) -> file_client.File:
    return data_models.FileFactory.create(organization_id=config.organization_id)


@pytest.fixture
def member(file, members) -> member_client.Member:
    return data_models.MemberFactory.create(
        organization_id=file.organization_id, file_id=file.id
    )


@pytest.fixture
def member_versioned(
    file, members_versioned
) -> member_versioned_client.MemberVersioned:
    return data_models.MemberVersionedFactory.create(
        organization_id=file.organization_id, file_id=file.id
    )


@pytest.fixture
def header_mapping(config) -> header_aliases_client.HeaderMapping:
    headers = data_models.HeaderAliasFactory.create_batch(
        5, organization_id=config.organization_id
    )
    return client.HeaderMapping({h.header: h.alias for h in headers})


@pytest.fixture(scope="package")
def MockPipeline():
    with mock.patch("aioredis.client.Pipeline", autospec=True, spec_set=True) as mp:
        mp.__aenter__.return_value = mp
        mp.__await__ = mp
        yield mp


@pytest.fixture(scope="package")
def MockRedis(MockPipeline):
    with mock.patch("aioredis.ConnectionPool", autospec=True, spec_set=True) as mcp:
        with mock.patch("aioredis.Redis", autospec=True) as mr:
            r = mr.return_value
            r.__aenter__.return_value = r
            r.__await__.return_value = r
            r.client.return_value = r
            r.pipeline.return_value = MockPipeline.return_value
            object.__setattr__(r, "connection_pool", mcp.return_value)
            yield mr


@pytest.fixture(scope="package")
async def tmp_connector(MockRedis) -> redis.RedisConnector:
    connector = redis.RedisConnector()
    connector.redis = MockRedis.return_value
    return connector
