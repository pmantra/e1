from typing import Iterator, Sequence, TypeVar
from unittest import mock

import aiomysql
import factory
import orjson
import pytest
from tests.factories import data_models as base_factory

from app.eligibility import translate
from app.tasks import sync
from app.utils import format
from db.mono import client
from db.mono.client import MavenOrganization, MavenOrgExternalID

pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="module", autouse=True)
def mock_sync_context():
    with mock.patch.object(sync, "sync_context", autospec=True) as m:
        yield m


# region sync mono


async def test_sync_all_mono_external_ids(configs, maven):
    # Given
    base_factory.MavenOrgExternalIDFactory.create_batch(size=100)

    # When
    await sync.sync_all_mono_external_ids(configs, maven)
    # Then
    assert configs.delete_and_recreate_all_external_ids.called


async def test_sync_all_mono_orgs(configs, header_aliases, maven):
    # Given
    orgs: list = factory.create_batch(
        dict,
        size=50,
        FACTORY_CLASS=base_factory.MavenOrganizationFactory,
        json__headers={"foo": "bar"},
        directory_name=None,
    )
    seen = set()
    maven.get_orgs_for_sync_cursor.return_value = patch_cursor(infchunks(orgs, 10))
    # Get the expected calls to downstream.
    expected_config_calls = []
    expected_header_alias_calls = []
    for batch in chunks(orgs, 10):
        given_headers = {}
        given_configs = []
        for org in batch:
            # We don't need to test that this works, we test it elsewhere.
            config, headers = translate.org_to_config(
                client.MavenOrganization(**org), config_cls=dict
            )
            # Filter out non-unique orgs.
            #   This can happen because there's only so much entropy in the org name factory.
            #   It's usually 1:100 if it occurs.
            if config["directory_name"] in seen:
                orgs.remove(org)
                continue

            seen.add(config["directory_name"])
            given_configs.append(config)
            given_headers[config["organization_id"]] = headers
            test_org_email_domains = format.sanitize_json_input(org["email_domains"])
            test_config_email_domains = format.sanitize_json_input(
                config["email_domains"]
            )
            test_org_json = format.sanitize_json_input(org["json"])
            config.update(email_domains={*orjson.loads(test_config_email_domains)})
            org.update(
                email_domains={*orjson.loads(test_org_email_domains)},
                json=orjson.loads(test_org_json),
            )
        expected_config_calls.append(mock.call(data=given_configs, coerce=mock.ANY))
        expected_header_alias_calls.append(
            mock.call(given_headers.items(), coerce=mock.ANY)
        )
    # Add a duplicated org config (should be ignored)
    dupe = orgs[-1].copy()
    dupe["id"] += 1000
    orgs.append(dupe)
    # When
    ignored = await sync.sync_all_mono_orgs(configs, header_aliases, maven)
    # parse expected and actual kwargs from function calls
    expected_call_data = []
    for expected_call in expected_config_calls:
        expected_call_data.append(expected_call.kwargs["data"][0])
    actual_call_data = []
    for actual_call in configs.bulk_persist.call_args_list:
        actual_call_data.append(actual_call.kwargs["data"][0])

    # parse expected and actual args from header
    expected_header_data = []
    for call in expected_header_alias_calls:
        expected_header_data.extend(dict(call.args[0]).keys())

    actual_header_data = []
    for call in header_aliases.bulk_refresh.call_args_list:
        actual_header_data.extend(dict(call.args[0]).keys())

    # Then
    assert dupe["id"] in ignored
    assert all(item in actual_call_data for item in expected_call_data)
    assert all(item in actual_header_data for item in expected_header_data)


@pytest.mark.parametrize(
    argnames="org_id,external_ids",
    argvalues=[
        (1, []),
        (
            1,
            [
                MavenOrgExternalID(
                    source="mock-source",
                    external_id="mock-external-id",
                    organization_id=1,
                    data_provider_organization_id=1,
                ),
            ],
        ),
    ],
    ids=[
        "without_external_ids",
        "with_external_ids",
    ],
)
async def test_sync_single_mono_org_for_directory(
    configs, header_aliases, maven, org_id, external_ids
):
    # Given
    org: MavenOrganization = base_factory.MavenOrganizationFactory.create(id=org_id)
    maven.get_org_from_directory.return_value = org
    maven.get_org_external_ids_for_org.return_value = external_ids
    config, headers = translate.org_to_config(org)
    configs.persist.return_value = config

    # When
    ret = await sync.sync_single_mono_org_for_directory(
        configs, header_aliases, maven, org.directory_name
    )
    assert ret == config
    configs.persist.assert_called_once_with(model=config)
    header_aliases.persist_header_mapping.assert_called_once_with(
        config.organization_id, headers
    )
    configs.delete_external_ids_for_org.assert_called_once_with(organization_id=org.id)
    configs.delete_external_ids_for_data_provider_org.assert_called_once_with(
        data_provider_organization_id=org.id
    )
    if external_ids:
        configs.bulk_add_external_id.assert_called_once_with(external_ids)


async def test_sync_single_mono_org_for_directory_with_sub_org(
    configs, header_aliases, maven
):
    # Given
    org: MavenOrganization = base_factory.MavenOrganizationFactory.create(id=1)
    sub_org: MavenOrganization = base_factory.MavenOrganizationFactory.create(id=2)
    external_ids = [
        MavenOrgExternalID(
            source="mock-source",
            external_id="mock-external-id",
            organization_id=org.id,
            data_provider_organization_id=org.id,
        ),
        MavenOrgExternalID(
            source="mock-source",
            external_id="mock-external-id",
            organization_id=sub_org.id,
            data_provider_organization_id=sub_org.id,
        ),
    ]
    maven.get_org_from_directory.return_value = org
    config, headers = translate.org_to_config(org)
    configs.persist.return_value = config

    maven.get_org_external_ids_for_org.return_value = external_ids
    maven.get_org_from_id.return_value = sub_org
    sub_config, sub_headers = translate.org_to_config(sub_org)

    # When
    ret = await sync.sync_single_mono_org_for_directory(
        configs, header_aliases, maven, org.directory_name
    )
    assert ret == config

    header_aliases.persist_header_mapping.assert_called_once_with(
        config.organization_id, headers
    )
    configs.delete_external_ids_for_org.assert_called_once_with(organization_id=org.id)
    configs.delete_external_ids_for_data_provider_org.assert_called_once_with(
        data_provider_organization_id=org.id
    )
    configs.bulk_add_external_id.assert_called_once_with(external_ids)

    # verify sub org is persisted
    maven.get_org_from_id.assert_called_once_with(sub_org.id)
    configs.persist.assert_called_with(model=sub_config)


# endregion

# region bigquery


class _ctx:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        ...


async def test_sync(configs, header_aliases, maven):
    # Given
    with mock.patch.object(sync, "sync_all_mono_orgs", autospec=True) as mock_sync_orgs:
        with mock.patch.object(
            sync, "sync_all_mono_external_ids", autospec=True
        ) as mock_sync_external_ids:
            # When
            await sync.sync()
    # Then
    assert (
        mock_sync_orgs.called,
        mock_sync_external_ids.called,
    ) == (True, True)


# endregion


def patch_cursor(
    side_effect: Iterator, target: str = "fetchmany", spec: type = aiomysql.Cursor
):
    cursor = mock.create_autospec(spec, instance=True)
    if hasattr(cursor, "__aenter__"):
        cursor.__aenter__.return_value = cursor
    method = mock.AsyncMock(side_effect=side_effect)
    setattr(cursor, target, method)
    return cursor


T = TypeVar("T")


def chunks(seq: Sequence[T], n: int) -> Iterator[Sequence[T]]:
    """Yield successive n-sized chunks from seq."""
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def infchunks(seq: Sequence[T], n: int) -> Iterator[Sequence[T]]:
    """Yield successive n-sized chunks from seq, then empty chunks."""
    for c in chunks(seq, n):
        yield c
    while True:
        yield seq.__class__()
