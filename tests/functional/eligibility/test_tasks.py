from typing import List, Tuple

import pytest
from tests.factories import data_models as factory

from app.tasks import sync
from db import model
from db.clients import configuration_client, header_aliases_client
from db.mono import client as mclient
from db.mono.client import MavenOrganization, MavenOrgExternalID

pytestmark = pytest.mark.asyncio


async def test_sync_orgs(
    configuration_test_client: configuration_client.Configurations,
    header_test_client: header_aliases_client.HeaderAliases,
    maven_org: Tuple[MavenOrganization, List[MavenOrgExternalID]],
    maven: mclient.MavenMonoClient,
):
    # Given
    expected_org = maven_org[0]

    config = factory.ConfigurationFactory.create(
        organization_id=expected_org.id,
        implementation=model.ClientSpecificImplementation.MICROSOFT,
    )
    config: model.Configuration = await configuration_test_client.persist(model=config)
    # When
    await sync.sync_all_mono_orgs(configuration_test_client, header_test_client, maven)
    updated: model.Configuration = await configuration_test_client.get(
        pk=config.organization_id
    )
    # The representation for this value changes from int to bool when translating from
    # MavenOrganization to Configuration - this is done in translate.org_to_config
    expected_data_provider = True if expected_org.data_provider == 1 else False
    # Then
    assert (
        updated.organization_id,
        updated.directory_name,
        updated.email_domains,
        updated.implementation,
        updated.data_provider,
        updated.activated_at,
        updated.terminated_at,
    ) == (
        expected_org.id,
        expected_org.directory_name,
        expected_org.email_domains,
        config.implementation,
        expected_data_provider,
        expected_org.activated_at,
        expected_org.terminated_at,
    )


async def test_sync_org_external_ids(
    configuration_test_client: configuration_client.Configurations,
    header_test_client: header_aliases_client.HeaderAliases,
    maven_org: Tuple[MavenOrganization, List[MavenOrgExternalID]],
    maven: mclient.MavenMonoClient,
):
    # Given
    expected_org: MavenOrganization = maven_org[0]

    config = factory.ConfigurationFactory.create(
        organization_id=expected_org.id,
        implementation=model.ClientSpecificImplementation.MICROSOFT,
    )
    config: model.Configuration = await configuration_test_client.persist(model=config)
    await sync.sync_all_mono_orgs(configuration_test_client, header_test_client, maven)

    # When
    await sync.sync_all_mono_external_ids(configuration_test_client, maven)
    updated = await configuration_test_client.get_external_ids(
        organization_id=config.organization_id
    )
    # Then
    assert updated
