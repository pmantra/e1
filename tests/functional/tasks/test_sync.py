from typing import List, Tuple
from unittest.mock import patch

import aiomysql
import pytest
from tests.factories import data_models
from tests.functional import conftest

from app.eligibility import translate
from app.tasks import sync
from db import model
from db.clients.configuration_client import Configurations
from db.clients.header_aliases_client import HeaderAliases
from db.mono.client import MavenMonoClient, MavenOrganization, MavenOrgExternalID

pytestmark = pytest.mark.asyncio


@pytest.fixture
def test_header_mapping():
    return {"date_of_birth": "dob"}


@pytest.fixture
def mono_org_external_id():
    return MavenOrgExternalID(
        source="optum",
        data_provider_organization_id=None,
        external_id="foobar",
        organization_id=1,
    )


@pytest.fixture
def mono_organization(mono_org_external_id, test_header_mapping):
    return MavenOrganization(
        id=1,
        name="Foo",
        directory_name="test_dir",
        json={translate.FIELD_MAP_KEY: test_header_mapping},
        external_ids=[mono_org_external_id],
    )


@pytest.fixture
def get_all_mono_external_ids(mono_org_external_id):
    return mono_org_external_id


class TestFileSync:
    @staticmethod
    async def test_sync_single_mono_org(
        maven_org_with_external_ids: Tuple[MavenOrganization, List[MavenOrgExternalID]],
        configuration_test_client: Configurations,
        header_test_client: HeaderAliases,
        maven: MavenMonoClient,
    ):
        """Test the sync of a typical organization's configuration, external ID mappings and headers"""
        # Given
        expected_org: MavenOrganization
        expected_external_ids: List[MavenOrgExternalID]
        expected_org, expected_external_ids = (
            maven_org_with_external_ids[0],
            maven_org_with_external_ids[1],
        )

        # When
        # call sync function
        await sync.sync_single_mono_org_for_directory(
            configuration_test_client,
            header_test_client,
            maven,
            expected_org.directory_name,
        )

        # Then
        # query for organization in e9y DB
        synced_org: model.Configuration = (
            await configuration_test_client.get_by_directory_name(
                directory_name=expected_org.directory_name
            )
        )
        # query for oei's in e9y DB that were synced
        synced_org_external_ids: List[
            MavenOrgExternalID
        ] = await configuration_test_client.get_external_ids(
            organization_id=expected_org.id
        )
        # query for headers in e9y DB that were synced
        synced_headers: List[model.HeaderAlias] = await header_test_client.get_for_org(
            organization_id=expected_org.id
        )

        # confirm that data is in e9y DB
        # - Org configuration is synced
        assert (
            expected_org.id,
            expected_org.directory_name,
            expected_org.email_domains,
            expected_org.data_provider,
            expected_org.employee_only,
            expected_org.medical_plan_only,
        ) == (
            synced_org.organization_id,
            synced_org.directory_name,
            synced_org.email_domains,
            synced_org.data_provider,
            synced_org.employee_only,
            synced_org.medical_plan_only,
        )
        # - External Identities are synced
        assert set(
            [
                (
                    eid.external_id,
                    eid.organization_id,
                    eid.data_provider_organization_id,
                )
                for eid in expected_external_ids
            ]
        ) == set(
            [
                (
                    eid.external_id,
                    eid.organization_id,
                    eid.data_provider_organization_id,
                )
                for eid in synced_org_external_ids
            ]
        )
        # - Headers are synced
        assert set(
            [
                (header, alias)
                for header, alias in expected_org.json[translate.FIELD_MAP_KEY].items()
            ]
        ) == set([(h.header, h.alias) for h in synced_headers])

    @staticmethod
    async def test_sync_single_mono_org_activated_terminated_at(
        maven_org_with_activated_terminated_at: MavenOrganization,
        configuration_test_client: Configurations,
        header_test_client: HeaderAliases,
        maven: MavenMonoClient,
    ):
        """Test the sync of an organiation when activated_at and terminated_at are set"""
        # Given
        expected_data_provider_org: MavenOrganization
        expected_data_provider_org = maven_org_with_activated_terminated_at

        # When
        await sync.sync_single_mono_org_for_directory(
            configuration_test_client,
            header_test_client,
            maven,
            expected_data_provider_org.directory_name,
        )

        # Then
        # query for organization in e9y DB
        synced_data_provider_org: model.Configuration = (
            await configuration_test_client.get_by_directory_name(
                directory_name=expected_data_provider_org.directory_name
            )
        )

        # Check that org and it's dates are synced
        assert (
            expected_data_provider_org.id,
            expected_data_provider_org.activated_at,
            expected_data_provider_org.terminated_at,
        ) == (
            synced_data_provider_org.organization_id,
            synced_data_provider_org.activated_at,
            synced_data_provider_org.terminated_at,
        )

    @staticmethod
    async def test_sync_single_mono_org_data_provider(
        maven_data_provider_org_with_external_ids: Tuple[
            MavenOrganization, List[Tuple[MavenOrganization, MavenOrgExternalID]]
        ],
        configuration_test_client: Configurations,
        header_test_client: HeaderAliases,
        maven: MavenMonoClient,
    ):
        """Test the sync of a data provider organization's configuration, external ID mappings and headers"""
        # Given
        expected_data_provider_org: MavenOrganization
        expected_sub_orgs: List[Tuple[MavenOrganization, MavenOrgExternalID]]
        expected_data_provider_org, expected_sub_orgs = (
            maven_data_provider_org_with_external_ids[0],
            maven_data_provider_org_with_external_ids[1],
        )

        # When
        await sync.sync_single_mono_org_for_directory(
            configuration_test_client,
            header_test_client,
            maven,
            expected_data_provider_org.directory_name,
        )

        # Then
        synced_external_ids: List[
            MavenOrgExternalID
        ] = await configuration_test_client.get_external_ids_by_data_provider(
            data_provider_organization_id=expected_data_provider_org.id
        )
        # query for organization in e9y DB
        synced_data_provider_org: model.Configuration = (
            await configuration_test_client.get_by_directory_name(
                directory_name=expected_data_provider_org.directory_name
            )
        )
        synced_sub_orgs: List[
            model.Configuration
        ] = await configuration_test_client.get_sub_orgs_by_data_provider(
            data_provider_org_id=expected_data_provider_org.id
        )
        # query for headers in e9y DB that were synced
        synced_headers: List[model.HeaderAlias] = await header_test_client.get_for_org(
            organization_id=expected_data_provider_org.id
        )

        # Check that data provider org is synced
        assert (
            expected_data_provider_org.id,
            expected_data_provider_org.directory_name,
            expected_data_provider_org.email_domains,
            expected_data_provider_org.data_provider,
            expected_data_provider_org.activated_at,
            expected_data_provider_org.terminated_at,
        ) == (
            synced_data_provider_org.organization_id,
            synced_data_provider_org.directory_name,
            synced_data_provider_org.email_domains,
            synced_data_provider_org.data_provider,
            synced_data_provider_org.activated_at,
            synced_data_provider_org.terminated_at,
        )
        # Check that the sub orgs are synced
        assert set(
            [
                (
                    sub_org[0].id,
                    sub_org[0].directory_name,
                    tuple(sorted(sub_org[0].email_domains)),
                    sub_org[0].data_provider,
                )
                for sub_org in expected_sub_orgs
            ]
        ) == set(
            [
                (
                    sub_org.organization_id,
                    sub_org.directory_name,
                    tuple(sorted(sub_org.email_domains)),
                    sub_org.data_provider,
                )
                for sub_org in synced_sub_orgs
            ]
        )
        # External Identities are synced
        assert set(
            [
                (
                    eid[1].external_id,
                    eid[1].organization_id,
                    eid[1].data_provider_organization_id,
                )
                for eid in expected_sub_orgs
            ]
        ) == set(
            [
                (
                    eid.external_id,
                    eid.organization_id,
                    eid.data_provider_organization_id,
                )
                for eid in synced_external_ids
            ]
        )
        # Headers are synced for data provider organization
        assert set(
            [
                (header, alias)
                for header, alias in expected_data_provider_org.json[
                    translate.FIELD_MAP_KEY
                ].items()
            ]
        ) == set([(h.header, h.alias) for h in synced_headers])

    @staticmethod
    async def test_sync_single_mono_org_remove_eid(
        maven_org_with_external_ids: Tuple[MavenOrganization, List[MavenOrgExternalID]],
        configuration_test_client: Configurations,
        header_test_client: HeaderAliases,
        maven: MavenMonoClient,
        maven_connection: aiomysql.Connection,
    ):
        """Test the sync of a typical organization's configuration, external ID mappings and headers, then remove an external ID in mono and confirm it is removed in e9y as well"""
        # Given
        expected_org: MavenOrganization
        expected_org = maven_org_with_external_ids[0]
        # Call sync function to do the initial sync
        await sync.sync_single_mono_org_for_directory(
            configuration_test_client,
            header_test_client,
            maven,
            expected_org.directory_name,
        )

        # When
        # 1. We remove all of the old eids from mono
        await conftest.delete_external_ids_for_org(
            organization_id=expected_org.id, conn=maven_connection
        )
        # 2. We add new eids
        updated_org_external_ids: List[
            MavenOrgExternalID
        ] = data_models.MavenOrgExternalIDFactory.create_batch(
            size=5, organization_id=expected_org.id
        )
        await conftest.persist_external_ids(
            org_external_ids=updated_org_external_ids, conn=maven_connection
        )
        # 3. call sync function again
        await sync.sync_single_mono_org_for_directory(
            configuration_test_client,
            header_test_client,
            maven,
            expected_org.directory_name,
        )

        # Then
        # query for organization in e9y DB
        synced_org: model.Configuration = (
            await configuration_test_client.get_by_directory_name(
                directory_name=expected_org.directory_name
            )
        )
        # query for oei's in e9y DB that were synced
        synced_org_external_ids: List[
            MavenOrgExternalID
        ] = await configuration_test_client.get_external_ids(
            organization_id=expected_org.id
        )
        # query for headers in e9y DB that were synced
        synced_headers: List[model.HeaderAlias] = await header_test_client.get_for_org(
            organization_id=expected_org.id
        )

        # confirm that data is in e9y DB
        # - Org configuration is synced
        assert (
            expected_org.id,
            expected_org.directory_name,
            expected_org.email_domains,
            expected_org.data_provider,
        ) == (
            synced_org.organization_id,
            synced_org.directory_name,
            synced_org.email_domains,
            synced_org.data_provider,
        )
        # - External Identities are synced
        assert set(
            [
                (
                    eid.external_id,
                    eid.organization_id,
                    eid.data_provider_organization_id,
                )
                for eid in updated_org_external_ids
            ]
        ) == set(
            [
                (
                    eid.external_id,
                    eid.organization_id,
                    eid.data_provider_organization_id,
                )
                for eid in synced_org_external_ids
            ]
        )
        # - Headers are synced
        assert set(
            [
                (header, alias)
                for header, alias in expected_org.json[translate.FIELD_MAP_KEY].items()
            ]
        ) == set([(h.header, h.alias) for h in synced_headers])

    @staticmethod
    async def test_sync_all_mono_external_ids(
        multiple_maven_org_with_external_ids: Tuple[
            List[MavenOrganization], List[MavenOrgExternalID]
        ],
        configuration_test_client: Configurations,
        header_test_client: HeaderAliases,
        maven: MavenMonoClient,
        maven_connection: aiomysql.Connection,
    ):
        # Given
        # 1. Sync the orgs first
        await sync.sync_all_mono_orgs(
            configuration_test_client, header_test_client, maven
        )
        # 2. Sync the org external IDs
        await sync.sync_all_mono_external_ids(configuration_test_client, maven)
        # 3. We remove all of the old eids from mono
        await conftest.delete_all_external_ids(conn=maven_connection)
        # 2. We add new eids
        updated_org_external_ids: List[MavenOrgExternalID] = []
        for org in multiple_maven_org_with_external_ids[0]:
            updated_org_external_ids.extend(
                data_models.MavenOrgExternalIDFactory.create_batch(
                    size=3, organization_id=org.id, data_provider_organization_id=None
                )
            )
        await conftest.persist_external_ids(
            org_external_ids=updated_org_external_ids, conn=maven_connection
        )

        # When
        # 1. Sync the external_ids
        await sync.sync_all_mono_external_ids(configuration_test_client, maven)

        # Then
        # query for oei's in e9y DB that were synced
        synced_org_external_ids: List[
            MavenOrgExternalID
        ] = await configuration_test_client.get_all_external_ids()

        # 1. Check that everything is synced
        assert set(
            [
                (
                    eid.external_id,
                    eid.organization_id,
                    eid.data_provider_organization_id,
                )
                for eid in updated_org_external_ids
            ]
        ) == set(
            [
                (
                    eid.external_id,
                    eid.organization_id,
                    eid.data_provider_organization_id,
                )
                for eid in synced_org_external_ids
            ]
        )

    @staticmethod
    async def test_sync_all_mono_external_ids_with_duplicate_directory_orgs(
        duplicate_directory_maven_org_with_external_ids: Tuple[
            List[MavenOrganization], List[MavenOrgExternalID]
        ],
        configuration_test_client: Configurations,
        header_test_client: HeaderAliases,
        maven: MavenMonoClient,
        maven_connection: aiomysql.Connection,
    ):
        # Given
        # 1. Sync the orgs first
        await sync.sync_all_mono_orgs(
            configuration_test_client, header_test_client, maven
        )

        # when
        with patch("mmlib.ops.stats.increment") as mock_increment:
            await sync.sync_all_mono_external_ids(configuration_test_client, maven)
            # then
            mock_increment.assert_called_once_with(
                metric_name="eligibility.tasks.sync.sync_external_ids.skipped",
                pod_name="core_services",
            )

    @staticmethod
    async def test_sync_all_mono_external_ids_with_exception(
        multiple_maven_org_with_external_ids: Tuple[
            List[MavenOrganization], List[MavenOrgExternalID]
        ],
        configuration_test_client: Configurations,
        header_test_client: HeaderAliases,
        maven: MavenMonoClient,
        maven_connection: aiomysql.Connection,
    ):
        # Given
        await sync.sync_all_mono_orgs(
            configuration_test_client, header_test_client, maven
        )

        with patch("mmlib.ops.stats.increment") as mock_increment, patch(
            "db.clients.configuration_client.Configurations.delete_and_recreate_all_external_ids",
            side_effect=Exception("mocked FK exception"),
        ):
            # when
            await sync.sync_all_mono_external_ids(configuration_test_client, maven)
            # then
            mock_increment.assert_called_once_with(
                metric_name="eligibility.tasks.sync.sync_external_ids.failed",
                pod_name="core_services",
            )

    @staticmethod
    async def test_sync_all_mono_external_ids_with_empty_external_id(
        maven_org_with_empty_external_id: Tuple[
            List[MavenOrganization], List[MavenOrgExternalID]
        ],
        configuration_test_client: Configurations,
        header_test_client: HeaderAliases,
        maven: MavenMonoClient,
        maven_connection: aiomysql.Connection,
    ):
        # Given
        # 1. Sync the orgs first
        await sync.sync_all_mono_orgs(
            configuration_test_client, header_test_client, maven
        )

        # when
        with patch("mmlib.ops.stats.increment") as mock_increment:
            await sync.sync_all_mono_external_ids(configuration_test_client, maven)
            # then
            mock_increment.assert_called_once_with(
                metric_name="eligibility.tasks.sync.sync_external_ids.skipped",
                pod_name="core_services",
            )
