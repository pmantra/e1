from __future__ import annotations

from typing import List

import pytest
from tests.factories import data_models
from tests.functional.conftest import NUMBER_TEST_OBJECTS

from db.clients import configuration_client, file_client
from db.model import Configuration, ExternalMavenOrgInfo
from db.mono.client import MavenOrgExternalID

pytestmark = pytest.mark.asyncio


class TestConfigurationClient:

    # region fetch
    @staticmethod
    async def test_return_all(
        multiple_test_config: configuration_client.Configuration,
        configuration_test_client,
    ):
        # return all our configs
        all_configs = await configuration_test_client.all()
        # Then
        assert len(all_configs) == 10

    @staticmethod
    async def test_get(
        test_config: configuration_client.Configuration, configuration_test_client
    ):
        # When
        returned_organization_config = await configuration_test_client.get(
            test_config.organization_id
        )
        # Then
        assert returned_organization_config == test_config

    @staticmethod
    async def test_get_for_orgs(
        multiple_test_config: configuration_client.Configuration,
        configuration_test_client,
    ):

        input_ids = [config.organization_id for config in multiple_test_config]
        # return configs using their org ids
        returned_organization_configs = await configuration_test_client.get_for_orgs(
            *input_ids
        )

        assert len(returned_organization_configs) == len(input_ids)

        # We can't confirm the full object is the same, but ensure we have the right orgIDs and directory names
        for conf in returned_organization_configs:
            assert conf.organization_id in input_ids

    @staticmethod
    async def test_get_for_file(
        test_config: configuration_client.Configuration,
        test_file: file_client.File,
        configuration_test_client,
    ):
        # When
        returned_organization_config = await configuration_test_client.get_for_file(
            test_file.id
        )
        # Then
        assert returned_organization_config == test_config

    @staticmethod
    async def test_get_for_files(
        multiple_test_file: file_client.File,
        multiple_test_config: configuration_client.Configuration,
        file_test_client,
        configuration_test_client,
    ):

        # Grab the expected files belonging to a few of our configurations
        input_ids = [config.organization_id for config in multiple_test_config]
        test_files = [
            await file_test_client.get_all_for_org(org_id) for org_id in input_ids
        ]

        # Grab all the configurations for the fileIDs we expect
        returned_organization_configs = await configuration_test_client.get_for_files(
            *[f[0].id for f in test_files]
        )

        assert len(returned_organization_configs) == len(input_ids)

        # We can't confirm the full object is the same, but ensure we have the right orgIDs and directory names
        for org_conf in returned_organization_configs:
            assert org_conf.organization_id in input_ids

    @staticmethod
    async def test_get_by_directory_name(
        test_config: configuration_client.Configuration, configuration_test_client
    ):
        # When
        fetched = await configuration_test_client.get_by_directory_name(
            test_config.directory_name
        )
        # Then
        assert fetched == test_config

    @staticmethod
    async def test_add_get_external_id(
        test_config: configuration_client.Configuration, configuration_test_client
    ):
        # Given
        extid = "foo"
        source = "bar"

        # When
        await configuration_test_client.add_external_id(
            organization_id=test_config.organization_id,
            source=source,
            external_id=extid,
        )

        # Then
        fetched = await configuration_test_client.get_by_external_id(
            source=source, external_id=extid
        )
        external_ids: List[
            MavenOrgExternalID
        ] = await configuration_test_client.get_external_ids(
            test_config.organization_id
        )

        assert fetched == test_config
        assert (external_ids[0].source, external_ids[0].external_id) == (source, extid)

    @staticmethod
    async def test_get_external_ids(
        test_config: configuration_client.Configuration, configuration_test_client
    ):

        external_ids = []

        for i in range(2):
            external_ids.append(
                await configuration_test_client.add_external_id(
                    organization_id=test_config.organization_id,
                    source=f"foo_{i}",
                    external_id=f"bar_{i}",
                )
            )

        fetched = await configuration_test_client.get_all_external_ids()

        assert len(external_ids) == len(fetched)
        for ext_id in fetched:
            assert ext_id in external_ids

    @staticmethod
    async def test_get_external_ids_by_data_provider(
        test_config: configuration_client.Configuration, configuration_test_client
    ):
        # Given
        # Create some external mappings
        test_config_2 = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create()
        )
        expected_external_ids = []
        for i in range(3):
            await configuration_test_client.persist(
                model=data_models.ConfigurationFactory.create(organization_id=i)
            )
            expected_external_ids.append(
                await configuration_test_client.add_external_id(
                    data_provider_organization_id=test_config.organization_id,
                    organization_id=i,
                    external_id=f"foo_{i}",
                )
            )
            # Create some dummy mappings to help ensure we actually get the values from our get statement we expect
            await configuration_test_client.add_external_id(
                data_provider_organization_id=test_config_2.organization_id,
                organization_id=i,
                external_id=f"bar_{i}",
            )

        # When
        returned_ids = (
            await configuration_test_client.get_external_ids_by_data_provider(
                data_provider_organization_id=test_config.organization_id
            )
        )
        # Massage for an easy format to verify we did indeed get our external IDs
        returned_external_id_mappings = {
            r.external_id: r.organization_id for r in returned_ids
        }

        # Then
        assert len(returned_ids) == len(expected_external_ids)

        for expected in expected_external_ids:
            external_id = expected.external_id
            assert (
                expected.organization_id == returned_external_id_mappings[external_id]
            )

    @staticmethod
    async def test_get_sub_orgs_by_data_provider(
        configuration_test_client: configuration_client.Configurations,
    ):
        # Given
        data_provider_config: Configuration = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create(data_provider=True)
        )

        expected_sub_orgs: List[
            Configuration
        ] = data_models.ConfigurationFactory.create_batch(size=10, data_provider=False)

        other_sub_orgs: List[
            Configuration
        ] = data_models.ConfigurationFactory.create_batch(size=10, data_provider=False)

        await configuration_test_client.bulk_persist(
            models=expected_sub_orgs + other_sub_orgs
        )
        org_external_ids: List[MavenOrgExternalID] = [
            data_models.MavenOrgExternalIDFactory.create(
                organization_id=org.organization_id,
                data_provider_organization_id=data_provider_config.organization_id,
            )
            for org in expected_sub_orgs
        ]
        await configuration_test_client.bulk_add_external_id(
            identities=org_external_ids
        )

        # When
        fetched_orgs: List[
            Configuration
        ] = await configuration_test_client.get_sub_orgs_by_data_provider(
            data_provider_org_id=data_provider_config.organization_id
        )
        for org in fetched_orgs:
            org.updated_at = None
            org.created_at = None

        # Then
        assert fetched_orgs == expected_sub_orgs

    @staticmethod
    async def test_get_external_infos_by_value_source(
        test_config: configuration_client.Configuration,
        configuration_test_client,
        test_multiple_organization_external_id: List[MavenOrgExternalID],
    ):
        # Given
        # When: matching case
        retrieved_external_infos: List[
            ExternalMavenOrgInfo
        ] = await configuration_test_client.get_external_org_infos_by_value_and_source(
            external_id=test_multiple_organization_external_id[0].external_id,
            source=test_multiple_organization_external_id[0].source,
        )

        # Then
        assert len(retrieved_external_infos) == 1
        assert [
            retrieved_external_infos[0].organization_id,
            retrieved_external_infos[0].directory_name,
            retrieved_external_infos[0].activated_at,
        ] == [
            test_multiple_organization_external_id[0].organization_id,
            test_config.directory_name,
            test_config.activated_at,
        ]

        # When
        # no-matching case: wrong external_id
        no_external_infos: List[
            ExternalMavenOrgInfo
        ] = await configuration_test_client.get_external_org_infos_by_value_and_source(
            external_id="wrong-external-id",
            source=test_multiple_organization_external_id[0].source,
        )

        # Then
        assert len(no_external_infos) == 0

        # When
        # no-matching case: wrong source
        no_external_infos = (
            await configuration_test_client.get_external_org_infos_by_value_and_source(
                external_id=test_multiple_organization_external_id[0].external_id,
                source="wrong-source",
            )
        )

        # Then
        assert len(no_external_infos) == 0

    @staticmethod
    async def test_get_external_infos_by_value_data_provider_id(
        configuration_test_client,
        multiple_test_config_with_data_provider,
        test_multiple_organization_external_id_with_data_provider: List[
            MavenOrgExternalID
        ],
    ):
        # Given
        configuration = multiple_test_config_with_data_provider["organization"]
        data_provider_organization_id = multiple_test_config_with_data_provider[
            "data_provider_organization"
        ].organization_id

        # When
        retrieved_external_infos: List[
            MavenOrgExternalID
        ] = await configuration_test_client.get_external_org_infos_by_value_and_data_provider(
            external_id=test_multiple_organization_external_id_with_data_provider[
                0
            ].external_id,
            data_provider_organization_id=data_provider_organization_id,
        )

        # Then
        assert len(retrieved_external_infos) == 1
        assert [
            retrieved_external_infos[0].organization_id,
            retrieved_external_infos[0].directory_name,
            retrieved_external_infos[0].activated_at,
        ] == [
            configuration.organization_id,
            configuration.directory_name,
            configuration.activated_at,
        ]

        # When
        # no-matching: wrong external_id
        no_external_infos: List[
            MavenOrgExternalID
        ] = await configuration_test_client.get_external_org_infos_by_value_and_data_provider(
            external_id="wrong-external-id",
            data_provider_organization_id=data_provider_organization_id,
        )

        # Then
        assert len(no_external_infos) == 0

        # When
        # no-matching: wrong data_provider_organization_id
        no_external_infos: List[
            MavenOrgExternalID
        ] = await configuration_test_client.get_external_org_infos_by_value_and_data_provider(
            external_id=test_multiple_organization_external_id_with_data_provider[
                0
            ].external_id,
            data_provider_organization_id=-999,
        )

        # Then
        assert len(no_external_infos) == 0

    @staticmethod
    async def test_get_configs_for_optum(
        multiple_test_file: file_client.File,
        multiple_test_config: configuration_client.Configuration,
        file_test_client,
        configuration_test_client,
    ):

        # Given
        num_optum_configs = 5
        await configuration_test_client.bulk_persist(
            models=[
                data_models.ConfigurationFactory.create()
                for _ in range(num_optum_configs)
            ]
        )
        file_org_ids = [org.organization_id for org in multiple_test_config]

        # When
        optum_orgs = await configuration_test_client.get_configs_for_optum()

        # Then
        assert len(optum_orgs) == num_optum_configs
        for o in optum_orgs:
            assert o.organization_id not in file_org_ids

    # endregion

    # region persist/update
    @staticmethod
    @pytest.mark.parametrize(
        "id,dir_name,data_provider", [(123, "foo", True), (123, "foo", False)]
    )
    async def test_persist(configuration_test_client, id, dir_name, data_provider):
        # Given
        config = Configuration(id, dir_name, data_provider=data_provider)

        # When
        await configuration_test_client.persist(model=config)

        # Then
        config_persisted = await configuration_test_client.all()

        assert (
            config.organization_id,
            config.directory_name,
            config.data_provider,
        ) == (
            config_persisted[0].organization_id,
            config_persisted[0].directory_name,
            config_persisted[0].data_provider,
        )

    @staticmethod
    async def test_bulk_persist(configuration_test_client):
        # Given
        configurations = data_models.ConfigurationFactory.create_batch(
            NUMBER_TEST_OBJECTS
        )

        # When
        await configuration_test_client.bulk_persist(models=configurations)

        # Then
        persisted_configurations = await configuration_test_client.all()

        # Set the DB generated timestamps to None
        for configuration in persisted_configurations:
            configuration.updated_at = None
            configuration.created_at = None

        assert configurations == persisted_configurations

    @staticmethod
    @pytest.mark.parametrize(
        "id,dir_name,data_provider", [(123, "foo", True), (123, "foo", False)]
    )
    async def test_update(configuration_test_client, id, dir_name, data_provider):
        # Given
        config = Configuration(id, dir_name, data_provider=data_provider)
        await configuration_test_client.persist(model=config)

        # When
        config.directory_name = "updated_foo"
        config.data_provider = not data_provider
        await configuration_test_client.persist(model=config)

        # Then
        config_persisted = await configuration_test_client.all()

        assert (
            config.organization_id,
            config.directory_name,
            config.data_provider,
        ) == (
            config_persisted[0].organization_id,
            config_persisted[0].directory_name,
            config_persisted[0].data_provider,
        )

    @staticmethod
    async def test_bulk_update(configuration_test_client):
        # Given
        configurations = data_models.ConfigurationFactory.create_batch(
            NUMBER_TEST_OBJECTS
        )
        await configuration_test_client.bulk_persist(models=configurations)

        # When
        for configuration in configurations:
            configuration.directory_name = configuration.directory_name + "_updated"
            configuration.data_provider = not configuration.data_provider

        await configuration_test_client.bulk_persist(models=configurations)

        # Then
        persisted_configurations = await configuration_test_client.all()

        # Set the DB generated timestamps to None
        for configuration in persisted_configurations:
            configuration.updated_at = None
            configuration.created_at = None

        assert configurations == persisted_configurations

    @staticmethod
    async def test_bulk_add_external_id(test_config, configuration_test_client):
        # Given
        org_external_ids: List[
            MavenOrgExternalID
        ] = data_models.MavenOrgExternalIDFactory.create_batch(
            size=10, organization_id=test_config.organization_id
        )
        # When
        await configuration_test_client.bulk_add_external_id(org_external_ids)
        saved_external_ids: List[
            MavenOrgExternalID
        ] = await configuration_test_client.get_external_ids(
            organization_id=test_config.organization_id
        )
        # Then
        assert saved_external_ids == org_external_ids

    @staticmethod
    async def test_bulk_add_external_id_conflict(
        test_config, configuration_test_client
    ):
        # Given
        org_external_id: MavenOrgExternalID = (
            data_models.MavenOrgExternalIDFactory.create(
                organization_id=test_config.organization_id
            )
        )
        await configuration_test_client.add_external_id(
            organization_id=org_external_id.organization_id,
            external_id=org_external_id.external_id,
            source=org_external_id.source,
            data_provider_organization_id=org_external_id.data_provider_organization_id,
        )

        # When
        await configuration_test_client.bulk_add_external_id([org_external_id])
        saved_external_ids = await configuration_test_client.get_external_ids(
            organization_id=test_config.organization_id
        )
        # Then
        assert saved_external_ids[0] == org_external_id

    @staticmethod
    async def test_bulk_add_external_id_data_provider_id(configuration_test_client):
        # Given
        data_provider_config = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create(data_provider=True)
        )
        sub_config = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create(data_provider=False)
        )

        org_external_id: MavenOrgExternalID = (
            data_models.MavenOrgExternalIDFactory.create(
                organization_id=sub_config.organization_id,
                data_provider_organization_id=data_provider_config.organization_id,
            )
        )

        # When
        await configuration_test_client.bulk_add_external_id([org_external_id])
        found_external_ids_raw = (
            await configuration_test_client.get_external_ids_by_data_provider(
                data_provider_organization_id=data_provider_config.organization_id,
            )
        )

        # Then
        assert found_external_ids_raw[0] == org_external_id

    @staticmethod
    async def test_bulk_add_external_id_data_provider_id_conflict(
        configuration_test_client,
    ):
        # Given
        data_provider_config = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create(data_provider=True)
        )
        sub_config = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create(data_provider=False)
        )
        expected_org_external_id = data_models.MavenOrgExternalIDFactory.create(
            organization_id=sub_config.organization_id,
            data_provider_organization_id=data_provider_config.organization_id,
        )
        await configuration_test_client.bulk_add_external_id([expected_org_external_id])

        # When
        # Try adding the org external ID again
        await configuration_test_client.bulk_add_external_id([expected_org_external_id])
        found_external_ids = (
            await configuration_test_client.get_external_ids_by_data_provider(
                data_provider_organization_id=data_provider_config.organization_id,
            )
        )

        # Then
        assert found_external_ids[0] == expected_org_external_id

    # endregion

    @staticmethod
    @pytest.mark.xfail(
        reason="This query can only UPSERT non data-providers. "
        "PostgreSQL only allows one conflict targer when performing DO UPDATE."
    )
    async def test_bulk_add_external_id_data_provider_id_and_source_conflict(
        configuration_test_client,
    ):
        # Given
        data_provider_config = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create(data_provider=True)
        )
        sub_config = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create(data_provider=False)
        )
        org_external_id_data_provider = data_models.MavenOrgExternalIDFactory.create(
            organization_id=sub_config.organization_id,
            data_provider_organization_id=data_provider_config.organization_id,
            source=None,
        )
        org_external_id_source = data_models.MavenOrgExternalIDFactory.create(
            organization_id=sub_config.organization_id,
            data_provider_organization_id=None,
        )
        await configuration_test_client.add_external_id(**org_external_id_data_provider)
        await configuration_test_client.add_external_id(**org_external_id_source)
        expected_external_ids = {
            (
                r["data_provider_organization_id"],
                r["external_id"],
                r["organization_id"],
                r["source"],
            )
            for r in await configuration_test_client.get_all_external_ids()
        }
        # When
        await configuration_test_client.bulk_add_external_id(
            org_external_id_data_provider,
            org_external_id_source,
        )
        found_external_ids_raw = await configuration_test_client.get_all_external_ids()
        saved_external_ids = {
            (
                r["data_provider_organization_id"],
                r["external_id"],
                r["organization_id"],
                r["source"],
            )
            for r in found_external_ids_raw
        }
        # Then
        assert saved_external_ids == expected_external_ids

    # endregion

    # region delete
    @staticmethod
    async def test_delete(
        test_config: configuration_client.Configuration, configuration_test_client
    ):
        original_id = test_config.organization_id

        await configuration_test_client.delete(test_config.organization_id)

        returned = await configuration_test_client.get(original_id)
        assert returned is None  # noqa

    @staticmethod
    async def test_bulk_delete(
        multiple_test_config: configuration_client.Configuration,
        configuration_test_client,
    ):
        original_ids = [config.organization_id for config in multiple_test_config]

        await configuration_test_client.bulk_delete(*original_ids)

        returned = await configuration_test_client.get_for_orgs(*original_ids)
        assert returned == []

    @staticmethod
    async def test_delete_external_ids_for_org(
        configuration_test_client, test_organization_external_id: MavenOrgExternalID
    ):
        # Given
        org_id: int = test_organization_external_id.organization_id

        # When
        await configuration_test_client.delete_external_ids_for_org(
            organization_id=org_id
        )

        # Then
        retrieved_external_id = await configuration_test_client.get_external_ids(org_id)
        assert retrieved_external_id == []

    @staticmethod
    async def test_delete_external_ids_for_data_provider_org(
        multiple_test_config: list[configuration_client.Configuration],
        configuration_test_client,
    ):
        # Given
        data_provider_org_id = multiple_test_config[0].organization_id
        data_provider_external_id_mapping = (
            await configuration_test_client.add_external_id(
                organization_id=multiple_test_config[1].organization_id,
                source=None,
                data_provider_organization_id=data_provider_org_id,
                external_id="test123",
            )
        )

        # When
        deleted = (
            await configuration_test_client.delete_external_ids_for_data_provider_org(
                data_provider_organization_id=data_provider_org_id
            )
        )

        # Then
        assert deleted[0] == data_provider_external_id_mapping
        retrieved_external_id = (
            await configuration_test_client.get_external_ids_by_data_provider(
                data_provider_organization_id=data_provider_org_id
            )
        )
        assert retrieved_external_id == []

    @staticmethod
    async def test_delete_and_recreate_all_external_ids(
        test_organization_external_id: MavenOrgExternalID, configuration_test_client
    ):
        # Given
        org_id = test_organization_external_id.organization_id
        new_external_id = data_models.MavenOrgExternalIDFactory.create(
            organization_id=org_id
        )

        # When
        await configuration_test_client.delete_and_recreate_all_external_ids(
            identities=[new_external_id]
        )

        # Then
        # Ensure we deleted the old external ID and now only have our new one
        external_ids: List[
            MavenOrgExternalID
        ] = await configuration_test_client.get_all_external_ids()
        assert len(external_ids) == 1
        assert (external_ids[0].source, external_ids[0].external_id) == (
            new_external_id.source,
            external_ids[0].external_id,
        )

    # endregion
