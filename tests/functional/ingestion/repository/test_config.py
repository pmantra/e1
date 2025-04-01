from __future__ import annotations

from typing import Dict

import pytest
from ingestion import repository, service
from tests.factories import data_models as factories

from db import model as db_model
from db import redis
from db.clients import configuration_client, file_client, header_aliases_client

pytestmark = pytest.mark.asyncio


class TestCreateFile:
    @staticmethod
    async def test_create(
        ingest_config: repository.IngestConfigurationRepository,
        file_test_client: file_client.Files,
        configuration_test_client: configuration_client.Configurations,
    ):
        # Given
        org_config: db_model.Configuration = await configuration_test_client.persist(
            model=factories.ConfigurationFactory.create()
        )
        file_name: str = "primary/file.csv"
        # When
        expected_file: db_model.File = await ingest_config.create_file(
            filename=file_name, organization_id=org_config.organization_id
        )

        # Then
        file = await file_test_client.get(expected_file.id)
        assert file == expected_file


class TestSetStartedAt:
    @staticmethod
    async def test_set_started_at(
        ingest_config: repository.IngestConfigurationRepository,
        file_test_client: file_client.Files,
        configuration_test_client: configuration_client.Configurations,
    ):
        # Given
        org_config: db_model.Configuration = await configuration_test_client.persist(
            model=factories.ConfigurationFactory.create()
        )
        expected_file: db_model.File = await file_test_client.persist(
            model=factories.FileFactory.create(
                organization_id=org_config.organization_id
            )
        )

        # When
        await ingest_config.set_started_at(file_id=expected_file.id)

        # Then
        file: db_model.File = await file_test_client.get(expected_file.id)
        assert file.started_at

    @staticmethod
    async def test_set_started_at_not_called(
        ingest_config: repository.IngestConfigurationRepository,
        file_test_client: file_client.Files,
        configuration_test_client: configuration_client.Configurations,
    ):
        # Given
        org_config: db_model.Configuration = await configuration_test_client.persist(
            model=factories.ConfigurationFactory.create()
        )
        expected_file: db_model.File = await file_test_client.persist(
            model=factories.FileFactory.create(
                organization_id=org_config.organization_id
            )
        )

        # When - Then
        file: db_model.File = await file_test_client.get(expected_file.id)
        assert not file.started_at


class TestSetEncoding:
    @staticmethod
    async def test_set_encoding(
        ingest_config: repository.IngestConfigurationRepository,
        file_test_client: file_client.Files,
        configuration_test_client: configuration_client.Configurations,
    ):
        # Given
        org_config: db_model.Configuration = await configuration_test_client.persist(
            model=factories.ConfigurationFactory.create()
        )
        expected_file: db_model.File = await file_test_client.persist(
            model=factories.FileFactory.create(
                organization_id=org_config.organization_id
            )
        )
        expected_encoding: str = "ascii"
        # When
        await ingest_config.set_encoding(
            file_id=expected_file.id, encoding=expected_encoding
        )

        # Then
        file: db_model.File = await file_test_client.get(expected_file.id)
        assert file.encoding == expected_encoding

    @staticmethod
    async def test_set_encoding_not_called(
        ingest_config: repository.IngestConfigurationRepository,
        file_test_client: file_client.Files,
        configuration_test_client: configuration_client.Configurations,
    ):
        # Given
        org_config: db_model.Configuration = await configuration_test_client.persist(
            model=factories.ConfigurationFactory.create()
        )
        expected_file: db_model.File = await file_test_client.persist(
            model=factories.FileFactory.create(
                organization_id=org_config.organization_id, encoding=""
            )
        )

        # When
        file: db_model.File = await file_test_client.get(expected_file.id)

        # Then
        assert not file.encoding


class TestSetError:
    @staticmethod
    async def test_set_error(
        ingest_config: repository.IngestConfigurationRepository,
        file_test_client: file_client.Files,
        configuration_test_client: configuration_client.Configurations,
    ):
        # Given
        org_config: db_model.Configuration = await configuration_test_client.persist(
            model=factories.ConfigurationFactory.create()
        )
        expected_file: db_model.File = await file_test_client.persist(
            model=factories.FileFactory.create(
                organization_id=org_config.organization_id
            )
        )
        expected_error: db_model.FileError = db_model.FileError.MISSING
        # When
        await ingest_config.set_error(file_id=expected_file.id, error=expected_error)

        # Then
        file: db_model.File = await file_test_client.get(expected_file.id)
        assert file.error == expected_error

    @staticmethod
    async def test_set_error_incompatible_enum(
        ingest_config: repository.IngestConfigurationRepository,
        file_test_client: file_client.Files,
        configuration_test_client: configuration_client.Configurations,
    ):
        # Given
        org_config: db_model.Configuration = await configuration_test_client.persist(
            model=factories.ConfigurationFactory.create()
        )
        expected_file: db_model.File = await file_test_client.persist(
            model=factories.FileFactory.create(
                organization_id=org_config.organization_id
            )
        )

        # When - Then
        with pytest.raises(Exception):
            await ingest_config.set_error(
                file_id=expected_file.id, error="invalid_error"
            )

    @staticmethod
    async def test_set_error_not_called(
        ingest_config: repository.IngestConfigurationRepository,
        file_test_client: file_client.Files,
        configuration_test_client: configuration_client.Configurations,
    ):
        # Given
        org_config: db_model.Configuration = await configuration_test_client.persist(
            model=factories.ConfigurationFactory.create()
        )
        expected_file: db_model.File = await file_test_client.persist(
            model=factories.FileFactory.create(
                organization_id=org_config.organization_id
            )
        )
        # When
        file: db_model.File = await file_test_client.get(expected_file.id)

        # Then
        assert not file.error


class TestSetCache:
    @staticmethod
    async def test_set_cache(
        ingest_config: repository.IngestConfigurationRepository,
        keystore: redis.RedisKeyStore,
    ):
        # Given
        namespace: str = "fruit"
        id: int = 123
        key: str = "cost"
        expected_value: int = 11

        # When
        await ingest_config.set_cache(
            namespace=namespace, id=id, key=key, value=expected_value
        )

        # Then
        value = await keystore.get(f"{namespace}:{id}:{key}")
        assert value == expected_value

    @staticmethod
    async def test_set_cache_twice(
        ingest_config: repository.IngestConfigurationRepository,
        keystore: redis.RedisKeyStore,
    ):
        # Given
        namespace: str = "fruit"
        id: int = 123
        key: str = "cost"
        original_value: int = 8
        expected_value: int = 11

        # When
        await ingest_config.set_cache(
            namespace=namespace, id=id, key=key, value=original_value
        )
        await ingest_config.set_cache(
            namespace=namespace, id=id, key=key, value=expected_value
        )

        # Then
        value = await keystore.get(f"{namespace}:{id}:{key}")
        assert value == expected_value

    @staticmethod
    async def test_set_cache_not_called(
        ingest_config: repository.IngestConfigurationRepository,
        keystore: redis.RedisKeyStore,
    ):
        # Given
        namespace: str = "fruit"
        id: int = 123
        key: str = "cost"
        expected_value = None

        # When
        # Then
        value = await keystore.get(f"{namespace}:{id}:{key}")
        assert value == expected_value


class TestGetCache:
    @staticmethod
    async def test_get_cache(
        ingest_config: repository.IngestConfigurationRepository,
        keystore: redis.RedisKeyStore,
    ):
        # Given
        namespace: str = "fruit"
        id: int = 1
        key: str = "weight"
        expected_value: int = 10
        await keystore.set(key=f"{namespace}:{id}:{key}", value=expected_value)

        # When
        value: int = await ingest_config.get_cache(namespace=namespace, id=id, key=key)

        # Then
        assert value == expected_value

    @staticmethod
    async def test_get_cache_twice(
        ingest_config: repository.IngestConfigurationRepository,
        keystore: redis.RedisKeyStore,
    ):
        # Given
        namespace: str = "fruit"
        id: int = 1
        key: str = "weight"
        expected_value: int = 10
        await keystore.set(key=f"{namespace}:{id}:{key}", value=expected_value)

        # When
        value_first: int = await ingest_config.get_cache(
            namespace=namespace, id=id, key=key
        )
        value_second: int = await ingest_config.get_cache(
            namespace=namespace, id=id, key=key
        )

        # Then
        assert value_first == value_second == expected_value

    @staticmethod
    async def test_get_cache_key_not_set(
        ingest_config: repository.IngestConfigurationRepository,
        keystore: redis.RedisKeyStore,
    ):
        # Given
        namespace: str = "fruit"
        id: int = 1
        key: str = "weight"
        expected_value: int = 10
        await keystore.set(key=f"{namespace}:{id}:{key}", value=expected_value)

        # When
        value: int | None = await ingest_config.get_cache(
            namespace="vegetables", id=id, key=key
        )

        # Then
        assert value == 0


class TestIncrCache:
    @staticmethod
    async def test_incr_cache_not_set(
        ingest_config: repository.IngestConfigurationRepository,
        keystore: redis.RedisKeyStore,
    ):
        # Given
        namespace: str = "fruit"
        id: int = 123
        key: str = "cost"

        # When
        await ingest_config.incr_cache(namespace=namespace, id=id, key=key)

        # Then
        value = await keystore.get(f"{namespace}:{id}:{key}")
        assert value == 1

    @staticmethod
    async def test_incr_cache_called_twice(
        ingest_config: repository.IngestConfigurationRepository,
        keystore: redis.RedisKeyStore,
    ):
        # Given
        namespace: str = "fruit"
        id: int = 123
        key: str = "cost"

        # When
        await ingest_config.incr_cache(namespace=namespace, id=id, key=key)

        await ingest_config.incr_cache(namespace=namespace, id=id, key=key)

        # Then
        value = await keystore.get(f"{namespace}:{id}:{key}")
        assert value == 2


class TestDeleteCache:
    @staticmethod
    async def test_delete_cache(
        ingest_config: repository.IngestConfigurationRepository,
        keystore: redis.RedisKeyStore,
    ):
        # Given
        namespace: str = "fruit"
        id: int = 1
        pairs: Dict = {f"{namespace}:{str(id)}:{str(key)}": key for key in range(10)}
        await keystore.mset(**pairs)

        # When
        await ingest_config.delete_cache(namespace=namespace, id=id)

        # Then
        value = await keystore.get(f"{namespace}:{id}:1")
        assert not value

    @staticmethod
    async def test_delete_cache_deletes_correct_set(
        ingest_config: repository.IngestConfigurationRepository,
        keystore: redis.RedisKeyStore,
    ):
        # Given
        namespace: str = "fruit"
        id_to_delete: int = 1
        id_to_keep: int = 2
        pairs_to_delete: Dict = {
            f"{namespace}:{str(id_to_delete)}:{str(key)}": key for key in range(10)
        }
        pairs_to_keep: Dict = {
            f"{namespace}:{str(id_to_keep)}:{str(key)}": key for key in range(10)
        }
        await keystore.mset(**pairs_to_delete)
        await keystore.mset(**pairs_to_keep)

        # When
        await ingest_config.delete_cache(namespace=namespace, id=id_to_delete)

        # Then
        value = await keystore.get(f"{namespace}:{id_to_keep}:1")
        assert value

    @staticmethod
    async def test_delete_cache_delete_empty(
        ingest_config: repository.IngestConfigurationRepository,
        keystore: redis.RedisKeyStore,
    ):
        # Given
        namespace: str = "fruit"
        id: int = 1

        # When
        await ingest_config.delete_cache(namespace=namespace, id=id)

        # Then
        value = await keystore.get(f"{namespace}:{id}:1")
        assert not value


class TestGetHeaderMapping:
    @staticmethod
    async def test_get_header_mapping_no_custom_headers(
        ingest_config: repository.IngestConfigurationRepository,
    ):
        # Given
        organization_id = 1
        expected_mapping: Dict = {
            ext_h.lower(): mvn_h.lower()
            for mvn_h, ext_h in db_model.HeaderMapping().with_all_headers().items()
        }

        # When
        headers: Dict = await ingest_config.get_header_mapping(
            source=repository.IngestionType.FILE,
            organization_id=organization_id,
        )

        assert headers == expected_mapping

    @staticmethod
    async def test_get_header_mapping(
        ingest_config: repository.IngestConfigurationRepository,
        configuration_test_client: configuration_client.Configurations,
        header_test_client: header_aliases_client.HeaderAliases,
    ):
        # Given

        org: db_model.Configuration = await configuration_test_client.persist(
            model=factories.ConfigurationFactory.create()
        )
        internal_header: str = "unique_corp_id"
        external_header: str = "gobbledigook"
        await header_test_client.persist(
            model=factories.HeaderAliasFactory.create(
                organization_id=org.organization_id,
                header=internal_header,
                alias=external_header,
            )
        )

        # When
        headers: Dict = await ingest_config.get_header_mapping(
            source=repository.IngestionType.FILE,
            organization_id=org.organization_id,
        )

        assert headers[external_header] == internal_header

    @staticmethod
    async def test_get_header_mapping_not_configured(
        ingest_config: repository.IngestConfigurationRepository,
        configuration_test_client: configuration_client.Configurations,
        header_test_client: header_aliases_client.HeaderAliases,
    ):
        # Given

        org: db_model.Configuration = await configuration_test_client.persist(
            model=factories.ConfigurationFactory.create()
        )
        external_header: str = "gobbledigook"

        # When
        headers: Dict = await ingest_config.get_header_mapping(
            source=repository.IngestionType.FILE,
            organization_id=org.organization_id,
        )

        assert not headers.get(external_header)

    @staticmethod
    async def test_get_header_mapping_missing_organization_id(
        ingest_config: repository.IngestConfigurationRepository,
        configuration_test_client: configuration_client.Configurations,
        header_test_client: header_aliases_client.HeaderAliases,
    ):
        # Given
        ingestion_type = repository.IngestionType.FILE
        organization_id = None

        # When

        # THen
        with pytest.raises(ValueError):
            await ingest_config.get_header_mapping(
                source=ingestion_type, organization_id=organization_id
            )

    @staticmethod
    async def test_get_header_mapping_stream_record(
        ingest_config: repository.IngestConfigurationRepository,
        configuration_test_client: configuration_client.Configurations,
        header_test_client: header_aliases_client.HeaderAliases,
    ):
        # Given
        org: db_model.Configuration = await configuration_test_client.persist(
            model=factories.ConfigurationFactory.create()
        )

        # When
        with pytest.raises(NotImplementedError):
            await ingest_config.get_header_mapping(
                source=repository.IngestionType.STREAM,
                organization_id=org.organization_id,
            )


class TestGetExternalIDsForDataProvider:
    @staticmethod
    async def test_get_external_ids_for_data_provider(
        ingest_config: repository.IngestConfigurationRepository,
        configuration_test_client: configuration_client.Configurations,
    ):
        # Given
        data_provider_org: configuration_client.Configuration = (
            await configuration_test_client.persist(
                model=factories.ConfigurationFactory.create()
            )
        )
        sub_org: configuration_client.Configuration = (
            await configuration_test_client.persist(
                model=factories.ConfigurationFactory.create()
            )
        )
        external_id: str = "sub_org_id"
        await configuration_test_client.add_external_id(
            data_provider_organization_id=data_provider_org.organization_id,
            organization_id=sub_org.organization_id,
            external_id=external_id,
        )

        # When
        mapping: Dict[str, int] = await ingest_config.get_external_ids_by_data_provider(
            organization_id=data_provider_org.organization_id
        )

        # Then
        assert {external_id: sub_org.organization_id} == mapping

    @staticmethod
    async def test_get_external_ids_for_data_provider_no_result(
        ingest_config: repository.IngestConfigurationRepository,
        configuration_test_client: configuration_client.Configurations,
    ):
        # Given
        data_provider_org: configuration_client.Configuration = (
            await configuration_test_client.persist(
                model=factories.ConfigurationFactory.create()
            )
        )
        sub_org: configuration_client.Configuration = (
            await configuration_test_client.persist(
                model=factories.ConfigurationFactory.create()
            )
        )
        external_id: str = "sub_org_id"
        await configuration_test_client.add_external_id(
            data_provider_organization_id=data_provider_org.organization_id,
            organization_id=sub_org.organization_id,
            external_id=external_id,
        )

        # When
        mapping: Dict[str, int] = await ingest_config.get_external_ids_by_data_provider(
            organization_id=data_provider_org.organization_id + 1
        )

        # Then
        assert not mapping


class TestGetExternalOrgInfo:
    @staticmethod
    async def test_get_external_org_info_missing_param_for_file_type(
        ingest_config: repository.IngestConfigurationRepository,
    ):
        # Given
        source = repository.IngestionType.FILE
        organization_id = None

        # When - Then
        with pytest.raises(ValueError):
            await ingest_config.get_external_org_info(
                source=source, organization_id=organization_id
            )

    @staticmethod
    async def test_get_external_org_info_missing_param_for_stream_type(
        ingest_config: repository.IngestConfigurationRepository,
    ):
        # Given
        source = repository.IngestionType.STREAM
        client_id = None

        # When - Then
        with pytest.raises(ValueError):
            await ingest_config.get_external_org_info(
                source=source, client_id=client_id
            )

    @staticmethod
    async def test_get_external_org_info_file_non_data_provider(
        ingest_config: repository.IngestConfigurationRepository,
        configuration_test_client: configuration_client.Configurations,
    ):
        # Given
        source = repository.IngestionType.FILE
        org_config: db_model.Configuration = await configuration_test_client.persist(
            model=factories.ConfigurationFactory.create()
        )

        # When
        org_info: db_model.ExternalMavenOrgInfo | None = (
            await ingest_config.get_external_org_info(
                source=source, organization_id=org_config.organization_id
            )
        )

        # Then
        assert (
            org_info.organization_id,
            org_info.directory_name,
            org_info.activated_at,
        ) == (
            org_config.organization_id,
            org_config.directory_name,
            org_config.activated_at,
        )

    @staticmethod
    async def test_get_external_org_info_file_data_provider(
        ingest_config: repository.IngestConfigurationRepository,
        configuration_test_client: configuration_client.Configurations,
    ):
        # Given
        source = repository.IngestionType.FILE
        data_provider_org: db_model.Configuration = (
            await configuration_test_client.persist(
                model=factories.ConfigurationFactory.create(data_provider=True)
            )
        )
        sub_org: configuration_client.Configuration = (
            await configuration_test_client.persist(
                model=factories.ConfigurationFactory.create()
            )
        )
        external_id: str = "sub_org_id"
        await configuration_test_client.add_external_id(
            data_provider_organization_id=data_provider_org.organization_id,
            organization_id=sub_org.organization_id,
            external_id=external_id,
        )
        # When
        org_info: db_model.ExternalMavenOrgInfo | None = (
            await ingest_config.get_external_org_info(
                source=source,
                client_id=external_id,
                organization_id=data_provider_org.organization_id,
            )
        )

        # Then
        assert (
            org_info.organization_id,
            org_info.directory_name,
            org_info.activated_at,
        ) == (
            sub_org.organization_id,
            sub_org.directory_name,
            sub_org.activated_at,
        )

    @staticmethod
    async def test_get_external_org_info_file_data_provider_missing_client_id(
        ingest_config: repository.IngestConfigurationRepository,
        configuration_test_client: configuration_client.Configurations,
    ):
        # Given
        source = repository.IngestionType.FILE
        data_provider_org: db_model.Configuration = (
            await configuration_test_client.persist(
                model=factories.ConfigurationFactory.create(data_provider=True)
            )
        )
        sub_org: configuration_client.Configuration = (
            await configuration_test_client.persist(
                model=factories.ConfigurationFactory.create()
            )
        )
        external_id: str = "sub_org_id"
        await configuration_test_client.add_external_id(
            data_provider_organization_id=data_provider_org.organization_id,
            organization_id=sub_org.organization_id,
            external_id=external_id,
        )

        # When - Then
        with pytest.raises(ValueError):
            await ingest_config.get_external_org_info(
                source=source, organization_id=data_provider_org.organization_id
            )

    @staticmethod
    async def test_get_external_org_info_file_data_provider_unmapped_client_id(
        ingest_config: repository.IngestConfigurationRepository,
        configuration_test_client: configuration_client.Configurations,
    ):
        # Given
        source = repository.IngestionType.FILE
        data_provider_org: db_model.Configuration = (
            await configuration_test_client.persist(
                model=factories.ConfigurationFactory.create(data_provider=True)
            )
        )
        await configuration_test_client.persist(
            model=factories.ConfigurationFactory.create()
        )
        external_id: str = "sub_org_id"

        # When - Then
        with pytest.raises(service.UnmappedOrganizationError):
            await ingest_config.get_external_org_info(
                source=source,
                client_id=external_id,
                organization_id=data_provider_org.organization_id,
            )

    @staticmethod
    @pytest.mark.usefixtures("reset_external_id_cache")
    async def test_get_external_org_info_stream_mapped_composite_key(
        ingest_config: repository.IngestConfigurationRepository,
        configuration_test_client: configuration_client.Configurations,
    ):
        # Given
        source = repository.IngestionType.STREAM
        client_id: str = "royco"
        customer_id: str = "parks"
        external_id: str = f"{client_id}:{customer_id}"
        org: db_model.Configuration = await configuration_test_client.persist(
            model=factories.ConfigurationFactory.create()
        )
        await configuration_test_client.add_external_id(
            organization_id=org.organization_id, external_id=external_id, source="optum"
        )

        # When
        org_info: db_model.ExternalMavenOrgInfo | None = (
            await ingest_config.get_external_org_info(
                source=source, client_id=client_id, customer_id=customer_id
            )
        )

        assert (
            org_info.organization_id,
            org_info.directory_name,
            org_info.activated_at,
        ) == (
            org.organization_id,
            org.directory_name,
            org.activated_at,
        )

    @staticmethod
    @pytest.mark.usefixtures("reset_external_id_cache")
    async def test_get_external_org_info_stream_unmapped_key(
        ingest_config: repository.IngestConfigurationRepository,
        configuration_test_client: configuration_client.Configurations,
    ):
        # Given
        source = repository.IngestionType.STREAM
        client_id: str = "royco"
        customer_id: str = "parks"
        external_id: str = f"{client_id}:{customer_id}"
        org: db_model.Configuration = await configuration_test_client.persist(
            model=factories.ConfigurationFactory.create()
        )
        await configuration_test_client.add_external_id(
            organization_id=org.organization_id, external_id=external_id, source="optum"
        )

        # When
        org_info: db_model.ExternalMavenOrgInfo | None = (
            await ingest_config.get_external_org_info(
                source=source, client_id=client_id, customer_id="ads"
            )
        )

        assert not org_info

    @staticmethod
    @pytest.mark.usefixtures("reset_external_id_cache")
    async def test_get_external_org_info_stream_mapped_client_id(
        ingest_config: repository.IngestConfigurationRepository,
        configuration_test_client: configuration_client.Configurations,
    ):
        # Given
        source = repository.IngestionType.STREAM
        client_id: str = "royco"
        org: db_model.Configuration = await configuration_test_client.persist(
            model=factories.ConfigurationFactory.create()
        )
        await configuration_test_client.add_external_id(
            organization_id=org.organization_id, external_id=client_id, source="optum"
        )

        # When
        org_info: db_model.ExternalMavenOrgInfo | None = (
            await ingest_config.get_external_org_info(
                source=source, client_id=client_id, customer_id="ads"
            )
        )

        assert (
            org_info.organization_id,
            org_info.directory_name,
            org_info.activated_at,
        ) == (
            org.organization_id,
            org.directory_name,
            org.activated_at,
        )


class TestGetAffiliationsHeaderForOrg:
    @staticmethod
    async def test_get_affiliations_header_for_org_found_both(
        ingest_config: repository.IngestConfigurationRepository,
        header_test_client: header_aliases_client.HeaderAliases,
        configuration_test_client: configuration_client.Configurations,
    ):
        # Given
        config: configuration_client.Configuration = (
            await configuration_test_client.persist(
                model=factories.ConfigurationFactory.create()
            )
        )
        header1: header_aliases_client.HeaderAlias = (
            factories.HeaderAliasFactory.create()
        )
        header2: header_aliases_client.HeaderAlias = (
            factories.HeaderAliasFactory.create()
        )
        header1.organization_id = config.organization_id
        header1.alias = "client_id"
        header2.organization_id = config.organization_id
        header2.alias = "customer_id"
        await header_test_client.bulk_persist(models=[header1, header2])

        # When
        headers = await ingest_config.get_affiliations_header_for_org(
            organization_id=config.organization_id
        )

        # Then
        assert len(headers) == 2

    @staticmethod
    async def test_get_affiliations_header_for_org_found_client_id(
        ingest_config: repository.IngestConfigurationRepository,
        header_test_client: header_aliases_client.HeaderAliases,
        configuration_test_client: configuration_client.Configurations,
    ):
        # Given
        config: configuration_client.Configuration = (
            await configuration_test_client.persist(
                model=factories.ConfigurationFactory.create()
            )
        )
        header1: header_aliases_client.HeaderAlias = (
            factories.HeaderAliasFactory.create()
        )
        header2: header_aliases_client.HeaderAlias = (
            factories.HeaderAliasFactory.create()
        )
        header1.organization_id = config.organization_id
        header1.alias = "client_id"
        header2.organization_id = config.organization_id
        await header_test_client.bulk_persist(models=[header1, header2])

        # When
        headers = await ingest_config.get_affiliations_header_for_org(
            organization_id=config.organization_id
        )

        # Then
        assert len(headers) == 1

    @staticmethod
    async def test_get_head_alias_for_org_not_found(
        ingest_config: repository.IngestConfigurationRepository,
    ):
        # When
        headers = await ingest_config.get_affiliations_header_for_org(
            organization_id=999
        )

        # Then
        assert len(headers) == 0
