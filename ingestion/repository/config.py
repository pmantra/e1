from __future__ import annotations

import enum
import os
from typing import Dict, List, Tuple

import ddtrace
import structlog
from ingestion import service

from app.tasks import sync
from app.utils import async_ttl_cache
from app.worker import pubsub
from db import model as db_model
from db import redis
from db.clients import configuration_client as config_db_client
from db.clients import file_client as file_db_client
from db.clients import header_aliases_client as header_db_client
from db.mono import client as mono_db

__all__ = ("IngestConfigurationRepository", "IngestionType")


MODULE = __name__
logger = structlog.getLogger(MODULE)


class IngestConfigurationRepository:
    def __init__(
        self,
        file_client: file_db_client.Files | None = None,
        config_client: config_db_client.Configurations | None = None,
        header_client: header_db_client.HeaderAliases | None = None,
        mono_client: mono_db.MavenMonoClient | None = None,
        redis_store: redis.RedisKeyStore | None = None,
        use_tmp: bool | None = False,
    ):
        self._use_tmp = use_tmp
        self._file_client: file_db_client.Files = file_client or file_db_client.Files()
        self._config_client: config_db_client.Configurations = (
            config_client or config_db_client.Configurations()
        )
        self._header_client: header_db_client.HeaderAliases = (
            header_client or header_db_client.HeaderAliases()
        )
        self._mono_client: mono_db.MavenMonoClient = (
            mono_client or mono_db.MavenMonoClient()
        )
        self._redis_store: redis.RedisKeyStore = redis_store or redis.RedisKeyStore()

    # region file
    @ddtrace.tracer.wrap()
    async def create_file(
        self, *, organization_id: int, filename: str
    ) -> file_db_client.File:
        """Create a file record in the DB"""
        if self._use_tmp:
            file = await self._file_client.tmp_persist(
                model=file_db_client.File(
                    organization_id=organization_id, name=filename
                )
            )
        else:
            file = await self._file_client.persist(
                model=file_db_client.File(
                    organization_id=organization_id, name=filename
                )
            )

        return file

    @ddtrace.tracer.wrap()
    async def set_started_at(self, *, file_id: int):
        """Set the started_at timestamp for a file"""
        if self._use_tmp:
            await self._file_client.tmp_set_started_at(id=file_id)
        else:
            await self._file_client.set_started_at(id=file_id)

    @ddtrace.tracer.wrap()
    async def set_encoding(self, *, file_id: int, encoding: str):
        """Set the encoding for a file"""
        if self._use_tmp:
            return await self._file_client.tmp_set_encoding(
                id=file_id, encoding=encoding
            )
        else:
            return await self._file_client.set_encoding(id=file_id, encoding=encoding)

    @ddtrace.tracer.wrap()
    async def set_error(self, *, file_id: int, error: file_db_client.FileError):
        """Set the error for a file"""
        if self._use_tmp:
            return await self._file_client.tmp_set_error(id=file_id, error=error)
        else:
            return await self._file_client.set_error(id=file_id, error=error)

    @ddtrace.tracer.wrap()
    async def set_file_count(
        self, *, file_id: int, raw_count: int, success_count: int, failure_count: int
    ):

        """Set file count"""
        return await self._file_client.set_file_count(
            id=file_id,
            raw_count=raw_count,
            success_count=success_count,
            failure_count=failure_count,
        )

    # endregion

    @ddtrace.tracer.wrap()
    async def sync(self, *, filename: str) -> config_db_client.Configuration | None:
        """Sync the config for the directory associated to the filename"""
        directory, file = IngestConfigurationRepository._split_filename(filename)
        org_config: file_db_client.Configuration = (
            await sync.sync_single_mono_org_for_directory(
                configuration_client=self._config_client,
                header_client=self._header_client,
                mono_client=self._mono_client,
                directory=directory,
            )
        )
        return org_config

    @ddtrace.tracer.wrap()
    async def set_cache(self, *, namespace: str, id: int, key: str, value: int):
        """Set a key in the format of "{namespace}:{id}:{key} with value"""
        formatted_key: str = f"{namespace}:{str(id)}:{key}"
        await self._redis_store.set(key=formatted_key, value=value)

    @ddtrace.tracer.wrap()
    async def get_cache(self, *, namespace: str, id: int, key: str) -> int:
        """Get the value for key in the format of "{namespace}:{id}:{key}"""
        formatted_key: str = f"{namespace}:{str(id)}:{key}"
        return await self._redis_store.get(key=formatted_key) or 0

    @ddtrace.tracer.wrap()
    async def incr_cache(self, *, namespace: str, id: int, key: str) -> int:
        """Increment the key "{namespace}:{id}:{key} by 1 and return the value"""
        formatted_key: str = f"{namespace}:{str(id)}:{key}"
        return await self._redis_store.incr(key=formatted_key)

    @ddtrace.tracer.wrap()
    async def delete_cache(self, *, namespace: str, id: int):
        """Delete all keys at the namespace and id "{namespace}:{id}*"""
        pattern: str = f"{namespace}:{str(id)}:*"
        await self._redis_store.delete_pattern(pattern=pattern)

    @staticmethod
    @ddtrace.tracer.wrap()
    def _split_filename(filename: str) -> Tuple[str, str]:
        """Split a path into a directory and a name"""
        directory, file = os.path.split(filename)
        return directory, file

    @ddtrace.tracer.wrap()
    @async_ttl_cache.AsyncTTLCache(time_to_live=30 * 60, max_size=5_000)
    async def get_header_mapping(
        self, *, source: IngestionType, organization_id: int | None = None
    ) -> Dict[str, str]:
        """Get the header mappings for an organization"""
        if source == IngestionType.FILE and not organization_id:
            raise ValueError("organization_id param is required for file records")

        if source == IngestionType.STREAM:
            raise NotImplementedError

        header_mapping: db_model.HeaderMapping = (
            await self._header_client.get_header_mapping(
                organization_id=organization_id
            )
        )
        mvn_to_ext_headers: Dict = header_mapping.with_all_headers()
        return {
            ext_h.lower(): mvn_h.lower() for mvn_h, ext_h in mvn_to_ext_headers.items()
        }

    @ddtrace.tracer.wrap()
    @async_ttl_cache.AsyncTTLCache(time_to_live=30 * 60, max_size=5_000)
    async def get_external_ids_by_data_provider(
        self, *, organization_id: int
    ) -> Dict[str, int]:
        """Get the external ID to organization_id mappings for this data provider"""
        external_ids: List[
            mono_db.MavenOrgExternalID
        ] = await self._config_client.get_external_ids_by_data_provider(
            data_provider_organization_id=organization_id
        )
        return {eid.external_id: eid.organization_id for eid in external_ids}

    @ddtrace.tracer.wrap()
    @async_ttl_cache.AsyncTTLCache(time_to_live=30 * 60, max_size=5_000)
    async def get_external_org_info(
        self,
        *,
        source: IngestionType,
        client_id: str | None = None,
        customer_id: str | None = None,
        organization_id: int | None = None,
    ) -> db_model.ExternalMavenOrgInfo | None:
        """Get the ExternalMavenOrgInfo for an org, if they are mapped"""
        if source == IngestionType.FILE and not organization_id:
            raise ValueError("organization_id must be provided for file based records")
        elif source == IngestionType.STREAM and None in (client_id, customer_id):
            raise ValueError(
                "client_id and customer_id must be provided for stream based records"
            )

        # File configurations
        if source == IngestionType.FILE and organization_id:
            dp_org_config: db_model.Configuration = await self._config_client.get(
                organization_id
            )
            # It is a data provider
            if dp_org_config.data_provider:
                eid_org_mapping: Dict[
                    str, int
                ] = await self.get_external_ids_by_data_provider(
                    organization_id=organization_id
                )
                if not client_id:
                    raise ValueError(
                        "client_id must be provided for records from data providers"
                    )
                if client_id not in eid_org_mapping:
                    raise service.UnmappedOrganizationError(
                        f"client_id: {client_id} is not configured for processing"
                    )
                org_config: db_model.Configuration = await self._config_client.get(
                    eid_org_mapping.get(client_id)
                )
                return db_model.ExternalMavenOrgInfo(
                    organization_id=org_config.organization_id,
                    directory_name=org_config.directory_name,
                    activated_at=org_config.activated_at,
                )
            else:
                # This is not a data provider org, so the dp_org is just the normal org
                return db_model.ExternalMavenOrgInfo(
                    organization_id=dp_org_config.organization_id,
                    directory_name=dp_org_config.directory_name,
                    activated_at=dp_org_config.activated_at,
                )

        # Stream configurations
        if source == IngestionType.STREAM and client_id and customer_id:
            org: db_model.ExternalMavenOrgInfo | None = (
                await pubsub.retrieve_external_org_info(
                    client_id=client_id,
                    customer_id=customer_id,
                    source="optum",
                    configs=self._config_client,
                )
            )
            if org:
                return org

            return None

    @ddtrace.tracer.wrap()
    @async_ttl_cache.AsyncTTLCache(time_to_live=30 * 60, max_size=2_000)
    async def get_affiliations_header_for_org(
        self, *, organization_id: int
    ) -> List[db_model.HeaderAlias]:
        """
        get affiliation header only
        @param organization_id:  organization id
        @return:  list of HeaderAlias.
        """
        return await self._header_client.get_affiliations_header_for_org(
            organization_id
        )


class IngestionType(str, enum.Enum):
    FILE = "file"
    STREAM = "stream"
