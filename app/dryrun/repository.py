from __future__ import annotations

import os
from datetime import datetime
from typing import (
    DefaultDict,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    TypeVar,
)

import asyncpg
import structlog

from app.dryrun import model as dry_run_model
from app.dryrun import utils as dry_run_utils
from app.eligibility.populations import model as pop_model
from db import model as db_model
from db.clients import (
    configuration_client,
    file_client,
    header_aliases_client,
    member_versioned_client,
    population_client,
)
from db.mono.client import MavenOrgExternalID

logger = structlog.getLogger(__name__)


class Files:
    DRY_RUN_FOLDER = "dryrun"

    def __init__(self) -> None:
        self.file: db_model.File | None = None
        self.file_client = file_client.Files()

    def build_for_dry_run(self, file_name) -> db_model.File:
        organization_id_str = file_name.split("_")[0]
        try:
            organization_id = int(organization_id_str)
            self.file = db_model.File(
                id=-1,
                organization_id=organization_id,
                name=os.path.join(Files.DRY_RUN_FOLDER, file_name),
                created_at=datetime.now(),
            )
            return self.file
        except ValueError:
            raise dry_run_model.ParseOrganizationError(file_name)

    async def set_started_at(self, id: int) -> Optional[datetime]:
        self.file.started_at = datetime.now()
        return self.file.started_at

    async def set_completed_at(self, id: int) -> Optional[datetime]:
        self.file.completed_at = datetime.now()
        return self.file.completed_at

    async def set_encoding(
        self,
        id: int,
        encoding: str,
    ) -> Optional[str]:
        self.file.encoding = encoding
        return self.file.encoding

    async def set_file_count(
        self,
        *,
        id: int,
        raw_count: int,
        success_count: int,
        failure_count: int,
    ) -> None:
        self.file.raw_count = raw_count
        self.file.success_count = success_count
        self.file.failure_count = failure_count

    async def get_one_before_latest_for_org(
        self,
        organization_id: int,
    ):
        return await self.file_client.get_latest_for_org(
            organization_id=organization_id
        )

    async def get_success_count(
        self,
        id: int,
    ) -> Optional[int]:
        try:
            return await self.file_client.get_success_count(id=id)
        except Exception:
            logger.info("Dry-run: no previous count find, return dry run file count")
            return self.file.success_count


T = TypeVar("T")


class FileParseResults:
    def __init__(self) -> None:
        self.parse_errors = []
        self.parsed_members: Dict[int, List[db_model.MemberVersioned]] = DefaultDict(
            list
        )

    async def bulk_persist_file_parse_errors(
        self,
        data: Iterable[T] = (),
        *,
        errors: Iterable[db_model.FileParseError],
    ) -> int:
        self.parse_errors.extend(errors)
        return len(errors)

    async def bulk_persist_file_parse_results(
        self,
        data: Iterable[T] = (),
        *,
        results: Iterable[db_model.FileParseResult] = (),
    ) -> int:
        if results:
            for r in results:
                self.parsed_members[r.organization_id].append(
                    dry_run_utils.to_member(r)
                )
        return len(results)

    def _iterdump(self, models: Iterable[T]) -> Iterator[T]:
        kvs = self._get_kvs
        yield from (kvs(m) for m in models)

    @classmethod
    def _get_kvs(cls, model: T) -> Mapping:
        return {
            field: value
            for field, value in cls.iterator(model)
            if field not in cls.__exclude_fields__
        }

    async def delete_file_parse_errors_for_files(
        self,
        *files: int,
    ) -> int:
        logger.info("Dry-run: skip delete_file_parse_errors_for_files")

    async def bulk_persist_parsed_records_for_files_dual_write_hash(
        self, *files: int
    ) -> List:
        logger.info(
            "Dry-run: skip bulk_persist_parsed_records_for_files_dual_write_hash"
        )
        return []

    async def expire_missing_records_for_file_versioned(
        self, file_id: int, organization_id: int
    ) -> int:
        logger.info("Dry-run: skip expire_missing_records_for_file_versioned")
        return 0

    async def expire_missing_records_for_file(
        self, file_id: int, organization_id: int
    ) -> int:
        logger.info("Dry-run: skip expire_missing_records_for_file")
        return 0

    async def get_count_hashed_inserted_for_file(
        self,
        file_id: int,
        file_created_at: datetime,
    ):
        logger.info("Dry-run: skip get_count_hashed_inserted_for_file")
        return {
            "hashed_count": 0,
            "new_count": 0,
        }


class Configurations:
    def __init__(self) -> None:
        self.config_client = configuration_client.Configurations()

    async def get(self, organization_id: int) -> db_model.Configuration:
        return await self.config_client.get(organization_id)

    async def get_external_ids_by_data_provider(
        self,
        data_provider_organization_id: int,
    ) -> List[MavenOrgExternalID]:
        return await self.config_client.get_external_ids_by_data_provider(
            data_provider_organization_id=data_provider_organization_id
        )


class HeaderAliases:
    def __init__(self) -> None:
        self.header_aliases_client = header_aliases_client.HeaderAliases()

    async def get_header_mapping(
        self,
        organization_id: int,
    ):
        return await self.header_aliases_client.get_header_mapping(organization_id)


class Members:
    def __init__(self) -> None:
        pass


class MembersVersioned:
    def __init__(self) -> None:
        self.member_versioned_client = member_versioned_client.MembersVersioned()
        self.client = self.member_versioned_client.client


class Verifications:
    def __init__(self) -> None:
        pass

    async def batch_pre_verify_records_by_org(
        self,
        *,
        organization_id: int,
        batch_size: int,
        file_id: int | None = None,
        connection: asyncpg.Connection = None,
    ) -> int:
        logger.info("Dry-run: skip batch_pre_verify_records_by_org")
        return 0


class Populations:
    def __init__(self):
        self.population_client = population_client.Populations()

    async def get_all_for_organization_id(
        self,
        organization_id: int,
    ) -> List[pop_model.Population]:
        return await self.population_client.get_all_for_organization_id(
            organization_id=organization_id
        )

    async def get_active_population_for_organization_id(
        self,
        organization_id: int,
    ) -> pop_model.Population | None:
        return await self.population_client.get_active_population_for_organization_id(
            organization_id=organization_id
        )

    async def get(self, id: int) -> pop_model.Population:
        return await self.population_client.get(id)
