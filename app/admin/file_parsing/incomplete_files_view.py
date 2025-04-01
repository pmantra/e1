from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
from typing import Collection, Dict, Iterable, List
import urllib

from cachetools import cached, TTLCache
import structlog.stdlib
import typic
from ddtrace import tracer
from flask import flash, request
from flask_admin import expose
from structlog import BoundLogger
from werkzeug.datastructures import MultiDict

from app.admin.base.base_view import BaseViewWithSessionRollback
from app.admin.utils import transform_entry_utils
from app.eligibility.domain import repository, service
from app.eligibility.domain.model import FileActions
from db import model
from db.clients import file_client, file_parse_results_client, member_client
from db.flask import synchronize
from db.sqlalchemy.models.file import File
from db.sqlalchemy.models.incomplete_file_records_by_org import IncompleteFilesByOrg
from db.sqlalchemy.sqlalchemy_config import Session

logger = structlog.getLogger(__name__)
metadata_v2_cache = TTLCache(maxsize=100, ttl=600)


INGESTION_URL = "http://e9y-ingestion-mvn-webserver-service.e9y.svc.cluster.local/api/v1/e9y-ingestion/"


@typic.slotted
@dataclass
class FileMetadata:
    file: model.File
    to_persist: int
    to_delete: int
    errors: int


@typic.slotted
@dataclass
class OrganizationMetadata:
    id: int
    config: model.Configuration
    existing_members: int
    incomplete: Collection[FileMetadata]


@typic.slotted
@dataclass
class OrganizationMetadataV2:
    id: int
    directory_name: str
    transform_entries: List[Dict]


class IncompleteFilesView(BaseViewWithSessionRollback):
    @tracer.wrap()
    @expose(url="/", methods=("GET",))
    def index(self, *, log: BoundLogger = None):
        log = log or logger.bind(view=self.__class__.__name__)
        log.info("Gathering metadata for organizations with incomplete files.")
        metadata = self.gather_metadata()
        metadata_v2 = self.gather_metadata_v2()
        return self.render("pending-files-pg.html", orgs=metadata, orgs_v2=metadata_v2)

    @classmethod
    def _parse_form(cls, form: MultiDict) -> dict[int, FileActions]:
        actions_by_file = {}
        actions_by_file_v2 = {}
        for action in form.keys():
            action_key = cls._form_action_to_file_action[action]
            for fid in form.getlist(action):
                # Handle v2 pending file actions
                if action == "review_action":
                    file_attrs = fid.split("_")
                    review_action, entry_id = file_attrs
                    actions_by_file_v2[entry_id] = review_action
                else:
                    file_action = actions_by_file.setdefault(int(fid), {})
                    file_action[action_key] = True

        return actions_by_file, actions_by_file_v2

    _form_action_to_file_action = {
        "error": "clear_errors",
        "expire": "expire",
        "persist": "persist",
        "purge": "purge_all",
        "review_action": "review_action",
    }

    @tracer.wrap()
    @expose(url="/", methods=("POST",))
    def actions(self):
        actions, actions_v2 = self._parse_form(request.form)
        log = logger.bind(view=self.__class__.__name__)
        if actions:
            log.info("Scheduling actions for persistence.")
            log.debug("Actions to schedule.", actions=actions)

            results = self.handle_actions(actions, log=log)
            errors = [(fid, r) for fid, r in results if isinstance(r, Exception)]
            successes = [(fid, r) for fid, r in results if not isinstance(r, Exception)]

            log.debug("Results.", actions=results)

            for fid, error in errors:
                flash(
                    f"⚠️ Encountered {error.__class__.__name__} for File {fid}: {str(error)!r}",
                    category="warning",
                )

            if successes:
                file_num = len(successes)
                file_name = "File" if file_num == 1 else "Files"
                action_num = len(successes)
                action_name = "action" if action_num == 1 else "actions"
                flash(
                    f"✔️ Ran {action_num} {action_name} for {file_num} {file_name}.",
                    category="success",
                )

        if actions_v2:
            log.info("Scheduling actions v2 for persistence.")
            log.debug("Actions_v2 to schedule.", actions=actions_v2)

            results = self.handle_actions_v2(actions_v2, log=log)
            errors = [(fid, r) for fid, r in results if isinstance(r, Exception)]
            successes = [(fid, r) for fid, r in results if not isinstance(r, Exception)]

            log.debug("Results.", actions_v2=results)

            for fid, error in errors:
                flash(
                    f"⚠️ Encountered {error.__class__.__name__} for transform entry {fid}: {str(error)!r}",
                    category="warning",
                )

            if successes:
                file_num = len(successes)
                file_name = "Entry" if file_num == 1 else "Entries"
                action_num = len(successes)
                action_name = "action" if action_num == 1 else "actions"
                metadata_v2_cache.clear()
                flash(
                    f"✔️ Ran {action_num} {action_name} for {file_num} {file_name}.",
                    category="success",
                )

        return self.index(log=log)

    @tracer.wrap()
    def gather_metadata(
        self, *, log: BoundLogger = None
    ) -> Collection[OrganizationMetadata]:
        log = log or logger.bind(view=self.__class__.__name__)

        with Session.begin() as session:
            files_by_org = session.query(IncompleteFilesByOrg).all()

        metadata = []
        for org in files_by_org:
            incomplete_files = []

            for incomplete_files_and_stats in org.incomplete:

                incomplete_file = incomplete_files_and_stats["file"]

                file = File(
                    id=incomplete_file["id"],
                    organization_id=incomplete_file["organization_id"],
                    name=incomplete_file["name"],
                    encoding=incomplete_file["encoding"],
                    started_at=incomplete_file["started_at"],
                    created_at=incomplete_file["created_at"],
                    updated_at=incomplete_file["updated_at"],
                )
                file_metadata = FileMetadata(
                    file=file,
                    to_persist=incomplete_files_and_stats["total_parsed"],
                    to_delete=incomplete_files_and_stats["total_missing"],
                    errors=incomplete_files_and_stats["total_errors"],
                )

                config = model.Configuration(
                    organization_id=org.config["organization_id"],
                    directory_name=org.config["directory_name"],
                    email_domains=org.config["email_domains"],
                    implementation=org.config["implementation"],
                    data_provider=org.config["data_provider"],
                    created_at=org.config["created_at"],
                    updated_at=org.config["updated_at"],
                )

                incomplete_files.append(file_metadata)

            org_metadata = OrganizationMetadata(
                id=org.id,
                config=config,
                existing_members=org.total_members,
                incomplete=incomplete_files,
            )

            metadata.append(org_metadata)

        log.info(f"Gathered {len(metadata)} organizations with incomplete data")
        return metadata

    @tracer.wrap()
    @cached(metadata_v2_cache)
    def gather_metadata_v2(
        self, *, log: BoundLogger = None
    ) -> Collection[OrganizationMetadataV2]:
        log = log or logger.bind(view=self.__class__.__name__)

        transform_entry_url = INGESTION_URL + "transform_entries"
        # Only fetch entries pending review
        params = {"count": 100, "status": "pending_review"}
        data = json.dumps(params).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "x-maven-user-id": "e9y-admin-user-id",
        }

        request = urllib.request.Request(
            transform_entry_url,
            data=data,
            headers=headers,
            method="POST",
        )

        with urllib.request.urlopen(request) as response:
            if 200 <= response.status < 399:
                data = response.read()
                transform_entries = json.loads(data)
                for tr in transform_entries:
                    transform_entry_utils.format_entry(tr)
            else:
                transform_entries = []

        org_mapping = {}
        for tr in transform_entries:
            org_id = tr["org_id"]
            if org_id not in org_mapping:
                org_metadata = OrganizationMetadataV2(
                    id=org_id,
                    directory_name=tr["directory_name"],
                    transform_entries=[],
                )
                org_mapping[org_id] = org_metadata
            else:
                org_metadata = org_mapping[org_id]
            org_metadata.transform_entries.append(tr)

        return org_mapping.values()

    @synchronize
    @tracer.wrap()
    async def handle_actions(
        self, actions: dict[int, FileActions], *, log: BoundLogger = None
    ) -> Iterable[int, dict | Exception]:
        """
        We will look through a dict where the key is the file_id and the value is an instance
        of the FileActions object

        Args:
            actions:
            log:

        Returns: Iterable[int, dict | Exception]

        """
        db_repository: repository.ParsedRecordsDatabaseRepository = (
            repository.ParsedRecordsDatabaseRepository(
                fpr_client=file_parse_results_client.FileParseResults(),
                member_client=member_client.Members(),
                file_client=file_client.Files(),
            )
        )

        tasks = []

        fid: int | str
        action: FileActions
        for fid, action in actions.items():
            tasks.append(
                asyncio.create_task(
                    service.process_actions_for_file(
                        file_id=fid, repo=db_repository, **action
                    )
                )
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)

        return [*zip(actions.keys(), results)]

    @tracer.wrap()
    def handle_actions_v2(
        self, actions: dict[str, FileActions], *, log: BoundLogger = None
    ) -> Iterable[int, dict | Exception]:
        def submit_review_request(entry_id, review_action):
            review_url = INGESTION_URL + "transform_review"
            params = {"transform_id": entry_id, "action": review_action}
            data = json.dumps(params).encode("utf-8")
            headers = {
                "Content-Type": "application/json",
                "x-maven-user-id": "e9y-admin-user-id",
            }

            request = urllib.request.Request(
                review_url,
                data=data,
                headers=headers,
                method="POST",
            )

            try:
                resp = urllib.request.urlopen(request)
                return resp
            except Exception as e:
                return e

        results = []
        for entry_id, review_action in actions.items():
            result = submit_review_request(entry_id, review_action)
            results.append(result)
        return [*zip(actions.keys(), results)]
