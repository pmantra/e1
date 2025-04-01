from __future__ import annotations

from datetime import datetime
import json
import urllib

import flask_admin
from flask_admin import expose
import structlog.stdlib

logger = structlog.getLogger(__name__)

TRANSFORM_ENTRIES_URL = "http://e9y-ingestion-mvn-webserver-service.e9y.svc.cluster.local/api/v1/e9y-ingestion/transform_entries"
TRANSFORM_ENTRY_DETAILS_URL = "http://e9y-ingestion-mvn-webserver-service.e9y.svc.cluster.local/api/v1/e9y-ingestion/transform_entry_details"


class TransformEntriesView(flask_admin.BaseView):
    @expose(url="/", methods=("GET",))
    def index(self):
        transform_entries = []
        error_message = None

        try:
            params = {"count": 100}
            data = json.dumps(params).encode("utf-8")
            headers = {
                "Content-Type": "application/json",
                "x-maven-user-id": "e9y-admin-user-id",
            }
            request = urllib.request.Request(
                TRANSFORM_ENTRIES_URL,
                data=data,
                headers=headers,
                method="POST",
            )

            with urllib.request.urlopen(request) as response:
                if 200 <= response.status < 399:
                    data = response.read()
                    transform_entries = json.loads(data)

                    for tr in transform_entries:
                        self._format_entry(tr)
                else:
                    error_message = f"Error fetching data: HTTP {response.status}; reason {response.reason}"
        except urllib.error.URLError as e:
            # Handle URL and connection errors
            error_message = f"Error fetching transform entries: {e.reason}"
        except urllib.error.HTTPError as e:
            # Handle HTTP-specific errors
            error_message = f"HTTP Error {e.code}: {e.reason}"

        return self.render(
            "transform-entries.html", entries=transform_entries, error=error_message
        )

    @expose("/details/<string:entry_id>")
    def details(self, entry_id):
        entry_records = []
        error_message = None
        try:
            params = {"transform_id": entry_id}
            data = json.dumps(params).encode("utf-8")
            headers = {
                "Content-Type": "application/json",
                "x-maven-user-id": "e9y-admin-user-id",
            }
            request = urllib.request.Request(
                TRANSFORM_ENTRY_DETAILS_URL,
                data=data,
                headers=headers,
                method="POST",
            )
            with urllib.request.urlopen(request) as response:
                if 200 <= response.status < 399:
                    data = response.read()
                    entry_records = json.loads(data)
                else:
                    error_message = f"Error fetching data: HTTP {response.status}; reason {response.reason}"
        except urllib.error.URLError as e:
            # Handle URL and connection errors
            error_message = f"Error fetching transform entries: {e.reason}"
        except urllib.error.HTTPError as e:
            # Handle HTTP-specific errors
            error_message = f"HTTP Error {e.code}: {e.reason}"

        return self.render(
            "transform-entry-details.html", records=entry_records, error=error_message
        )

    def _format_entry(self, entry):
        entry["raw_file_path"] = self._gs_to_https(entry["raw_file_path"])
        entry["transformed_file_path"] = self._gs_to_https(
            entry["transformed_file_path"]
        )
        entry["error_file_path"] = self._gs_to_https(entry["error_file_path"])
        entry["orphan_file_path"] = self._gs_to_https(entry["orphan_file_path"])
        if entry["details"] and len(entry["details"]) > 100:
            entry["details"] = entry["details"][:100] + "..."
        if entry["created_at"]:
            entry["created_at"] = self._epoch_to_datetime_string(entry["created_at"])
        if entry["completed_at"]:
            entry["completed_at"] = self._epoch_to_datetime_string(
                entry["completed_at"]
            )

    def _gs_to_https(self, gs_urls):
        if not gs_urls:
            return []
        gs_url_list = gs_urls.split(",")
        results = []
        for gs_url in gs_url_list:
            results.append(
                gs_url.replace(
                    "gs://", "https://console.cloud.google.com/storage/browser/"
                )
            )
        return results

    def _epoch_to_datetime_string(self, epoch_microseconds):
        epoch_seconds = epoch_microseconds / 1_000_000
        dt = datetime.fromtimestamp(epoch_seconds)
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")
