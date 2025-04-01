from __future__ import annotations

from datetime import datetime

import structlog.stdlib

logger = structlog.getLogger(__name__)


def format_entry(entry):
    entry["raw_file_path"] = _gs_to_https(entry["raw_file_path"])
    entry["transformed_file_path"] = _gs_to_https(entry["transformed_file_path"])
    entry["error_file_path"] = _gs_to_https(entry["error_file_path"])
    entry["orphan_file_path"] = _gs_to_https(entry["orphan_file_path"])
    if entry["details"] and len(entry["details"]) > 100:
        entry["details"] = entry["details"][:100] + "..."
    if entry["created_at"]:
        entry["created_at"] = _epoch_to_datetime_string(entry["created_at"])
    if entry["completed_at"]:
        entry["completed_at"] = _epoch_to_datetime_string(entry["completed_at"])


def _gs_to_https(gs_urls):
    if not gs_urls:
        return []
    gs_url_list = gs_urls.split(",")
    results = []
    for gs_url in gs_url_list:
        results.append(
            gs_url.replace("gs://", "https://console.cloud.google.com/storage/browser/")
        )
    return results


def _epoch_to_datetime_string(epoch_microseconds):
    epoch_seconds = epoch_microseconds / 1_000_000
    dt = datetime.fromtimestamp(epoch_seconds)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")
