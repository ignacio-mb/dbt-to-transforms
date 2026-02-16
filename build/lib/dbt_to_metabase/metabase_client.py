"""
Metabase API client for Transforms, Tags, and Jobs.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

import requests

from .config import MetabaseConfig

logger = logging.getLogger(__name__)


class MetabaseApiError(Exception):
    def __init__(self, status_code, message, response=None):
        # type: (int, str, Optional[dict]) -> None
        self.status_code = status_code
        self.response = response
        super(MetabaseApiError, self).__init__(
            "Metabase API error {}: {}".format(status_code, message)
        )


class MetabaseClient:
    def __init__(self, config):
        # type: (MetabaseConfig) -> None
        self.config = config
        self.base_url = "{}/api".format(config.url)
        self.session = requests.Session()
        self._authenticated = False
        self._authenticate()

    def _authenticate(self):
        # type: () -> None
        if self.config.api_key:
            self.session.headers["x-api-key"] = self.config.api_key
            self._authenticated = True
            logger.info("Authenticated with Metabase using API key")
        elif self.config.username and self.config.password:
            resp = self.session.post(
                "{}/session".format(self.base_url),
                json={
                    "username": self.config.username,
                    "password": self.config.password,
                },
            )
            resp.raise_for_status()
            token = resp.json().get("id")
            self.session.headers["X-Metabase-Session"] = token
            self._authenticated = True
            logger.info("Authenticated with Metabase using session token")
        else:
            raise ValueError(
                "Metabase authentication required: provide api_key or username/password"
            )

    def _request(self, method, endpoint, json=None, params=None):
        # type: (str, str, Optional[dict], Optional[dict]) -> Any
        url = "{}/{}".format(self.base_url, endpoint.lstrip("/"))
        resp = self.session.request(method, url, json=json, params=params)
        if resp.status_code >= 400:
            try:
                body = resp.json()
            except Exception:
                body = None
            if isinstance(body, dict):
                msg = body.get("message", resp.text)
            else:
                msg = resp.text
            raise MetabaseApiError(resp.status_code, msg, body if isinstance(body, dict) else None)
        if resp.status_code == 204:
            return None
        return resp.json()

    def _get(self, endpoint, params=None):
        # type: (str, Optional[dict]) -> Any
        return self._request("GET", endpoint, params=params)

    def _post(self, endpoint, json=None):
        # type: (str, Optional[dict]) -> Any
        return self._request("POST", endpoint, json=json)

    def _put(self, endpoint, json=None):
        # type: (str, Optional[dict]) -> Any
        return self._request("PUT", endpoint, json=json)

    def _delete(self, endpoint):
        # type: (str) -> Any
        return self._request("DELETE", endpoint)

    # Transforms API

    def list_transforms(self):
        # type: () -> List[dict]
        return self._get("transform")

    def get_transform(self, transform_id):
        # type: (int) -> dict
        return self._get("transform/{}".format(transform_id))

    def create_transform(
        self, name, query, database_id, schema_name, table_name,
        description="", folder_id=None, transform_type="query",
    ):
        # type: (str, str, int, str, str, str, Optional[int], str) -> dict
        payload = {
            "name": name,
            "database_id": database_id,
            "source": {
                "type": transform_type,
                "query": {
                    "database": database_id,
                    "type": "native",
                    "native": {"query": query},
                },
            },
            "target": {
                "type": "table",
                "schema": schema_name,
                "name": table_name,
            },
        }  # type: Dict[str, Any]
        if description:
            payload["description"] = description
        if folder_id:
            payload["collection_id"] = folder_id

        result = self._post("transform", json=payload)
        logger.info(
            "Created transform '%s' (id=%s) -> %s.%s",
            name, result.get("id"), schema_name, table_name,
        )
        return result

    def update_transform(self, transform_id, **kwargs):
        # type: (int, **Any) -> dict
        return self._put("transform/{}".format(transform_id), json=kwargs)

    def delete_transform(self, transform_id):
        # type: (int) -> None
        self._delete("transform/{}".format(transform_id))
        logger.info("Deleted transform id=%d", transform_id)

    def set_transform_incremental(self, transform_id, checkpoint_column):
        # type: (int, str) -> dict
        return self._put(
            "transform/{}/settings".format(transform_id),
            json={"incremental": True, "checkpoint_column": checkpoint_column},
        )

    def run_transform(self, transform_id):
        # type: (int) -> dict
        result = self._post("transform/{}/run".format(transform_id))
        logger.info("Started run for transform id=%d", transform_id)
        return result

    def get_transform_runs(self, transform_id):
        # type: (int) -> List[dict]
        return self._get("transform/{}/runs".format(transform_id))

    # Tags API

    def list_tags(self):
        # type: () -> List[dict]
        return self._get("transform-tag")

    def create_tag(self, name):
        # type: (str) -> dict
        result = self._post("transform-tag", json={"name": name})
        logger.info("Created tag '%s' (id=%s)", name, result.get("id"))
        return result

    def get_or_create_tag(self, name):
        # type: (str) -> dict
        tags = self.list_tags()
        for tag in tags:
            if tag.get("name", "").lower() == name.lower():
                return tag
        return self.create_tag(name)

    def add_tag_to_transform(self, transform_id, tag_id):
        # type: (int, int) -> None
        self._post(
            "transform/{}/tag".format(transform_id),
            json={"tag_id": tag_id},
        )
        logger.debug("Added tag %d to transform %d", tag_id, transform_id)

    # Jobs API

    def list_jobs(self):
        # type: () -> List[dict]
        return self._get("transform-job")

    def create_job(self, name, schedule, tag_ids):
        # type: (str, str, List[int]) -> dict
        result = self._post(
            "transform-job",
            json={"name": name, "schedule": schedule, "tag_ids": tag_ids},
        )
        logger.info(
            "Created job '%s' (id=%s) with schedule '%s'",
            name, result.get("id"), schedule,
        )
        return result

    def get_job(self, job_id):
        # type: (int) -> dict
        return self._get("transform-job/{}".format(job_id))

    def update_job(self, job_id, **kwargs):
        # type: (int, **Any) -> dict
        return self._put("transform-job/{}".format(job_id), json=kwargs)

    # Collections (folders)

    def list_collections(self, namespace=None):
        # type: (Optional[str]) -> List[dict]
        params = {}  # type: Dict[str, Any]
        if namespace:
            params["namespace"] = namespace
        return self._get("collection", params=params)

    def find_or_create_collection(self, name, parent_id=None, namespace=None):
        # type: (str, Optional[int], Optional[str]) -> dict
        collections = self.list_collections(namespace=namespace)
        for c in collections:
            if c.get("name") == name and c.get("namespace") == namespace:
                if parent_id is None or c.get("parent_id") == parent_id:
                    return c
        payload = {"name": name}  # type: Dict[str, Any]
        if parent_id:
            payload["parent_id"] = parent_id
        if namespace:
            payload["namespace"] = namespace
        result = self._post("collection", json=payload)
        logger.info("Created collection/folder '%s' (id=%s, namespace=%s)", name, result.get("id"), namespace)
        return result

    def create_folder_hierarchy(self, path, namespace=None):
        # type: (str, Optional[str]) -> int
        parts = [p for p in path.split("/") if p]
        parent_id = None  # type: Optional[int]
        for part in parts:
            collection = self.find_or_create_collection(part, parent_id, namespace=namespace)
            parent_id = collection["id"]
        return parent_id  # type: ignore

    # Database / sync

    def get_database(self, database_id):
        # type: (int) -> dict
        return self._get("database/{}".format(database_id))

    def sync_database(self, database_id):
        # type: (int) -> None
        self._post("database/{}/sync".format(database_id))
        logger.info("Triggered sync for database id=%d", database_id)

    def trigger_remote_sync(self):
        # type: () -> None
        try:
            self._post("serialization/export")
            logger.info("Triggered remote sync (serialization export)")
        except MetabaseApiError as e:
            if e.status_code == 404:
                logger.warning("Remote sync not available (may not be enabled)")
            else:
                raise

    def wait_for_run(self, transform_id, run_id, timeout=300, poll_interval=5):
        # type: (int, int, int, int) -> dict
        elapsed = 0
        while elapsed < timeout:
            runs = self.get_transform_runs(transform_id)
            for run in runs:
                if run.get("id") == run_id:
                    status = run.get("status")
                    if status in ("completed", "success"):
                        return run
                    elif status in ("failed", "error"):
                        raise MetabaseApiError(
                            500,
                            "Transform run {} failed: {}".format(run_id, run.get("error")),
                        )
            time.sleep(poll_interval)
            elapsed += poll_interval
        raise TimeoutError(
            "Transform run {} did not complete within {}s".format(run_id, timeout)
        )
