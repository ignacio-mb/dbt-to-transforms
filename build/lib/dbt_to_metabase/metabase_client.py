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
        is_incremental=False, checkpoint_column=None, tag_ids=None,
    ):
        # type: (str, str, int, str, str, str, Optional[int], str, bool, Optional[str], Optional[List[int]]) -> dict
        #
        # Metabase API schema (from 400 response):
        #   source.type          -> must always equal "query" (never "incremental-query")
        #   source-incremental-strategy (optional, inside source):
        #     type               -> must equal "checkpoint"
        #     checkpoint-filter  -> string (the column name used as the incremental cursor)
        #
        source = {
            "type": "query",          # always "query" regardless of incremental flag
            "query": {
                "database": database_id,
                "type": "native",
                "native": {"query": query},
            },
        }  # type: Dict[str, Any]

        if is_incremental and checkpoint_column:
            # Embed the strategy inside the source object, not at payload root.
            # Key is "source-incremental-strategy", inner type is "checkpoint".
            source["source-incremental-strategy"] = {
                "type": "checkpoint",
                "checkpoint-filter": checkpoint_column,
            }

        payload = {
            "name": name,
            "database_id": database_id,
            "source": source,
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

        # Include tag_ids in the create payload. Metabase does not expose a
        # working POST /api/transform/{id}/tag endpoint in all versions — the
        # only reliable way to associate tags with a transform is at creation.
        if tag_ids:
            payload["tag_ids"] = tag_ids

        result = self._post("transform", json=payload)
        logger.info(
            "Created transform '%s' (id=%s) -> %s.%s (incremental=%s, checkpoint=%s)",
            name, result.get("id"), schema_name, table_name,
            is_incremental, checkpoint_column,
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
            "ee/transform/{}".format(transform_id),
            json={
                "source-incremental-strategy": {
                    "column": checkpoint_column,
                },
            },
        )

    def upgrade_to_incremental(self, transform_id, checkpoint_column):
        # type: (int, str) -> dict
        """Add incremental checkpoint strategy to an existing transform.

        This is used after bootstrapping: create without incremental strategy,
        run once to create the target table, then upgrade to incremental so
        future runs use the checkpoint filter.
        """
        payload = {
            "source": {
                "source-incremental-strategy": {
                    "type": "checkpoint",
                    "checkpoint-filter": checkpoint_column,
                },
            },
        }
        result = self._put("transform/{}".format(transform_id), json=payload)
        logger.info(
            "Upgraded transform id=%d to incremental (checkpoint=%s)",
            transform_id, checkpoint_column,
        )
        return result

    def run_transform(self, transform_id, full_refresh=False):
        # type: (int, bool) -> dict
        payload = {"full-refresh": True} if full_refresh else None
        result = self._post("transform/{}/run".format(transform_id), json=payload)
        logger.info(
            "Started run for transform id=%d (full_refresh=%s)",
            transform_id, full_refresh,
        )
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
            self._post("ee/remote-sync/export")
            logger.info("Triggered remote sync (export)")
        except MetabaseApiError as e:
            if e.status_code in (400, 404):
                # 404 → remote sync feature not enabled in this Metabase edition.
                # 400 → remote sync not configured (no git repo set up).
                # Both are expected non-fatal conditions; re-raise so the
                # migrator's _trigger_remote_sync() can log it as a warning.
                raise
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

    # Tables API

    def get_database_metadata(self, database_id):
        # type: (int) -> dict
        return self._get("database/{}".format(database_id), params={"include": "tables"})

    def list_tables(self, database_id):
        # type: (int) -> List[dict]
        """Get all tables for a database, including schema info."""
        meta = self.get_database_metadata(database_id)
        return meta.get("tables", [])

    def get_table(self, table_id):
        # type: (int) -> dict
        return self._get("table/{}".format(table_id))

    def activate_table(self, schema_name, table_name, database_id):
        # type: (str, str, int) -> bool
        """Find a Metabase table entry by schema+name and set it active.

        When a transform is deleted (or its first run fails), Metabase keeps the
        table in its metadata catalog but marks it active=false.  Any subsequent
        run against that table returns 500 "is inactive" until it is reactivated.

        Returns True if a matching table was found and activated, False if not found.
        """
        tables = self.list_tables(database_id)
        for t in tables:
            if (
                t.get("schema", "").lower() == schema_name.lower()
                and t.get("name", "").lower() == table_name.lower()
            ):
                table_id = t["id"]
                if not t.get("active", True):
                    self._put("table/{}".format(table_id), json={"active": True})
                    logger.info(
                        "Reactivated stale table entry %s.%s (id=%d)",
                        schema_name, table_name, table_id,
                    )
                else:
                    logger.debug(
                        "Table %s.%s (id=%d) is already active",
                        schema_name, table_name, table_id,
                    )
                return True
        return False

    def retire_stale_table(self, schema_name, table_name, database_id):
        # type: (str, str, int) -> bool
        """Remove a stale table so a new transform can target the same schema+table.

        The Create Transform API returns 403 "A table with that name already
        exists" when either:
          (a) Metabase's metadata catalog has an entry for schema.table, OR
          (b) The physical table/view still exists in the connected database.

        Setting active=false on the catalog entry handles case (a), but not (b).
        To handle (b) we execute DROP TABLE / DROP VIEW via Metabase's own query
        execution endpoint, which uses its existing DB connection so no separate
        psql or DB credentials are needed in the migrator.

        Returns True if a matching catalog entry was found and cleaned up,
        False if no entry was found or cleanup failed.
        """
        tables = self.list_tables(database_id)
        for t in tables:
            if (
                t.get("schema", "").lower() == schema_name.lower()
                and t.get("name", "").lower() == table_name.lower()
            ):
                table_id = t["id"]

                # Step 1: deactivate the catalog entry.
                try:
                    self._put(
                        "table/{}".format(table_id),
                        json={"active": False},
                    )
                    logger.info(
                        "Deactivated catalog entry %s.%s (id=%d)",
                        schema_name, table_name, table_id,
                    )
                except MetabaseApiError as e:
                    logger.warning(
                        "Could not deactivate catalog entry %s.%s (id=%d): %s",
                        schema_name, table_name, table_id, e,
                    )
                    return False

                # Step 2: drop the physical table/view from the database.
                # Metabase checks the live DB when creating a transform, so a
                # stale catalog entry alone is not enough — the physical object
                # must also be gone. We issue DROPs through Metabase's own query
                # execution so no direct DB connection is needed here.
                #
                # IMPORTANT: Metabase's /api/dataset endpoint only executes the
                # FIRST statement in a multi-statement query. We must issue
                # DROP TABLE and DROP VIEW as two separate requests, otherwise
                # the VIEW drop is silently skipped (staging models are views).
                dropped = False
                for drop_sql, obj_type in [
                    (
                        'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE'.format(
                            schema=schema_name, table=table_name
                        ),
                        "table",
                    ),
                    (
                        'DROP VIEW IF EXISTS "{schema}"."{table}" CASCADE'.format(
                            schema=schema_name, table=table_name
                        ),
                        "view",
                    ),
                ]:
                    try:
                        self.execute_query(database_id, drop_sql)
                        dropped = True
                        logger.info(
                            "Dropped physical %s %s.%s from database",
                            obj_type, schema_name, table_name,
                        )
                    except MetabaseApiError as e:
                        logger.debug(
                            "DROP %s %s.%s: %s (may not be that type, continuing)",
                            obj_type, schema_name, table_name, e,
                        )

                if not dropped:
                    logger.warning(
                        "Could not drop %s.%s via either TABLE or VIEW — "
                        "transform creation may still fail",
                        schema_name, table_name,
                    )
                    # Don't return False -- the catalog deactivation succeeded;
                    # let the caller attempt the create and surface any error.

                return True
        return False

    def get_table_fields(self, table_id):
        # type: (int) -> List[dict]
        """Get fields/columns for a table."""
        meta = self._get("table/{}/query_metadata".format(table_id))
        return meta.get("fields", [])

    def execute_query(self, database_id, sql):
        # type: (int, str) -> dict
        """Execute a native SQL query against the given database via Metabase.

        Uses POST /api/dataset, which runs the query through Metabase's existing
        database connection.  This lets the migrator issue DDL statements (DROP,
        etc.) without needing a separate direct database connection or psql.
        """
        payload = {
            "database": database_id,
            "type": "native",
            "native": {"query": sql},
        }
        return self._post("dataset", json=payload)

    # Cards API

    def list_cards(self):
        # type: () -> List[dict]
        return self._get("card")

    def get_card(self, card_id):
        # type: (int) -> dict
        return self._get("card/{}".format(card_id))

    def update_card(self, card_id, **kwargs):
        # type: (int, **Any) -> dict
        return self._put("card/{}".format(card_id), json=kwargs)

    # Dashboards API

    def list_dashboards(self):
        # type: () -> List[dict]
        return self._get("dashboard")

    def get_dashboard(self, dashboard_id):
        # type: (int) -> dict
        return self._get("dashboard/{}".format(dashboard_id))

    def update_dashboard_card(self, dashboard_id, cards_payload):
        # type: (int, List[dict]) -> Any
        return self._put(
            "dashboard/{}".format(dashboard_id),
            json={"dashcards": cards_payload},
        )
