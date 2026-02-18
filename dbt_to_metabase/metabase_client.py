"""
Core migration engine: orchestrates parsing, rewriting, and API calls
to migrate dbt models into Metabase transforms.
"""

from __future__ import annotations

import fnmatch
import json
import re
import logging
from typing import Any, Dict, List, Optional

from .config import MigrationConfig
from .dbt_parser import GitHubDbtParser
from .dependency_resolver import CyclicDependencyError, DependencyResolver
from .metabase_client import MetabaseApiError, MetabaseClient
from .models import (
    DbtMaterialization,
    DbtModel,
    DbtProject,
    MetabaseJob,
    MetabaseTag,
    MetabaseTransform,
    MigrationPlan,
)
from .sql_rewriter import SqlRewriter

logger = logging.getLogger(__name__)


class MigrationError(Exception):
    pass


class Migrator:
    def __init__(self, config):
        # type: (MigrationConfig) -> None
        self.config = config
        self.parser = GitHubDbtParser(config.github)
        self.client = MetabaseClient(config.metabase)
        self.project = None  # type: Optional[DbtProject]
        self.plan = None  # type: Optional[MigrationPlan]
        self.execution_layers = []  # type: List[List[str]]

    def run(self):
        # type: () -> MigrationPlan
        logger.info("=" * 60)
        logger.info("dbt -> Metabase Transforms Migration")
        logger.info("=" * 60)

        logger.info("Step 1/6: Parsing dbt project from GitHub...")
        self.project = self.parser.parse()

        logger.info("Step 2/6: Resolving model dependencies...")
        resolver = DependencyResolver(self.project)
        try:
            execution_order = resolver.resolve(exclude_ephemeral=True)
        except CyclicDependencyError as e:
            raise MigrationError("Cannot migrate: {}".format(e))

        layers = resolver.get_execution_layers()
        self.execution_layers = layers
        logger.info(
            "Execution plan: %d models in %d layers",
            len(execution_order), len(layers),
        )

        logger.info("Step 3/6: Building migration plan...")
        self.plan = self._build_plan(execution_order)
        self._log_plan_summary()

        if self.config.dry_run:
            logger.info("DRY RUN -- skipping execution. Plan saved.")
            return self.plan

        logger.info("Step 4/6: Executing migration plan...")
        self._execute_plan()

        logger.info("Step 5/6: Running transforms in dependency order...")
        self._run_transforms()

        if self.config.remote_sync.enabled:
            logger.info("Step 6/6: Triggering Metabase remote sync...")
            self._trigger_remote_sync()
        else:
            logger.info("Step 6/6: Remote sync not enabled, skipping.")

        logger.info("=" * 60)
        logger.info("Migration complete!")
        logger.info("=" * 60)
        return self.plan

    def _build_plan(self, execution_order):
        # type: (List[str]) -> MigrationPlan
        assert self.project is not None

        plan = MigrationPlan(execution_order=execution_order)
        model_lookup = {m.name: m for m in self.project.models.values()}

        schema_overrides = {}  # type: Dict[str, str]
        for mapping in self.config.schema_mappings:
            schema_overrides[mapping.dbt_folder_pattern] = mapping.metabase_schema

        rewriter = SqlRewriter(
            self.project,
            default_schema=self.config.metabase.default_schema,
            schema_overrides=schema_overrides,
        )

        all_tags = set()  # type: set

        for model_name in execution_order:
            model = model_lookup.get(model_name)
            if not model:
                plan.warnings.append("Model '{}' in execution order but not found".format(model_name))
                continue

            if not self._should_include_model(model):
                plan.skipped_models.append(
                    (model.name, "Excluded by include/exclude filters")
                )
                continue

            if model.is_ephemeral:
                plan.skipped_models.append(
                    (model.name, "Ephemeral materialization -- not a persisted table")
                )
                continue

            if model.config.get("is_seed"):
                plan.skipped_models.append(
                    (model.name, "Seed -- already exists as a database table")
                )
                continue

            if model.materialization == DbtMaterialization.VIEW:
                plan.warnings.append(
                    "Model '{}' is a VIEW in dbt. "
                    "Metabase transforms create tables. Converting to table.".format(model.name)
                )

            rewritten_sql, warnings = rewriter.rewrite(model)
            plan.warnings.extend(
                "[{}] {}".format(model.name, w) for w in warnings
            )

            schema = self._resolve_schema(model, schema_overrides)
            folder = self._resolve_folder(model)
            tags = self._resolve_tags(model)
            all_tags.update(tags)

            # Get checkpoint column detected by the rewriter (from the
            # MAX() expression in the incremental block), not unique_key
            checkpoint_col = getattr(rewriter, '_last_checkpoint_column', None)

            transform = MetabaseTransform(
                name=model.name,
                query=rewritten_sql,
                database_id=self.config.metabase.database_id,
                schema_name=schema,
                table_name=model.name,
                description=self._build_description(model),
                folder=folder,
                tags=tags,
                is_incremental=model.is_incremental,
                checkpoint_column=checkpoint_col,
                dbt_model_unique_id=model.unique_id,
            )
            plan.transforms.append(transform)

        for tag_name in sorted(all_tags):
            plan.tags.append(MetabaseTag(name=tag_name))

        for job_def in self.config.jobs:
            plan.jobs.append(
                MetabaseJob(
                    name=job_def.name,
                    schedule=job_def.schedule,
                    tags=job_def.tags,
                )
            )

        return plan

    def _should_include_model(self, model):
        # type: (DbtModel) -> bool
        if self.config.include_models:
            if not any(
                fnmatch.fnmatch(model.name, pattern)
                for pattern in self.config.include_models
            ):
                return False

        if self.config.exclude_models:
            if any(
                fnmatch.fnmatch(model.name, pattern)
                for pattern in self.config.exclude_models
            ):
                return False

        return True

    def _resolve_schema(self, model, schema_overrides):
        # type: (DbtModel, Dict[str, str]) -> str
        for pattern, schema in schema_overrides.items():
            if fnmatch.fnmatch(model.folder, pattern) or model.folder == pattern:
                return schema

        if model.schema_name and model.schema_name != self.project.target_schema:
            return model.schema_name

        return self.config.metabase.default_schema

    def _resolve_folder(self, model):
        # type: (DbtModel) -> str
        if not self.config.preserve_folder_structure:
            return self.config.folder_prefix if self.config.folder_prefix else ""

        if model.folder:
            return model.folder
        return ""

    def _resolve_tags(self, model):
        # type: (DbtModel) -> List[str]
        tags = set()  # type: set
        tag_map = {m.dbt_tag: m.metabase_tag for m in self.config.tag_mappings}
        for dbt_tag in model.tags:
            if dbt_tag in tag_map:
                tags.add(tag_map[dbt_tag])
            else:
                tags.add(dbt_tag)

        if not tags:
            tags.update(self.config.default_tags)

        return sorted(tags)

    def _build_description(self, model):
        # type: (DbtModel) -> str
        parts = []
        if model.description:
            parts.append(model.description)
        parts.append("Migrated from dbt model: {}".format(model.unique_id))
        parts.append("Source: {}".format(model.path))
        if model.depends_on_models:
            parts.append("Dependencies: {}".format(", ".join(model.depends_on_models)))
        return "\n".join(parts)

    def _execute_plan(self):
        # type: () -> None
        assert self.plan is not None

        existing_transforms = self._get_existing_transforms()

        tag_id_map = {}  # type: Dict[str, int]
        for tag in self.plan.tags:
            result = self.client.get_or_create_tag(tag.name)
            tag.tag_id = result["id"]
            tag_id_map[tag.name] = result["id"]
            logger.info("Tag '%s' -> id=%d", tag.name, tag.tag_id)

        for transform in self.plan.transforms:
            existing = existing_transforms.get(transform.name)

            if existing:
                if self.config.on_conflict == "skip":
                    transform.transform_id = existing["id"]
                    logger.info(
                        "Skipping existing transform '%s' (id=%d)",
                        transform.name, existing["id"],
                    )
                    continue
                elif self.config.on_conflict == "replace":
                    logger.info(
                        "Replacing existing transform '%s' (id=%d)",
                        transform.name, existing["id"],
                    )
                    self.client.delete_transform(existing["id"])
                elif self.config.on_conflict == "error":
                    raise MigrationError(
                        "Transform '{}' already exists (id={})".format(
                            transform.name, existing["id"]
                        )
                    )

            folder_id = None
            if transform.folder:
                try:
                    folder_id = self.client.create_folder_hierarchy(transform.folder, namespace="transforms")
                    logger.info("Folder '%s' -> collection_id=%s", transform.folder, folder_id)
                except Exception as e:
                    logger.warning("Failed to create folder '%s': %s", transform.folder, e)

            try:
                result = self.client.create_transform(
                    name=transform.name,
                    query=transform.query,
                    database_id=transform.database_id,
                    schema_name=transform.schema_name,
                    table_name=transform.table_name,
                    description=transform.description,
                    folder_id=folder_id,
                    # Pass incremental settings at creation time so Metabase sets
                    # source.type="incremental-query" correctly.  A post-creation
                    # PUT to ee/transform/{id} is silently ignored when the type
                    # was set to "query" at creation.
                    is_incremental=transform.is_incremental,
                    checkpoint_column=transform.checkpoint_column,
                )
                transform.transform_id = result["id"]

                # Keep the PUT call as a belt-and-suspenders fallback for older
                # Metabase versions that may require it separately.
                if transform.is_incremental and transform.checkpoint_column:
                    try:
                        self.client.set_transform_incremental(
                            transform.transform_id,
                            transform.checkpoint_column,
                        )
                    except MetabaseApiError as e:
                        # Log but don't fail — the CREATE already embedded the strategy
                        self.plan.warnings.append(
                            "[{}] set_transform_incremental PUT returned error "
                            "(incremental-strategy was already set in CREATE): {}".format(
                                transform.name, e
                            )
                        )

                for tag_name in transform.tags:
                    if tag_name in tag_id_map:
                        try:
                            self.client.add_tag_to_transform(
                                transform.transform_id, tag_id_map[tag_name]
                            )
                        except MetabaseApiError as e:
                            self.plan.warnings.append(
                                "[{}] Failed to add tag '{}': {}".format(
                                    transform.name, tag_name, e
                                )
                            )

            except MetabaseApiError as e:
                self.plan.warnings.append(
                    "[{}] Failed to create transform: {}".format(transform.name, e)
                )
                logger.error("Failed to create transform '%s': %s", transform.name, e)

        for job in self.plan.jobs:
            job_tag_ids = [
                tag_id_map[t] for t in job.tags if t in tag_id_map
            ]
            if not job_tag_ids:
                self.plan.warnings.append(
                    "Job '{}' has no valid tags, skipping".format(job.name)
                )
                continue
            try:
                result = self.client.create_job(
                    name=job.name,
                    schedule=job.schedule,
                    tag_ids=job_tag_ids,
                )
                job.job_id = result["id"]
            except MetabaseApiError as e:
                self.plan.warnings.append(
                    "Failed to create job '{}': {}".format(job.name, e)
                )

    def _run_transforms(self):
        # type: () -> None
        assert self.plan is not None

        # Build name -> transform_id map for created transforms
        transform_ids = {}  # type: Dict[str, int]
        for t in self.plan.transforms:
            if t.transform_id:
                transform_ids[t.name] = t.transform_id

        # Track which transform IDs are incremental.  On the very first migration
        # the target table does not exist yet, so Metabase marks the transform as
        # "inactive" and refuses to run the incremental (checkpoint) query.
        # We must run those with full_refresh=True the first time to create and
        # populate the table; subsequent runs will use the checkpoint correctly.
        incremental_ids = {
            t.transform_id
            for t in self.plan.transforms
            if t.is_incremental and t.checkpoint_column and t.transform_id
        }

        # Build name -> MigrationTransform so we can look up schema/table/database
        # when we need to call activate_table before running.
        transform_by_name = {
            t.name: t
            for t in self.plan.transforms
            if t.transform_id
        }

        if not transform_ids:
            logger.warning("No transforms to run (none were created successfully)")
            return

        total_layers = len(self.execution_layers)
        for layer_idx, layer in enumerate(self.execution_layers):
            layer_transforms = [
                (name, transform_ids[name])
                for name in layer if name in transform_ids
            ]

            if not layer_transforms:
                continue

            logger.info(
                "Layer %d/%d: Running %d transforms: %s",
                layer_idx + 1, total_layers,
                len(layer_transforms),
                ", ".join(name for name, _ in layer_transforms),
            )

            # Start all transforms in this layer
            running = []  # type: List[tuple]
            for name, tid in layer_transforms:
                try:
                    # Before running, reactivate any stale inactive table entry
                    # in Metabase's metadata catalog.  This happens when a previous
                    # migration run created the transform (registering the target
                    # table in the catalog) but the run failed before populating it,
                    # leaving Metabase with an active=false entry.  Metabase refuses
                    # to run any transform targeting an inactive table (500 "is inactive").
                    t_obj = transform_by_name.get(name)
                    if t_obj:
                        try:
                            found = self.client.activate_table(
                                t_obj.schema_name, t_obj.table_name, t_obj.database_id
                            )
                            if not found:
                                logger.debug(
                                    "No existing table entry for %s.%s — "
                                    "will be created on first run.",
                                    t_obj.schema_name, t_obj.table_name,
                                )
                        except MetabaseApiError as ae:
                            # Non-fatal: log and continue — worst case the run
                            # fails with the original inactive error.
                            logger.warning(
                                "activate_table for '%s' failed (non-fatal): %s", name, ae
                            )

                    needs_full_refresh = tid in incremental_ids
                    if needs_full_refresh:
                        logger.info(
                            "Transform '%s' is incremental and newly created — "
                            "running full refresh to bootstrap target table.", name
                        )
                    result = self.client.run_transform(tid, full_refresh=needs_full_refresh)
                    run_id = result.get("id")
                    if run_id:
                        running.append((name, tid, run_id))
                    else:
                        logger.info("Transform '%s' started (no run_id returned)", name)
                except MetabaseApiError as e:
                    # If still inactive after reactivation attempt, log clearly.
                    self.plan.warnings.append(
                        "[{}] Failed to run transform: {}".format(name, e)
                    )
                    logger.error("Failed to run transform '%s': %s", name, e)

            # Wait for all transforms in this layer to complete
            for name, tid, run_id in running:
                try:
                    run = self.client.wait_for_run(tid, run_id, timeout=600)
                    status = run.get("status", "unknown")
                    logger.info(
                        "Transform '%s' completed with status: %s", name, status
                    )
                except (MetabaseApiError, TimeoutError) as e:
                    self.plan.warnings.append(
                        "[{}] Transform run failed: {}".format(name, e)
                    )
                    logger.error("Transform '%s' run failed: %s", name, e)

            logger.info("Layer %d/%d complete", layer_idx + 1, total_layers)

        logger.info("All transforms executed")

    def _get_existing_transforms(self):
        # type: () -> Dict[str, dict]
        try:
            transforms = self.client.list_transforms()
            return {t["name"]: t for t in transforms}
        except MetabaseApiError:
            return {}

    def _trigger_remote_sync(self):
        # type: () -> None
        try:
            self.client.trigger_remote_sync()
            logger.info("Remote sync triggered -- transforms will be serialized to git")
        except Exception as e:
            self.plan.warnings.append("Remote sync failed: {}".format(e))
            logger.warning("Remote sync failed: %s", e)

    def _log_plan_summary(self):
        # type: () -> None
        assert self.plan is not None

        logger.info("-" * 40)
        logger.info("MIGRATION PLAN SUMMARY")
        logger.info("-" * 40)
        logger.info("Transforms to create: %d", len(self.plan.transforms))
        logger.info("Tags to create: %d", len(self.plan.tags))
        logger.info("Jobs to create: %d", len(self.plan.jobs))
        logger.info("Skipped models: %d", len(self.plan.skipped_models))

        if self.plan.skipped_models:
            logger.info("Skipped:")
            for name, reason in self.plan.skipped_models:
                logger.info("  - %s: %s", name, reason)

        if self.plan.warnings:
            logger.warning("Warnings (%d):", len(self.plan.warnings))
            for w in self.plan.warnings:
                logger.warning("  ! %s", w)

        logger.info("Execution order: %s", " -> ".join(self.plan.execution_order))
        logger.info("-" * 40)

    def export_plan(self, path):
        # type: (str) -> None
        assert self.plan is not None

        data = {
            "transforms": [
                {
                    "name": t.name,
                    "dbt_model": t.dbt_model_unique_id,
                    "schema": t.schema_name,
                    "table": t.table_name,
                    "folder": t.folder,
                    "tags": t.tags,
                    "incremental": t.is_incremental,
                    "checkpoint_column": t.checkpoint_column,
                    "query_preview": t.query[:200] + "..." if len(t.query) > 200 else t.query,
                }
                for t in self.plan.transforms
            ],
            "tags": [{"name": t.name} for t in self.plan.tags],
            "jobs": [
                {"name": j.name, "schedule": j.schedule, "tags": j.tags}
                for j in self.plan.jobs
            ],
            "execution_order": self.plan.execution_order,
            "skipped_models": [
                {"name": n, "reason": r} for n, r in self.plan.skipped_models
            ],
            "warnings": self.plan.warnings,
        }

        with open(path, "w") as f:
            json.dump(data, f, indent=2)
        logger.info("Plan exported to %s", path)

    # ──────────────────────────────────────────────
    # Remap command
    # ──────────────────────────────────────────────

    def run_remap(self):
        # type: () -> Dict[str, Any]
        """
        Remap Metabase cards from dbt model tables to Metabase transform tables.

        Steps:
        1. List all tables in the target database
        2. Build table ID mapping: dbt table → transform table (by name)
        3. Find all cards referencing dbt tables
        4. Update each card's query to reference transform tables instead
        """
        logger.info("=" * 60)
        logger.info("dbt -> Metabase Transforms: REMAP")
        logger.info("=" * 60)

        remap_map = self.config.remap_schema_map
        if not remap_map:
            raise MigrationError(
                "remap_schema_map required. Map dbt schemas to transform schemas, e.g.:\n"
                "remap_schema_map:\n"
                "  dbt_models_staging: transforms_staging\n"
                "  dbt_models_marts: transforms_marts"
            )

        results = {
            "table_mappings": [],
            "cards_updated": [],
            "cards_skipped": [],
            "warnings": [],
        }  # type: Dict[str, Any]

        # Step 1: Get all tables from Metabase for this database
        logger.info("Step 1/3: Building table ID mapping...")
        database_id = self.config.metabase.database_id

        try:
            # Sync database first to ensure tables are up to date
            self.client.sync_database(database_id)
            import time as _time
            logger.info("Waiting for sync to complete...")
            _time.sleep(5)
        except MetabaseApiError as e:
            logger.warning("Database sync failed (continuing anyway): %s", e)

        tables = self.client.list_tables(database_id)
        logger.info("Found %d tables in database %d", len(tables), database_id)

        # Build schema.table_name -> table_id lookup
        table_lookup = {}  # type: Dict[str, int]
        for t in tables:
            schema = t.get("schema", "")
            name = t.get("name", "")
            table_id = t.get("id")
            if schema and name and table_id:
                key = "{}.{}".format(schema, name)
                table_lookup[key] = table_id

        # Build old_table_id -> new_table_id mapping
        table_id_map = {}  # type: Dict[int, int]
        for dbt_schema, transform_schema in remap_map.items():
            # Find all tables in the dbt schema
            dbt_tables = [
                t for t in tables
                if t.get("schema", "") == dbt_schema
            ]

            for dt in dbt_tables:
                old_id = dt.get("id")
                table_name = dt.get("name", "")
                new_key = "{}.{}".format(transform_schema, table_name)
                new_id = table_lookup.get(new_key)

                if old_id and new_id:
                    table_id_map[old_id] = new_id
                    results["table_mappings"].append({
                        "table_name": table_name,
                        "old_id": old_id,
                        "new_id": new_id,
                        "old_schema": dbt_schema,
                        "new_schema": transform_schema,
                    })
                    logger.info(
                        "Mapped %s.%s (id=%d) -> %s.%s (id=%d)",
                        dbt_schema, table_name, old_id,
                        transform_schema, table_name, new_id,
                    )
                elif old_id and not new_id:
                    msg = "No matching transform table for {}.{} (looked for {})".format(
                        dbt_schema, table_name, new_key
                    )
                    results["warnings"].append(msg)
                    logger.warning(msg)

        if not table_id_map:
            raise MigrationError(
                "No table mappings found. Check that transform tables exist "
                "and remap_schema_map is correct."
            )

        logger.info("Built %d table ID mappings", len(table_id_map))

        # Build field ID mapping: old_field_id -> new_field_id (matched by column name)
        field_id_map = {}  # type: Dict[int, int]
        for old_table_id, new_table_id in table_id_map.items():
            try:
                old_fields = self.client.get_table_fields(old_table_id)
                new_fields = self.client.get_table_fields(new_table_id)
            except MetabaseApiError as e:
                msg = "Failed to get fields for table {} or {}: {}".format(
                    old_table_id, new_table_id, e
                )
                results["warnings"].append(msg)
                logger.warning(msg)
                continue

            new_by_name = {}  # type: Dict[str, int]
            for f in new_fields:
                fname = f.get("name", "")
                fid = f.get("id")
                if fname and fid:
                    new_by_name[fname] = fid

            for f in old_fields:
                old_fid = f.get("id")
                fname = f.get("name", "")
                new_fid = new_by_name.get(fname)
                if old_fid and new_fid:
                    field_id_map[old_fid] = new_fid

        logger.info("Built %d field ID mappings", len(field_id_map))

        # Also build schema name map for native SQL rewriting
        schema_name_map = {}  # type: Dict[str, str]
        for dbt_schema, transform_schema in remap_map.items():
            schema_name_map[dbt_schema] = transform_schema

        # Step 2: Find and update all cards
        logger.info("Step 2/3: Scanning and updating cards...")
        try:
            cards = self.client.list_cards()
        except MetabaseApiError as e:
            raise MigrationError("Failed to list cards: {}".format(e))

        logger.info("Found %d cards to scan", len(cards))

        for card in cards:
            card_id = card.get("id")
            card_name = card.get("name", "unknown")
            dataset_query = card.get("dataset_query", {})

            if not dataset_query:
                continue

            # Skip Metabase internal analytics cards
            card_db = dataset_query.get("database")
            if card_db != database_id:
                continue

            query_type = dataset_query.get("type")
            lib_type = dataset_query.get("lib/type", "")
            changed = False

            if query_type == "query" or lib_type == "mbql/query":
                # MBQL query — update source-table, field refs, and joins
                changed = self._remap_mbql_query(dataset_query, table_id_map, field_id_map)
            elif query_type == "native":
                # Native SQL — replace schema references
                changed = self._remap_native_query(dataset_query, schema_name_map)
            else:
                # Unknown type but might still have stages (lib/type mbql)
                # Try field remapping anyway
                if field_id_map and self._remap_field_ids(dataset_query, field_id_map):
                    changed = True

            if changed:
                try:
                    self.client.update_card(card_id, dataset_query=dataset_query)
                    logger.info("Updated card '%s' (id=%d)", card_name, card_id)
                    results["cards_updated"].append({
                        "id": card_id,
                        "name": card_name,
                        "type": query_type or lib_type,
                    })
                except MetabaseApiError as e:
                    msg = "[card {}] Failed to update: {}".format(card_name, e)
                    results["warnings"].append(msg)
                    logger.error(msg)
            else:
                results["cards_skipped"].append({
                    "id": card_id,
                    "name": card_name,
                })

        # Step 3: Update dashboard cards that have custom queries
        logger.info("Step 3/3: Scanning dashboards for inline card overrides...")
        try:
            dashboards = self.client.list_dashboards()
        except MetabaseApiError as e:
            logger.warning("Failed to list dashboards: %s", e)
            dashboards = []

        for dash_summary in dashboards:
            dash_id = dash_summary.get("id")
            if not dash_id:
                continue

            try:
                dash = self.client.get_dashboard(dash_id)
            except MetabaseApiError:
                continue

            dash_changed = False
            dashcards = dash.get("dashcards", [])

            for dc in dashcards:
                # Dashboard cards can override queries via card_overrides or parameter_mappings
                card_data = dc.get("card", {})
                if not card_data:
                    continue

                dq = card_data.get("dataset_query", {})
                if not dq:
                    continue

                qt = dq.get("type")
                lt = dq.get("lib/type", "")
                if qt == "query" or lt == "mbql/query":
                    if self._remap_mbql_query(dq, table_id_map, field_id_map):
                        dash_changed = True
                elif qt == "native":
                    if self._remap_native_query(dq, schema_name_map):
                        dash_changed = True

            if dash_changed:
                try:
                    self.client.update_dashboard_card(dash_id, dashcards)
                    logger.info(
                        "Updated dashboard '%s' (id=%d)",
                        dash_summary.get("name", "unknown"), dash_id,
                    )
                except MetabaseApiError as e:
                    msg = "[dashboard {}] Failed to update: {}".format(
                        dash_summary.get("name", "unknown"), e
                    )
                    results["warnings"].append(msg)
                    logger.error(msg)

        logger.info("=" * 60)
        logger.info("Remap complete!")
        logger.info("=" * 60)
        return results

    def _remap_mbql_query(self, dataset_query, table_id_map, field_id_map=None):
        # type: (dict, Dict[int, int], Optional[Dict[int, int]]) -> bool
        """Walk an MBQL dataset_query and replace source-table and field IDs. Returns True if changed."""
        if field_id_map is None:
            field_id_map = {}

        changed = False

        # New format: stages array (Metabase v1.50+)
        stages = dataset_query.get("stages", [])
        for stage in stages:
            # Remap source-table in each stage
            source_table = stage.get("source-table")
            if isinstance(source_table, int) and source_table in table_id_map:
                stage["source-table"] = table_id_map[source_table]
                changed = True

            # Remap joins in each stage
            for join in stage.get("joins", []):
                jt = join.get("source-table")
                if isinstance(jt, int) and jt in table_id_map:
                    join["source-table"] = table_id_map[jt]
                    changed = True

                # Joins can also have nested stages
                for join_stage in join.get("stages", []):
                    jst = join_stage.get("source-table")
                    if isinstance(jst, int) and jst in table_id_map:
                        join_stage["source-table"] = table_id_map[jst]
                        changed = True

        # Old format: query object (pre-v1.50)
        query = dataset_query.get("query", {})
        if query:
            source_table = query.get("source-table")
            if isinstance(source_table, int) and source_table in table_id_map:
                query["source-table"] = table_id_map[source_table]
                changed = True

            for join in query.get("joins", []):
                jt = join.get("source-table")
                if isinstance(jt, int) and jt in table_id_map:
                    join["source-table"] = table_id_map[jt]
                    changed = True

            source_query = query.get("source-query")
            if isinstance(source_query, dict):
                inner_dq = {"query": source_query}
                if self._remap_mbql_query(inner_dq, table_id_map, field_id_map):
                    query["source-query"] = inner_dq["query"]
                    changed = True

        # Remap field IDs throughout the entire structure
        if field_id_map and self._remap_field_ids(dataset_query, field_id_map):
            changed = True

        return changed

    def _remap_field_ids(self, obj, field_id_map):
        # type: (Any, Dict[int, int]) -> bool
        """Recursively walk a data structure and remap field ID references.

        Field refs look like: ['field', {...opts...}, <int_field_id>]
        or: ['field', {...opts...}, <int_field_id>] with field ID as the last element.
        Also handles: [..., ['field', {...}, <id>], ...] nested anywhere.
        """
        changed = False

        if isinstance(obj, list):
            # Check if this is a field reference: ['field', <opts>, <int_id>]
            if (
                len(obj) >= 3
                and obj[0] == "field"
                and isinstance(obj[-1], int)
                and obj[-1] in field_id_map
            ):
                obj[-1] = field_id_map[obj[-1]]
                changed = True

            # Also check for integer field IDs in opts dict (e.g., join conditions)
            for i, item in enumerate(obj):
                if isinstance(item, (list, dict)):
                    if self._remap_field_ids(item, field_id_map):
                        changed = True
                # Direct integer field refs in some contexts (e.g., old format)
                elif isinstance(item, int) and i > 0 and obj[0] == "field" and item in field_id_map:
                    obj[i] = field_id_map[item]
                    changed = True

        elif isinstance(obj, dict):
            for key, value in obj.items():
                if isinstance(value, (list, dict)):
                    if self._remap_field_ids(value, field_id_map):
                        changed = True

        return changed

    def _remap_native_query(self, dataset_query, schema_name_map):
        # type: (dict, Dict[str, str]) -> bool
        """Replace schema names in native SQL queries. Returns True if changed."""
        native = dataset_query.get("native", {})
        sql = native.get("query", "")

        if not sql:
            return False

        new_sql = sql
        for old_schema, new_schema in schema_name_map.items():
            # Replace "old_schema". and old_schema. patterns
            new_sql = new_sql.replace(
                '"{}"'.format(old_schema), '"{}"'.format(new_schema)
            )
            new_sql = new_sql.replace(
                "{}.".format(old_schema), "{}.".format(new_schema)
            )

        if new_sql != sql:
            native["query"] = new_sql
            return True

        return False
