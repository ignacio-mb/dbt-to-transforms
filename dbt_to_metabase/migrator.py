"""
Core migration engine: orchestrates parsing, rewriting, and API calls
to migrate dbt models into Metabase transforms.
"""

from __future__ import annotations

import fnmatch
import json
import logging
from typing import Dict, List, Optional

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

    def run(self):
        # type: () -> MigrationPlan
        logger.info("=" * 60)
        logger.info("dbt -> Metabase Transforms Migration")
        logger.info("=" * 60)

        logger.info("Step 1/5: Parsing dbt project from GitHub...")
        self.project = self.parser.parse()

        logger.info("Step 2/5: Resolving model dependencies...")
        resolver = DependencyResolver(self.project)
        try:
            execution_order = resolver.resolve(exclude_ephemeral=True)
        except CyclicDependencyError as e:
            raise MigrationError("Cannot migrate: {}".format(e))

        layers = resolver.get_execution_layers()
        logger.info(
            "Execution plan: %d models in %d layers",
            len(execution_order), len(layers),
        )

        logger.info("Step 3/5: Building migration plan...")
        self.plan = self._build_plan(execution_order)
        self._log_plan_summary()

        if self.config.dry_run:
            logger.info("DRY RUN -- skipping execution. Plan saved.")
            return self.plan

        logger.info("Step 4/5: Executing migration plan...")
        self._execute_plan()

        if self.config.remote_sync.enabled:
            logger.info("Step 5/5: Triggering Metabase remote sync...")
            self._trigger_remote_sync()
        else:
            logger.info("Step 5/5: Remote sync not enabled, skipping.")

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
                checkpoint_column=model.config.get("unique_key"),
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
                )
                transform.transform_id = result["id"]

                if transform.is_incremental and transform.checkpoint_column:
                    try:
                        self.client.set_transform_incremental(
                            transform.transform_id,
                            transform.checkpoint_column,
                        )
                    except MetabaseApiError as e:
                        self.plan.warnings.append(
                            "[{}] Failed to set incremental: {}".format(transform.name, e)
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
