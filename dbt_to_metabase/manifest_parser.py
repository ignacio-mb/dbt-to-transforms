"""
Parse a compiled dbt manifest.json into DbtProject models.

Replaces the old GitHubDbtParser that fetched raw files via the GitHub API and
parsed Jinja with regexes.  The manifest is dbt's own artifact, so all Jinja,
macros, ref()s, source()s, and config inheritance are already fully resolved.
"""

from __future__ import annotations

import json
import logging
from pathlib import PurePosixPath
from typing import Any, Dict, List, Optional, Tuple

from .models import (
    DbtMaterialization,
    DbtModel,
    DbtModelColumn,
    DbtProject,
    DbtSource,
    DbtSourceTable,
)

logger = logging.getLogger(__name__)


class ManifestParser:
    """Parse a dbt manifest.json file into a DbtProject."""

    def __init__(self, manifest_path):
        # type: (str) -> None
        self.manifest_path = manifest_path

    def parse(self):
        # type: () -> DbtProject
        logger.info("Parsing manifest: %s", self.manifest_path)

        with open(self.manifest_path) as f:
            manifest = json.load(f)

        project = self._extract_project(manifest)
        self._extract_models(manifest, project)
        self._extract_sources(manifest, project)
        self._extract_seeds(manifest, project)
        self._extract_snapshots(manifest, project)
        self._resolve_dependencies(project)

        seed_count = len([m for m in project.models.values() if m.config.get("is_seed")])
        snapshot_count = len([m for m in project.models.values() if m.config.get("is_snapshot")])
        model_count = len(project.models) - seed_count - snapshot_count
        logger.info(
            "Parsed %d models, %d seeds, %d snapshots, and %d sources from manifest (project '%s')",
            model_count,
            seed_count,
            snapshot_count,
            len(project.sources),
            project.name,
        )
        return project

    def _extract_project(self, manifest):
        # type: (dict) -> DbtProject
        metadata = manifest.get("metadata", {})
        project_name = metadata.get("project_name", metadata.get("project_id", "unknown"))

        # Try to extract vars from the metadata or first model's config
        project_vars = {}  # type: Dict[str, Any]

        # Infer target_schema from the first model node
        target_schema = "public"
        for node in manifest.get("nodes", {}).values():
            if node.get("resource_type") == "model":
                target_schema = node.get("schema", "public")
                break

        return DbtProject(
            name=project_name,
            version=metadata.get("dbt_version", "0.0.0"),
            profile="",
            model_paths=["models"],
            target_schema=target_schema,
            vars=project_vars,
        )

    def _extract_models(self, manifest, project):
        # type: (dict, DbtProject) -> None
        nodes = manifest.get("nodes", {})

        for node_id, node in nodes.items():
            if node.get("resource_type") != "model":
                continue

            # compiled_code (dbt >= 1.4) or compiled_sql (dbt < 1.4)
            compiled_sql = node.get("compiled_code") or node.get("compiled_sql") or ""
            raw_sql = node.get("raw_code") or node.get("raw_sql") or ""

            mat_str = node.get("config", {}).get("materialized", "table")
            try:
                materialization = DbtMaterialization(mat_str)
            except ValueError:
                materialization = DbtMaterialization.TABLE

            columns = self._parse_columns(node)
            depends_on_models = self._extract_model_deps(node)
            depends_on_sources = self._extract_source_deps(node)
            folder = self._extract_folder(node)

            model = DbtModel(
                unique_id=node["unique_id"],
                name=node["name"],
                path=node.get("original_file_path", node.get("path", "")),
                raw_sql=raw_sql,
                compiled_sql=compiled_sql,
                materialization=materialization,
                schema_name=node.get("schema"),
                database=node.get("database"),
                alias=node.get("alias"),
                tags=node.get("tags", []),
                description=node.get("description", ""),
                columns=columns,
                depends_on_models=depends_on_models,
                depends_on_sources=depends_on_sources,
                config=node.get("config", {}),
                folder=folder,
            )
            project.models[node["unique_id"]] = model

    def _extract_sources(self, manifest, project):
        # type: (dict, DbtProject) -> None
        sources = manifest.get("sources", {})

        # Group source nodes by source name
        source_groups = {}  # type: Dict[str, Dict]
        for source_id, source_node in sources.items():
            source_name = source_node.get("source_name", "")
            if source_name not in source_groups:
                source_groups[source_name] = {
                    "name": source_name,
                    "schema": source_node.get("schema", source_name),
                    "database": source_node.get("database"),
                    "tables": [],
                }
            source_groups[source_name]["tables"].append(
                DbtSourceTable(
                    name=source_node.get("name", ""),
                    identifier=source_node.get("identifier"),
                )
            )

        for name, data in source_groups.items():
            project.sources[name] = DbtSource(
                name=name,
                schema_name=data["schema"],
                database=data["database"],
                tables=data["tables"],
            )

    def _extract_seeds(self, manifest, project):
        # type: (dict, DbtProject) -> None
        nodes = manifest.get("nodes", {})

        for node_id, node in nodes.items():
            if node.get("resource_type") != "seed":
                continue

            # Don't overwrite if a model with the same name exists
            if any(m.name == node["name"] for m in project.models.values()):
                logger.debug("Seed '%s' shadowed by existing model, skipping", node["name"])
                continue

            seed_schema = node.get("schema", "seeds")

            model = DbtModel(
                unique_id=node["unique_id"],
                name=node["name"],
                path=node.get("original_file_path", node.get("path", "")),
                raw_sql="",
                compiled_sql="",
                materialization=DbtMaterialization.TABLE,
                schema_name=seed_schema,
                database=node.get("database"),
                alias=node.get("alias"),
                tags=node.get("tags", []),
                description=node.get("description", "dbt seed: {}".format(node["name"])),
                config={"is_seed": True},
                folder="",
            )
            project.models[node["unique_id"]] = model
            logger.info("Registered seed '%s' -> %s.%s", node["name"], seed_schema, node["name"])

    def _extract_snapshots(self, manifest, project):
        # type: (dict, DbtProject) -> None
        """Register dbt snapshots as read-only table references.

        Snapshots are SCD2 tables managed by ``dbt snapshot``.  They already
        exist in the database and should NOT be converted to Metabase
        transforms.  We register them in the dependency graph so that
        downstream models that ``ref('snapshot_xxx')`` can resolve correctly.
        """
        nodes = manifest.get("nodes", {})

        for node_id, node in nodes.items():
            if node.get("resource_type") != "snapshot":
                continue

            # Don't overwrite if a model with the same name exists
            if any(m.name == node["name"] for m in project.models.values()):
                logger.debug("Snapshot '%s' shadowed by existing model, skipping", node["name"])
                continue

            snapshot_schema = node.get("schema", "snapshots")

            model = DbtModel(
                unique_id=node["unique_id"],
                name=node["name"],
                path=node.get("original_file_path", node.get("path", "")),
                raw_sql=node.get("raw_sql", node.get("raw_code", "")),
                compiled_sql="",  # Not needed — we don't create a transform
                materialization=DbtMaterialization.TABLE,
                schema_name=snapshot_schema,
                database=node.get("database"),
                alias=node.get("alias"),
                tags=node.get("tags", []),
                description=node.get("description", "dbt snapshot: {}".format(node["name"])),
                config={"is_snapshot": True},
                folder="",
            )
            project.models[node["unique_id"]] = model
            logger.info(
                "Registered snapshot '%s' -> %s.%s (SCD2, managed by dbt)",
                node["name"], snapshot_schema, node["name"],
            )

    def _parse_columns(self, node):
        # type: (dict) -> List[DbtModelColumn]
        columns = []
        for col_name, col_data in node.get("columns", {}).items():
            tests = []
            for test in col_data.get("tests", []):
                if isinstance(test, str):
                    tests.append(test)
                elif isinstance(test, dict):
                    tests.append(list(test.keys())[0])
            columns.append(
                DbtModelColumn(
                    name=col_name,
                    description=col_data.get("description", ""),
                    tests=tests,
                )
            )
        return columns

    def _extract_model_deps(self, node):
        # type: (dict) -> List[str]
        """Extract model dependency names from depends_on.nodes."""
        deps = []
        for dep_id in node.get("depends_on", {}).get("nodes", []):
            # dep_id looks like "model.project.model_name", "seed.project.seed_name",
            # or "snapshot.project.snapshot_name"
            parts = dep_id.split(".")
            if len(parts) >= 3 and parts[0] in ("model", "seed", "snapshot"):
                deps.append(parts[-1])
        return deps

    def _extract_source_deps(self, node):
        # type: (dict) -> List[Tuple[str, str]]
        """Extract source dependencies as (source_name, table_name) tuples."""
        deps = []
        for dep_id in node.get("depends_on", {}).get("nodes", []):
            parts = dep_id.split(".")
            if len(parts) >= 3 and parts[0] == "source":
                # source.project_name.source_name
                source_name = parts[2] if len(parts) > 2 else parts[1]
                table_name = node.get("name", "")
                deps.append((source_name, table_name))
        return deps

    def _extract_folder(self, node):
        # type: (dict) -> str
        """Derive the dbt folder path for a model (e.g. 'staging', 'marts/finance')."""
        file_path = node.get("original_file_path", node.get("path", ""))
        if not file_path:
            return ""

        p = PurePosixPath(file_path)
        # Strip the top-level model directory (e.g. "models/")
        parts = list(p.parent.parts)
        if parts and parts[0] in ("models", "model"):
            parts = parts[1:]
        return "/".join(parts) if parts else ""

    def _resolve_dependencies(self, project):
        # type: (DbtProject) -> None
        model_names = {m.name for m in project.models.values()}
        for model in project.models.values():
            for dep in model.depends_on_models:
                if dep not in model_names:
                    logger.warning(
                        "Model '%s' references '%s' which was not found in the project",
                        model.name,
                        dep,
                    )