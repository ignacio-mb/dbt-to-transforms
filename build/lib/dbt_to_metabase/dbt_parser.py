"""
Parse a dbt project from a GitHub repository.
"""

from __future__ import annotations

import base64
import logging
import re
from pathlib import PurePosixPath
from typing import Any, Dict, List, Optional, Tuple

import requests
import yaml

from .config import GitHubConfig
from .models import (
    DbtMaterialization,
    DbtModel,
    DbtModelColumn,
    DbtProject,
    DbtSource,
    DbtSourceTable,
)

logger = logging.getLogger(__name__)

REF_PATTERN = re.compile(
    r"""\{\{\s*ref\(\s*['"]([^'"]+)['"]\s*\)\s*\}\}"""
)
SOURCE_PATTERN = re.compile(
    r"""\{\{\s*source\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)\s*\}\}"""
)
CONFIG_PATTERN = re.compile(
    r"""\{\{\s*config\((.*?)\)\s*\}\}""", re.DOTALL
)
VAR_PATTERN = re.compile(
    r"""\{\{\s*var\(\s*['"]([^'"]+)['"](?:\s*,\s*['"]?([^'")]+)['"]?)?\s*\)\s*\}\}"""
)
INCREMENTAL_BLOCK_PATTERN = re.compile(
    r"""\{%[-\s]*if\s+is_incremental\(\)\s*[-\s]*%\}(.*?)\{%[-\s]*endif\s*[-\s]*%\}""",
    re.DOTALL,
)
JINJA_BLOCK_PATTERN = re.compile(r"\{%.*?%\}", re.DOTALL)
JINJA_EXPR_PATTERN = re.compile(r"\{\{.*?\}\}", re.DOTALL)
JINJA_COMMENT_PATTERN = re.compile(r"\{#.*?#\}", re.DOTALL)


def _removeprefix(s, prefix):
    # type: (str, str) -> str
    """str.removeprefix() backport for Python < 3.9."""
    if s.startswith(prefix):
        return s[len(prefix):]
    return s


class GitHubDbtParser:
    """Parses a dbt project directly from a GitHub repository via the API."""

    def __init__(self, config):
        # type: (GitHubConfig) -> None
        self.config = config
        self.session = requests.Session()
        if config.token:
            self.session.headers["Authorization"] = "Bearer {}".format(config.token)
        self.session.headers["Accept"] = "application/vnd.github.v3+json"
        self._file_cache = {}  # type: Dict[str, str]
        self._model_meta_cache = {}  # type: Dict[str, dict]
        self._project_model_configs = {}  # type: Dict[str, Any]

    # GitHub API helpers

    def _api_get(self, endpoint, params=None):
        # type: (str, Optional[dict]) -> Any
        url = "{}/{}".format(self.config.api_base, endpoint)
        resp = self.session.get(url, params=params or {})
        resp.raise_for_status()
        return resp.json()

    def _get_file_content(self, path):
        # type: (str) -> str
        if path in self._file_cache:
            return self._file_cache[path]
        full_path = (
            "{}/{}".format(self.config.subdirectory, path).lstrip("/")
            if self.config.subdirectory
            else path
        )
        data = self._api_get(
            "contents/{}".format(full_path), {"ref": self.config.branch}
        )
        content = base64.b64decode(data["content"]).decode("utf-8")
        self._file_cache[path] = content
        return content

    def _list_directory(self, path):
        # type: (str) -> List[dict]
        full_path = (
            "{}/{}".format(self.config.subdirectory, path).lstrip("/")
            if self.config.subdirectory
            else path
        )
        try:
            return self._api_get(
                "contents/{}".format(full_path), {"ref": self.config.branch}
            )
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                return []
            raise

    def _walk_directory(self, path, extension=".sql"):
        # type: (str, str) -> List[dict]
        results = []  # type: List[dict]
        items = self._list_directory(path)
        if not isinstance(items, list):
            return results
        for item in items:
            if item["type"] == "file" and item["name"].endswith(extension):
                results.append(item)
            elif item["type"] == "dir":
                rel = item["path"]
                if self.config.subdirectory:
                    rel = _removeprefix(rel, self.config.subdirectory + "/")
                results.extend(self._walk_directory(rel, extension))
        return results

    # Parsing entry point

    def parse(self):
        # type: () -> DbtProject
        logger.info("Parsing dbt project from %s (%s)", self.config.repo, self.config.branch)

        project = self._parse_project_yml()
        self._parse_schema_files(project)
        self._parse_model_files(project)
        self._resolve_dependencies(project)

        logger.info(
            "Parsed %d models and %d sources from dbt project '%s'",
            len(project.models),
            len(project.sources),
            project.name,
        )
        return project

    def _parse_project_yml(self):
        # type: () -> DbtProject
        content = self._get_file_content("dbt_project.yml")
        raw = yaml.safe_load(content)  # type: dict

        project = DbtProject(
            name=raw.get("name", "unknown"),
            version=str(raw.get("version", "0.0.0")),
            profile=raw.get("profile", "default"),
            model_paths=raw.get("model-paths", raw.get("source-paths", ["models"])),
            target_schema=self._extract_target_schema(raw),
            vars=raw.get("vars", {}),
        )

        self._project_model_configs = raw.get("models", {})
        return project

    def _extract_target_schema(self, raw):
        # type: (dict) -> str
        models_cfg = raw.get("models", {})
        project_name = raw.get("name", "")
        if project_name in models_cfg:
            return models_cfg[project_name].get("schema", "public")
        return "public"

    def _parse_schema_files(self, project):
        # type: (DbtProject) -> None
        for model_path in project.model_paths:
            yml_files = self._walk_directory(model_path, extension=".yml")
            yml_files.extend(self._walk_directory(model_path, extension=".yaml"))

            for yml_file in yml_files:
                rel_path = yml_file["path"]
                if self.config.subdirectory:
                    rel_path = _removeprefix(rel_path, self.config.subdirectory + "/")
                try:
                    content = self._get_file_content(rel_path)
                    parsed = yaml.safe_load(content)
                    if not isinstance(parsed, dict):
                        continue

                    for src in parsed.get("sources", []):
                        source = DbtSource(
                            name=src["name"],
                            schema_name=src.get("schema", src["name"]),
                            database=src.get("database"),
                            tables=[
                                DbtSourceTable(
                                    name=t["name"],
                                    identifier=t.get("identifier"),
                                )
                                for t in src.get("tables", [])
                            ],
                        )
                        project.sources[source.name] = source

                    for model_meta in parsed.get("models", []):
                        self._model_meta_cache[model_meta["name"]] = model_meta

                except Exception as e:
                    logger.warning("Failed to parse YAML %s: %s", rel_path, e)

    def _parse_model_files(self, project):
        # type: (DbtProject) -> None
        for model_path in project.model_paths:
            sql_files = self._walk_directory(model_path, extension=".sql")

            for sql_file in sql_files:
                rel_path = sql_file["path"]
                if self.config.subdirectory:
                    rel_path = _removeprefix(rel_path, self.config.subdirectory + "/")
                try:
                    raw_sql = self._get_file_content(rel_path)
                    model = self._parse_single_model(rel_path, raw_sql, project)
                    project.models[model.unique_id] = model
                except Exception as e:
                    logger.warning("Failed to parse model %s: %s", rel_path, e)

    def _parse_single_model(self, rel_path, raw_sql, project):
        # type: (str, str, DbtProject) -> DbtModel
        p = PurePosixPath(rel_path)
        model_name = p.stem

        folder = ""
        for mp in project.model_paths:
            if rel_path.startswith(mp + "/"):
                relative = rel_path[len(mp) + 1:]
                folder_path = PurePosixPath(relative).parent
                folder = str(folder_path) if str(folder_path) != "." else ""
                break

        unique_id = "model.{}.{}".format(project.name, model_name)

        config = self._extract_config(raw_sql)

        mat_str = config.get("materialized", "table")
        mat_str = self._get_project_config(folder, "materialized", default=mat_str)
        try:
            materialization = DbtMaterialization(mat_str)
        except ValueError:
            materialization = DbtMaterialization.TABLE

        schema_name = config.get("schema", None)
        if not schema_name:
            schema_name = self._get_project_config(
                folder, "schema", default=project.target_schema
            )

        tags = config.get("tags", [])
        if isinstance(tags, str):
            tags = [tags]
        project_tags = self._get_project_config(folder, "tags", default=[])
        if isinstance(project_tags, str):
            project_tags = [project_tags]
        tags = list(set(tags + project_tags))

        depends_on_models = REF_PATTERN.findall(raw_sql)
        depends_on_sources = SOURCE_PATTERN.findall(raw_sql)

        meta = self._model_meta_cache.get(model_name, {})
        description = meta.get("description", config.get("description", ""))
        columns = [
            DbtModelColumn(
                name=c["name"],
                description=c.get("description", ""),
                tests=[
                    (t if isinstance(t, str) else list(t.keys())[0])
                    for t in c.get("tests", [])
                ],
            )
            for c in meta.get("columns", [])
        ]

        return DbtModel(
            unique_id=unique_id,
            name=model_name,
            path=rel_path,
            raw_sql=raw_sql,
            materialization=materialization,
            schema_name=schema_name,
            database=config.get("database"),
            tags=tags,
            description=description,
            columns=columns,
            depends_on_models=depends_on_models,
            depends_on_sources=depends_on_sources,
            config=config,
            folder=folder,
        )

    def _extract_config(self, sql):
        # type: (str) -> Dict[str, Any]
        match = CONFIG_PATTERN.search(sql)
        if not match:
            return {}
        raw = match.group(1).strip()
        config = {}  # type: Dict[str, Any]
        kv_pattern = re.compile(
            r"""(\w+)\s*=\s*(?:'([^']*)'|"([^"]*)"|(\[.*?\])|(\w+))"""
        )
        for m in kv_pattern.finditer(raw):
            key = m.group(1)
            val = m.group(2) or m.group(3) or m.group(4) or m.group(5)
            if val and val.startswith("["):
                items = re.findall(r"""['"]([^'"]+)['"]""", val)
                config[key] = items
            elif val in ("True", "true"):
                config[key] = True
            elif val in ("False", "false"):
                config[key] = False
            else:
                config[key] = val
        return config

    def _get_project_config(self, folder, key, default=None):
        # type: (str, str, Any) -> Any
        configs = self._project_model_configs
        if not configs:
            return default

        parts = [p for p in folder.split("/") if p]
        current = configs
        for proj_key in current:
            if isinstance(current[proj_key], dict):
                sub = current[proj_key]
                val = sub.get(key, default)
                for part in parts:
                    if part in sub and isinstance(sub[part], dict):
                        sub = sub[part]
                        val = sub.get(key, val)
                return val
        return default

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
