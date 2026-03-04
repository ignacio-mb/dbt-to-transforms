"""
Configuration model and loader for dbt-to-metabase migration.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml


@dataclass
class DbtProjectConfig:
    """Configuration for locating and compiling the dbt project.

    Supports three input modes:
      A) Local path  — project_path points to a checked-out dbt project.
      B) GitHub repo — github_repo is cloned to a temp directory.
      C) Pre-built manifest — manifest_path points to an existing manifest.json
         (skips clone + compile entirely).

    When compiling (modes A and B), database credentials are required so that
    ``dbt compile`` can resolve introspective queries.  Credentials can be
    supplied inline (db_* fields) or by pointing to an existing profiles.yml.
    """

    # --- Source: pick one ---
    project_path: Optional[str] = None
    github_repo: Optional[str] = None
    github_branch: str = "main"
    github_token: Optional[str] = None
    github_subdirectory: str = ""
    manifest_path: Optional[str] = None

    # --- dbt compile settings ---
    target: str = "prod"
    profiles_dir: Optional[str] = None

    # --- Inline database credentials (used to generate profiles.yml) ---
    db_type: str = "postgres"
    db_host: Optional[str] = None
    db_port: int = 5432
    db_user: Optional[str] = None
    db_password: Optional[str] = None
    db_name: Optional[str] = None
    db_schema: Optional[str] = None
    db_threads: int = 4

    def __post_init__(self):
        # type: () -> None
        if not self.github_token:
            self.github_token = os.environ.get("GITHUB_TOKEN")
        if not self.db_password:
            self.db_password = os.environ.get("DBT_DB_PASSWORD")
        if not self.db_user:
            self.db_user = os.environ.get("DBT_DB_USER")
        if not self.db_host:
            self.db_host = os.environ.get("DBT_DB_HOST")
        if not self.db_name:
            self.db_name = os.environ.get("DBT_DB_NAME")

    @property
    def has_inline_credentials(self) -> bool:
        return bool(self.db_host and self.db_user and self.db_name)

    @property
    def needs_compilation(self) -> bool:
        return self.manifest_path is None


@dataclass
class MetabaseConfig:
    url: str
    username: Optional[str] = None
    password: Optional[str] = None
    api_key: Optional[str] = None
    database_id: int = 1
    default_schema: str = "analytics"

    def __post_init__(self):
        # type: () -> None
        self.url = self.url.rstrip("/")
        if not self.api_key:
            self.api_key = os.environ.get("METABASE_API_KEY")
        if not self.username:
            self.username = os.environ.get("METABASE_USERNAME")
        if not self.password:
            self.password = os.environ.get("METABASE_PASSWORD")


@dataclass
class RemoteSyncConfig:
    enabled: bool = False
    github_repo: Optional[str] = None
    branch: str = "main"


@dataclass
class TagMapping:
    dbt_tag: str
    metabase_tag: str


@dataclass
class JobDefinition:
    name: str
    schedule: str
    tags: List[str] = field(default_factory=list)


@dataclass
class SchemaMapping:
    dbt_folder_pattern: str
    metabase_schema: str


@dataclass
class CheckpointColumnMapping:
    """Maps a dbt model name to its Metabase checkpoint column.

    Metabase incremental (query) transforms use a checkpoint column to track
    which rows have already been processed.  Since ``dbt compile`` evaluates
    ``is_incremental()`` as False, the incremental block is compiled away and
    the checkpoint column cannot be auto-detected.  Users must declare these
    mappings explicitly.
    """
    model: str
    column: str


@dataclass
class MigrationConfig:
    dbt: DbtProjectConfig
    metabase: MetabaseConfig
    remote_sync: RemoteSyncConfig = field(default_factory=RemoteSyncConfig)
    schema_mappings: List[SchemaMapping] = field(default_factory=list)
    tag_mappings: List[TagMapping] = field(default_factory=list)
    default_tags: List[str] = field(default_factory=lambda: ["daily"])
    jobs: List[JobDefinition] = field(default_factory=list)
    folder_prefix: str = "dbt"
    preserve_folder_structure: bool = True
    dry_run: bool = False
    include_models: List[str] = field(default_factory=list)
    exclude_models: List[str] = field(default_factory=list)
    on_conflict: str = "skip"
    remap_schema_map: Dict[str, str] = field(default_factory=dict)
    transform_schema_prefix: str = "transforms_"
    checkpoint_columns: List[CheckpointColumnMapping] = field(default_factory=list)
    schema_remap: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_yaml(cls, path):
        # type: (Union[str, Path]) -> MigrationConfig
        with open(str(path)) as f:
            raw = yaml.safe_load(f)  # type: Dict[str, Any]

        # Support both old "github:" key and new "dbt:" key
        if "dbt" in raw:
            dbt = DbtProjectConfig(**raw["dbt"])
        elif "github" in raw:
            # Backward compatibility: map old github config to new dbt config
            gh = raw["github"]
            dbt = DbtProjectConfig(
                github_repo=gh.get("repo"),
                github_branch=gh.get("branch", "main"),
                github_token=gh.get("token"),
                github_subdirectory=gh.get("subdirectory", ""),
            )
        else:
            dbt = DbtProjectConfig()

        metabase = MetabaseConfig(**raw.get("metabase", {}))
        remote_sync = RemoteSyncConfig(**raw.get("remote_sync", {}))

        schema_mappings = [
            SchemaMapping(**m) for m in raw.get("schema_mappings", [])
        ]
        tag_mappings = [TagMapping(**m) for m in raw.get("tag_mappings", [])]
        jobs = [JobDefinition(**j) for j in raw.get("jobs", [])]
        checkpoint_columns = [
            CheckpointColumnMapping(**c) for c in raw.get("checkpoint_columns", [])
        ]

        return cls(
            dbt=dbt,
            metabase=metabase,
            remote_sync=remote_sync,
            schema_mappings=schema_mappings,
            tag_mappings=tag_mappings,
            default_tags=raw.get("default_tags", ["daily"]),
            jobs=jobs,
            folder_prefix=raw.get("folder_prefix", "dbt"),
            preserve_folder_structure=raw.get("preserve_folder_structure", True),
            dry_run=raw.get("dry_run", False),
            include_models=raw.get("include_models", []),
            exclude_models=raw.get("exclude_models", []),
            on_conflict=raw.get("on_conflict", "skip"),
            remap_schema_map=raw.get("remap_schema_map", {}),
            transform_schema_prefix=raw.get("transform_schema_prefix", "transforms_"),
            checkpoint_columns=checkpoint_columns,
            schema_remap=raw.get("schema_remap", {}),
        )
