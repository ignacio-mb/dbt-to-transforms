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
class GitHubConfig:
    repo: str  # "org/repo"
    branch: str = "main"
    token: Optional[str] = None
    subdirectory: str = ""

    def __post_init__(self):
        # type: () -> None
        if not self.token:
            self.token = os.environ.get("GITHUB_TOKEN")

    @property
    def api_base(self) -> str:
        return "https://api.github.com/repos/{}".format(self.repo)


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
class MigrationConfig:
    github: GitHubConfig
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

    @classmethod
    def from_yaml(cls, path):
        # type: (Union[str, Path]) -> MigrationConfig
        with open(str(path)) as f:
            raw = yaml.safe_load(f)  # type: Dict[str, Any]

        github = GitHubConfig(**raw.get("github", {}))
        metabase = MetabaseConfig(**raw.get("metabase", {}))
        remote_sync = RemoteSyncConfig(**raw.get("remote_sync", {}))

        schema_mappings = [
            SchemaMapping(**m) for m in raw.get("schema_mappings", [])
        ]
        tag_mappings = [TagMapping(**m) for m in raw.get("tag_mappings", [])]
        jobs = [JobDefinition(**j) for j in raw.get("jobs", [])]

        return cls(
            github=github,
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
        )
