"""
Data models representing dbt project entities and Metabase transform entities.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


# dbt-side models

class DbtMaterialization(enum.Enum):
    TABLE = "table"
    VIEW = "view"
    INCREMENTAL = "incremental"
    EPHEMERAL = "ephemeral"


@dataclass
class DbtSource:
    name: str
    schema_name: str
    database: Optional[str] = None
    tables: List["DbtSourceTable"] = field(default_factory=list)


@dataclass
class DbtSourceTable:
    name: str
    identifier: Optional[str] = None

    @property
    def resolved_name(self) -> str:
        return self.identifier or self.name


@dataclass
class DbtModelColumn:
    name: str
    description: str = ""
    tests: List[str] = field(default_factory=list)


@dataclass
class DbtModel:
    unique_id: str
    name: str
    path: str
    raw_sql: str
    materialization: DbtMaterialization = DbtMaterialization.TABLE
    schema_name: Optional[str] = None
    database: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    description: str = ""
    columns: List[DbtModelColumn] = field(default_factory=list)
    depends_on_models: List[str] = field(default_factory=list)
    depends_on_sources: List[Tuple[str, str]] = field(default_factory=list)
    config: Dict[str, Any] = field(default_factory=dict)
    folder: str = ""

    @property
    def is_ephemeral(self) -> bool:
        return self.materialization == DbtMaterialization.EPHEMERAL

    @property
    def is_incremental(self) -> bool:
        return self.materialization == DbtMaterialization.INCREMENTAL


@dataclass
class DbtProject:
    name: str
    version: str
    profile: str
    model_paths: List[str] = field(default_factory=list)
    target_schema: str = "public"
    models: Dict[str, DbtModel] = field(default_factory=dict)
    sources: Dict[str, DbtSource] = field(default_factory=dict)
    vars: Dict[str, Any] = field(default_factory=dict)


# Metabase-side models

@dataclass
class MetabaseTransform:
    name: str
    query: str
    database_id: int
    schema_name: str
    table_name: str
    description: str = ""
    folder: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    is_incremental: bool = False
    checkpoint_column: Optional[str] = None
    transform_id: Optional[int] = None
    dbt_model_unique_id: Optional[str] = None


@dataclass
class MetabaseJob:
    name: str
    schedule: str
    tags: List[str] = field(default_factory=list)
    job_id: Optional[int] = None


@dataclass
class MetabaseTag:
    name: str
    tag_id: Optional[int] = None


@dataclass
class MigrationPlan:
    transforms: List[MetabaseTransform] = field(default_factory=list)
    tags: List[MetabaseTag] = field(default_factory=list)
    jobs: List[MetabaseJob] = field(default_factory=list)
    execution_order: List[str] = field(default_factory=list)
    skipped_models: List[Tuple[str, str]] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
