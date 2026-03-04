"""
Core tests for the refactored dbt-to-transforms pipeline.

Tests the manifest parser, transform SQL adapter, and dbt compiler
configuration logic.
"""

import json
import os
import tempfile

import pytest

from dbt_to_metabase.config import (
    DbtProjectConfig,
    MigrationConfig,
    CheckpointColumnMapping,
)
from dbt_to_metabase.manifest_parser import ManifestParser
from dbt_to_metabase.transform_sql_adapter import TransformSqlAdapter
from dbt_to_metabase.models import DbtMaterialization, DbtModel, DbtProject


# ── Fixtures ──────────────────────────────────────────────────────────

SAMPLE_MANIFEST = {
    "metadata": {
        "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v11.json",
        "dbt_version": "1.7.0",
        "project_name": "jaffle_shop",
    },
    "nodes": {
        "model.jaffle_shop.stg_orders": {
            "unique_id": "model.jaffle_shop.stg_orders",
            "resource_type": "model",
            "name": "stg_orders",
            "schema": "staging",
            "database": "warehouse",
            "alias": "stg_orders",
            "original_file_path": "models/staging/stg_orders.sql",
            "path": "staging/stg_orders.sql",
            "raw_code": "SELECT * FROM {{ source('raw', 'orders') }}",
            "compiled_code": 'SELECT * FROM "raw"."orders"',
            "description": "Staged orders",
            "tags": ["daily"],
            "config": {"materialized": "view"},
            "columns": {
                "order_id": {"name": "order_id", "description": "Primary key"},
                "customer_id": {"name": "customer_id", "description": "FK to customers"},
            },
            "depends_on": {
                "nodes": ["source.jaffle_shop.raw.orders"],
            },
        },
        "model.jaffle_shop.fct_orders": {
            "unique_id": "model.jaffle_shop.fct_orders",
            "resource_type": "model",
            "name": "fct_orders",
            "schema": "marts",
            "database": "warehouse",
            "alias": "fct_orders",
            "original_file_path": "models/marts/fct_orders.sql",
            "path": "marts/fct_orders.sql",
            "raw_code": "SELECT * FROM {{ ref('stg_orders') }}",
            "compiled_code": 'SELECT * FROM "staging"."stg_orders"',
            "description": "Fact table for orders",
            "tags": ["daily", "finance"],
            "config": {"materialized": "table"},
            "columns": {},
            "depends_on": {
                "nodes": ["model.jaffle_shop.stg_orders"],
            },
        },
        "model.jaffle_shop.fct_events": {
            "unique_id": "model.jaffle_shop.fct_events",
            "resource_type": "model",
            "name": "fct_events",
            "schema": "marts",
            "database": "warehouse",
            "alias": "fct_events",
            "original_file_path": "models/marts/fct_events.sql",
            "path": "marts/fct_events.sql",
            "raw_code": "SELECT * FROM {{ source('raw', 'events') }}",
            "compiled_code": 'SELECT * FROM "raw"."events"',
            "description": "Incremental events",
            "tags": [],
            "config": {"materialized": "incremental"},
            "columns": {},
            "depends_on": {
                "nodes": ["source.jaffle_shop.raw.events"],
            },
        },
        "seed.jaffle_shop.raw_payments": {
            "unique_id": "seed.jaffle_shop.raw_payments",
            "resource_type": "seed",
            "name": "raw_payments",
            "schema": "seeds",
            "database": "warehouse",
            "alias": "raw_payments",
            "original_file_path": "seeds/raw_payments.csv",
            "path": "raw_payments.csv",
            "raw_code": "",
            "compiled_code": "",
            "description": "Payment seed data",
            "tags": [],
            "config": {"materialized": "seed"},
            "columns": {},
            "depends_on": {"nodes": []},
        },
    },
    "sources": {
        "source.jaffle_shop.raw.orders": {
            "unique_id": "source.jaffle_shop.raw.orders",
            "source_name": "raw",
            "name": "orders",
            "schema": "raw",
            "database": "warehouse",
            "identifier": "orders",
        },
        "source.jaffle_shop.raw.events": {
            "unique_id": "source.jaffle_shop.raw.events",
            "source_name": "raw",
            "name": "events",
            "schema": "raw",
            "database": "warehouse",
            "identifier": "events",
        },
    },
}


@pytest.fixture
def manifest_file():
    """Write the sample manifest to a temp file and yield its path."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as f:
        json.dump(SAMPLE_MANIFEST, f)
        path = f.name
    yield path
    os.unlink(path)


@pytest.fixture
def parsed_project(manifest_file):
    parser = ManifestParser(manifest_file)
    return parser.parse()


# ── ManifestParser tests ──────────────────────────────────────────────

class TestManifestParser:
    def test_parse_project_metadata(self, parsed_project):
        assert parsed_project.name == "jaffle_shop"
        assert parsed_project.version == "1.7.0"

    def test_parse_models(self, parsed_project):
        model_names = {m.name for m in parsed_project.models.values()}
        assert "stg_orders" in model_names
        assert "fct_orders" in model_names
        assert "fct_events" in model_names

    def test_parse_seeds(self, parsed_project):
        seed_names = {
            m.name
            for m in parsed_project.models.values()
            if m.config.get("is_seed")
        }
        assert "raw_payments" in seed_names

    def test_parse_sources(self, parsed_project):
        assert "raw" in parsed_project.sources
        table_names = {t.name for t in parsed_project.sources["raw"].tables}
        assert "orders" in table_names
        assert "events" in table_names

    def test_compiled_sql_populated(self, parsed_project):
        models = {m.name: m for m in parsed_project.models.values()}
        assert models["stg_orders"].compiled_sql == 'SELECT * FROM "raw"."orders"'
        assert models["fct_orders"].compiled_sql == 'SELECT * FROM "staging"."stg_orders"'

    def test_materialization_parsed(self, parsed_project):
        models = {m.name: m for m in parsed_project.models.values()}
        assert models["stg_orders"].materialization == DbtMaterialization.VIEW
        assert models["fct_orders"].materialization == DbtMaterialization.TABLE
        assert models["fct_events"].materialization == DbtMaterialization.INCREMENTAL

    def test_dependencies_resolved(self, parsed_project):
        models = {m.name: m for m in parsed_project.models.values()}
        assert "stg_orders" in models["fct_orders"].depends_on_models

    def test_columns_parsed(self, parsed_project):
        models = {m.name: m for m in parsed_project.models.values()}
        col_names = {c.name for c in models["stg_orders"].columns}
        assert "order_id" in col_names
        assert "customer_id" in col_names

    def test_folder_extracted(self, parsed_project):
        models = {m.name: m for m in parsed_project.models.values()}
        assert models["stg_orders"].folder == "staging"
        assert models["fct_orders"].folder == "marts"

    def test_schema_from_manifest(self, parsed_project):
        models = {m.name: m for m in parsed_project.models.values()}
        assert models["stg_orders"].schema_name == "staging"
        assert models["fct_orders"].schema_name == "marts"

    def test_tags_parsed(self, parsed_project):
        models = {m.name: m for m in parsed_project.models.values()}
        assert "daily" in models["stg_orders"].tags
        assert "finance" in models["fct_orders"].tags


# ── TransformSqlAdapter tests ────────────────────────────────────────

class TestTransformSqlAdapter:
    def test_remap_quoted_schema(self, parsed_project):
        adapter = TransformSqlAdapter(
            parsed_project,
            schema_remap={"staging": "transforms_staging"},
            default_schema="transforms_analytics",
            transform_schema_prefix="transforms_",
        )
        models = {m.name: m for m in parsed_project.models.values()}
        sql, warnings = adapter.adapt(models["fct_orders"])
        assert "transforms_staging" in sql
        assert '"staging"' not in sql

    def test_remap_unquoted_schema(self, parsed_project):
        # Modify compiled_sql to use unquoted schema
        models = {m.name: m for m in parsed_project.models.values()}
        models["fct_orders"].compiled_sql = "SELECT * FROM staging.stg_orders"

        adapter = TransformSqlAdapter(
            parsed_project,
            schema_remap={"staging": "transforms_staging"},
            default_schema="transforms_analytics",
            transform_schema_prefix="transforms_",
        )
        sql, warnings = adapter.adapt(models["fct_orders"])
        assert "transforms_staging.stg_orders" in sql

    def test_no_remap_when_empty(self, parsed_project):
        adapter = TransformSqlAdapter(
            parsed_project,
            schema_remap={},
            default_schema="transforms_analytics",
            transform_schema_prefix="transforms_",
        )
        models = {m.name: m for m in parsed_project.models.values()}
        sql, warnings = adapter.adapt(models["stg_orders"])
        assert sql == 'SELECT * FROM "raw"."orders"'

    def test_empty_compiled_sql_passthrough(self, parsed_project):
        models = {m.name: m for m in parsed_project.models.values()}
        models["stg_orders"].compiled_sql = ""

        adapter = TransformSqlAdapter(
            parsed_project,
            schema_remap={},
            default_schema="transforms_analytics",
            transform_schema_prefix="transforms_",
        )
        sql, warnings = adapter.adapt(models["stg_orders"])
        assert "SELECT * FROM" in sql
        assert len(warnings) == 1
        assert "passthrough" in warnings[0].lower()


# ── Config tests ──────────────────────────────────────────────────────

class TestConfig:
    def test_dbt_config_inline_credentials(self):
        cfg = DbtProjectConfig(
            db_host="localhost",
            db_user="user",
            db_name="mydb",
        )
        assert cfg.has_inline_credentials is True

    def test_dbt_config_no_credentials(self):
        cfg = DbtProjectConfig()
        assert cfg.has_inline_credentials is False

    def test_dbt_config_needs_compilation(self):
        cfg = DbtProjectConfig(project_path="/some/path")
        assert cfg.needs_compilation is True

        cfg2 = DbtProjectConfig(manifest_path="/some/manifest.json")
        assert cfg2.needs_compilation is False

    def test_config_from_yaml_new_format(self, tmp_path):
        config_yaml = tmp_path / "config.yaml"
        config_yaml.write_text("""
dbt:
  project_path: "/my/project"
  target: "dev"
  db_type: "postgres"
  db_host: "localhost"
  db_user: "user"
  db_name: "db"
metabase:
  url: "http://localhost:3000"
  api_key: "test"
checkpoint_columns:
  - model: "fct_orders"
    column: "updated_at"
schema_remap:
  staging: transforms_staging
""")
        config = MigrationConfig.from_yaml(str(config_yaml))
        assert config.dbt.project_path == "/my/project"
        assert config.dbt.target == "dev"
        assert len(config.checkpoint_columns) == 1
        assert config.checkpoint_columns[0].model == "fct_orders"
        assert config.schema_remap == {"staging": "transforms_staging"}

    def test_config_backward_compat_github_key(self, tmp_path):
        config_yaml = tmp_path / "config.yaml"
        config_yaml.write_text("""
github:
  repo: "org/repo"
  branch: "main"
  subdirectory: "dbt_project"
metabase:
  url: "http://localhost:3000"
  api_key: "test"
""")
        config = MigrationConfig.from_yaml(str(config_yaml))
        assert config.dbt.github_repo == "org/repo"
        assert config.dbt.github_branch == "main"
        assert config.dbt.github_subdirectory == "dbt_project"
