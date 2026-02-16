"""Tests for dbt-to-metabase core logic."""

from dbt_to_metabase.dependency_resolver import CyclicDependencyError, DependencyResolver
from dbt_to_metabase.models import (
    DbtMaterialization,
    DbtModel,
    DbtProject,
    DbtSource,
    DbtSourceTable,
)
from dbt_to_metabase.sql_rewriter import SqlRewriter


def make_model(name, sql, materialization="table", deps=None, sources=None, folder=""):
    return DbtModel(
        unique_id="model.test.{}".format(name),
        name=name,
        path="models/{}/{}.sql".format(folder, name) if folder else "models/{}.sql".format(name),
        raw_sql=sql,
        materialization=DbtMaterialization(materialization),
        schema_name="analytics",
        depends_on_models=deps or [],
        depends_on_sources=sources or [],
        folder=folder,
    )


def make_project(models=None, sources=None, vars=None):
    project = DbtProject(
        name="test",
        version="1.0.0",
        profile="test",
        model_paths=["models"],
        target_schema="public",
        vars=vars or {},
    )
    for m in (models or []):
        project.models[m.unique_id] = m
    for s in (sources or []):
        project.sources[s.name] = s
    return project
