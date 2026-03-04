"""
Adapt dbt-compiled SQL for Metabase Transforms.

Since ``dbt compile`` has already resolved all Jinja (ref, source, var,
macros, conditionals) into plain SQL, the only remaining work is:

1. **Schema remapping** — rewrite schema references so that models point at
   their ``transforms_*`` schemas rather than the original dbt schemas.
2. **Whitespace cleanup** — collapse blank lines for readability.

This replaces the old ``SqlRewriter`` which contained ~460 lines of regex-based
Jinja rewriting.
"""

from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional, Tuple

from .models import DbtModel, DbtProject

logger = logging.getLogger(__name__)


class TransformSqlAdapter:
    """Remap schema references in compiled SQL for Metabase transforms."""

    def __init__(
        self,
        project,
        schema_remap,
        default_schema="analytics",
        transform_schema_prefix="transforms_",
    ):
        # type: (DbtProject, Dict[str, str], str, str) -> None
        self.project = project
        self.schema_remap = schema_remap
        self.default_schema = default_schema
        self.transform_schema_prefix = transform_schema_prefix
        self._model_lookup = {
            m.name: m for m in project.models.values()
        }  # type: Dict[str, DbtModel]

    def adapt(self, model):
        # type: (DbtModel) -> Tuple[str, List[str]]
        """Adapt a model's compiled SQL for Metabase transforms.

        Returns (sql, warnings).
        """
        sql = model.compiled_sql
        warnings = []  # type: List[str]

        if not sql or not sql.strip():
            # No compiled SQL available — emit a passthrough query
            schema = self._resolve_transform_schema(model)
            table = model.alias or model.name
            sql = "SELECT * FROM {}.{}".format(schema, table)
            warnings.append(
                "No compiled SQL available for '{}'; using passthrough query".format(
                    model.name
                )
            )
            return sql, warnings

        # Apply schema remapping
        sql = self._remap_schemas(sql, warnings)

        # Clean up whitespace
        sql = self._clean_whitespace(sql)

        return sql, warnings

    def _remap_schemas(self, sql, warnings):
        # type: (str, List[str]) -> str
        """Replace dbt schema references with transform schema references.

        Uses the user-provided schema_remap mapping.  Handles both quoted
        ("schema"."table") and unquoted (schema.table) patterns.
        """

        if not self.schema_remap:
            return sql

        for old_schema, new_schema in self.schema_remap.items():
            if old_schema == new_schema:
                continue

            # Pattern 1: "old_schema"."table" -> "new_schema"."table"
            quoted_pattern = re.compile(
                r'"{}"\s*\.'.format(re.escape(old_schema))
            )
            sql = quoted_pattern.sub('"{}".'.format(new_schema), sql)

            # Pattern 2: old_schema.table -> new_schema.table
            # (only when not inside quotes, preceded by word boundary or start)
            unquoted_pattern = re.compile(
                r'(?<!["\w]){}\.(?=["\w])'.format(re.escape(old_schema))
            )
            sql = unquoted_pattern.sub("{}.".format(new_schema), sql)

        return sql

    def _resolve_transform_schema(self, model):
        # type: (DbtModel) -> str
        """Resolve the transform schema for a model.

        Seeds are NOT remapped — they live in their original schema.
        """
        if model.config.get("is_seed"):
            return model.schema_name or "seeds"

        original_schema = model.schema_name or self.default_schema

        # Check the remap table
        if original_schema in self.schema_remap:
            return self.schema_remap[original_schema]

        # Default: prepend the transform prefix
        return "{}{}".format(self.transform_schema_prefix, original_schema)

    def _clean_whitespace(self, sql):
        # type: (str) -> str
        lines = sql.split("\n")
        cleaned = []  # type: List[str]
        prev_blank = False
        for line in lines:
            is_blank = not line.strip()
            if is_blank and prev_blank:
                continue
            cleaned.append(line)
            prev_blank = is_blank
        return "\n".join(cleaned).strip()
