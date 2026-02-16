"""
Rewrite dbt Jinja-templated SQL into plain SQL compatible with Metabase transforms.
"""

from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional, Tuple

from .dbt_parser import (
    CONFIG_PATTERN,
    INCREMENTAL_BLOCK_PATTERN,
    JINJA_BLOCK_PATTERN,
    JINJA_COMMENT_PATTERN,
    JINJA_EXPR_PATTERN,
    REF_PATTERN,
    SOURCE_PATTERN,
    VAR_PATTERN,
)
from .models import DbtModel, DbtProject

logger = logging.getLogger(__name__)


class SqlRewriter:
    def __init__(self, project, default_schema="analytics", schema_overrides=None):
        # type: (DbtProject, str, Optional[Dict[str, str]]) -> None
        self.project = project
        self.default_schema = default_schema
        self.schema_overrides = schema_overrides or {}
        self._model_lookup = {
            m.name: m for m in project.models.values()
        }  # type: Dict[str, DbtModel]

    def rewrite(self, model):
        # type: (DbtModel) -> Tuple[str, List[str]]
        sql = model.raw_sql
        warnings = []  # type: List[str]

        # 1. Strip Jinja comments
        sql = JINJA_COMMENT_PATTERN.sub("", sql)

        # 2. Strip {{ config(...) }}
        sql = CONFIG_PATTERN.sub("", sql)

        # 3. Replace {{ ref('model_name') }}
        sql = self._replace_refs(sql, model, warnings)

        # 4. Replace {{ source('source_name', 'table_name') }}
        sql = self._replace_sources(sql, model, warnings)

        # 5. Replace {{ var('name') }}
        sql = self._replace_vars(sql, warnings)

        # 6. Handle incremental blocks (uses a placeholder to survive cleanup)
        _CHECKPOINT_PLACEHOLDER = "__METABASE_CHECKPOINT__"
        sql = self._handle_incremental(sql, model, warnings)
        # Temporarily protect Metabase checkpoint syntax from Jinja cleanup
        sql = sql.replace("{{checkpoint}}", _CHECKPOINT_PLACEHOLDER)

        # 7. Strip any remaining Jinja blocks
        remaining_blocks = JINJA_BLOCK_PATTERN.findall(sql)
        if remaining_blocks:
            for block in remaining_blocks:
                warnings.append(
                    "Unsupported Jinja block stripped: {}...".format(block[:80])
                )
            sql = JINJA_BLOCK_PATTERN.sub("", sql)

        remaining_exprs = JINJA_EXPR_PATTERN.findall(sql)
        if remaining_exprs:
            for expr in remaining_exprs:
                warnings.append(
                    "Unsupported Jinja expression stripped: {}...".format(expr[:80])
                )
            sql = JINJA_EXPR_PATTERN.sub("", sql)

        # 8. Clean up whitespace
        sql = self._clean_whitespace(sql)

        # 9. Restore Metabase checkpoint syntax
        sql = sql.replace(_CHECKPOINT_PLACEHOLDER, "{{checkpoint}}")

        return sql, warnings

    def _replace_refs(self, sql, model, warnings):
        # type: (str, DbtModel, List[str]) -> str
        def replacer(match):
            # type: (re.Match) -> str
            ref_name = match.group(1)
            target = self._model_lookup.get(ref_name)
            if target:
                schema = self._resolve_schema(target)
                return "{}.{}".format(schema, ref_name)
            else:
                warnings.append(
                    "ref('{}') target not found in project; "
                    "using default schema: {}.{}".format(
                        ref_name, self.default_schema, ref_name
                    )
                )
                return "{}.{}".format(self.default_schema, ref_name)

        return REF_PATTERN.sub(replacer, sql)

    def _replace_sources(self, sql, model, warnings):
        # type: (str, DbtModel, List[str]) -> str
        def replacer(match):
            # type: (re.Match) -> str
            source_name = match.group(1)
            table_name = match.group(2)
            source = self.project.sources.get(source_name)
            if source:
                schema = source.schema_name
                resolved_table = table_name
                for st in source.tables:
                    if st.name == table_name:
                        resolved_table = st.resolved_name
                        break
                return "{}.{}".format(schema, resolved_table)
            else:
                warnings.append(
                    "source('{}', '{}') not found; using source name as schema".format(
                        source_name, table_name
                    )
                )
                return "{}.{}".format(source_name, table_name)

        return SOURCE_PATTERN.sub(replacer, sql)

    def _replace_vars(self, sql, warnings):
        # type: (str, List[str]) -> str
        def replacer(match):
            # type: (re.Match) -> str
            var_name = match.group(1)
            default = match.group(2) if match.group(2) else None
            val = self.project.vars.get(var_name, default)
            if val is None:
                warnings.append(
                    "var('{}') has no value and no default; replaced with NULL".format(var_name)
                )
                return "NULL"
            if isinstance(val, str):
                return "'{}'".format(val)
            return str(val)

        return VAR_PATTERN.sub(replacer, sql)

    def _handle_incremental(self, sql, model, warnings):
        # type: (str, DbtModel, List[str]) -> str
        if not model.is_incremental:
            sql = INCREMENTAL_BLOCK_PATTERN.sub("", sql)
            return sql

        match = INCREMENTAL_BLOCK_PATTERN.search(sql)
        if not match:
            warnings.append(
                "Model is marked incremental but no is_incremental() block found"
            )
            return sql

        inc_block = match.group(1).strip()

        checkpoint_pattern = re.compile(
            r"(?:WHERE|AND)\s+(\w+)\s*([>>=]+)\s*\("
            r"SELECT\s+(?:MAX|max)\((\w+)\)\s+FROM\s+\{\{\s*this\s*\}\}"
            r"\)",
            re.IGNORECASE,
        )
        cp_match = checkpoint_pattern.search(inc_block)

        if cp_match:
            col = cp_match.group(1)
            cast_suffix = ""
            if any(
                hint in col.lower()
                for hint in ["_at", "date", "time", "created", "updated", "timestamp"]
            ):
                cast_suffix = "::timestamp"

            metabase_clause = "[[WHERE {} > {{{{checkpoint}}}}{}]]".format(col, cast_suffix)
            sql = INCREMENTAL_BLOCK_PATTERN.sub(metabase_clause, sql)
        else:
            warnings.append(
                "Could not auto-convert incremental block: {}... "
                "Please manually add Metabase checkpoint syntax.".format(inc_block[:100])
            )
            sql = INCREMENTAL_BLOCK_PATTERN.sub(
                "\n-- TODO: Convert to Metabase incremental syntax:\n"
                "-- Original: {}\n"
                "-- Metabase syntax: [[WHERE col > {{{{checkpoint}}}}]]\n".format(inc_block),
                sql,
            )

        this_pattern = re.compile(r"\{\{\s*this\s*\}\}")
        if this_pattern.search(sql):
            schema = self._resolve_schema(model)
            sql = this_pattern.sub("{}.{}".format(schema, model.name), sql)

        return sql

    def _resolve_schema(self, model):
        # type: (DbtModel) -> str
        if model.folder in self.schema_overrides:
            return self.schema_overrides[model.folder]
        if model.schema_name:
            return model.schema_name
        return self.default_schema

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
