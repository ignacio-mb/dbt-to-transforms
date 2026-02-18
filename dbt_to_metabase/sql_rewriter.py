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

# Maps dbt datepart strings to PostgreSQL interval literals
_DATEPART_TO_INTERVAL = {
    "day": "1 day",
    "week": "1 week",
    "month": "1 month",
    "quarter": "3 months",
    "year": "1 year",
    "hour": "1 hour",
    "minute": "1 minute",
    "second": "1 second",
}


def _split_macro_args(args_str):
    # type: (str) -> List[str]
    """Split macro args on top-level commas, respecting nested parens and quotes."""
    args = []
    depth = 0
    current = []
    in_single = False
    in_double = False
    for char in args_str:
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double
        elif not in_single and not in_double:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            elif char == "," and depth == 0:
                args.append("".join(current).strip())
                current = []
                continue
        current.append(char)
    if current:
        args.append("".join(current).strip())
    return [a for a in args if a]


def _strip_jinja_string(s):
    # type: (str) -> str
    """Strip outer Jinja-string quotes from a macro arg.
    'col_name'  -> col_name
    "sum(x)"    -> sum(x)
    sum(x)      -> sum(x)   (unquoted, left alone)
    """
    s = s.strip()
    if (s.startswith("'") and s.endswith("'")) or (
        s.startswith('"') and s.endswith('"')
    ):
        return s[1:-1]
    return s


def _eval_jinja_string_concat(s, project_vars=None):
    # type: (str, Optional[Dict[str, Any]]) -> str
    """Evaluate Jinja ~ string-concatenation and resolve bare var() calls.

    dbt macro args often build SQL strings using Jinja ~ concatenation:
        start_date="cast('" ~ var('start_date') ~ "' as date)"

    This is valid Jinja-in-macro-context syntax.  Note that var() here is
    NOT wrapped in {{ }}, so _replace_vars (which matches {{ var(...) }})
    never fires on it.  This function handles both steps:

    Step A — resolve bare var() calls using project_vars:
        var('start_date')  ->  '2020-01-01'   (single-quoted string literal)

    Step B — collapse ~ concatenation between adjacent string literals:
        "cast('" ~ '2020-01-01' ~ "' as date)"
            ->  "cast('2020-01-01' as date)"

    Result is a normal double-quoted string that the kwarg regex can parse.
    """
    # ── Step A: resolve bare var() calls ────────────────────────────────────
    if project_vars:
        bare_var_pat = re.compile(
            r"""var\(\s*['"]([^'"]+)['"]\s*(?:,\s*(?:['"]([^'"]*)['"]|([^\)]*)))?\s*\)""",
            re.DOTALL,
        )
        def var_replacer(mv):
            # type: (re.Match) -> str
            var_name = mv.group(1)
            default  = mv.group(2) if mv.group(2) is not None else mv.group(3)
            val = project_vars.get(var_name, default)
            if val is None:
                return mv.group(0)          # leave untouched — no value available
            return "'{}'".format(str(val))  # emit as single-quoted string literal
        s = bare_var_pat.sub(var_replacer, s)

    # ── Step B: collapse ~ between adjacent string literals ──────────────────
    tilde_pat = re.compile(
        r"""(?:'([^']*)'|"([^"]*)")\s*~\s*(?:'([^']*)'|"([^"]*)")""",
        re.DOTALL,
    )
    for _ in range(20):
        m = tilde_pat.search(s)
        if not m:
            break
        left  = m.group(1) if m.group(1) is not None else m.group(2)
        right = m.group(3) if m.group(3) is not None else m.group(4)
        combined = '"' + left + right + '"'
        s = s[:m.start()] + combined + s[m.end():]
    return s


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

        # 6. Handle incremental blocks
        sql = self._handle_incremental(sql, model, warnings)

        # 6.5  MACRO TRANSLATION — must run BEFORE steps 7/8 so known macros
        #      are expanded to real SQL instead of being nuked to NULL.
        sql = self._translate_macros(sql, model, warnings)

        # 7. Strip any remaining Jinja blocks  {% ... %}
        remaining_blocks = JINJA_BLOCK_PATTERN.findall(sql)
        if remaining_blocks:
            for block in remaining_blocks:
                warnings.append(
                    "Unsupported Jinja block stripped: {}...".format(block[:80])
                )
            sql = JINJA_BLOCK_PATTERN.sub("", sql)

        # 8. Replace remaining Jinja expressions  {{ ... }}  with NULL
        remaining_exprs = JINJA_EXPR_PATTERN.findall(sql)
        if remaining_exprs:
            for expr in remaining_exprs:
                warnings.append(
                    "Unsupported Jinja expression replaced with NULL: {}...".format(
                        expr[:80]
                    )
                )
            sql = JINJA_EXPR_PATTERN.sub("NULL", sql)

        # 8.5  Fix Python/BigQuery-style JSONB bracket access left after Jinja stripping.
        #      col['key']  ->  col->>'key'
        sql = self._fix_json_bracket_access(sql)

        # 9. Check if NULL replacement produced structurally broken SQL.
        if self._is_structurally_broken(sql):
            schema = self._resolve_schema(model)
            passthrough = "SELECT * FROM {}.{}".format(schema, model.name)
            warnings.append(
                "Model relies on untranslatable macros. "
                "Using passthrough query: {}".format(passthrough)
            )
            sql = passthrough

        # 10. Clean up whitespace
        sql = self._clean_whitespace(sql)

        return sql, warnings

    # ------------------------------------------------------------------
    # Step 6.5 — Macro translation
    # ------------------------------------------------------------------

    def _translate_macros(self, sql, model, warnings):
        # type: (str, DbtModel, List[str]) -> str
        """Translate known dbt / dbt_utils macros into native PostgreSQL."""
        sql = self._translate_days_between(sql, warnings)
        sql = self._translate_safe_divide(sql, warnings)
        sql = self._translate_date_spine(sql, warnings)
        return sql

    def _translate_days_between(self, sql, warnings):
        # type: (str, List[str]) -> str
        """
        {{ days_between('start_col', 'end_col') }}
            -> (end_col::date - start_col::date)

        PostgreSQL date subtraction returns INTEGER days.
        Args may be column names or any SQL date expression.
        """
        pattern = re.compile(
            r"\{\{\s*days_between\(\s*(.*?)\s*\)\s*\}\}", re.DOTALL
        )

        def replacer(m):
            # type: (re.Match) -> str
            args = _split_macro_args(m.group(1))
            if len(args) != 2:
                warnings.append(
                    "days_between: expected 2 args, got {}; replaced with NULL".format(
                        len(args)
                    )
                )
                return "NULL"
            start = _strip_jinja_string(args[0])
            end = _strip_jinja_string(args[1])
            return "({end}::date - {start}::date)".format(start=start, end=end)

        return pattern.sub(replacer, sql)

    def _translate_safe_divide(self, sql, warnings):
        # type: (str, List[str]) -> str
        """
        {{ safe_divide(numerator, denominator) }}
            -> (numerator)::numeric / NULLIF(denominator, 0)

        Both args may be Jinja string literals ('col') or raw SQL (sum(x)).
        If the denominator already contains NULLIF the outer wrap is redundant
        but valid: NULLIF(NULLIF(x,0), 0) == NULLIF(x, 0).
        """
        pattern = re.compile(
            r"\{\{\s*safe_divide\(\s*(.*?)\s*\)\s*\}\}", re.DOTALL
        )

        def replacer(m):
            # type: (re.Match) -> str
            args = _split_macro_args(m.group(1))
            if len(args) != 2:
                warnings.append(
                    "safe_divide: expected 2 args, got {}; replaced with NULL".format(
                        len(args)
                    )
                )
                return "NULL"
            num = _strip_jinja_string(args[0])
            den = _strip_jinja_string(args[1])
            return "({num})::numeric / NULLIF({den}, 0)".format(num=num, den=den)

        return pattern.sub(replacer, sql)

    def _translate_date_spine(self, sql, warnings):
        # type: (str, List[str]) -> str
        """
        {{ dbt_utils.date_spine(
               datepart="day",
               start_date="cast('2020-01-01' as date)",
               end_date="cast('2030-12-31' as date)"
           ) }}

        ->  SELECT generate_series(
                (cast('2020-01-01' as date))::timestamp,
                (cast('2030-12-31' as date))::timestamp,
                '1 day'::interval
            )::date AS date_day

        The output is a bare SELECT so it slots directly into a CTE body:
            with date_spine as (
                SELECT generate_series(...)::date AS date_day
            )
        which is valid PostgreSQL.
        """
        pattern = re.compile(
            r"\{\{\s*dbt_utils\.date_spine\(\s*(.*?)\s*\)\s*\}\}", re.DOTALL
        )

        def replacer(m):
            # type: (re.Match) -> str
            # Evaluate Jinja ~ string concatenation BEFORE parsing kwargs.
            # e.g. start_date="cast('" ~ '2020-01-01' ~ "' as date)"
            #   -> start_date="cast('2020-01-01' as date)"
            raw = _eval_jinja_string_concat(m.group(1), self.project.vars)
            # Parse  key="value"  or  key='value'  keyword args
            kw_pat = re.compile(
                r'(\w+)\s*=\s*(?:"([^"]*)"|\'([^\']*)\')', re.DOTALL
            )
            kwargs = {}  # type: Dict[str, str]
            for kw_m in kw_pat.finditer(raw):
                key = kw_m.group(1)
                val = (
                    kw_m.group(2)
                    if kw_m.group(2) is not None
                    else kw_m.group(3)
                )
                kwargs[key] = val.strip()

            datepart = kwargs.get("datepart", "day").lower()
            start_date = kwargs.get(
                "start_date", "current_date - interval '1 year'"
            )
            end_date = kwargs.get("end_date", "current_date")
            interval = _DATEPART_TO_INTERVAL.get(
                datepart, "1 {}".format(datepart)
            )

            return (
                "SELECT generate_series(\n"
                "        ({start})::timestamp,\n"
                "        ({end})::timestamp,\n"
                "        '{interval}'::interval\n"
                "    )::date AS date_day".format(
                    start=start_date, end=end_date, interval=interval
                )
            )

        return pattern.sub(replacer, sql)

    # ------------------------------------------------------------------
    # Step 8.5 — JSONB bracket access fix
    # ------------------------------------------------------------------

    def _fix_json_bracket_access(self, sql):
        # type: (str) -> str
        """
        Convert Python/BigQuery JSONB bracket access to PostgreSQL operators.
            col['key']   ->  col->>'key'   (returns text)
            col["key"]   ->  col->>'key'
        Integer index access (col[0]) is not remapped here.
        """
        sq = re.compile(r"(\b\w+(?:\.\w+)*)\['([^']+)'\]")
        dq = re.compile(r'(\b\w+(?:\.\w+)*)\["([^"]+)"\]')
        original = sql
        sql = sq.sub(lambda mo: mo.group(1) + "->>" + "'" + mo.group(2) + "'", sql)
        sql = dq.sub(lambda mo: mo.group(1) + "->>" + "'" + mo.group(2) + "'", sql)
        if sql != original:
            logger.info(
                "Fixed Python-style JSON bracket access -> PostgreSQL ->> operator"
            )
        return sql

    # ------------------------------------------------------------------
    # Structural-break detector
    # ------------------------------------------------------------------

    def _is_structurally_broken(self, sql):
        # type: (str) -> bool
        """Detect if NULL replacement left the SQL structurally invalid."""
        if re.search(r"\bas\b\s*\(\s*NULL\s*\)", sql, re.IGNORECASE | re.DOTALL):
            logger.warning(
                "_is_structurally_broken: NULL CTE body found. "
                "A macro was not translated. Add a handler in _translate_macros()."
            )
            return True
        stripped = sql.strip().rstrip(";").strip()
        if stripped.upper() == "NULL":
            return True
        if re.search(r"\bFROM\s+NULL\b", sql, re.IGNORECASE):
            return True
        return False

    # ------------------------------------------------------------------
    # Ref / source / var / incremental helpers (unchanged from original)
    # ------------------------------------------------------------------

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
                    "var('{}') has no value and no default; replaced with NULL".format(
                        var_name
                    )
                )
                return "NULL"
            if isinstance(val, str):
                return "'{}'".format(val)
            return str(val)

        return VAR_PATTERN.sub(replacer, sql)

    def _handle_incremental(self, sql, model, warnings):
        # type: (str, DbtModel, List[str]) -> str
        self._last_checkpoint_column = None

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
            r"(?:WHERE|AND)\s+([\w.]+)\s*[>>=]+\s*\("
            r"SELECT\s+(?:MAX|max)\(([\w.]+)\)\s+FROM\s+\{\{\s*this\s*\}\}"
            r"\)",
            re.IGNORECASE,
        )
        cp_match = checkpoint_pattern.search(inc_block)

        if cp_match:
            max_col = cp_match.group(2)
            col = max_col.split(".")[-1]
            self._last_checkpoint_column = col
            warnings.append(
                "Incremental checkpoint column '{}' detected. "
                "Will configure via Metabase API.".format(col)
            )
        else:
            warnings.append(
                "Could not detect checkpoint column from incremental block: {}... "
                "Set it manually in Metabase.".format(inc_block[:100])
            )

        sql = INCREMENTAL_BLOCK_PATTERN.sub("", sql)

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
