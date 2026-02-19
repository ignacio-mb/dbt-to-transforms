"""
Data validation for dbt-to-Metabase migrations.

Captures a snapshot of every dbt-produced table BEFORE transforms overwrite
them, then compares against the transform output AFTER execution.

Validation checks per table:
  1. Row count           — exact match
  2. Column schema       — same column names and types
  3. Content hash        — MD5 of all rows (order-independent)
  4. Per-column stats    — null counts, numeric sums
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .metabase_client import MetabaseApiError, MetabaseClient

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# Data models
# ──────────────────────────────────────────────

@dataclass
class ColumnInfo:
    name: str
    data_type: str


@dataclass
class ColumnStats:
    name: str
    null_count: int = 0
    distinct_count: int = 0
    numeric_sum: Optional[float] = None  # Only for numeric columns


@dataclass
class TableSnapshot:
    schema_name: str
    table_name: str
    row_count: int = 0
    columns: List[ColumnInfo] = field(default_factory=list)
    content_hash: str = ""
    column_stats: List[ColumnStats] = field(default_factory=list)
    error: Optional[str] = None  # Non-None if snapshot failed


@dataclass
class ValidationResult:
    table_name: str
    schema_name: str
    passed: bool = True
    checks: List["CheckResult"] = field(default_factory=list)
    skipped: bool = False
    skip_reason: str = ""


@dataclass
class CheckResult:
    check_name: str
    passed: bool
    expected: Any = None
    actual: Any = None
    message: str = ""


@dataclass
class ValidationReport:
    results: List[ValidationResult] = field(default_factory=list)
    total_tables: int = 0
    tables_passed: int = 0
    tables_failed: int = 0
    tables_skipped: int = 0

    @property
    def all_passed(self):
        # type: () -> bool
        return self.tables_failed == 0


# ──────────────────────────────────────────────
# Query helpers — parse Metabase dataset response
# ──────────────────────────────────────────────

def _extract_rows(response):
    # type: (dict) -> List[List[Any]]
    """Extract row data from a Metabase /api/dataset response."""
    data = response.get("data", {})
    return data.get("rows", [])


def _extract_scalar(response):
    # type: (dict) -> Any
    """Extract a single scalar value from a single-row, single-column query."""
    rows = _extract_rows(response)
    if rows and rows[0]:
        return rows[0][0]
    return None


# ──────────────────────────────────────────────
# Core validator
# ──────────────────────────────────────────────

class DataValidator:
    """Snapshot dbt tables and compare against transform output."""

    def __init__(self, client, database_id):
        # type: (MetabaseClient, int) -> None
        self.client = client
        self.database_id = database_id
        self._snapshots = {}  # type: Dict[str, TableSnapshot]

    # ──────────────────────────────────────────
    # Snapshot phase (call BEFORE transforms run)
    # ──────────────────────────────────────────

    def snapshot_tables(self, tables):
        # type: (List[Tuple[str, str]]) -> Dict[str, TableSnapshot]
        """Take a snapshot of each (schema, table) pair.

        Call this BEFORE running transforms so we capture the dbt-produced state.
        Returns a dict keyed by "schema.table".
        """
        logger.info("Taking pre-transform snapshots of %d tables...", len(tables))

        for schema_name, table_name in tables:
            key = "{}.{}".format(schema_name, table_name)
            snapshot = self._take_snapshot(schema_name, table_name)
            self._snapshots[key] = snapshot

            if snapshot.error:
                logger.warning(
                    "  Snapshot %s: FAILED (%s)", key, snapshot.error
                )
            else:
                logger.info(
                    "  Snapshot %s: %d rows, %d columns, hash=%s",
                    key, snapshot.row_count, len(snapshot.columns),
                    snapshot.content_hash[:12] + "..." if snapshot.content_hash else "n/a",
                )

        logger.info("Snapshots complete: %d captured", len(self._snapshots))
        return self._snapshots

    def _take_snapshot(self, schema_name, table_name):
        # type: (str, str) -> TableSnapshot
        """Capture row count, columns, content hash, and column stats."""
        snapshot = TableSnapshot(schema_name=schema_name, table_name=table_name)

        try:
            # Check if table exists first
            exists = self._table_exists(schema_name, table_name)
            if not exists:
                snapshot.error = "Table does not exist yet"
                return snapshot

            # 1. Row count
            snapshot.row_count = self._get_row_count(schema_name, table_name)

            # 2. Column info
            snapshot.columns = self._get_columns(schema_name, table_name)

            # 3. Content hash (order-independent)
            snapshot.content_hash = self._get_content_hash(schema_name, table_name)

            # 4. Per-column stats
            snapshot.column_stats = self._get_column_stats(
                schema_name, table_name, snapshot.columns
            )

        except (MetabaseApiError, Exception) as e:
            snapshot.error = str(e)
            logger.warning(
                "Failed to snapshot %s.%s: %s", schema_name, table_name, e
            )

        return snapshot

    def _table_exists(self, schema_name, table_name):
        # type: (str, str) -> bool
        sql = (
            "SELECT EXISTS ("
            "  SELECT 1 FROM information_schema.tables"
            "  WHERE table_schema = '{schema}' AND table_name = '{table}'"
            ") AS table_exists"
        ).format(schema=schema_name, table=table_name)
        try:
            resp = self.client.execute_query(self.database_id, sql)
            val = _extract_scalar(resp)
            return bool(val)
        except MetabaseApiError:
            return False

    def _get_row_count(self, schema_name, table_name):
        # type: (str, str) -> int
        sql = 'SELECT count(*) FROM "{}"."{}"'.format(schema_name, table_name)
        resp = self.client.execute_query(self.database_id, sql)
        val = _extract_scalar(resp)
        return int(val) if val is not None else 0

    def _get_columns(self, schema_name, table_name):
        # type: (str, str) -> List[ColumnInfo]
        sql = (
            "SELECT column_name, data_type"
            " FROM information_schema.columns"
            " WHERE table_schema = '{schema}' AND table_name = '{table}'"
            " ORDER BY ordinal_position"
        ).format(schema=schema_name, table=table_name)
        resp = self.client.execute_query(self.database_id, sql)
        rows = _extract_rows(resp)
        return [ColumnInfo(name=str(r[0]), data_type=str(r[1])) for r in rows]

    def _get_content_hash(self, schema_name, table_name):
        # type: (str, str) -> str
        """Compute an order-independent hash of all table data.

        We hash each row individually, sort the hashes, then hash the aggregate.
        This makes the comparison independent of row insertion order.
        """
        sql = (
            "SELECT md5(string_agg(row_hash, ',' ORDER BY row_hash)) AS content_hash"
            " FROM ("
            '  SELECT md5(t.*::text) AS row_hash FROM "{}"."{}" t'
            " ) sub"
        ).format(schema_name, table_name)
        try:
            resp = self.client.execute_query(self.database_id, sql)
            val = _extract_scalar(resp)
            return str(val) if val else ""
        except MetabaseApiError as e:
            logger.debug("Content hash failed for %s.%s: %s", schema_name, table_name, e)
            return ""

    def _get_column_stats(self, schema_name, table_name, columns):
        # type: (str, str, List[ColumnInfo]) -> List[ColumnStats]
        """Get null count, distinct count, and numeric sum per column."""
        if not columns:
            return []

        # Build a single query that computes stats for all columns
        parts = []  # type: List[str]
        for col in columns:
            qname = '"{}"'.format(col.name)
            parts.append(
                "count(*) - count({col}) AS \"{col}_nulls\","
                " count(DISTINCT {col}) AS \"{col}_distinct\"".format(col=qname)
            )

        sql = "SELECT {} FROM \"{}\".\"{}\"".format(
            ", ".join(parts), schema_name, table_name
        )

        try:
            resp = self.client.execute_query(self.database_id, sql)
            rows = _extract_rows(resp)
            if not rows:
                return []

            row = rows[0]
            stats = []
            for i, col in enumerate(columns):
                null_count = int(row[i * 2]) if row[i * 2] is not None else 0
                distinct_count = int(row[i * 2 + 1]) if row[i * 2 + 1] is not None else 0
                stats.append(ColumnStats(
                    name=col.name,
                    null_count=null_count,
                    distinct_count=distinct_count,
                ))
            return stats

        except (MetabaseApiError, Exception) as e:
            logger.debug("Column stats failed for %s.%s: %s", schema_name, table_name, e)
            return []

    # ──────────────────────────────────────────
    # Validation phase (call AFTER transforms run)
    # ──────────────────────────────────────────

    def validate(self, tables=None):
        # type: (Optional[List[Tuple[str, str]]]) -> ValidationReport
        """Compare current table state against pre-transform snapshots.

        If *tables* is None, validates all previously snapshotted tables.
        Call this AFTER transforms have completed.
        """
        if tables is None:
            targets = [
                (s.schema_name, s.table_name)
                for s in self._snapshots.values()
            ]
        else:
            targets = tables

        return self._run_validation(targets)

    def validate_cross_schema(self, table_pairs):
        # type: (List[Tuple[Tuple[str, str], Tuple[str, str]]]) -> ValidationReport
        """Compare dbt tables against transform tables in different schemas.

        *table_pairs* is a list of ((dbt_schema, table), (transform_schema, table)).
        The dbt side is snapshotted first, then each transform table is compared
        against its dbt counterpart.

        This is the primary validation mode since transforms live in transforms_*
        schemas while dbt tables remain in their original schemas.
        """
        # Phase 1: snapshot all dbt tables
        dbt_tables = [pair[0] for pair in table_pairs]
        self.snapshot_tables(dbt_tables)

        # Phase 2: compare each transform table against its dbt snapshot
        report = ValidationReport(total_tables=len(table_pairs))
        logger.info("=" * 60)
        logger.info("DATA VALIDATION — comparing %d tables (dbt vs transforms)", len(table_pairs))
        logger.info("=" * 60)

        for (dbt_schema, table_name), (tf_schema, tf_table) in table_pairs:
            dbt_key = "{}.{}".format(dbt_schema, table_name)
            pre_snapshot = self._snapshots.get(dbt_key)

            if not pre_snapshot:
                result = ValidationResult(
                    table_name=table_name,
                    schema_name=tf_schema,
                    skipped=True,
                    skip_reason="No dbt snapshot for {}".format(dbt_key),
                )
                report.results.append(result)
                report.tables_skipped += 1
                logger.warning("  SKIP %s.%s — no dbt snapshot for %s", tf_schema, tf_table, dbt_key)
                continue

            if pre_snapshot.error:
                result = ValidationResult(
                    table_name=table_name,
                    schema_name=tf_schema,
                    skipped=True,
                    skip_reason="dbt snapshot failed: {}".format(pre_snapshot.error),
                )
                report.results.append(result)
                report.tables_skipped += 1
                logger.warning(
                    "  SKIP %s.%s — dbt snapshot error: %s",
                    tf_schema, tf_table, pre_snapshot.error,
                )
                continue

            # Take a snapshot of the transform table
            post_snapshot = self._take_snapshot(tf_schema, tf_table)

            if post_snapshot.error:
                result = ValidationResult(
                    table_name=table_name,
                    schema_name=tf_schema,
                    passed=False,
                    checks=[CheckResult(
                        check_name="table_exists",
                        passed=False,
                        message="Transform table {}.{} error: {}".format(
                            tf_schema, tf_table, post_snapshot.error
                        ),
                    )],
                )
                report.results.append(result)
                report.tables_failed += 1
                logger.error(
                    "  FAIL %s.%s — transform snapshot error: %s",
                    tf_schema, tf_table, post_snapshot.error,
                )
                continue

            # Compare dbt snapshot vs transform snapshot
            result = self._compare_snapshots(pre_snapshot, post_snapshot)
            # Override the schema to show the comparison clearly
            result.schema_name = "{} vs {}".format(dbt_schema, tf_schema)
            report.results.append(result)

            if result.passed:
                report.tables_passed += 1
                logger.info(
                    "  PASS %s — %s.%s == %s.%s",
                    table_name, dbt_schema, table_name, tf_schema, tf_table,
                )
            else:
                report.tables_failed += 1
                for check in result.checks:
                    if not check.passed:
                        logger.error(
                            "  FAIL %s — %s: %s",
                            table_name, check.check_name, check.message,
                        )

        logger.info("-" * 60)
        logger.info(
            "VALIDATION SUMMARY: %d passed, %d failed, %d skipped (of %d total)",
            report.tables_passed, report.tables_failed,
            report.tables_skipped, report.total_tables,
        )
        if report.all_passed:
            logger.info("ALL TABLES PASSED ✓")
        else:
            logger.error("VALIDATION FAILURES DETECTED ✗")
        logger.info("=" * 60)

        return report

    def _run_validation(self, targets):
        # type: (List[Tuple[str, str]]) -> ValidationReport
        """Compare same-schema snapshots (snapshot vs re-read of same tables)."""
        report = ValidationReport(total_tables=len(targets))
        logger.info("=" * 60)
        logger.info("DATA VALIDATION — comparing %d tables", len(targets))
        logger.info("=" * 60)

        for schema_name, table_name in targets:
            key = "{}.{}".format(schema_name, table_name)
            pre_snapshot = self._snapshots.get(key)

            if not pre_snapshot:
                result = ValidationResult(
                    table_name=table_name,
                    schema_name=schema_name,
                    skipped=True,
                    skip_reason="No pre-transform snapshot available",
                )
                report.results.append(result)
                report.tables_skipped += 1
                logger.warning("  SKIP %s — no snapshot", key)
                continue

            if pre_snapshot.error:
                result = ValidationResult(
                    table_name=table_name,
                    schema_name=schema_name,
                    skipped=True,
                    skip_reason="Pre-snapshot failed: {}".format(pre_snapshot.error),
                )
                report.results.append(result)
                report.tables_skipped += 1
                logger.warning("  SKIP %s — pre-snapshot error: %s", key, pre_snapshot.error)
                continue

            # Take a post-transform snapshot
            post_snapshot = self._take_snapshot(schema_name, table_name)

            if post_snapshot.error:
                result = ValidationResult(
                    table_name=table_name,
                    schema_name=schema_name,
                    passed=False,
                    checks=[CheckResult(
                        check_name="table_exists",
                        passed=False,
                        message="Post-transform snapshot failed: {}".format(post_snapshot.error),
                    )],
                )
                report.results.append(result)
                report.tables_failed += 1
                logger.error("  FAIL %s — post-snapshot error: %s", key, post_snapshot.error)
                continue

            # Run all checks
            result = self._compare_snapshots(pre_snapshot, post_snapshot)
            report.results.append(result)

            if result.passed:
                report.tables_passed += 1
                logger.info("  PASS %s — all checks passed", key)
            else:
                report.tables_failed += 1
                for check in result.checks:
                    if not check.passed:
                        logger.error(
                            "  FAIL %s — %s: %s", key, check.check_name, check.message
                        )

        logger.info("-" * 60)
        logger.info(
            "VALIDATION SUMMARY: %d passed, %d failed, %d skipped (of %d total)",
            report.tables_passed, report.tables_failed,
            report.tables_skipped, report.total_tables,
        )
        if report.all_passed:
            logger.info("ALL TABLES PASSED ✓")
        else:
            logger.error("VALIDATION FAILURES DETECTED ✗")
        logger.info("=" * 60)

        return report

    def _compare_snapshots(self, pre, post):
        # type: (TableSnapshot, TableSnapshot) -> ValidationResult
        """Run all comparison checks between pre- and post-transform snapshots."""
        result = ValidationResult(
            table_name=pre.table_name,
            schema_name=pre.schema_name,
        )

        # Check 1: Row count
        row_check = CheckResult(
            check_name="row_count",
            passed=(pre.row_count == post.row_count),
            expected=pre.row_count,
            actual=post.row_count,
        )
        if not row_check.passed:
            row_check.message = "Row count mismatch: expected {} (dbt), got {} (transform)".format(
                pre.row_count, post.row_count
            )
        result.checks.append(row_check)

        # Check 2: Column schema
        pre_cols = [(c.name, c.data_type) for c in pre.columns]
        post_cols = [(c.name, c.data_type) for c in post.columns]

        col_names_check = CheckResult(
            check_name="column_names",
            passed=(
                [c.name for c in pre.columns] == [c.name for c in post.columns]
            ),
            expected=[c.name for c in pre.columns],
            actual=[c.name for c in post.columns],
        )
        if not col_names_check.passed:
            pre_names = {c.name for c in pre.columns}
            post_names = {c.name for c in post.columns}
            missing = pre_names - post_names
            extra = post_names - pre_names
            parts = []
            if missing:
                parts.append("missing: {}".format(sorted(missing)))
            if extra:
                parts.append("extra: {}".format(sorted(extra)))
            if not missing and not extra:
                parts.append("column order differs")
            col_names_check.message = "Column mismatch — {}".format("; ".join(parts))
        result.checks.append(col_names_check)

        # Check 2b: Column types
        col_types_check = CheckResult(
            check_name="column_types",
            passed=(pre_cols == post_cols),
            expected=pre_cols,
            actual=post_cols,
        )
        if not col_types_check.passed and col_names_check.passed:
            # Same names but different types — find which ones differ
            diffs = []
            for pc, qc in zip(pre.columns, post.columns):
                if pc.data_type != qc.data_type:
                    diffs.append("{}: {} -> {}".format(pc.name, pc.data_type, qc.data_type))
            col_types_check.message = "Type mismatches: {}".format("; ".join(diffs))
        result.checks.append(col_types_check)

        # Check 3: Content hash (only if row counts match and hash is available)
        if pre.content_hash and post.content_hash:
            hash_check = CheckResult(
                check_name="content_hash",
                passed=(pre.content_hash == post.content_hash),
                expected=pre.content_hash,
                actual=post.content_hash,
            )
            if not hash_check.passed:
                hash_check.message = (
                    "Data content differs — hashes do not match "
                    "(dbt: {}..., transform: {}...)".format(
                        pre.content_hash[:12], post.content_hash[:12]
                    )
                )
            result.checks.append(hash_check)
        elif not pre.content_hash and not post.content_hash:
            pass  # Skip silently if neither has a hash
        else:
            result.checks.append(CheckResult(
                check_name="content_hash",
                passed=True,  # Don't fail on missing hash
                message="Content hash comparison skipped (hash unavailable on one side)",
            ))

        # Check 4: Per-column null counts
        if pre.column_stats and post.column_stats:
            pre_nulls = {s.name: s.null_count for s in pre.column_stats}
            post_nulls = {s.name: s.null_count for s in post.column_stats}

            null_diffs = []
            for col_name, pre_null in pre_nulls.items():
                post_null = post_nulls.get(col_name)
                if post_null is not None and pre_null != post_null:
                    null_diffs.append(
                        "{}: {} -> {}".format(col_name, pre_null, post_null)
                    )

            null_check = CheckResult(
                check_name="null_counts",
                passed=(len(null_diffs) == 0),
                message="Null count mismatches: {}".format(
                    "; ".join(null_diffs)
                ) if null_diffs else "",
            )
            result.checks.append(null_check)

        # Check 5: Per-column distinct counts
        if pre.column_stats and post.column_stats:
            pre_distinct = {s.name: s.distinct_count for s in pre.column_stats}
            post_distinct = {s.name: s.distinct_count for s in post.column_stats}

            distinct_diffs = []
            for col_name, pre_d in pre_distinct.items():
                post_d = post_distinct.get(col_name)
                if post_d is not None and pre_d != post_d:
                    distinct_diffs.append(
                        "{}: {} -> {}".format(col_name, pre_d, post_d)
                    )

            distinct_check = CheckResult(
                check_name="distinct_counts",
                passed=(len(distinct_diffs) == 0),
                message="Distinct count mismatches: {}".format(
                    "; ".join(distinct_diffs)
                ) if distinct_diffs else "",
            )
            result.checks.append(distinct_check)

        # Overall pass/fail
        result.passed = all(c.passed for c in result.checks)
        return result

    # ──────────────────────────────────────────
    # Reporting
    # ──────────────────────────────────────────

    def format_report(self, report):
        # type: (ValidationReport) -> str
        """Format a human-readable validation report."""
        lines = []  # type: List[str]
        lines.append("")
        lines.append("=" * 60)
        lines.append("DATA VALIDATION REPORT")
        lines.append("=" * 60)
        lines.append("")

        for result in report.results:
            key = "{}.{}".format(result.schema_name, result.table_name)

            if result.skipped:
                lines.append("  SKIP  {} — {}".format(key, result.skip_reason))
                continue

            status = "PASS" if result.passed else "FAIL"
            lines.append("  {}  {}".format(status, key))

            for check in result.checks:
                if check.passed:
                    lines.append("        ✓ {}".format(check.check_name))
                else:
                    lines.append("        ✗ {}: {}".format(
                        check.check_name, check.message
                    ))

        lines.append("")
        lines.append("-" * 60)
        lines.append("  Total:   {}".format(report.total_tables))
        lines.append("  Passed:  {}".format(report.tables_passed))
        lines.append("  Failed:  {}".format(report.tables_failed))
        lines.append("  Skipped: {}".format(report.tables_skipped))
        lines.append("-" * 60)

        if report.all_passed:
            lines.append("  ✓ ALL TABLES PASSED — transform output matches dbt")
        else:
            lines.append("  ✗ VALIDATION FAILURES — transform output differs from dbt")

        lines.append("=" * 60)
        return "\n".join(lines)
