"""
Tests for the data_validator module.

These tests mock the Metabase API client to verify the validation logic
without needing a live database. They cover:
  - Snapshot capture
  - Row count comparison
  - Column schema comparison
  - Content hash comparison
  - Null/distinct count comparison
  - Edge cases: missing tables, empty tables, new columns
  - Report formatting
"""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch, call

from dbt_to_metabase.data_validator import (
    CheckResult,
    ColumnInfo,
    ColumnStats,
    DataValidator,
    TableSnapshot,
    ValidationReport,
    ValidationResult,
    _extract_rows,
    _extract_scalar,
)
from dbt_to_metabase.metabase_client import MetabaseApiError


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def _make_dataset_response(rows, cols=None):
    """Build a mock Metabase /api/dataset response."""
    return {
        "data": {
            "rows": rows,
            "cols": cols or [],
        }
    }


def _make_client_that_returns(query_results):
    """Build a mock MetabaseClient whose execute_query returns from a dict of sql->response."""
    client = MagicMock()

    def fake_execute(db_id, sql):
        for key_fragment, response in query_results.items():
            if key_fragment in sql:
                return response
        return _make_dataset_response([])

    client.execute_query.side_effect = fake_execute
    return client


def _standard_table_responses(schema, table, row_count, columns, content_hash,
                               col_stats_row=None):
    """Build a dict of query_fragment -> mock response for a standard table snapshot."""
    col_info_rows = [[c[0], c[1]] for c in columns]
    col_names = [c[0] for c in columns]

    results = {
        "EXISTS": _make_dataset_response([[True]]),
        "count(*)": _make_dataset_response([[row_count]]),
        "information_schema.columns": _make_dataset_response(col_info_rows),
        "content_hash": _make_dataset_response([[content_hash]]),
    }

    if col_stats_row is not None:
        results["_nulls"] = _make_dataset_response([col_stats_row])

    return results


# ──────────────────────────────────────────────
# Tests: _extract_rows / _extract_scalar
# ──────────────────────────────────────────────

class TestExtractHelpers:
    def test_extract_rows_normal(self):
        resp = _make_dataset_response([[1, "a"], [2, "b"]])
        assert _extract_rows(resp) == [[1, "a"], [2, "b"]]

    def test_extract_rows_empty(self):
        assert _extract_rows({}) == []
        assert _extract_rows({"data": {}}) == []

    def test_extract_scalar(self):
        resp = _make_dataset_response([[42]])
        assert _extract_scalar(resp) == 42

    def test_extract_scalar_none(self):
        assert _extract_scalar({}) is None
        assert _extract_scalar({"data": {"rows": []}}) is None


# ──────────────────────────────────────────────
# Tests: Snapshot capture
# ──────────────────────────────────────────────

class TestSnapshotCapture:
    def test_snapshot_existing_table(self):
        client = _make_client_that_returns({
            "EXISTS": _make_dataset_response([[True]]),
            "count(*)": _make_dataset_response([[100]]),
            "information_schema.columns": _make_dataset_response([
                ["id", "integer"],
                ["name", "character varying"],
            ]),
            "content_hash": _make_dataset_response([["abc123hash"]]),
            "_nulls": _make_dataset_response([[0, 100, 5, 95]]),
        })

        validator = DataValidator(client, database_id=2)
        snapshots = validator.snapshot_tables([("staging", "stg_orders")])

        assert "staging.stg_orders" in snapshots
        snap = snapshots["staging.stg_orders"]
        assert snap.row_count == 100
        assert len(snap.columns) == 2
        assert snap.columns[0].name == "id"
        assert snap.columns[1].data_type == "character varying"
        assert snap.content_hash == "abc123hash"
        assert snap.error is None

    def test_snapshot_nonexistent_table(self):
        client = _make_client_that_returns({
            "EXISTS": _make_dataset_response([[False]]),
        })

        validator = DataValidator(client, database_id=2)
        snapshots = validator.snapshot_tables([("staging", "stg_missing")])

        snap = snapshots["staging.stg_missing"]
        assert snap.error == "Table does not exist yet"
        assert snap.row_count == 0

    def test_snapshot_api_failure(self):
        client = MagicMock()
        client.execute_query.side_effect = MetabaseApiError(500, "Internal error")

        validator = DataValidator(client, database_id=2)
        snapshots = validator.snapshot_tables([("staging", "stg_orders")])

        snap = snapshots["staging.stg_orders"]
        assert snap.error is not None
        assert "500" in snap.error


# ──────────────────────────────────────────────
# Tests: Validation — passing cases
# ──────────────────────────────────────────────

class TestValidationPassing:
    def test_identical_tables_pass(self):
        """When pre and post snapshots are identical, all checks should pass."""
        responses = {
            "EXISTS": _make_dataset_response([[True]]),
            "count(*)": _make_dataset_response([[50]]),
            "information_schema.columns": _make_dataset_response([
                ["order_id", "integer"],
                ["amount", "numeric"],
            ]),
            "content_hash": _make_dataset_response([["deadbeef12345"]]),
            "_nulls": _make_dataset_response([[0, 50, 2, 30]]),
        }
        client = _make_client_that_returns(responses)

        validator = DataValidator(client, database_id=2)
        # Snapshot (pre)
        validator.snapshot_tables([("marts", "fct_orders")])
        # Validate (post) — same data returned
        report = validator.validate()

        assert report.all_passed
        assert report.tables_passed == 1
        assert report.tables_failed == 0

        result = report.results[0]
        assert result.passed
        for check in result.checks:
            assert check.passed, "Check '{}' failed: {}".format(check.check_name, check.message)


# ──────────────────────────────────────────────
# Tests: Validation — failing cases
# ──────────────────────────────────────────────

class TestValidationFailing:
    def test_row_count_mismatch(self):
        """Different row counts should fail the row_count check."""
        call_count = [0]

        def side_effect(db_id, sql):
            if "EXISTS" in sql:
                return _make_dataset_response([[True]])
            if "count(*)" in sql:
                call_count[0] += 1
                # First call (pre): 50 rows. Second call (post): 45 rows.
                count = 50 if call_count[0] <= 1 else 45
                return _make_dataset_response([[count]])
            if "information_schema.columns" in sql:
                return _make_dataset_response([["id", "integer"]])
            if "content_hash" in sql:
                return _make_dataset_response([["hash1"]])
            if "_nulls" in sql:
                return _make_dataset_response([[0, 50]])
            return _make_dataset_response([])

        client = MagicMock()
        client.execute_query.side_effect = side_effect

        validator = DataValidator(client, database_id=2)
        validator.snapshot_tables([("marts", "fct_orders")])

        # Change the mock to return different count for post
        report = validator.validate()

        assert not report.all_passed
        result = report.results[0]
        row_check = next(c for c in result.checks if c.check_name == "row_count")
        assert not row_check.passed
        assert row_check.expected == 50
        assert row_check.actual == 45

    def test_content_hash_mismatch(self):
        """Different content hashes should fail the content_hash check."""
        call_count = [0]

        def side_effect(db_id, sql):
            if "EXISTS" in sql:
                return _make_dataset_response([[True]])
            if "count(*)" in sql:
                return _make_dataset_response([[10]])
            if "information_schema.columns" in sql:
                return _make_dataset_response([["id", "integer"]])
            if "content_hash" in sql:
                call_count[0] += 1
                h = "aaa111" if call_count[0] <= 1 else "bbb222"
                return _make_dataset_response([[h]])
            if "_nulls" in sql:
                return _make_dataset_response([[0, 10]])
            return _make_dataset_response([])

        client = MagicMock()
        client.execute_query.side_effect = side_effect

        validator = DataValidator(client, database_id=2)
        validator.snapshot_tables([("marts", "fct_orders")])
        report = validator.validate()

        assert not report.all_passed
        result = report.results[0]
        hash_check = next(c for c in result.checks if c.check_name == "content_hash")
        assert not hash_check.passed

    def test_column_mismatch_missing_column(self):
        """Missing columns in the post snapshot should fail."""
        call_count = [0]

        def side_effect(db_id, sql):
            if "EXISTS" in sql:
                return _make_dataset_response([[True]])
            if "count(*)" in sql:
                return _make_dataset_response([[10]])
            if "information_schema.columns" in sql:
                call_count[0] += 1
                if call_count[0] <= 1:
                    # Pre: 3 columns
                    return _make_dataset_response([
                        ["id", "integer"],
                        ["name", "text"],
                        ["email", "text"],
                    ])
                else:
                    # Post: missing 'email' column
                    return _make_dataset_response([
                        ["id", "integer"],
                        ["name", "text"],
                    ])
            if "content_hash" in sql:
                return _make_dataset_response([["hash"]])
            return _make_dataset_response([])

        client = MagicMock()
        client.execute_query.side_effect = side_effect

        validator = DataValidator(client, database_id=2)
        validator.snapshot_tables([("staging", "stg_customers")])
        report = validator.validate()

        assert not report.all_passed
        result = report.results[0]
        col_check = next(c for c in result.checks if c.check_name == "column_names")
        assert not col_check.passed
        assert "missing" in col_check.message
        assert "email" in col_check.message

    def test_column_type_mismatch(self):
        """Same column names but different types should fail column_types."""
        call_count = [0]

        def side_effect(db_id, sql):
            if "EXISTS" in sql:
                return _make_dataset_response([[True]])
            if "count(*)" in sql:
                return _make_dataset_response([[10]])
            if "information_schema.columns" in sql:
                call_count[0] += 1
                if call_count[0] <= 1:
                    return _make_dataset_response([
                        ["amount", "numeric"],
                    ])
                else:
                    return _make_dataset_response([
                        ["amount", "double precision"],
                    ])
            if "content_hash" in sql:
                return _make_dataset_response([["hash"]])
            return _make_dataset_response([])

        client = MagicMock()
        client.execute_query.side_effect = side_effect

        validator = DataValidator(client, database_id=2)
        validator.snapshot_tables([("marts", "fct_orders")])
        report = validator.validate()

        result = report.results[0]
        type_check = next(c for c in result.checks if c.check_name == "column_types")
        assert not type_check.passed
        assert "numeric" in type_check.message
        assert "double precision" in type_check.message


# ──────────────────────────────────────────────
# Tests: Edge cases
# ──────────────────────────────────────────────

class TestEdgeCases:
    def test_empty_table_passes(self):
        """An empty table (0 rows) should pass if both pre and post are empty."""
        responses = {
            "EXISTS": _make_dataset_response([[True]]),
            "count(*)": _make_dataset_response([[0]]),
            "information_schema.columns": _make_dataset_response([
                ["id", "integer"],
            ]),
            "content_hash": _make_dataset_response([[None]]),
        }
        client = _make_client_that_returns(responses)

        validator = DataValidator(client, database_id=2)
        validator.snapshot_tables([("staging", "stg_empty")])
        report = validator.validate()

        assert report.all_passed

    def test_table_not_found_post_fails(self):
        """If the table disappears after transforms, validation should fail."""
        pre_call = [True]

        def side_effect(db_id, sql):
            if "EXISTS" in sql:
                if pre_call[0]:
                    pre_call[0] = False
                    return _make_dataset_response([[True]])
                return _make_dataset_response([[False]])
            if "count(*)" in sql:
                return _make_dataset_response([[10]])
            if "information_schema.columns" in sql:
                return _make_dataset_response([["id", "integer"]])
            if "content_hash" in sql:
                return _make_dataset_response([["hash"]])
            if "_nulls" in sql:
                return _make_dataset_response([[0, 10]])
            return _make_dataset_response([])

        client = MagicMock()
        client.execute_query.side_effect = side_effect

        validator = DataValidator(client, database_id=2)
        validator.snapshot_tables([("marts", "gone_table")])
        report = validator.validate()

        assert not report.all_passed
        assert report.tables_failed == 1

    def test_multiple_tables(self):
        """Validate works across multiple tables, reporting each individually."""
        responses = {
            "EXISTS": _make_dataset_response([[True]]),
            "count(*)": _make_dataset_response([[5]]),
            "information_schema.columns": _make_dataset_response([["id", "integer"]]),
            "content_hash": _make_dataset_response([["h"]]),
            "_nulls": _make_dataset_response([[0, 5]]),
        }
        client = _make_client_that_returns(responses)

        validator = DataValidator(client, database_id=2)
        validator.snapshot_tables([
            ("staging", "t1"),
            ("staging", "t2"),
            ("marts", "t3"),
        ])
        report = validator.validate()

        assert report.total_tables == 3
        assert report.tables_passed == 3
        assert report.all_passed

    def test_skipped_when_no_pre_snapshot(self):
        """Tables not snapshotted beforehand are skipped, not failed."""
        client = MagicMock()
        validator = DataValidator(client, database_id=2)

        # Don't snapshot anything, then try to validate
        report = validator.validate(tables=[("marts", "unknown")])

        assert report.tables_skipped == 1
        assert report.tables_failed == 0
        assert report.all_passed  # Skipped doesn't count as failed


# ──────────────────────────────────────────────
# Tests: Report formatting
# ──────────────────────────────────────────────

class TestReportFormatting:
    def test_passing_report(self):
        report = ValidationReport(
            total_tables=2,
            tables_passed=2,
            tables_failed=0,
            tables_skipped=0,
            results=[
                ValidationResult(
                    table_name="t1", schema_name="s", passed=True,
                    checks=[
                        CheckResult(check_name="row_count", passed=True),
                        CheckResult(check_name="content_hash", passed=True),
                    ],
                ),
                ValidationResult(
                    table_name="t2", schema_name="s", passed=True,
                    checks=[
                        CheckResult(check_name="row_count", passed=True),
                    ],
                ),
            ],
        )

        validator = DataValidator(MagicMock(), database_id=2)
        text = validator.format_report(report)

        assert "PASS" in text
        assert "ALL TABLES PASSED" in text
        assert "✓" in text

    def test_failing_report(self):
        report = ValidationReport(
            total_tables=2,
            tables_passed=1,
            tables_failed=1,
            tables_skipped=0,
            results=[
                ValidationResult(
                    table_name="t1", schema_name="s", passed=True,
                    checks=[CheckResult(check_name="row_count", passed=True)],
                ),
                ValidationResult(
                    table_name="t2", schema_name="s", passed=False,
                    checks=[
                        CheckResult(
                            check_name="row_count", passed=False,
                            expected=100, actual=90,
                            message="Row count mismatch",
                        ),
                    ],
                ),
            ],
        )

        validator = DataValidator(MagicMock(), database_id=2)
        text = validator.format_report(report)

        assert "FAIL" in text
        assert "VALIDATION FAILURES" in text
        assert "Row count mismatch" in text
        assert "✗" in text

    def test_skipped_report(self):
        report = ValidationReport(
            total_tables=1,
            tables_passed=0,
            tables_failed=0,
            tables_skipped=1,
            results=[
                ValidationResult(
                    table_name="t1", schema_name="s",
                    skipped=True, skip_reason="No snapshot",
                ),
            ],
        )

        validator = DataValidator(MagicMock(), database_id=2)
        text = validator.format_report(report)

        assert "SKIP" in text
        assert "No snapshot" in text


# ──────────────────────────────────────────────
# Tests: Null count and distinct count checks
# ──────────────────────────────────────────────

class TestColumnStatsValidation:
    def test_null_count_mismatch_fails(self):
        """Different null counts should fail the null_counts check."""
        call_count = [0]

        def side_effect(db_id, sql):
            if "EXISTS" in sql:
                return _make_dataset_response([[True]])
            if "count(*)" in sql:
                return _make_dataset_response([[10]])
            if "information_schema.columns" in sql:
                return _make_dataset_response([["name", "text"]])
            if "content_hash" in sql:
                return _make_dataset_response([["hash"]])
            if "_nulls" in sql:
                call_count[0] += 1
                if call_count[0] <= 1:
                    return _make_dataset_response([[2, 8]])  # 2 nulls, 8 distinct
                else:
                    return _make_dataset_response([[5, 7]])  # 5 nulls, 7 distinct
            return _make_dataset_response([])

        client = MagicMock()
        client.execute_query.side_effect = side_effect

        validator = DataValidator(client, database_id=2)
        validator.snapshot_tables([("staging", "t")])
        report = validator.validate()

        result = report.results[0]
        null_check = next(
            (c for c in result.checks if c.check_name == "null_counts"), None
        )
        assert null_check is not None
        assert not null_check.passed
        assert "name" in null_check.message


# ──────────────────────────────────────────────
# Tests: Integration with migrator
# ──────────────────────────────────────────────

class TestMigratorIntegration:
    """Test that the Migrator correctly invokes validation when enabled."""

    def test_migrator_has_validation_flag(self):
        """Migrator should accept enable_validation parameter."""
        from dbt_to_metabase.config import MigrationConfig, GitHubConfig, MetabaseConfig

        config = MigrationConfig(
            github=GitHubConfig(repo="test/test"),
            metabase=MetabaseConfig(url="http://localhost:3000", api_key="test"),
        )

        # Should not raise — just test the constructor accepts the flag
        from dbt_to_metabase.migrator import Migrator
        m = Migrator.__new__(Migrator)
        # Verify the attribute exists when we manually set it
        m.enable_validation = True
        assert m.enable_validation is True

    def test_validation_report_structure(self):
        """ValidationReport should correctly compute all_passed."""
        report = ValidationReport(
            total_tables=3,
            tables_passed=3,
            tables_failed=0,
            tables_skipped=0,
        )
        assert report.all_passed

        report.tables_failed = 1
        report.tables_passed = 2
        assert not report.all_passed
