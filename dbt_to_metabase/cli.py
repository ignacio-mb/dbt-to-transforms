"""
CLI interface for dbt-to-metabase migration tool.
"""

from __future__ import annotations

import argparse
import logging
import sys

from . import __version__
from .config import MigrationConfig
from .migrator import Migrator, MigrationError


def setup_logging(verbose=False):
    # type: (bool) -> None
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s [%(levelname)-7s] %(name)s: %(message)s"
    logging.basicConfig(level=level, format=fmt, stream=sys.stderr)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


def _apply_cli_overrides(config, args):
    # type: (MigrationConfig, argparse.Namespace) -> None
    """Apply CLI flag overrides to the loaded config."""
    if getattr(args, "manifest_path", None):
        config.dbt.manifest_path = args.manifest_path
        # Clear other source options — manifest takes precedence
        config.dbt.project_path = None
        config.dbt.github_repo = None
    if getattr(args, "dbt_project_path", None):
        config.dbt.project_path = args.dbt_project_path
        # Clear github — local path takes precedence
        config.dbt.github_repo = None
    if getattr(args, "dbt_target", None):
        config.dbt.target = args.dbt_target


def cmd_migrate(args):
    # type: (argparse.Namespace) -> int
    config = MigrationConfig.from_yaml(args.config)
    _apply_cli_overrides(config, args)

    if args.dry_run:
        config.dry_run = True

    migrator = Migrator(config, enable_validation=args.validate)
    try:
        plan = migrator.run()
    except MigrationError as e:
        logging.error("Migration failed: %s", e)
        return 1

    if args.output:
        migrator.export_plan(args.output)

    print("\n" + "=" * 50)
    print("MIGRATION RESULTS")
    print("=" * 50)
    print("  Transforms created: {}".format(sum(1 for t in plan.transforms if t.transform_id)))
    print("  Tags created:       {}".format(sum(1 for t in plan.tags if t.tag_id)))
    print("  Jobs created:       {}".format(sum(1 for j in plan.jobs if j.job_id)))
    print("  Models skipped:     {}".format(len(plan.skipped_models)))
    print("  Warnings:           {}".format(len(plan.warnings)))

    if plan.warnings:
        print("\n" + "=" * 50)
        print("WARNINGS")
        print("=" * 50)
        for w in plan.warnings:
            print("  !  {}".format(w))

    if args.validate and migrator.validation_report:
        report = migrator.validation_report
        if not report.all_passed:
            print("\n  X DATA VALIDATION FAILED: {} of {} tables differ".format(
                report.tables_failed, report.total_tables
            ))
            return 2
        else:
            print("\n  OK DATA VALIDATION PASSED: all {} tables match".format(
                report.tables_passed
            ))

    return 0


def cmd_plan(args):
    # type: (argparse.Namespace) -> int
    config = MigrationConfig.from_yaml(args.config)
    _apply_cli_overrides(config, args)
    config.dry_run = True

    migrator = Migrator(config)
    try:
        plan = migrator.run()
    except MigrationError as e:
        logging.error("Planning failed: %s", e)
        return 1

    output = args.output or "migration_plan.json"
    migrator.export_plan(output)
    print("Migration plan written to {}".format(output))

    if args.stdout:
        with open(output) as f:
            print(f.read())

    return 0


def cmd_remap(args):
    # type: (argparse.Namespace) -> int
    config = MigrationConfig.from_yaml(args.config)

    migrator = Migrator(config)
    try:
        results = migrator.run_remap()
    except MigrationError as e:
        logging.error("Remap failed: %s", e)
        return 1

    print("\n" + "=" * 50)
    print("REMAP RESULTS")
    print("=" * 50)
    print("  Table mappings:      {}".format(len(results["table_mappings"])))
    print("  Cards updated:       {}".format(len(results["cards_updated"])))
    print("  Cards skipped:       {}".format(len(results["cards_skipped"])))
    print("  Warnings:            {}".format(len(results["warnings"])))

    if results["table_mappings"]:
        print("\n  Table ID mappings:")
        for m in results["table_mappings"]:
            print("    {}.{} (id={}) -> {}.{} (id={})".format(
                m["old_schema"], m["table_name"], m["old_id"],
                m["new_schema"], m["table_name"], m["new_id"],
            ))

    if results["cards_updated"]:
        print("\n  Cards updated:")
        for c in results["cards_updated"]:
            print("    {} (id={}, type={})".format(c["name"], c["id"], c["type"]))

    if results["warnings"]:
        print("\n" + "=" * 50)
        print("WARNINGS")
        print("=" * 50)
        for w in results["warnings"]:
            print("  !  {}".format(w))

    return 0


def cmd_validate(args):
    # type: (argparse.Namespace) -> int
    """Standalone validation: check that transform tables match dbt expectations."""
    config = MigrationConfig.from_yaml(args.config)
    _apply_cli_overrides(config, args)

    migrator = Migrator(config)
    try:
        report = migrator.run_validate_only()
    except MigrationError as e:
        logging.error("Validation failed: %s", e)
        return 1

    if not report.all_passed:
        return 2
    return 0


def cmd_check(args):
    # type: (argparse.Namespace) -> int
    """Validate config and connectivity."""
    try:
        config = MigrationConfig.from_yaml(args.config)
        print("OK Config loaded from {}".format(args.config))
    except Exception as e:
        print("FAIL Config error: {}".format(e))
        return 1

    from .dbt_compiler import DbtCompiler, DbtCompilationError
    from .manifest_parser import ManifestParser
    from .dependency_resolver import CyclicDependencyError, DependencyResolver

    try:
        compiler = DbtCompiler(config.dbt)
        manifest_path = compiler.compile()
        parser = ManifestParser(manifest_path)
        project = parser.parse()
        compiler.cleanup()
        print("OK dbt project '{}' compiled and parsed successfully".format(project.name))
        print("  Models:  {}".format(len(project.models)))
        print("  Sources: {}".format(len(project.sources)))
    except (DbtCompilationError, Exception) as e:
        print("FAIL dbt compilation/parsing error: {}".format(e))
        return 1

    try:
        resolver = DependencyResolver(project)
        order = resolver.resolve()
        layers = resolver.get_execution_layers()
        print("OK Dependencies resolved: {} models in {} layers".format(len(order), len(layers)))
    except CyclicDependencyError as e:
        print("FAIL Dependency error: {}".format(e))
        return 1

    from .metabase_client import MetabaseClient

    try:
        client = MetabaseClient(config.metabase)
        db = client.get_database(config.metabase.database_id)
        print("OK Metabase connected. Target database: {}".format(db.get("name", "unknown")))
    except Exception as e:
        print("FAIL Metabase connection error: {}".format(e))
        return 1

    print("\nOK All checks passed. Ready to migrate.")
    return 0


def _add_dbt_source_args(parser):
    # type: (argparse.ArgumentParser) -> None
    """Add common dbt source override flags to a subparser."""
    parser.add_argument(
        "--manifest-path",
        help="Path to a pre-compiled manifest.json (skips dbt compile)",
    )
    parser.add_argument(
        "--dbt-project-path",
        help="Path to local dbt project directory (overrides config)",
    )
    parser.add_argument(
        "--dbt-target",
        help="dbt target name for compilation (default: from config)",
    )


def main():
    # type: () -> int
    parser = argparse.ArgumentParser(
        prog="dbt-to-metabase",
        description="Migrate dbt models into Metabase Transforms",
    )
    parser.add_argument(
        "--version", action="version", version="%(prog)s {}".format(__version__)
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable debug logging"
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # migrate
    p_migrate = subparsers.add_parser("migrate", help="Run the full migration")
    p_migrate.add_argument("--config", "-c", required=True, help="Path to config YAML")
    p_migrate.add_argument("--dry-run", action="store_true", help="Plan only")
    p_migrate.add_argument("--output", "-o", help="Export plan to JSON file")
    p_migrate.add_argument(
        "--validate", action="store_true",
        help="Run data validation after transforms complete",
    )
    _add_dbt_source_args(p_migrate)
    p_migrate.set_defaults(func=cmd_migrate)

    # plan
    p_plan = subparsers.add_parser("plan", help="Generate plan without executing")
    p_plan.add_argument("--config", "-c", required=True, help="Path to config YAML")
    p_plan.add_argument("--output", "-o", help="Output JSON file")
    p_plan.add_argument("--stdout", action="store_true", help="Print plan to stdout")
    _add_dbt_source_args(p_plan)
    p_plan.set_defaults(func=cmd_plan)

    # validate
    p_validate = subparsers.add_parser(
        "validate",
        help="Validate that transform output tables match dbt expectations",
    )
    p_validate.add_argument("--config", "-c", required=True, help="Path to config YAML")
    _add_dbt_source_args(p_validate)
    p_validate.set_defaults(func=cmd_validate)

    # check
    p_check = subparsers.add_parser("check", help="Validate config and connectivity")
    p_check.add_argument("--config", "-c", required=True, help="Path to config YAML")
    p_check.set_defaults(func=cmd_check)

    # remap
    p_remap = subparsers.add_parser(
        "remap",
        help="Remap Metabase cards from dbt tables to transform tables",
    )
    p_remap.add_argument("--config", "-c", required=True, help="Path to config YAML")
    p_remap.set_defaults(func=cmd_remap)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return 1

    setup_logging(args.verbose)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
