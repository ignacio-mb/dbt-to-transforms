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


def cmd_migrate(args):
    # type: (argparse.Namespace) -> int
    config = MigrationConfig.from_yaml(args.config)
    if args.dry_run:
        config.dry_run = True

    migrator = Migrator(config)
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

    return 0


def cmd_plan(args):
    # type: (argparse.Namespace) -> int
    config = MigrationConfig.from_yaml(args.config)
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


def cmd_validate(args):
    # type: (argparse.Namespace) -> int
    try:
        config = MigrationConfig.from_yaml(args.config)
        print("OK Config loaded from {}".format(args.config))
    except Exception as e:
        print("FAIL Config error: {}".format(e))
        return 1

    from .dbt_parser import GitHubDbtParser
    from .dependency_resolver import CyclicDependencyError, DependencyResolver

    try:
        parser = GitHubDbtParser(config.github)
        project = parser.parse()
        print("OK dbt project '{}' parsed successfully".format(project.name))
        print("  Models:  {}".format(len(project.models)))
        print("  Sources: {}".format(len(project.sources)))
    except Exception as e:
        print("FAIL dbt parsing error: {}".format(e))
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

    print("\nOK All validations passed. Ready to migrate.")
    return 0


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

    p_migrate = subparsers.add_parser("migrate", help="Run the full migration")
    p_migrate.add_argument("--config", "-c", required=True, help="Path to config YAML")
    p_migrate.add_argument("--dry-run", action="store_true", help="Plan only")
    p_migrate.add_argument("--output", "-o", help="Export plan to JSON file")
    p_migrate.set_defaults(func=cmd_migrate)

    p_plan = subparsers.add_parser("plan", help="Generate plan without executing")
    p_plan.add_argument("--config", "-c", required=True, help="Path to config YAML")
    p_plan.add_argument("--output", "-o", help="Output JSON file")
    p_plan.add_argument("--stdout", action="store_true", help="Print plan to stdout")
    p_plan.set_defaults(func=cmd_plan)

    p_validate = subparsers.add_parser("validate", help="Validate config and project")
    p_validate.add_argument("--config", "-c", required=True, help="Path to config YAML")
    p_validate.set_defaults(func=cmd_validate)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return 1

    setup_logging(args.verbose)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
