"""
Compile a dbt project to produce manifest.json.

Supports three input modes:
  A) Local dbt project path
  B) GitHub repo (cloned to a temp directory)
  C) Pre-built manifest.json (no compilation needed)
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Optional

import yaml

from .config import DbtProjectConfig

logger = logging.getLogger(__name__)


class DbtCompilationError(Exception):
    """Raised when dbt compile fails."""
    pass


class DbtCompiler:
    """Clone (if needed), install deps, compile, and return the manifest path."""

    def __init__(self, config):
        # type: (DbtProjectConfig) -> None
        self.config = config
        self._temp_dirs = []  # type: list

    def compile(self):
        # type: () -> str
        """Run the full compilation pipeline and return the path to manifest.json.

        For pre-built manifests (mode C), returns the path immediately.
        """
        if not self.config.needs_compilation:
            manifest = self.config.manifest_path
            if not manifest or not Path(manifest).is_file():
                raise DbtCompilationError(
                    "manifest_path '{}' does not exist".format(manifest)
                )
            logger.info("Using pre-built manifest: %s", manifest)
            return manifest

        project_dir = self._resolve_project_dir()
        profiles_dir = self._ensure_profiles_dir(project_dir)

        self._run_dbt_deps(project_dir, profiles_dir)
        self._run_dbt_compile(project_dir, profiles_dir)

        manifest = os.path.join(project_dir, "target", "manifest.json")
        if not Path(manifest).is_file():
            raise DbtCompilationError(
                "dbt compile succeeded but manifest.json not found at {}".format(manifest)
            )

        logger.info("Compilation complete: %s", manifest)
        return manifest

    def cleanup(self):
        # type: () -> None
        """Remove temporary directories created during compilation."""
        for d in self._temp_dirs:
            try:
                shutil.rmtree(d, ignore_errors=True)
                logger.debug("Cleaned up temp dir: %s", d)
            except Exception as e:
                logger.warning("Failed to clean up %s: %s", d, e)

    # ------------------------------------------------------------------
    # Step 1: Resolve the project directory
    # ------------------------------------------------------------------

    def _resolve_project_dir(self):
        # type: () -> str
        if self.config.project_path:
            project_dir = os.path.expanduser(self.config.project_path)
            if self.config.github_subdirectory:
                project_dir = os.path.join(project_dir, self.config.github_subdirectory)
            if not Path(project_dir).is_dir():
                raise DbtCompilationError(
                    "project_path '{}' is not a directory".format(project_dir)
                )
            dbt_project_yml = os.path.join(project_dir, "dbt_project.yml")
            if not Path(dbt_project_yml).is_file():
                raise DbtCompilationError(
                    "No dbt_project.yml found in '{}'".format(project_dir)
                )
            logger.info("Using local dbt project: %s", project_dir)
            return project_dir

        if self.config.github_repo:
            return self._clone_repo()

        raise DbtCompilationError(
            "No dbt project source configured. Provide one of: "
            "project_path, github_repo, or manifest_path"
        )

    def _clone_repo(self):
        # type: () -> str
        repo = self.config.github_repo
        branch = self.config.github_branch
        token = self.config.github_token

        temp_dir = tempfile.mkdtemp(prefix="dbt_to_transforms_")
        self._temp_dirs.append(temp_dir)

        if token:
            clone_url = "https://x-access-token:{}@github.com/{}.git".format(token, repo)
        else:
            clone_url = "https://github.com/{}.git".format(repo)

        logger.info("Cloning %s (branch: %s) into %s", repo, branch, temp_dir)

        cmd = [
            "git", "clone",
            "--depth", "1",
            "--branch", branch,
            clone_url,
            temp_dir,
        ]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120,
            )
        except FileNotFoundError:
            raise DbtCompilationError(
                "git is not installed or not on PATH. "
                "Install git to clone GitHub repositories."
            )
        except subprocess.TimeoutExpired:
            raise DbtCompilationError(
                "git clone timed out after 120 seconds for {}".format(repo)
            )

        if result.returncode != 0:
            # Mask the token in error output
            stderr = result.stderr
            if token:
                stderr = stderr.replace(token, "***")
            raise DbtCompilationError(
                "git clone failed (exit {}): {}".format(result.returncode, stderr.strip())
            )

        project_dir = temp_dir
        if self.config.github_subdirectory:
            project_dir = os.path.join(temp_dir, self.config.github_subdirectory)
            if not Path(project_dir).is_dir():
                raise DbtCompilationError(
                    "Subdirectory '{}' not found in cloned repo".format(
                        self.config.github_subdirectory
                    )
                )

        dbt_project_yml = os.path.join(project_dir, "dbt_project.yml")
        if not Path(dbt_project_yml).is_file():
            raise DbtCompilationError(
                "No dbt_project.yml found in cloned repo at '{}'".format(project_dir)
            )

        logger.info("Cloned dbt project to %s", project_dir)
        return project_dir

    # ------------------------------------------------------------------
    # Step 2: Ensure profiles.yml exists
    # ------------------------------------------------------------------

    def _ensure_profiles_dir(self, project_dir):
        # type: (str) -> str
        """Return the profiles directory, generating profiles.yml if needed."""

        # Mode 1: User points to an existing profiles directory
        if self.config.profiles_dir:
            profiles_dir = os.path.expanduser(self.config.profiles_dir)
            if not Path(profiles_dir).is_dir():
                raise DbtCompilationError(
                    "profiles_dir '{}' is not a directory".format(profiles_dir)
                )
            logger.info("Using existing profiles dir: %s", profiles_dir)
            return profiles_dir

        # Mode 2: Generate profiles.yml from inline credentials
        if self.config.has_inline_credentials:
            return self._generate_profiles_yml(project_dir)

        # Mode 3: Fall back to default ~/.dbt
        default_dir = os.path.expanduser("~/.dbt")
        default_file = os.path.join(default_dir, "profiles.yml")
        if Path(default_file).is_file():
            logger.info("Using default profiles: %s", default_file)
            return default_dir

        raise DbtCompilationError(
            "No dbt profiles found. Provide one of:\n"
            "  - profiles_dir pointing to a directory with profiles.yml\n"
            "  - Inline db credentials (db_host, db_user, db_name, etc.)\n"
            "  - A profiles.yml at ~/.dbt/profiles.yml"
        )

    def _generate_profiles_yml(self, project_dir):
        # type: (str) -> str
        """Generate a temporary profiles.yml from inline credentials."""

        # Read the project's profile name from dbt_project.yml
        dbt_project_yml = os.path.join(project_dir, "dbt_project.yml")
        with open(dbt_project_yml) as f:
            project_config = yaml.safe_load(f)
        profile_name = project_config.get("profile", "default")

        cfg = self.config
        target = cfg.target

        output = {
            "type": cfg.db_type,
            "threads": cfg.db_threads,
            "host": cfg.db_host,
            "port": cfg.db_port,
            "user": cfg.db_user,
            "pass": cfg.db_password or "",
            "dbname": cfg.db_name,
            "schema": cfg.db_schema or "public",
        }

        profiles = {
            profile_name: {
                "target": target,
                "outputs": {
                    target: output,
                },
            }
        }

        temp_dir = tempfile.mkdtemp(prefix="dbt_profiles_")
        self._temp_dirs.append(temp_dir)

        profiles_path = os.path.join(temp_dir, "profiles.yml")
        with open(profiles_path, "w") as f:
            yaml.dump(profiles, f, default_flow_style=False)

        logger.info(
            "Generated profiles.yml for profile '%s' (target=%s, type=%s) at %s",
            profile_name, target, cfg.db_type, profiles_path,
        )
        return temp_dir

    # ------------------------------------------------------------------
    # Step 3: Run dbt deps + dbt compile
    # ------------------------------------------------------------------

    def _run_dbt_deps(self, project_dir, profiles_dir):
        # type: (str, str) -> None
        """Run ``dbt deps`` if packages.yml or packages.yaml exists."""

        packages_yml = os.path.join(project_dir, "packages.yml")
        packages_yaml = os.path.join(project_dir, "packages.yaml")

        if not (Path(packages_yml).is_file() or Path(packages_yaml).is_file()):
            logger.debug("No packages.yml found, skipping dbt deps")
            return

        logger.info("Running dbt deps...")
        self._run_dbt_command(["dbt", "deps"], project_dir, profiles_dir)

    def _run_dbt_compile(self, project_dir, profiles_dir):
        # type: (str, str) -> None
        """Run ``dbt compile`` against the project."""

        logger.info("Running dbt compile (target=%s, full-refresh)...", self.config.target)
        self._run_dbt_command(
            ["dbt", "compile", "--target", self.config.target, "--full-refresh"],
            project_dir,
            profiles_dir,
        )

    def _run_dbt_command(self, cmd, project_dir, profiles_dir):
        # type: (list, str, str) -> None
        full_cmd = cmd + [
            "--project-dir", project_dir,
            "--profiles-dir", profiles_dir,
        ]

        logger.debug("Running: %s", " ".join(full_cmd))

        try:
            result = subprocess.run(
                full_cmd,
                capture_output=True,
                text=True,
                timeout=600,
                cwd=project_dir,
            )
        except FileNotFoundError:
            raise DbtCompilationError(
                "dbt is not installed or not on PATH. "
                "Install dbt-core: pip install dbt-postgres  (or your adapter)"
            )
        except subprocess.TimeoutExpired:
            raise DbtCompilationError(
                "'{}' timed out after 600 seconds".format(" ".join(cmd))
            )

        if result.stdout:
            for line in result.stdout.strip().split("\n"):
                logger.debug("[dbt] %s", line)

        if result.returncode != 0:
            error_msg = result.stderr.strip() if result.stderr else result.stdout.strip()
            # Provide actionable hints for common failures
            if "Could not find profile" in error_msg:
                error_msg += (
                    "\n\nHint: Check that the 'profile' name in dbt_project.yml matches "
                    "the profile in your profiles.yml, or provide inline db credentials."
                )
            elif "Runtime Error" in error_msg and "connect" in error_msg.lower():
                error_msg += (
                    "\n\nHint: dbt compile requires a live database connection. "
                    "Check your db_host, db_port, db_user, db_password, and db_name."
                )
            raise DbtCompilationError(
                "dbt command failed (exit {}):\n{}".format(result.returncode, error_msg)
            )