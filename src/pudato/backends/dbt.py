"""dbt backend for invoking dbt commands via subprocess."""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from pudato.protocol import DataReference

logger = structlog.get_logger()


@dataclass
class DbtResult:
    """Result of a dbt command execution."""

    success: bool
    command: str
    return_code: int
    stdout: str
    stderr: str
    # Version tracking
    logic_version: str | None = None
    execution_id: str | None = None
    # Lineage tracking (populated from manifest.json after execution)
    inputs: list[DataReference] = field(default_factory=list)
    outputs: list[DataReference] = field(default_factory=list)


@dataclass
class DbtConfig:
    """Configuration for dbt execution."""

    project_dir: Path
    profiles_dir: Path | None = None
    target: str | None = None
    # Version tracking - passed to dbt as environment variables
    logic_version: str | None = None
    execution_id: str | None = None
    # Additional environment variables
    env: dict[str, str] = field(default_factory=dict)


class DbtBackend:
    """Backend for executing dbt commands.

    Invokes dbt via subprocess, which:
    - Keeps dbt execution isolated
    - Allows passing version info via environment variables
    - Works the same locally and in production (containers)
    """

    def __init__(self, config: DbtConfig) -> None:
        self._config = config
        self._log = logger.bind(backend="dbt", project_dir=str(config.project_dir))

    def run(
        self,
        select: str | None = None,
        exclude: str | None = None,
        full_refresh: bool = False,
    ) -> DbtResult:
        """Run dbt models.

        Args:
            select: Model selection syntax (e.g., "model_name", "+model_name", "tag:daily")
            exclude: Models to exclude
            full_refresh: Whether to full refresh incremental models
        """
        args = ["run"]
        if select:
            args.extend(["--select", select])
        if exclude:
            args.extend(["--exclude", exclude])
        if full_refresh:
            args.append("--full-refresh")

        return self._execute(args)

    def test(
        self,
        select: str | None = None,
        exclude: str | None = None,
    ) -> DbtResult:
        """Run dbt tests.

        Args:
            select: Test selection syntax
            exclude: Tests to exclude
        """
        args = ["test"]
        if select:
            args.extend(["--select", select])
        if exclude:
            args.extend(["--exclude", exclude])

        return self._execute(args)

    def seed(
        self,
        select: str | None = None,
        full_refresh: bool = False,
    ) -> DbtResult:
        """Load dbt seeds.

        Args:
            select: Seed selection
            full_refresh: Whether to drop and recreate seed tables
        """
        args = ["seed"]
        if select:
            args.extend(["--select", select])
        if full_refresh:
            args.append("--full-refresh")

        return self._execute(args)

    def build(
        self,
        select: str | None = None,
        exclude: str | None = None,
        full_refresh: bool = False,
    ) -> DbtResult:
        """Run dbt build (seeds, models, tests, snapshots).

        Args:
            select: Selection syntax
            exclude: Items to exclude
            full_refresh: Whether to full refresh
        """
        args = ["build"]
        if select:
            args.extend(["--select", select])
        if exclude:
            args.extend(["--exclude", exclude])
        if full_refresh:
            args.append("--full-refresh")

        return self._execute(args)

    def compile(
        self,
        select: str | None = None,
    ) -> DbtResult:
        """Compile dbt models without executing.

        Args:
            select: Model selection
        """
        args = ["compile"]
        if select:
            args.extend(["--select", select])

        return self._execute(args)

    def _execute(self, args: list[str]) -> DbtResult:
        """Execute a dbt command.

        Args:
            args: dbt command arguments (e.g., ["run", "--select", "model"])

        Returns:
            DbtResult with command output and status
        """
        cmd = ["dbt"] + args

        # Add project and profiles dir
        cmd.extend(["--project-dir", str(self._config.project_dir)])
        if self._config.profiles_dir:
            cmd.extend(["--profiles-dir", str(self._config.profiles_dir)])
        if self._config.target:
            cmd.extend(["--target", self._config.target])

        # Build environment with version tracking
        env = os.environ.copy()
        env.update(self._config.env)

        if self._config.logic_version:
            env["PUDATO_LOGIC_VERSION"] = self._config.logic_version
        if self._config.execution_id:
            env["PUDATO_EXECUTION_ID"] = self._config.execution_id

        self._log.info(
            "executing_dbt",
            command=" ".join(cmd),
            logic_version=self._config.logic_version,
            execution_id=self._config.execution_id,
        )

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                env=env,
                cwd=self._config.project_dir,
            )

            success = result.returncode == 0
            inputs: list[DataReference] = []
            outputs: list[DataReference] = []

            if success:
                self._log.info("dbt_success", return_code=result.returncode)
                # Parse lineage from manifest
                from pudato.backends.dbt_manifest import parse_lineage_from_manifest

                inputs, outputs = parse_lineage_from_manifest(self._config.project_dir)
            else:
                self._log.error(
                    "dbt_failed",
                    return_code=result.returncode,
                    stderr=result.stderr[:500] if result.stderr else None,
                )

            return DbtResult(
                success=success,
                command=" ".join(cmd),
                return_code=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
                logic_version=self._config.logic_version,
                execution_id=self._config.execution_id,
                inputs=inputs,
                outputs=outputs,
            )

        except FileNotFoundError:
            self._log.error("dbt_not_found")
            return DbtResult(
                success=False,
                command=" ".join(cmd),
                return_code=-1,
                stdout="",
                stderr="dbt command not found. Is dbt installed?",
                logic_version=self._config.logic_version,
                execution_id=self._config.execution_id,
            )
        except Exception as e:
            self._log.exception("dbt_execution_error", error=str(e))
            return DbtResult(
                success=False,
                command=" ".join(cmd),
                return_code=-1,
                stdout="",
                stderr=str(e),
                logic_version=self._config.logic_version,
                execution_id=self._config.execution_id,
            )
