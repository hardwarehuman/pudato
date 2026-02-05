"""Transform handler for dbt operations."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pudato.backends.dbt import DbtBackend, DbtConfig
from pudato.handlers.base import BaseHandler
from pudato.protocol import Command, ExecutionRecord, Result


class TransformHandler(BaseHandler):
    """Handler for transform operations (dbt).

    Translates standardized Command messages into dbt operations.
    Supports version tracking via logic_version and execution_id.

    Supported actions:
    - run: Run dbt models
    - test: Run dbt tests
    - seed: Load dbt seeds
    - build: Run full dbt build (seeds, models, tests)
    - compile: Compile models without executing
    """

    service_type = "transform"

    def __init__(
        self,
        project_dir: Path | str,
        profiles_dir: Path | str | None = None,
        target: str | None = None,
        default_logic_version: str | None = None,
    ) -> None:
        super().__init__()
        self._project_dir = Path(project_dir)
        self._profiles_dir = Path(profiles_dir) if profiles_dir else None
        self._target = target
        self._default_logic_version = default_logic_version

    def supported_actions(self) -> list[str]:
        """Return list of supported transform actions."""
        return ["run", "test", "seed", "build", "compile"]

    def _handle(self, command: Command) -> Result:
        """Route command to appropriate dbt operation."""
        # Extract version tracking from command metadata (falls back to repo version)
        logic_version = command.metadata.get("logic_version") or self._default_logic_version
        execution_id = command.metadata.get("execution_id", command.correlation_id)

        # Create backend with version tracking config
        config = DbtConfig(
            project_dir=self._project_dir,
            profiles_dir=self._profiles_dir or self._project_dir,
            target=self._target,
            logic_version=logic_version,
            execution_id=execution_id,
        )
        backend = DbtBackend(config)

        match command.action:
            case "run":
                return self._run(command, backend)
            case "test":
                return self._test(command, backend)
            case "seed":
                return self._seed(command, backend)
            case "build":
                return self._build(command, backend)
            case "compile":
                return self._compile(command, backend)
            case _:
                return self._error(command, [f"Unknown action: {command.action}"])

    def _run(self, command: Command, backend: DbtBackend) -> Result:
        """Handle run command."""
        payload = command.payload

        dbt_result = backend.run(
            select=payload.get("select"),
            exclude=payload.get("exclude"),
            full_refresh=payload.get("full_refresh", False),
        )

        return self._dbt_result_to_response(command, dbt_result)

    def _test(self, command: Command, backend: DbtBackend) -> Result:
        """Handle test command."""
        payload = command.payload

        dbt_result = backend.test(
            select=payload.get("select"),
            exclude=payload.get("exclude"),
        )

        return self._dbt_result_to_response(command, dbt_result)

    def _seed(self, command: Command, backend: DbtBackend) -> Result:
        """Handle seed command."""
        payload = command.payload

        dbt_result = backend.seed(
            select=payload.get("select"),
            full_refresh=payload.get("full_refresh", False),
        )

        return self._dbt_result_to_response(command, dbt_result)

    def _build(self, command: Command, backend: DbtBackend) -> Result:
        """Handle build command."""
        payload = command.payload

        dbt_result = backend.build(
            select=payload.get("select"),
            exclude=payload.get("exclude"),
            full_refresh=payload.get("full_refresh", False),
        )

        return self._dbt_result_to_response(command, dbt_result)

    def _compile(self, command: Command, backend: DbtBackend) -> Result:
        """Handle compile command."""
        payload = command.payload

        dbt_result = backend.compile(
            select=payload.get("select"),
        )

        return self._dbt_result_to_response(command, dbt_result)

    def _dbt_result_to_response(
        self,
        command: Command,
        dbt_result: Any,
    ) -> Result:
        """Convert dbt result to handler Result."""
        # Extract model selection from payload for lineage tracking
        payload = command.payload
        select = payload.get("select")
        models = [select] if select else None

        # Track dbt execution for lineage
        execution = ExecutionRecord.dbt(
            command=command.action,
            models=models,
            project_dir=str(self._project_dir),
            target=self._target,
        )

        if dbt_result.success:
            return self._success(
                command,
                data={
                    "command": dbt_result.command,
                    "logic_version": dbt_result.logic_version,
                    "execution_id": dbt_result.execution_id,
                    # Include stdout summary (truncated for large outputs)
                    "output_lines": len(dbt_result.stdout.splitlines()),
                },
                executions=[execution],
                inputs=dbt_result.inputs or None,
                outputs=dbt_result.outputs or None,
            )
        else:
            # Extract useful error info from stderr
            error_lines = dbt_result.stderr.strip().splitlines() if dbt_result.stderr else []
            # Take last few lines which usually contain the actual error
            error_summary = error_lines[-10:] if len(error_lines) > 10 else error_lines

            return self._error(
                command,
                errors=[
                    f"dbt command failed with return code {dbt_result.return_code}",
                    *error_summary,
                ],
            )
