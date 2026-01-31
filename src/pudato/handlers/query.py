"""Query handler for SQL operations."""

from __future__ import annotations

from pathlib import Path

from pudato.backends.query import DuckDBBackend, QueryBackend
from pudato.handlers.base import BaseHandler
from pudato.protocol import Command, ExecutionRecord, Result


class QueryHandler(BaseHandler):
    """Handler for query operations (SQL execution).

    Translates standardized Command messages into SQL operations.
    Supports different backends (DuckDB local, Athena AWS).

    Supported actions:
    - execute: Execute a SQL query and return results (sync for DuckDB)
    - execute_script: Execute multiple SQL statements (DDL)

    Future async pattern (for long-running warehouse queries):
    - execute_async: Submit query, return execution_id immediately
    - check_status: Poll for query completion by execution_id

    Long-running queries (e.g., hour-long ETL in Redshift/Athena) require
    the async pattern: Lambda confirms query submission, then schedules
    a follow-up handler (via EventBridge/SQS delay) to poll for completion.
    This will be implemented when we add warehouse backends.
    """

    service_type = "query"

    def __init__(self, backend: QueryBackend) -> None:
        super().__init__()
        self._backend = backend

    def supported_actions(self) -> list[str]:
        """Return list of supported query actions."""
        return ["execute", "execute_script"]

    def _handle(self, command: Command) -> Result:
        """Route command to appropriate query operation."""
        match command.action:
            case "execute":
                return self._execute(command)
            case "execute_script":
                return self._execute_script(command)
            case _:
                return self._error(command, [f"Unsupported action: {command.action}"])

    def _execute(self, command: Command) -> Result:
        """Handle execute command - run SQL and return results.

        For DuckDB (local dev), this is synchronous.
        For warehouse backends (Athena, Redshift), this would need to be
        async with follow-up polling - see execute_async pattern in docstring.
        """
        payload = command.payload

        sql = payload.get("sql")
        if not sql:
            return self._error(command, ["Missing required field: sql"])

        parameters = payload.get("parameters")
        max_rows = payload.get("max_rows", 1000)

        try:
            query_result = self._backend.execute(
                sql=sql,
                parameters=parameters,
                max_rows=max_rows,
            )

            # Track SQL execution for lineage
            execution = ExecutionRecord.sql(
                statements=[sql],
                dialect=self._backend.dialect if hasattr(self._backend, "dialect") else "sql",
                parameters=parameters,
            )

            return self._success(
                command,
                data=query_result.to_dict(),
                executions=[execution],
            )
        except Exception as e:
            return self._error(command, [f"Query execution failed: {e!s}"])

    def _execute_script(self, command: Command) -> Result:
        """Handle execute_script command - run multiple SQL statements."""
        payload = command.payload

        sql = payload.get("sql")
        if not sql:
            return self._error(command, ["Missing required field: sql"])

        try:
            # execute_script is only available on DuckDBBackend
            if hasattr(self._backend, "execute_script"):
                self._backend.execute_script(sql)
            else:
                return self._error(
                    command,
                    ["execute_script not supported by this backend"],
                )

            # Track SQL execution for lineage
            execution = ExecutionRecord.sql(
                statements=[sql],
                dialect=self._backend.dialect if hasattr(self._backend, "dialect") else "sql",
            )

            return self._success(
                command,
                data={"executed": True},
                executions=[execution],
            )
        except Exception as e:
            return self._error(command, [f"Script execution failed: {e!s}"])


def create_duckdb_handler(
    database_path: Path | str | None = None,
    read_only: bool = False,
) -> QueryHandler:
    """Factory function to create QueryHandler with DuckDB backend.

    Args:
        database_path: Path to DuckDB file. None for in-memory.
        read_only: Whether to open in read-only mode.

    Returns:
        QueryHandler configured with DuckDB backend.
    """
    backend = DuckDBBackend(database_path=database_path, read_only=read_only)
    return QueryHandler(backend=backend)
