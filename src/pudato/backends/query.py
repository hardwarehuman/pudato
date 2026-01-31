"""Query backend protocol and implementations."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

import duckdb
import structlog

logger = structlog.get_logger()


class QueryResult:
    """Result of a query execution."""

    def __init__(
        self,
        columns: list[str],
        rows: list[tuple[Any, ...]],
        row_count: int,
        truncated: bool = False,
    ) -> None:
        self.columns = columns
        self.rows = rows
        self.row_count = row_count
        self.truncated = truncated

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "columns": self.columns,
            "rows": [list(row) for row in self.rows],
            "row_count": self.row_count,
            "truncated": self.truncated,
        }


class QueryBackend(Protocol):
    """Protocol for query execution backends.

    Implementations handle SQL query execution for different
    backends (DuckDB, Athena, Snowflake, etc.).
    """

    def execute(
        self,
        sql: str,
        parameters: dict[str, Any] | None = None,
        max_rows: int = 1000,
    ) -> QueryResult:
        """Execute a SQL query.

        Args:
            sql: SQL query string
            parameters: Optional query parameters
            max_rows: Maximum rows to return (prevents memory issues)

        Returns:
            QueryResult with columns, rows, and metadata
        """
        ...

    def execute_many(
        self,
        sql: str,
        parameters: list[dict[str, Any]],
    ) -> int:
        """Execute a SQL statement with multiple parameter sets.

        Args:
            sql: SQL statement
            parameters: List of parameter dicts

        Returns:
            Number of rows affected
        """
        ...


class DuckDBBackend:
    """DuckDB query backend for local development.

    Connects to a DuckDB database file and executes queries.
    """

    dialect = "duckdb"

    def __init__(
        self,
        database_path: Path | str | None = None,
        read_only: bool = False,
    ) -> None:
        """Initialize DuckDB backend.

        Args:
            database_path: Path to DuckDB file. None for in-memory.
            read_only: Whether to open in read-only mode.
        """
        self._database_path = str(database_path) if database_path else ":memory:"
        self._read_only = read_only
        self._log = logger.bind(backend="duckdb", database=self._database_path)

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get a database connection."""
        return duckdb.connect(self._database_path, read_only=self._read_only)

    def execute(
        self,
        sql: str,
        parameters: dict[str, Any] | None = None,
        max_rows: int = 1000,
    ) -> QueryResult:
        """Execute a SQL query."""
        self._log.debug("executing_query", sql=sql[:100], max_rows=max_rows)

        conn = self._get_connection()
        try:
            if parameters:
                # DuckDB uses $name for named parameters
                result = conn.execute(sql, parameters)
            else:
                result = conn.execute(sql)

            # Get column names
            columns = [desc[0] for desc in result.description] if result.description else []

            # Fetch rows with limit
            rows = result.fetchmany(max_rows + 1)
            truncated = len(rows) > max_rows
            if truncated:
                rows = rows[:max_rows]

            row_count = len(rows)

            self._log.debug("query_complete", row_count=row_count, truncated=truncated)

            return QueryResult(
                columns=columns,
                rows=rows,
                row_count=row_count,
                truncated=truncated,
            )
        finally:
            conn.close()

    def execute_many(
        self,
        sql: str,
        parameters: list[dict[str, Any]],
    ) -> int:
        """Execute a SQL statement with multiple parameter sets."""
        self._log.debug("executing_many", sql=sql[:100], param_count=len(parameters))

        conn = self._get_connection()
        try:
            total_affected = 0
            for params in parameters:
                conn.execute(sql, params)
                total_affected += 1

            self._log.debug("execute_many_complete", affected=total_affected)
            return total_affected
        finally:
            conn.close()

    def execute_script(self, sql: str) -> None:
        """Execute a SQL script (multiple statements).

        Useful for DDL operations like creating tables.
        """
        self._log.debug("executing_script", sql_length=len(sql))

        conn = self._get_connection()
        try:
            conn.execute(sql)
        finally:
            conn.close()
