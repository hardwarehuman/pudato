"""Table backend protocol and implementations.

Abstracts table format operations to allow swapping between:
- DuckDB native tables (MVP)
- Apache Iceberg (production, via pyiceberg)
- DuckLake (future, when mature)
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import duckdb
import structlog

logger = structlog.get_logger()


@dataclass
class TableSchema:
    """Schema definition for a table."""

    columns: list[ColumnDef]

    def to_sql(self) -> str:
        """Generate SQL column definitions."""
        return ", ".join(col.to_sql() for col in self.columns)


@dataclass
class ColumnDef:
    """Column definition."""

    name: str
    dtype: str  # SQL type string (e.g., "INTEGER", "VARCHAR", "TIMESTAMP")
    nullable: bool = True
    primary_key: bool = False

    def to_sql(self) -> str:
        """Generate SQL column definition."""
        parts = [self.name, self.dtype]
        if self.primary_key:
            parts.append("PRIMARY KEY")
        elif not self.nullable:
            parts.append("NOT NULL")
        return " ".join(parts)


@dataclass
class TableInfo:
    """Information about a table."""

    name: str
    schema: str  # database schema (e.g., "main")
    columns: list[dict[str, Any]]  # column name -> type info
    row_count: int | None = None


@dataclass
class Snapshot:
    """Table snapshot for time travel (stub for future Iceberg support)."""

    snapshot_id: str
    timestamp: str
    # For DuckDB native, this is a no-op placeholder
    # For Iceberg, this would contain actual snapshot metadata


class TableBackend(Protocol):
    """Protocol for table format operations.

    Abstracts away the underlying table format (DuckDB native, Iceberg, DuckLake)
    to allow swapping implementations without changing handler code.

    MVP: DuckDB native tables
    Future: Iceberg for production (time travel, ACID), DuckLake when mature
    """

    def create_table(
        self,
        name: str,
        schema: TableSchema,
        if_not_exists: bool = True,
    ) -> None:
        """Create a table with the given schema."""
        ...

    def drop_table(self, name: str, if_exists: bool = True) -> None:
        """Drop a table."""
        ...

    def insert(
        self,
        name: str,
        rows: list[dict[str, Any]],
    ) -> int:
        """Insert rows into a table. Returns number of rows inserted."""
        ...

    def upsert(
        self,
        name: str,
        rows: list[dict[str, Any]],
        key_columns: list[str],
    ) -> int:
        """Upsert rows (insert or update on key conflict). Returns rows affected."""
        ...

    def truncate(self, name: str) -> None:
        """Remove all rows from a table."""
        ...

    def list_tables(self, schema: str = "main") -> list[str]:
        """List tables in a schema."""
        ...

    def get_table_info(self, name: str) -> TableInfo | None:
        """Get information about a table."""
        ...

    # Future: Snapshot operations for Iceberg/time travel
    def create_snapshot(self, name: str) -> Snapshot | None:
        """Create a snapshot of the table (no-op for DuckDB native)."""
        ...

    def list_snapshots(self, name: str) -> list[Snapshot]:
        """List snapshots for a table (empty for DuckDB native)."""
        ...


class DuckDBTableBackend:
    """DuckDB native table backend.

    Uses standard DuckDB tables without a special format.
    Provides the TableBackend interface for consistency with future
    Iceberg/DuckLake implementations.

    Note: Snapshot operations are stubs - DuckDB native tables don't
    support time travel. Use Iceberg backend for that functionality.
    """

    def __init__(
        self,
        database_path: Path | str | None = None,
        read_only: bool = False,
    ) -> None:
        """Initialize DuckDB table backend.

        Args:
            database_path: Path to DuckDB file. None for in-memory.
            read_only: Whether to open in read-only mode.
        """
        self._database_path = str(database_path) if database_path else ":memory:"
        self._read_only = read_only
        self._log = logger.bind(backend="duckdb_table", database=self._database_path)
        # For in-memory databases, we need a persistent connection
        # since each connect() to :memory: creates a new database
        self._persistent_conn: duckdb.DuckDBPyConnection | None = None
        if self._database_path == ":memory:":
            self._persistent_conn = duckdb.connect(":memory:")

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get a database connection."""
        if self._persistent_conn is not None:
            return self._persistent_conn
        return duckdb.connect(self._database_path, read_only=self._read_only)

    def _close_if_not_persistent(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Close connection if it's not the persistent one."""
        if conn is not self._persistent_conn:
            conn.close()

    def create_table(
        self,
        name: str,
        schema: TableSchema,
        if_not_exists: bool = True,
    ) -> None:
        """Create a table with the given schema."""
        exists_clause = "IF NOT EXISTS " if if_not_exists else ""
        sql = f"CREATE TABLE {exists_clause}{name} ({schema.to_sql()})"

        self._log.debug("creating_table", name=name, sql=sql)

        conn = self._get_connection()
        try:
            conn.execute(sql)
        finally:
            self._close_if_not_persistent(conn)

    def drop_table(self, name: str, if_exists: bool = True) -> None:
        """Drop a table."""
        exists_clause = "IF EXISTS " if if_exists else ""
        sql = f"DROP TABLE {exists_clause}{name}"

        self._log.debug("dropping_table", name=name)

        conn = self._get_connection()
        try:
            conn.execute(sql)
        finally:
            self._close_if_not_persistent(conn)

    def insert(
        self,
        name: str,
        rows: list[dict[str, Any]],
    ) -> int:
        """Insert rows into a table."""
        if not rows:
            return 0

        # Get column names from first row
        columns = list(rows[0].keys())
        placeholders = ", ".join(["?" for _ in columns])
        column_list = ", ".join(columns)

        sql = f"INSERT INTO {name} ({column_list}) VALUES ({placeholders})"

        self._log.debug("inserting_rows", name=name, row_count=len(rows))

        conn = self._get_connection()
        try:
            for row in rows:
                values = [row[col] for col in columns]
                conn.execute(sql, values)
            return len(rows)
        finally:
            self._close_if_not_persistent(conn)

    def upsert(
        self,
        name: str,
        rows: list[dict[str, Any]],
        key_columns: list[str],
    ) -> int:
        """Upsert rows using INSERT OR REPLACE."""
        if not rows:
            return 0

        # Get column names from first row
        columns = list(rows[0].keys())
        placeholders = ", ".join(["?" for _ in columns])
        column_list = ", ".join(columns)

        # DuckDB uses INSERT OR REPLACE for upsert
        sql = f"INSERT OR REPLACE INTO {name} ({column_list}) VALUES ({placeholders})"

        self._log.debug(
            "upserting_rows",
            name=name,
            row_count=len(rows),
            key_columns=key_columns,
        )

        conn = self._get_connection()
        try:
            for row in rows:
                values = [row[col] for col in columns]
                conn.execute(sql, values)
            return len(rows)
        finally:
            self._close_if_not_persistent(conn)

    def truncate(self, name: str) -> None:
        """Remove all rows from a table."""
        # DuckDB doesn't have TRUNCATE, use DELETE
        sql = f"DELETE FROM {name}"

        self._log.debug("truncating_table", name=name)

        conn = self._get_connection()
        try:
            conn.execute(sql)
        finally:
            self._close_if_not_persistent(conn)

    def list_tables(self, schema: str = "main") -> list[str]:
        """List tables in a schema."""
        sql = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = ?
            ORDER BY table_name
        """

        conn = self._get_connection()
        try:
            result = conn.execute(sql, [schema])
            return [row[0] for row in result.fetchall()]
        finally:
            self._close_if_not_persistent(conn)

    def get_table_info(self, name: str) -> TableInfo | None:
        """Get information about a table."""
        # Parse schema.table if provided
        if "." in name:
            schema, table_name = name.split(".", 1)
        else:
            schema, table_name = "main", name

        # Get column info
        sql = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = ? AND table_name = ?
            ORDER BY ordinal_position
        """

        conn = self._get_connection()
        try:
            result = conn.execute(sql, [schema, table_name])
            columns = [
                {"name": row[0], "type": row[1], "nullable": row[2] == "YES"}
                for row in result.fetchall()
            ]

            if not columns:
                return None

            # Get row count
            count_result = conn.execute(f"SELECT COUNT(*) FROM {name}")
            row_count = count_result.fetchone()[0]

            return TableInfo(
                name=table_name,
                schema=schema,
                columns=columns,
                row_count=row_count,
            )
        finally:
            self._close_if_not_persistent(conn)

    def create_snapshot(self, name: str) -> Snapshot | None:
        """Create a snapshot (no-op for DuckDB native tables).

        DuckDB native tables don't support snapshots/time travel.
        Returns None to indicate no snapshot was created.
        For time travel functionality, use IcebergTableBackend.
        """
        self._log.debug(
            "snapshot_not_supported",
            name=name,
            message="DuckDB native tables don't support snapshots",
        )
        return None

    def list_snapshots(self, name: str) -> list[Snapshot]:
        """List snapshots (empty for DuckDB native tables).

        DuckDB native tables don't support snapshots/time travel.
        Returns empty list.
        """
        return []
