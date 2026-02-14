"""Table handler for table operations."""

from __future__ import annotations

from pathlib import Path

from pudato.backends.table import (
    ColumnDef,
    DuckDBTableBackend,
    TableBackend,
    TableSchema,
)
from pudato.handlers.base import BaseHandler
from pudato.protocol import Command, DataReference, Result


class TableHandler(BaseHandler):
    """Handler for table operations (DDL and DML).

    Translates standardized Command messages into table operations.
    Supports different backends (DuckDB native, future Iceberg/DuckLake).

    Supported actions:
    - create_table: Create a new table with schema
    - drop_table: Drop a table
    - insert: Insert rows into a table
    - upsert: Insert or update rows by key
    - truncate: Remove all rows from a table
    - list_tables: List tables in database
    - get_info: Get table metadata

    Future (with Iceberg backend):
    - create_snapshot: Create a table snapshot
    - list_snapshots: List available snapshots
    - time_travel: Query at a specific snapshot
    """

    service_type = "table"

    def __init__(self, backend: TableBackend) -> None:
        super().__init__()
        self._backend = backend

    def supported_actions(self) -> list[str]:
        """Return list of supported table actions."""
        return [
            "create_table",
            "drop_table",
            "insert",
            "upsert",
            "truncate",
            "list_tables",
            "get_info",
            "create_snapshot",
            "list_snapshots",
        ]

    def _handle(self, command: Command) -> Result:
        """Route command to appropriate table operation."""
        match command.action:
            case "create_table":
                return self._create_table(command)
            case "drop_table":
                return self._drop_table(command)
            case "insert":
                return self._insert(command)
            case "upsert":
                return self._upsert(command)
            case "truncate":
                return self._truncate(command)
            case "list_tables":
                return self._list_tables(command)
            case "get_info":
                return self._get_info(command)
            case "create_snapshot":
                return self._create_snapshot(command)
            case "list_snapshots":
                return self._list_snapshots(command)
            case _:
                return self._error(command, [f"Unsupported action: {command.action}"])

    def _create_table(self, command: Command) -> Result:
        """Handle create_table command."""
        payload = command.payload

        name = payload.get("name")
        if not name:
            return self._error(command, ["Missing required field: name"])

        columns_data = payload.get("columns")
        if not columns_data:
            return self._error(command, ["Missing required field: columns"])

        try:
            # Parse column definitions
            columns = [
                ColumnDef(
                    name=col["name"],
                    dtype=col["type"],
                    nullable=col.get("nullable", True),
                    primary_key=col.get("primary_key", False),
                )
                for col in columns_data
            ]
            schema = TableSchema(columns=columns)

            self._backend.create_table(
                name=name,
                schema=schema,
                if_not_exists=payload.get("if_not_exists", True),
            )

            # Track output for lineage
            output_ref = DataReference(ref_type="table", location=name)

            return self._success(
                command,
                data={"table": name, "created": True},
                outputs=[output_ref],
            )
        except Exception as e:
            return self._error(command, [f"Failed to create table: {e!s}"])

    def _drop_table(self, command: Command) -> Result:
        """Handle drop_table command."""
        payload = command.payload

        name = payload.get("name")
        if not name:
            return self._error(command, ["Missing required field: name"])

        try:
            self._backend.drop_table(
                name=name,
                if_exists=payload.get("if_exists", True),
            )

            return self._success(
                command,
                data={"table": name, "dropped": True},
            )
        except Exception as e:
            return self._error(command, [f"Failed to drop table: {e!s}"])

    def _insert(self, command: Command) -> Result:
        """Handle insert command."""
        payload = command.payload

        name = payload.get("name")
        if not name:
            return self._error(command, ["Missing required field: name"])

        rows = payload.get("rows")
        if rows is None:
            return self._error(command, ["Missing required field: rows"])

        try:
            count = self._backend.insert(name=name, rows=rows)

            # Track output for lineage
            output_ref = DataReference(ref_type="table", location=name)

            return self._success(
                command,
                data={"table": name, "rows_inserted": count},
                outputs=[output_ref],
            )
        except Exception as e:
            return self._error(command, [f"Failed to insert rows: {e!s}"])

    def _upsert(self, command: Command) -> Result:
        """Handle upsert command."""
        payload = command.payload

        name = payload.get("name")
        if not name:
            return self._error(command, ["Missing required field: name"])

        rows = payload.get("rows")
        if rows is None:
            return self._error(command, ["Missing required field: rows"])

        key_columns = payload.get("key_columns")
        if not key_columns:
            return self._error(command, ["Missing required field: key_columns"])

        try:
            count = self._backend.upsert(
                name=name,
                rows=rows,
                key_columns=key_columns,
            )

            # Track output for lineage
            output_ref = DataReference(ref_type="table", location=name)

            return self._success(
                command,
                data={"table": name, "rows_affected": count},
                outputs=[output_ref],
            )
        except Exception as e:
            return self._error(command, [f"Failed to upsert rows: {e!s}"])

    def _truncate(self, command: Command) -> Result:
        """Handle truncate command."""
        payload = command.payload

        name = payload.get("name")
        if not name:
            return self._error(command, ["Missing required field: name"])

        try:
            self._backend.truncate(name=name)

            return self._success(
                command,
                data={"table": name, "truncated": True},
            )
        except Exception as e:
            return self._error(command, [f"Failed to truncate table: {e!s}"])

    def _list_tables(self, command: Command) -> Result:
        """Handle list_tables command."""
        payload = command.payload
        schema = payload.get("schema", "main")

        try:
            tables = self._backend.list_tables(schema=schema)

            return self._success(
                command,
                data={"schema": schema, "tables": tables},
            )
        except Exception as e:
            return self._error(command, [f"Failed to list tables: {e!s}"])

    def _get_info(self, command: Command) -> Result:
        """Handle get_info command."""
        payload = command.payload

        name = payload.get("name")
        if not name:
            return self._error(command, ["Missing required field: name"])

        try:
            info = self._backend.get_table_info(name=name)

            if info is None:
                return self._error(command, [f"Table not found: {name}"])

            return self._success(
                command,
                data={
                    "name": info.name,
                    "schema": info.schema,
                    "columns": info.columns,
                    "row_count": info.row_count,
                },
            )
        except Exception as e:
            return self._error(command, [f"Failed to get table info: {e!s}"])

    def _create_snapshot(self, command: Command) -> Result:
        """Handle create_snapshot command.

        Note: Only functional with Iceberg backend. DuckDB native returns null.
        """
        payload = command.payload

        name = payload.get("name")
        if not name:
            return self._error(command, ["Missing required field: name"])

        try:
            snapshot = self._backend.create_snapshot(name=name)

            if snapshot is None:
                return self._success(
                    command,
                    data={
                        "table": name,
                        "snapshot": None,
                        "message": "Snapshots not supported by this backend",
                    },
                )

            return self._success(
                command,
                data={
                    "table": name,
                    "snapshot_id": snapshot.snapshot_id,
                    "timestamp": snapshot.timestamp,
                },
            )
        except Exception as e:
            return self._error(command, [f"Failed to create snapshot: {e!s}"])

    def _list_snapshots(self, command: Command) -> Result:
        """Handle list_snapshots command.

        Note: Only functional with Iceberg backend. DuckDB native returns empty.
        """
        payload = command.payload

        name = payload.get("name")
        if not name:
            return self._error(command, ["Missing required field: name"])

        try:
            snapshots = self._backend.list_snapshots(name=name)

            return self._success(
                command,
                data={
                    "table": name,
                    "snapshots": [
                        {"snapshot_id": s.snapshot_id, "timestamp": s.timestamp} for s in snapshots
                    ],
                },
            )
        except Exception as e:
            return self._error(command, [f"Failed to list snapshots: {e!s}"])


def create_duckdb_table_handler(
    database_path: Path | str | None = None,
    read_only: bool = False,
) -> TableHandler:
    """Factory function to create TableHandler with DuckDB backend.

    Args:
        database_path: Path to DuckDB file. None for in-memory.
        read_only: Whether to open in read-only mode.

    Returns:
        TableHandler configured with DuckDB backend.
    """
    backend = DuckDBTableBackend(database_path=database_path, read_only=read_only)
    return TableHandler(backend=backend)
