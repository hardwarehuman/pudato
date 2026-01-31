"""Integration tests for table handler (DuckDB)."""

import json
import os

import pytest

from pudato.handlers.table import TableHandler, create_duckdb_table_handler
from pudato.protocol import Command


@pytest.fixture
def table_handler() -> TableHandler:
    """Create TableHandler with in-memory DuckDB."""
    return create_duckdb_table_handler(database_path=None)


@pytest.mark.integration
class TestTableHandler:
    """Test TableHandler with DuckDB backend."""

    def test_create_table(self, table_handler: TableHandler):
        """Test creating a table."""
        command = Command(
            type="table",
            action="create_table",
            payload={
                "name": "test_users",
                "columns": [
                    {"name": "id", "type": "INTEGER", "primary_key": True},
                    {"name": "name", "type": "VARCHAR"},
                    {"name": "email", "type": "VARCHAR", "nullable": False},
                ],
            },
        )

        result = table_handler.handle(command)

        assert result.status == "success"
        assert result.handler == "table"
        assert result.data["table"] == "test_users"
        assert result.data["created"] is True

    def test_create_table_missing_name(self, table_handler: TableHandler):
        """Test error handling for missing table name."""
        command = Command(
            type="table",
            action="create_table",
            payload={
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
        )

        result = table_handler.handle(command)

        assert result.status == "error"
        assert "Missing required field: name" in result.errors[0]

    def test_insert_rows(self, table_handler: TableHandler):
        """Test inserting rows into a table."""
        # Create table first
        table_handler.handle(
            Command(
                type="table",
                action="create_table",
                payload={
                    "name": "products",
                    "columns": [
                        {"name": "id", "type": "INTEGER"},
                        {"name": "name", "type": "VARCHAR"},
                        {"name": "price", "type": "DECIMAL(10,2)"},
                    ],
                },
            )
        )

        # Insert rows
        command = Command(
            type="table",
            action="insert",
            payload={
                "name": "products",
                "rows": [
                    {"id": 1, "name": "Widget", "price": 9.99},
                    {"id": 2, "name": "Gadget", "price": 19.99},
                    {"id": 3, "name": "Gizmo", "price": 29.99},
                ],
            },
        )

        result = table_handler.handle(command)

        assert result.status == "success"
        assert result.data["rows_inserted"] == 3

    def test_upsert_rows(self, table_handler: TableHandler):
        """Test upserting rows (insert or replace)."""
        # Create table with primary key
        table_handler.handle(
            Command(
                type="table",
                action="create_table",
                payload={
                    "name": "items",
                    "columns": [
                        {"name": "id", "type": "INTEGER", "primary_key": True},
                        {"name": "value", "type": "VARCHAR"},
                    ],
                },
            )
        )

        # Initial insert
        table_handler.handle(
            Command(
                type="table",
                action="insert",
                payload={
                    "name": "items",
                    "rows": [{"id": 1, "value": "original"}],
                },
            )
        )

        # Upsert (should update existing row)
        command = Command(
            type="table",
            action="upsert",
            payload={
                "name": "items",
                "rows": [
                    {"id": 1, "value": "updated"},
                    {"id": 2, "value": "new"},
                ],
                "key_columns": ["id"],
            },
        )

        result = table_handler.handle(command)

        assert result.status == "success"
        assert result.data["rows_affected"] == 2

    def test_truncate_table(self, table_handler: TableHandler):
        """Test truncating a table."""
        # Create and populate table
        table_handler.handle(
            Command(
                type="table",
                action="create_table",
                payload={
                    "name": "to_truncate",
                    "columns": [{"name": "id", "type": "INTEGER"}],
                },
            )
        )
        table_handler.handle(
            Command(
                type="table",
                action="insert",
                payload={
                    "name": "to_truncate",
                    "rows": [{"id": 1}, {"id": 2}],
                },
            )
        )

        # Truncate
        command = Command(
            type="table",
            action="truncate",
            payload={"name": "to_truncate"},
        )

        result = table_handler.handle(command)

        assert result.status == "success"
        assert result.data["truncated"] is True

    def test_drop_table(self, table_handler: TableHandler):
        """Test dropping a table."""
        # Create table
        table_handler.handle(
            Command(
                type="table",
                action="create_table",
                payload={
                    "name": "to_drop",
                    "columns": [{"name": "id", "type": "INTEGER"}],
                },
            )
        )

        # Drop it
        command = Command(
            type="table",
            action="drop_table",
            payload={"name": "to_drop"},
        )

        result = table_handler.handle(command)

        assert result.status == "success"
        assert result.data["dropped"] is True

    def test_list_tables(self, table_handler: TableHandler):
        """Test listing tables."""
        # Create some tables
        for name in ["table_a", "table_b", "table_c"]:
            table_handler.handle(
                Command(
                    type="table",
                    action="create_table",
                    payload={
                        "name": name,
                        "columns": [{"name": "id", "type": "INTEGER"}],
                    },
                )
            )

        command = Command(
            type="table",
            action="list_tables",
            payload={"schema": "main"},
        )

        result = table_handler.handle(command)

        assert result.status == "success"
        assert "table_a" in result.data["tables"]
        assert "table_b" in result.data["tables"]
        assert "table_c" in result.data["tables"]

    def test_get_table_info(self, table_handler: TableHandler):
        """Test getting table metadata."""
        # Create and populate table
        table_handler.handle(
            Command(
                type="table",
                action="create_table",
                payload={
                    "name": "info_test",
                    "columns": [
                        {"name": "id", "type": "INTEGER"},
                        {"name": "name", "type": "VARCHAR"},
                    ],
                },
            )
        )
        table_handler.handle(
            Command(
                type="table",
                action="insert",
                payload={
                    "name": "info_test",
                    "rows": [
                        {"id": 1, "name": "Alice"},
                        {"id": 2, "name": "Bob"},
                    ],
                },
            )
        )

        command = Command(
            type="table",
            action="get_info",
            payload={"name": "info_test"},
        )

        result = table_handler.handle(command)

        assert result.status == "success"
        assert result.data["name"] == "info_test"
        assert result.data["row_count"] == 2
        assert len(result.data["columns"]) == 2

    def test_get_info_table_not_found(self, table_handler: TableHandler):
        """Test get_info for non-existent table."""
        command = Command(
            type="table",
            action="get_info",
            payload={"name": "nonexistent"},
        )

        result = table_handler.handle(command)

        assert result.status == "error"
        assert "Table not found" in result.errors[0]

    def test_snapshot_not_supported(self, table_handler: TableHandler):
        """Test that snapshots gracefully indicate not supported."""
        table_handler.handle(
            Command(
                type="table",
                action="create_table",
                payload={
                    "name": "snap_test",
                    "columns": [{"name": "id", "type": "INTEGER"}],
                },
            )
        )

        command = Command(
            type="table",
            action="create_snapshot",
            payload={"name": "snap_test"},
        )

        result = table_handler.handle(command)

        assert result.status == "success"
        assert result.data["snapshot"] is None
        assert "not supported" in result.data["message"]

    def test_list_snapshots_empty(self, table_handler: TableHandler):
        """Test list_snapshots returns empty for DuckDB native."""
        table_handler.handle(
            Command(
                type="table",
                action="create_table",
                payload={
                    "name": "snap_list_test",
                    "columns": [{"name": "id", "type": "INTEGER"}],
                },
            )
        )

        command = Command(
            type="table",
            action="list_snapshots",
            payload={"name": "snap_list_test"},
        )

        result = table_handler.handle(command)

        assert result.status == "success"
        assert result.data["snapshots"] == []

    def test_unsupported_action(self, table_handler: TableHandler):
        """Test error handling for unsupported action."""
        command = Command(
            type="table",
            action="unsupported_action",
            payload={},
        )

        result = table_handler.handle(command)

        assert result.status == "error"
        assert "Unsupported action" in result.errors[0]


@pytest.mark.integration
class TestTableHandlerLambda:
    """Test Table handler via Lambda entry point."""

    def test_lambda_handler_table(self):
        """Test Lambda handler processes table commands."""
        from pudato.runtime.lambda_handler import handle

        os.environ["HANDLER_TYPE"] = "table"
        os.environ.pop("DUCKDB_PATH", None)

        command = Command(
            type="table",
            action="create_table",
            payload={
                "name": "lambda_test",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
        )

        event = {
            "Records": [
                {
                    "messageId": "test-table-123",
                    "body": json.dumps({"Message": command.to_json()}),
                    "receiptHandle": "test-handle",
                }
            ]
        }

        result = handle(event)

        assert result["processed"] == 1
        assert result["results"][0]["status"] == "success"
