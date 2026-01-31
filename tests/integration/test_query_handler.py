"""Integration tests for query handler (DuckDB)."""

import json
import os
from pathlib import Path

import pytest

from pudato.handlers.query import QueryHandler, create_duckdb_handler
from pudato.backends.query import DuckDBBackend
from pudato.protocol import Command


@pytest.fixture
def in_memory_handler() -> QueryHandler:
    """Create QueryHandler with in-memory DuckDB."""
    return create_duckdb_handler(database_path=None)


@pytest.fixture
def dbt_database_handler(dbt_project_dir: Path) -> QueryHandler:
    """Create QueryHandler pointing at the dbt project's DuckDB file."""
    db_path = dbt_project_dir / "pudato.duckdb"
    return create_duckdb_handler(database_path=db_path, read_only=True)


@pytest.fixture
def dbt_project_dir() -> Path:
    """Path to the sample dbt project."""
    return Path(__file__).parent.parent.parent / "dbt"


@pytest.mark.integration
class TestQueryHandler:
    """Test QueryHandler with DuckDB backend."""

    def test_execute_simple_query(self, in_memory_handler: QueryHandler):
        """Test executing a simple SELECT query."""
        command = Command(
            type="query",
            action="execute",
            payload={"sql": "SELECT 1 as num, 'hello' as greeting"},
        )

        result = in_memory_handler.handle(command)

        assert result.status == "success"
        assert result.handler == "query"
        assert result.data is not None
        assert result.data["columns"] == ["num", "greeting"]
        assert result.data["rows"] == [[1, "hello"]]
        assert result.data["row_count"] == 1
        assert result.data["truncated"] is False

    def test_execute_with_parameters(self, in_memory_handler: QueryHandler):
        """Test executing a query with parameters."""
        command = Command(
            type="query",
            action="execute",
            payload={
                "sql": "SELECT $1 as x, $2 as y",
                "parameters": [42, "test"],
            },
        )

        result = in_memory_handler.handle(command)

        assert result.status == "success"
        assert result.data["rows"] == [[42, "test"]]

    def test_execute_missing_sql(self, in_memory_handler: QueryHandler):
        """Test error handling for missing SQL."""
        command = Command(
            type="query",
            action="execute",
            payload={},
        )

        result = in_memory_handler.handle(command)

        assert result.status == "error"
        assert "Missing required field: sql" in result.errors[0]

    def test_execute_invalid_sql(self, in_memory_handler: QueryHandler):
        """Test error handling for invalid SQL."""
        command = Command(
            type="query",
            action="execute",
            payload={"sql": "SELECT * FROM nonexistent_table"},
        )

        result = in_memory_handler.handle(command)

        assert result.status == "error"
        assert "Query execution failed" in result.errors[0]

    def test_execute_with_max_rows(self, in_memory_handler: QueryHandler):
        """Test row limit truncation."""
        command = Command(
            type="query",
            action="execute",
            payload={
                "sql": "SELECT * FROM generate_series(1, 100) as t(n)",
                "max_rows": 10,
            },
        )

        result = in_memory_handler.handle(command)

        assert result.status == "success"
        assert result.data["row_count"] == 10
        assert result.data["truncated"] is True

    def test_execute_script(self, in_memory_handler: QueryHandler):
        """Test executing a multi-statement script."""
        command = Command(
            type="query",
            action="execute_script",
            payload={
                "sql": """
                    CREATE TABLE test_table (id INTEGER, name VARCHAR);
                    INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob');
                """
            },
        )

        result = in_memory_handler.handle(command)

        assert result.status == "success"
        assert result.data["executed"] is True

    def test_execute_script_missing_sql(self, in_memory_handler: QueryHandler):
        """Test error handling for missing SQL in script."""
        command = Command(
            type="query",
            action="execute_script",
            payload={},
        )

        result = in_memory_handler.handle(command)

        assert result.status == "error"
        assert "Missing required field: sql" in result.errors[0]

    def test_unsupported_action(self, in_memory_handler: QueryHandler):
        """Test error handling for unsupported action."""
        command = Command(
            type="query",
            action="unsupported_action",
            payload={},
        )

        result = in_memory_handler.handle(command)

        assert result.status == "error"
        assert "Unsupported action" in result.errors[0]

    def test_supported_actions(self, in_memory_handler: QueryHandler):
        """Test that supported_actions returns correct list."""
        actions = in_memory_handler.supported_actions()

        assert "execute" in actions
        assert "execute_script" in actions


@pytest.mark.integration
class TestQueryHandlerWithDbtData:
    """Test QueryHandler against dbt-generated data."""

    def test_query_dbt_output(
        self,
        dbt_database_handler: QueryHandler,
        dbt_project_dir: Path,
    ):
        """Test querying data from dbt models.

        This test requires dbt build to have run first.
        """
        # First ensure dbt has been built
        from pudato.handlers.transform import TransformHandler

        transform = TransformHandler(
            project_dir=dbt_project_dir,
            profiles_dir=dbt_project_dir,
        )
        build_result = transform.handle(
            Command(type="transform", action="build", payload={})
        )
        assert build_result.status == "success"

        # Now query the output - need a new handler to see the data
        handler = create_duckdb_handler(
            database_path=dbt_project_dir / "pudato.duckdb",
            read_only=True,
        )

        command = Command(
            type="query",
            action="execute",
            payload={
                "sql": "SELECT COUNT(*) as cnt FROM main.stg_departments",
            },
        )

        result = handler.handle(command)

        assert result.status == "success"
        assert result.data["rows"][0][0] > 0  # Has some rows


@pytest.mark.integration
class TestQueryHandlerLambda:
    """Test Query handler via Lambda entry point."""

    def test_lambda_handler_query(self):
        """Test Lambda handler processes query commands."""
        from pudato.runtime.lambda_handler import handle

        # Set up environment for in-memory DuckDB
        os.environ["HANDLER_TYPE"] = "query"
        os.environ.pop("DUCKDB_PATH", None)  # Ensure in-memory

        command = Command(
            type="query",
            action="execute",
            payload={"sql": "SELECT 42 as answer"},
        )

        event = {
            "Records": [
                {
                    "messageId": "test-query-123",
                    "body": json.dumps({"Message": command.to_json()}),
                    "receiptHandle": "test-handle",
                }
            ]
        }

        result = handle(event)

        assert result["processed"] == 1
        assert result["results"][0]["status"] == "success"
