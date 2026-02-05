"""Integration tests for transform handler (dbt)."""

import os
from pathlib import Path

import pytest

from pudato.handlers.transform import TransformHandler
from pudato.protocol import Command


@pytest.fixture
def dbt_project_dir() -> Path:
    """Path to the sample dbt project."""
    return Path(__file__).parent.parent.parent / "dbt"


@pytest.fixture
def transform_handler(dbt_project_dir: Path) -> TransformHandler:
    """Create TransformHandler with sample dbt project."""
    return TransformHandler(
        project_dir=dbt_project_dir,
        profiles_dir=dbt_project_dir,
    )


@pytest.mark.integration
class TestTransformHandler:
    """Test TransformHandler with dbt."""

    def test_seed(self, transform_handler: TransformHandler):
        """Test running dbt seed extracts table outputs."""
        command = Command(
            type="transform",
            action="seed",
            payload={},
            metadata={"logic_version": "test-abc123", "execution_id": "exec-001"},
        )

        result = transform_handler.handle(command)

        assert result.status == "success"
        assert result.handler == "transform"
        assert result.data is not None
        assert result.data["logic_version"] == "test-abc123"
        assert result.data["execution_id"] == "exec-001"

        # Seeds produce outputs (the seed tables)
        assert len(result.outputs) >= 1
        output_locations = [o.location for o in result.outputs]
        assert any("raw_departments" in loc or "raw_expenditures" in loc for loc in output_locations)

    def test_run(self, transform_handler: TransformHandler):
        """Test running dbt models."""
        # First seed to ensure data exists
        seed_command = Command(
            type="transform",
            action="seed",
            payload={},
        )
        transform_handler.handle(seed_command)

        # Then run models
        command = Command(
            type="transform",
            action="run",
            payload={},
            metadata={"logic_version": "v1.2.3"},
        )

        result = transform_handler.handle(command)

        assert result.status == "success"
        assert result.data is not None
        assert "dbt" in result.data["command"]

    def test_run_select_model(self, transform_handler: TransformHandler):
        """Test running a specific model extracts table-level lineage."""
        # First seed
        seed_command = Command(type="transform", action="seed", payload={})
        transform_handler.handle(seed_command)

        # Run specific model
        command = Command(
            type="transform",
            action="run",
            payload={"select": "stg_departments"},
        )

        result = transform_handler.handle(command)

        assert result.status == "success"

        # Verify outputs contain the model we ran
        assert len(result.outputs) >= 1
        output_locations = [o.location for o in result.outputs]
        assert any("stg_departments" in loc for loc in output_locations)

        # Verify inputs contain upstream dependency (raw_departments seed)
        assert len(result.inputs) >= 1
        input_locations = [i.location for i in result.inputs]
        assert any("raw_departments" in loc for loc in input_locations)

        # All refs should be tables
        for ref in result.outputs + result.inputs:
            assert ref.ref_type == "table"

    def test_test(self, transform_handler: TransformHandler):
        """Test running dbt tests."""
        # Seed and run first
        transform_handler.handle(Command(type="transform", action="seed", payload={}))
        transform_handler.handle(Command(type="transform", action="run", payload={}))

        # Run tests
        command = Command(
            type="transform",
            action="test",
            payload={},
        )

        result = transform_handler.handle(command)

        assert result.status == "success"

    def test_build(self, transform_handler: TransformHandler):
        """Test dbt build (seeds + models + tests)."""
        command = Command(
            type="transform",
            action="build",
            payload={},
            metadata={"logic_version": "build-test"},
        )

        result = transform_handler.handle(command)

        assert result.status == "success"
        assert result.data["logic_version"] == "build-test"

    def test_compile(self, transform_handler: TransformHandler):
        """Test dbt compile."""
        command = Command(
            type="transform",
            action="compile",
            payload={},
        )

        result = transform_handler.handle(command)

        assert result.status == "success"

    def test_unsupported_action(self, transform_handler: TransformHandler):
        """Test error handling for unsupported action."""
        command = Command(
            type="transform",
            action="unsupported_action",
            payload={},
        )

        result = transform_handler.handle(command)

        assert result.status == "error"
        assert "Unsupported action" in result.errors[0]

    def test_version_tracking_in_output(self, transform_handler: TransformHandler, dbt_project_dir: Path):
        """Test that version tracking vars are passed to dbt and appear in model output."""
        import duckdb

        # Run build with version tracking
        command = Command(
            type="transform",
            action="build",
            payload={"select": "dept_budget_summary"},
            metadata={
                "logic_version": "commit-xyz789",
                "execution_id": "exec-test-123",
            },
        )

        result = transform_handler.handle(command)
        assert result.status == "success"

        # Query the output table to verify version columns
        db_path = dbt_project_dir / "pudato.duckdb"
        conn = duckdb.connect(str(db_path), read_only=True)

        try:
            row = conn.execute(
                "SELECT _pudato_logic_version, _pudato_execution_id FROM main.dept_budget_summary LIMIT 1"
            ).fetchone()

            assert row is not None
            assert row[0] == "commit-xyz789"
            assert row[1] == "exec-test-123"
        finally:
            conn.close()


@pytest.mark.integration
class TestTransformHandlerLambda:
    """Test Transform handler via Lambda entry point."""

    def test_lambda_handler_transform(self, dbt_project_dir: Path):
        """Test Lambda handler processes transform commands."""
        import json

        from pudato.runtime.lambda_handler import handle

        # Set up environment
        os.environ["HANDLER_TYPE"] = "transform"
        os.environ["DBT_PROJECT_DIR"] = str(dbt_project_dir)
        os.environ["DBT_PROFILES_DIR"] = str(dbt_project_dir)

        command = Command(
            type="transform",
            action="build",
            payload={},
            metadata={"logic_version": "lambda-test"},
        )

        event = {
            "Records": [
                {
                    "messageId": "test-transform-123",
                    "body": json.dumps({"Message": command.to_json()}),
                    "receiptHandle": "test-handle",
                }
            ]
        }

        result = handle(event)

        assert result["processed"] == 1
        assert result["results"][0]["status"] == "success"
