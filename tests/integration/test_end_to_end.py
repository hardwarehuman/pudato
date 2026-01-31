"""End-to-end integration tests: storage → transform → query flow.

These tests demonstrate the complete data pipeline:
1. Storage: Upload raw data (CSV seed files already in dbt/seeds/)
2. Transform: Run dbt to process data with version tracking
3. Query: Execute SQL against the transformed data
"""

from pathlib import Path

import pytest

from pudato.handlers.query import create_duckdb_handler
from pudato.handlers.transform import TransformHandler
from pudato.protocol import Command


@pytest.fixture
def dbt_project_dir() -> Path:
    """Path to the sample dbt project."""
    return Path(__file__).parent.parent.parent / "dbt"


@pytest.mark.integration
class TestEndToEndFlow:
    """Test complete storage → transform → query pipeline."""

    def test_full_pipeline_with_version_tracking(self, dbt_project_dir: Path):
        """Test the full ELT flow with version tracking.

        Simulates:
        1. Raw data already in dbt/seeds/ (storage step done offline)
        2. Transform: dbt build processes seeds → staging → marts
        3. Query: Verify transformed data with version columns
        """
        # Version tracking metadata (simulating Airflow passing commit hash)
        logic_version = "e2e-test-abc123"
        execution_id = "exec-e2e-001"

        # --- Step 1: Transform (dbt build) ---
        transform_handler = TransformHandler(
            project_dir=dbt_project_dir,
            profiles_dir=dbt_project_dir,
        )

        transform_command = Command(
            type="transform",
            action="build",
            payload={},  # Build all models
            metadata={
                "logic_version": logic_version,
                "execution_id": execution_id,
            },
        )

        transform_result = transform_handler.handle(transform_command)

        assert transform_result.status == "success", (
            f"Transform failed: {transform_result.errors}"
        )
        assert transform_result.data["logic_version"] == logic_version
        assert transform_result.data["execution_id"] == execution_id

        # --- Step 2: Query transformed data ---
        query_handler = create_duckdb_handler(
            database_path=dbt_project_dir / "pudato.duckdb",
            read_only=True,
        )

        # Query the final mart model
        query_command = Command(
            type="query",
            action="execute",
            payload={
                "sql": """
                    SELECT
                        department_name,
                        budget_amount,
                        total_spent,
                        _pudato_logic_version,
                        _pudato_execution_id
                    FROM main.dept_budget_summary
                    ORDER BY budget_amount DESC
                """
            },
        )

        query_result = query_handler.handle(query_command)

        assert query_result.status == "success", (
            f"Query failed: {query_result.errors}"
        )

        # Verify data structure
        data = query_result.data
        assert "department_name" in data["columns"]
        assert "budget_amount" in data["columns"]
        assert "_pudato_logic_version" in data["columns"]
        assert "_pudato_execution_id" in data["columns"]

        # Verify we got results
        assert data["row_count"] > 0

        # Verify version tracking columns are populated
        # Find column indices
        version_idx = data["columns"].index("_pudato_logic_version")
        exec_idx = data["columns"].index("_pudato_execution_id")

        first_row = data["rows"][0]
        assert first_row[version_idx] == logic_version
        assert first_row[exec_idx] == execution_id

    def test_selective_model_rebuild(self, dbt_project_dir: Path):
        """Test running specific models and querying results."""
        # Run just the staging models
        transform_handler = TransformHandler(
            project_dir=dbt_project_dir,
            profiles_dir=dbt_project_dir,
        )

        # First seed, then run specific staging model
        transform_handler.handle(
            Command(type="transform", action="seed", payload={})
        )

        run_result = transform_handler.handle(
            Command(
                type="transform",
                action="run",
                payload={"select": "stg_departments"},
                metadata={"logic_version": "selective-v1"},
            )
        )

        assert run_result.status == "success"

        # Query the staging model
        query_handler = create_duckdb_handler(
            database_path=dbt_project_dir / "pudato.duckdb",
            read_only=True,
        )

        result = query_handler.handle(
            Command(
                type="query",
                action="execute",
                payload={"sql": "SELECT COUNT(*) as cnt FROM main.stg_departments"},
            )
        )

        assert result.status == "success"
        assert result.data["rows"][0][0] > 0

    def test_query_aggregation(self, dbt_project_dir: Path):
        """Test querying with aggregations and filters."""
        # Ensure data exists
        transform_handler = TransformHandler(
            project_dir=dbt_project_dir,
            profiles_dir=dbt_project_dir,
        )
        transform_handler.handle(
            Command(type="transform", action="build", payload={})
        )

        query_handler = create_duckdb_handler(
            database_path=dbt_project_dir / "pudato.duckdb",
            read_only=True,
        )

        # Analytical query
        result = query_handler.handle(
            Command(
                type="query",
                action="execute",
                payload={
                    "sql": """
                        SELECT
                            SUM(budget_amount) as total_budget_across_depts,
                            SUM(total_spent) as total_spent_across_depts,
                            COUNT(*) as dept_count
                        FROM main.dept_budget_summary
                    """
                },
            )
        )

        assert result.status == "success"
        data = result.data
        assert "total_budget_across_depts" in data["columns"]
        assert "total_spent_across_depts" in data["columns"]
        assert "dept_count" in data["columns"]

        # Verify we got a single aggregated row
        assert data["row_count"] == 1

    def test_multiple_version_runs(self, dbt_project_dir: Path):
        """Test that version tracking updates across runs.

        Demonstrates that each execution can have different version info,
        supporting reproducibility and lineage tracking.
        """
        transform_handler = TransformHandler(
            project_dir=dbt_project_dir,
            profiles_dir=dbt_project_dir,
        )

        # First run with version 1
        transform_handler.handle(
            Command(
                type="transform",
                action="build",
                payload={},
                metadata={
                    "logic_version": "v1.0.0",
                    "execution_id": "run-001",
                },
            )
        )

        # Second run with version 2 (overwrites data)
        transform_handler.handle(
            Command(
                type="transform",
                action="build",
                payload={},
                metadata={
                    "logic_version": "v2.0.0",
                    "execution_id": "run-002",
                },
            )
        )

        # Query should show the latest version
        query_handler = create_duckdb_handler(
            database_path=dbt_project_dir / "pudato.duckdb",
            read_only=True,
        )

        result = query_handler.handle(
            Command(
                type="query",
                action="execute",
                payload={
                    "sql": """
                        SELECT DISTINCT
                            _pudato_logic_version,
                            _pudato_execution_id
                        FROM main.dept_budget_summary
                    """
                },
            )
        )

        assert result.status == "success"
        row = result.data["rows"][0]
        version_idx = result.data["columns"].index("_pudato_logic_version")
        exec_idx = result.data["columns"].index("_pudato_execution_id")

        assert row[version_idx] == "v2.0.0"
        assert row[exec_idx] == "run-002"
