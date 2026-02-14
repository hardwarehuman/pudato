"""End-to-end integration tests: storage → transform → query flow.

These tests demonstrate the complete data pipeline:
1. Storage: Upload raw data (CSV seed files already in dbt/seeds/)
2. Transform: Run dbt to process data with version tracking
3. Query: Execute SQL against the transformed data

Also tests the complete lineage tracking flow:
1. Create job and steps in registry
2. Execute commands with job_id/step_id
3. Process results through results consumer
4. Query lineage from registry
"""

from pathlib import Path

import pytest

from pudato.backends.registry import InMemoryRegistryBackend
from pudato.handlers.query import create_duckdb_handler
from pudato.handlers.registry import RegistryHandler
from pudato.handlers.transform import TransformHandler
from pudato.protocol import Command
from pudato.runtime.results_consumer import process_result


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

        assert transform_result.status == "success", f"Transform failed: {transform_result.errors}"
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

        assert query_result.status == "success", f"Query failed: {query_result.errors}"

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
        transform_handler.handle(Command(type="transform", action="seed", payload={}))

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
        transform_handler.handle(Command(type="transform", action="build", payload={}))

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


@pytest.mark.integration
class TestLineageTrackingFlow:
    """Test complete lineage tracking: job → steps → results → lineage queries.

    This simulates the full orchestration flow:
    1. Orchestrator creates a job with planned steps
    2. Commands are sent with job_id/step_id
    3. Handlers execute and produce Results with lineage
    4. Results consumer persists lineage to registry
    5. Lineage is queryable for data governance
    """

    def test_full_lineage_flow_transform_pipeline(self, dbt_project_dir: Path):
        """Test complete lineage tracking for a dbt transform pipeline.

        Simulates an Airflow DAG that:
        1. Creates a job for "daily-transform" pipeline
        2. Creates steps for seed, staging, and mart models
        3. Executes each step with proper job_id/step_id
        4. Results consumer persists lineage
        5. Lineage queries show data flow
        """
        # --- Setup: Create registry ---
        registry = RegistryHandler(backend=InMemoryRegistryBackend())

        # --- Step 1: Orchestrator creates job ---
        create_job_result = registry.handle(
            Command(
                type="registry",
                action="create_job",
                payload={
                    "pipeline": "daily-transform",
                    "environment": "test",
                    "logic_version": "abc123",
                    "parameters": {"date": "2024-01-15"},
                },
            )
        )
        assert create_job_result.status == "success"
        job_id = create_job_result.data["job_id"]

        # --- Step 2: Orchestrator creates planned steps ---
        steps = {}

        # Step: dbt seed
        seed_step_result = registry.handle(
            Command(
                type="registry",
                action="add_step",
                payload={
                    "job_id": job_id,
                    "step_name": "dbt-seed",
                    "handler_type": "transform",
                    "action": "seed",
                },
            )
        )
        assert seed_step_result.status == "success"
        steps["seed"] = seed_step_result.data["step_id"]

        # Step: dbt run staging
        staging_step_result = registry.handle(
            Command(
                type="registry",
                action="add_step",
                payload={
                    "job_id": job_id,
                    "step_name": "dbt-run-staging",
                    "handler_type": "transform",
                    "action": "run",
                },
            )
        )
        assert staging_step_result.status == "success"
        steps["staging"] = staging_step_result.data["step_id"]

        # Step: dbt run marts
        marts_step_result = registry.handle(
            Command(
                type="registry",
                action="add_step",
                payload={
                    "job_id": job_id,
                    "step_name": "dbt-run-marts",
                    "handler_type": "transform",
                    "action": "run",
                },
            )
        )
        assert marts_step_result.status == "success"
        steps["marts"] = marts_step_result.data["step_id"]

        # --- Step 3: Execute handlers with job_id/step_id ---
        transform_handler = TransformHandler(
            project_dir=dbt_project_dir,
            profiles_dir=dbt_project_dir,
        )

        # Execute seed step
        seed_result = transform_handler.handle(
            Command(
                type="transform",
                action="seed",
                payload={},
                job_id=job_id,
                step_id=steps["seed"],
            )
        )
        assert seed_result.status == "success"
        assert seed_result.job_id == job_id
        assert seed_result.step_id == steps["seed"]

        # Execute staging step
        staging_result = transform_handler.handle(
            Command(
                type="transform",
                action="run",
                payload={"select": "stg_departments stg_budget_items"},
                job_id=job_id,
                step_id=steps["staging"],
            )
        )
        assert staging_result.status == "success"
        assert staging_result.step_id == steps["staging"]

        # Execute marts step
        marts_result = transform_handler.handle(
            Command(
                type="transform",
                action="run",
                payload={"select": "dept_budget_summary"},
                job_id=job_id,
                step_id=steps["marts"],
            )
        )
        assert marts_result.status == "success"
        assert marts_result.step_id == steps["marts"]

        # --- Step 4: Results consumer persists lineage ---
        # In production, results would flow through SNS → SQS → results consumer
        # Here we call process_result directly to simulate that flow

        seed_outcome = process_result(seed_result, registry)
        assert seed_outcome["status"] == "success"

        staging_outcome = process_result(staging_result, registry)
        assert staging_outcome["status"] == "success"

        marts_outcome = process_result(marts_result, registry)
        assert marts_outcome["status"] == "success"

        # --- Step 5: Verify lineage is persisted and queryable ---

        # Check seed step has execution record
        seed_step = registry.handle(
            Command(
                type="registry",
                action="get_step",
                payload={"step_id": steps["seed"]},
            )
        )
        assert seed_step.data["status"] == "success"
        assert len(seed_step.data["executions"]) > 0
        assert seed_step.data["executions"][0]["execution_type"] == "dbt"

        # Check marts step completed
        marts_step = registry.handle(
            Command(
                type="registry",
                action="get_step",
                payload={"step_id": steps["marts"]},
            )
        )
        assert marts_step.data["status"] == "success"
        assert marts_step.data["duration_ms"] > 0

        # Query all steps for the job
        job_steps = registry.handle(
            Command(
                type="registry",
                action="get_job_steps",
                payload={"job_id": job_id},
            )
        )
        assert job_steps.data["count"] == 3
        assert all(s["status"] == "success" for s in job_steps.data["steps"])

    def test_lineage_with_query_handler(self, dbt_project_dir: Path):
        """Test lineage tracking for query handler operations.

        Shows how SQL queries track inputs (tables read) and executions.
        """
        # Setup
        registry = RegistryHandler(backend=InMemoryRegistryBackend())

        # Ensure data exists
        transform_handler = TransformHandler(
            project_dir=dbt_project_dir,
            profiles_dir=dbt_project_dir,
        )
        transform_handler.handle(Command(type="transform", action="build", payload={}))

        # Create job and step
        job_result = registry.handle(
            Command(
                type="registry",
                action="create_job",
                payload={"pipeline": "ad-hoc-query", "environment": "test"},
            )
        )
        job_id = job_result.data["job_id"]

        step_result = registry.handle(
            Command(
                type="registry",
                action="add_step",
                payload={
                    "job_id": job_id,
                    "step_name": "aggregate-budgets",
                    "handler_type": "query",
                    "action": "execute",
                },
            )
        )
        step_id = step_result.data["step_id"]

        # Execute query with lineage tracking
        query_handler = create_duckdb_handler(
            database_path=dbt_project_dir / "pudato.duckdb",
            read_only=True,
        )

        query_result = query_handler.handle(
            Command(
                type="query",
                action="execute",
                payload={
                    "sql": """
                        SELECT department_name, budget_amount
                        FROM main.dept_budget_summary
                        WHERE budget_amount > 50000
                    """
                },
                job_id=job_id,
                step_id=step_id,
            )
        )

        assert query_result.status == "success"
        assert query_result.job_id == job_id
        assert query_result.step_id == step_id

        # Query handler should track SQL execution
        assert len(query_result.executions) > 0
        assert query_result.executions[0].execution_type == "sql"

        # Process through results consumer
        outcome = process_result(query_result, registry)
        assert outcome["status"] == "success"

        # Verify step was updated with execution details
        step_data = registry.handle(
            Command(
                type="registry",
                action="get_step",
                payload={"step_id": step_id},
            )
        )

        assert step_data.data["status"] == "success"
        assert len(step_data.data["executions"]) > 0
        assert "SELECT" in step_data.data["executions"][0]["details"]["statements"][0]

    def test_error_tracking_in_lineage(self, dbt_project_dir: Path):
        """Test that failed steps are tracked with error information."""
        registry = RegistryHandler(backend=InMemoryRegistryBackend())

        # Create job and step
        job_result = registry.handle(
            Command(
                type="registry",
                action="create_job",
                payload={"pipeline": "failing-pipeline", "environment": "test"},
            )
        )
        job_id = job_result.data["job_id"]

        step_result = registry.handle(
            Command(
                type="registry",
                action="add_step",
                payload={
                    "job_id": job_id,
                    "step_name": "bad-query",
                    "handler_type": "query",
                    "action": "execute",
                },
            )
        )
        step_id = step_result.data["step_id"]

        # Execute a failing query
        query_handler = create_duckdb_handler(
            database_path=dbt_project_dir / "pudato.duckdb",
            read_only=True,
        )

        query_result = query_handler.handle(
            Command(
                type="query",
                action="execute",
                payload={"sql": "SELECT * FROM nonexistent_table_xyz"},
                job_id=job_id,
                step_id=step_id,
            )
        )

        assert query_result.status == "error"
        assert query_result.job_id == job_id
        assert query_result.step_id == step_id
        assert len(query_result.errors) > 0

        # Process through results consumer
        outcome = process_result(query_result, registry)
        assert outcome["status"] == "success"  # Consumer succeeded

        # Verify step shows failure
        step_data = registry.handle(
            Command(
                type="registry",
                action="get_step",
                payload={"step_id": step_id},
            )
        )

        assert step_data.data["status"] == "failed"
        assert "nonexistent_table_xyz" in step_data.data["error"]

    def test_multi_step_job_summary(self, dbt_project_dir: Path):
        """Test querying job summary after all steps complete."""
        registry = RegistryHandler(backend=InMemoryRegistryBackend())

        # Create job
        job_result = registry.handle(
            Command(
                type="registry",
                action="create_job",
                payload={
                    "pipeline": "multi-step-demo",
                    "environment": "test",
                    "logic_version": "v1.0.0",
                },
            )
        )
        job_id = job_result.data["job_id"]

        # Create multiple steps
        step_names = ["extract", "transform", "load"]
        step_ids = []

        for name in step_names:
            result = registry.handle(
                Command(
                    type="registry",
                    action="add_step",
                    payload={
                        "job_id": job_id,
                        "step_name": name,
                        "handler_type": "transform",
                        "action": "run",
                    },
                )
            )
            step_ids.append(result.data["step_id"])

        # Simulate processing results for each step
        transform_handler = TransformHandler(
            project_dir=dbt_project_dir,
            profiles_dir=dbt_project_dir,
        )

        for i, step_id in enumerate(step_ids):  # noqa: B007
            # Use seed for simplicity - just need valid results
            result = transform_handler.handle(
                Command(
                    type="transform",
                    action="seed",
                    payload={},
                    job_id=job_id,
                    step_id=step_id,
                )
            )
            process_result(result, registry)

        # Update job as completed
        registry.handle(
            Command(
                type="registry",
                action="update_job",
                payload={"job_id": job_id, "status": "success"},
            )
        )

        # Query job summary
        job_data = registry.handle(
            Command(
                type="registry",
                action="get_job",
                payload={"job_id": job_id},
            )
        )

        assert job_data.data["status"] == "success"
        assert job_data.data["pipeline"] == "multi-step-demo"
        assert job_data.data["logic_version"] == "v1.0.0"

        # Query all steps
        steps_data = registry.handle(
            Command(
                type="registry",
                action="get_job_steps",
                payload={"job_id": job_id},
            )
        )

        assert steps_data.data["count"] == 3
        completed_steps = [s for s in steps_data.data["steps"] if s["status"] == "success"]
        assert len(completed_steps) == 3
