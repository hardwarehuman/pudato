"""Integration tests for registry handler."""

import json
import os

import pytest

from pudato.handlers.registry import RegistryHandler, create_memory_registry_handler
from pudato.protocol import Command, DataReference, ExecutionRecord


@pytest.fixture
def registry_handler() -> RegistryHandler:
    """Create RegistryHandler with in-memory backend."""
    return create_memory_registry_handler()


@pytest.mark.integration
class TestRegistryHandlerJobs:
    """Test RegistryHandler job operations."""

    def test_create_job(self, registry_handler: RegistryHandler):
        """Test creating a job."""
        command = Command(
            type="registry",
            action="create_job",
            payload={
                "pipeline": "monthly_report",
                "environment": "dev",
                "namespace": "dev_alice",
                "logic_version": "git:abc123",
                "request": {"fiscal_year": 2025},
                "parameters": {"fiscal_year": 2025, "output_format": "parquet"},
            },
        )

        result = registry_handler.handle(command)

        assert result.status == "success"
        assert result.data["pipeline"] == "monthly_report"
        assert result.data["environment"] == "dev"
        assert result.data["namespace"] == "dev_alice"
        assert result.data["status"] == "pending"
        assert "job_id" in result.data

    def test_get_job(self, registry_handler: RegistryHandler):
        """Test retrieving a job by ID."""
        # Create job first
        create_result = registry_handler.handle(
            Command(
                type="registry",
                action="create_job",
                payload={
                    "pipeline": "test_pipeline",
                    "logic_version": "v1.0.0",
                },
            )
        )
        job_id = create_result.data["job_id"]

        # Get the job
        result = registry_handler.handle(
            Command(
                type="registry",
                action="get_job",
                payload={"job_id": job_id},
            )
        )

        assert result.status == "success"
        assert result.data["found"] is True
        assert result.data["job_id"] == job_id
        assert result.data["pipeline"] == "test_pipeline"
        assert result.data["logic_version"] == "v1.0.0"

    def test_get_job_not_found(self, registry_handler: RegistryHandler):
        """Test get_job for non-existent job."""
        result = registry_handler.handle(
            Command(
                type="registry",
                action="get_job",
                payload={"job_id": "nonexistent"},
            )
        )

        assert result.status == "success"
        assert result.data["found"] is False

    def test_update_job_status(self, registry_handler: RegistryHandler):
        """Test updating job status."""
        # Create job
        create_result = registry_handler.handle(
            Command(
                type="registry",
                action="create_job",
                payload={"pipeline": "test", "logic_version": "v1"},
            )
        )
        job_id = create_result.data["job_id"]

        # Update to running
        result = registry_handler.handle(
            Command(
                type="registry",
                action="update_job",
                payload={"job_id": job_id, "status": "running"},
            )
        )

        assert result.status == "success"
        assert result.data["status"] == "running"

        # Verify started_at was set
        get_result = registry_handler.handle(
            Command(
                type="registry",
                action="get_job",
                payload={"job_id": job_id},
            )
        )
        assert get_result.data["started_at"] is not None

    def test_update_job_to_success(self, registry_handler: RegistryHandler):
        """Test completing a job successfully."""
        # Create and start job
        create_result = registry_handler.handle(
            Command(
                type="registry",
                action="create_job",
                payload={"pipeline": "test", "logic_version": "v1"},
            )
        )
        job_id = create_result.data["job_id"]

        registry_handler.handle(
            Command(
                type="registry",
                action="update_job",
                payload={"job_id": job_id, "status": "running"},
            )
        )

        # Complete the job
        result = registry_handler.handle(
            Command(
                type="registry",
                action="update_job",
                payload={"job_id": job_id, "status": "success"},
            )
        )

        assert result.status == "success"

        # Verify completed_at was set
        get_result = registry_handler.handle(
            Command(
                type="registry",
                action="get_job",
                payload={"job_id": job_id},
            )
        )
        assert get_result.data["completed_at"] is not None
        assert get_result.data["status"] == "success"

    def test_query_jobs(self, registry_handler: RegistryHandler):
        """Test querying jobs by criteria."""
        # Create multiple jobs
        for i, (pipeline, env) in enumerate([
            ("pipeline_a", "dev"),
            ("pipeline_a", "prod"),
            ("pipeline_b", "dev"),
        ]):
            registry_handler.handle(
                Command(
                    type="registry",
                    action="create_job",
                    payload={
                        "pipeline": pipeline,
                        "environment": env,
                        "logic_version": f"v{i}",
                    },
                )
            )

        # Query by pipeline
        result = registry_handler.handle(
            Command(
                type="registry",
                action="query_jobs",
                payload={"pipeline": "pipeline_a"},
            )
        )

        assert result.status == "success"
        assert result.data["count"] == 2

        # Query by environment
        result = registry_handler.handle(
            Command(
                type="registry",
                action="query_jobs",
                payload={"environment": "dev"},
            )
        )

        assert result.data["count"] == 2


@pytest.mark.integration
class TestRegistryHandlerSteps:
    """Test RegistryHandler step operations."""

    def test_add_step(self, registry_handler: RegistryHandler):
        """Test adding a step to a job."""
        # Create job first
        create_result = registry_handler.handle(
            Command(
                type="registry",
                action="create_job",
                payload={"pipeline": "test", "logic_version": "v1"},
            )
        )
        job_id = create_result.data["job_id"]

        # Add step
        result = registry_handler.handle(
            Command(
                type="registry",
                action="add_step",
                payload={
                    "job_id": job_id,
                    "step_name": "build_staging",
                    "handler_type": "transform",
                    "action": "build",
                },
            )
        )

        assert result.status == "success"
        assert result.data["step_name"] == "build_staging"
        assert result.data["status"] == "pending"
        assert "step_id" in result.data

    def test_update_step_with_lineage(self, registry_handler: RegistryHandler):
        """Test updating a step with inputs/outputs/executions."""
        # Create job and step
        create_result = registry_handler.handle(
            Command(
                type="registry",
                action="create_job",
                payload={"pipeline": "test", "logic_version": "v1"},
            )
        )
        job_id = create_result.data["job_id"]

        add_result = registry_handler.handle(
            Command(
                type="registry",
                action="add_step",
                payload={
                    "job_id": job_id,
                    "step_name": "transform_data",
                    "handler_type": "transform",
                    "action": "build",
                },
            )
        )
        step_id = add_result.data["step_id"]

        # Update step with lineage data
        result = registry_handler.handle(
            Command(
                type="registry",
                action="update_step",
                payload={
                    "step_id": step_id,
                    "status": "success",
                    "duration_ms": 1500,
                    "inputs": [
                        {"ref_type": "table", "location": "raw.departments"},
                        {"ref_type": "table", "location": "raw.employees"},
                    ],
                    "outputs": [
                        {"ref_type": "table", "location": "main.stg_departments"},
                    ],
                    "executions": [
                        {
                            "execution_type": "sql",
                            "details": {
                                "dialect": "duckdb",
                                "statements": ["SELECT * FROM raw.departments"],
                            },
                        },
                    ],
                },
            )
        )

        assert result.status == "success"

        # Verify step has lineage data
        get_result = registry_handler.handle(
            Command(
                type="registry",
                action="get_step",
                payload={"step_id": step_id},
            )
        )

        assert get_result.data["found"] is True
        assert len(get_result.data["inputs"]) == 2
        assert len(get_result.data["outputs"]) == 1
        assert len(get_result.data["executions"]) == 1
        assert get_result.data["duration_ms"] == 1500

    def test_get_job_steps(self, registry_handler: RegistryHandler):
        """Test getting all steps for a job."""
        # Create job
        create_result = registry_handler.handle(
            Command(
                type="registry",
                action="create_job",
                payload={"pipeline": "test", "logic_version": "v1"},
            )
        )
        job_id = create_result.data["job_id"]

        # Add multiple steps
        for name in ["seed_data", "build_staging", "build_marts"]:
            registry_handler.handle(
                Command(
                    type="registry",
                    action="add_step",
                    payload={
                        "job_id": job_id,
                        "step_name": name,
                        "handler_type": "transform",
                        "action": "build",
                    },
                )
            )

        # Get all steps
        result = registry_handler.handle(
            Command(
                type="registry",
                action="get_job_steps",
                payload={"job_id": job_id},
            )
        )

        assert result.status == "success"
        assert result.data["count"] == 3


@pytest.mark.integration
class TestRegistryHandlerLineage:
    """Test RegistryHandler lineage operations."""

    def test_get_lineage_for_output(self, registry_handler: RegistryHandler):
        """Test getting lineage for a produced output."""
        # Create job with step that has outputs
        create_result = registry_handler.handle(
            Command(
                type="registry",
                action="create_job",
                payload={
                    "pipeline": "test",
                    "logic_version": "v1",
                    "environment": "dev",
                },
            )
        )
        job_id = create_result.data["job_id"]

        add_result = registry_handler.handle(
            Command(
                type="registry",
                action="add_step",
                payload={
                    "job_id": job_id,
                    "step_name": "build_summary",
                    "handler_type": "transform",
                    "action": "build",
                },
            )
        )
        step_id = add_result.data["step_id"]

        # Complete step with output
        registry_handler.handle(
            Command(
                type="registry",
                action="update_step",
                payload={
                    "step_id": step_id,
                    "status": "success",
                    "outputs": [
                        {"ref_type": "table", "location": "main.dept_summary"},
                    ],
                    "executions": [
                        {
                            "execution_type": "sql",
                            "details": {"statements": ["SELECT ..."]},
                        },
                    ],
                },
            )
        )

        # Query lineage
        result = registry_handler.handle(
            Command(
                type="registry",
                action="get_lineage",
                payload={
                    "location": "main.dept_summary",
                    "direction": "producers",
                },
            )
        )

        assert result.status == "success"
        assert len(result.data["producers"]) == 1
        assert result.data["producers"][0]["step_name"] == "build_summary"
        assert result.data["producers"][0]["logic_version"] == "v1"

    def test_get_lineage_for_input(self, registry_handler: RegistryHandler):
        """Test getting lineage for consumed inputs."""
        # Create job with step that has inputs
        create_result = registry_handler.handle(
            Command(
                type="registry",
                action="create_job",
                payload={"pipeline": "test", "logic_version": "v2"},
            )
        )
        job_id = create_result.data["job_id"]

        add_result = registry_handler.handle(
            Command(
                type="registry",
                action="add_step",
                payload={
                    "job_id": job_id,
                    "step_name": "aggregate_data",
                    "handler_type": "query",
                    "action": "execute",
                },
            )
        )
        step_id = add_result.data["step_id"]

        registry_handler.handle(
            Command(
                type="registry",
                action="update_step",
                payload={
                    "step_id": step_id,
                    "status": "success",
                    "inputs": [
                        {"ref_type": "table", "location": "main.stg_orders"},
                    ],
                },
            )
        )

        # Query consumers
        result = registry_handler.handle(
            Command(
                type="registry",
                action="get_lineage",
                payload={
                    "location": "main.stg_orders",
                    "direction": "consumers",
                },
            )
        )

        assert result.status == "success"
        assert len(result.data["consumers"]) == 1
        assert result.data["consumers"][0]["step_name"] == "aggregate_data"


@pytest.mark.integration
class TestRegistryHandlerLambda:
    """Test Registry handler via Lambda entry point."""

    def test_lambda_handler_registry(self):
        """Test Lambda handler processes registry commands."""
        from pudato.runtime.lambda_handler import handle

        os.environ["HANDLER_TYPE"] = "registry"

        command = Command(
            type="registry",
            action="create_job",
            payload={
                "pipeline": "lambda_test",
                "logic_version": "v1",
            },
        )

        event = {
            "Records": [
                {
                    "messageId": "test-registry-123",
                    "body": json.dumps({"Message": command.to_json()}),
                    "receiptHandle": "test-handle",
                }
            ]
        }

        result = handle(event)

        assert result["processed"] == 1
        assert result["results"][0]["status"] == "success"
