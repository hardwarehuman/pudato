"""Integration tests for PostgreSQL registry backend.

These tests require a PostgreSQL database. They're skipped if the
REGISTRY_DATABASE_URL environment variable isn't set.

To run locally:
    docker compose --profile postgres up -d
    export REGISTRY_DATABASE_URL="postgresql://pudato:pudato@localhost:5433/pudato"
    pytest tests/integration/test_postgres_registry.py -v
"""

import os

import pytest

from pudato.backends.registry import (
    Job,
    JobQuery,
    JobStep,
    PostgreSQLRegistryBackend,
)
from pudato.protocol import DataReference, ExecutionRecord

# Skip all tests if PostgreSQL connection not available
pytestmark = pytest.mark.skipif(
    not os.environ.get("REGISTRY_DATABASE_URL"),
    reason="REGISTRY_DATABASE_URL not set - PostgreSQL tests skipped",
)


@pytest.fixture
def pg_backend():
    """Create a PostgreSQL backend for testing.

    Each test gets a clean schema by truncating tables.
    """
    import psycopg

    database_url = os.environ.get("REGISTRY_DATABASE_URL")
    conn = psycopg.connect(database_url)

    backend = PostgreSQLRegistryBackend(conn)
    backend.initialize_schema()

    # Clean up tables before each test
    with conn.cursor() as cur:
        cur.execute("TRUNCATE pudato_job_steps CASCADE")
        cur.execute("TRUNCATE pudato_jobs CASCADE")
    conn.commit()

    yield backend

    conn.close()


@pytest.mark.integration
class TestPostgreSQLRegistryJobs:
    """Test PostgreSQL registry job operations."""

    def test_create_job(self, pg_backend: PostgreSQLRegistryBackend):
        """Test creating a job."""
        job = Job.create(
            pipeline="test_pipeline",
            environment="dev",
            namespace="dev_alice",
            logic_version="git:abc123",
            request={"year": 2025},
            parameters={"year": 2025, "format": "parquet"},
        )

        created = pg_backend.create_job(job)

        assert created.job_id == job.job_id
        assert created.pipeline == "test_pipeline"
        assert created.environment == "dev"
        assert created.status == "pending"

    def test_get_job(self, pg_backend: PostgreSQLRegistryBackend):
        """Test retrieving a job."""
        job = Job.create(
            pipeline="get_test",
            environment="staging",
            namespace="default",
            logic_version="v1.0.0",
            request={},
        )
        pg_backend.create_job(job)

        retrieved = pg_backend.get_job(job.job_id)

        assert retrieved is not None
        assert retrieved.job_id == job.job_id
        assert retrieved.pipeline == "get_test"
        assert retrieved.environment == "staging"

    def test_get_job_not_found(self, pg_backend: PostgreSQLRegistryBackend):
        """Test get_job for nonexistent job."""
        retrieved = pg_backend.get_job("nonexistent-job-id")
        assert retrieved is None

    def test_update_job(self, pg_backend: PostgreSQLRegistryBackend):
        """Test updating job fields."""
        job = Job.create(
            pipeline="update_test",
            environment="prod",
            namespace="default",
            logic_version="v2.0.0",
            request={},
        )
        pg_backend.create_job(job)

        # Update to running
        updated = pg_backend.update_job(
            job_id=job.job_id,
            status="running",
            started_at="2025-01-10T12:00:00Z",
        )

        assert updated is not None
        assert updated.status == "running"
        assert updated.started_at is not None

        # Update to success
        updated = pg_backend.update_job(
            job_id=job.job_id,
            status="success",
            completed_at="2025-01-10T12:05:00Z",
            metadata={"duration_sec": 300},
        )

        assert updated is not None
        assert updated.status == "success"
        assert updated.completed_at is not None
        assert updated.metadata.get("duration_sec") == 300

    def test_query_jobs_by_pipeline(self, pg_backend: PostgreSQLRegistryBackend):
        """Test querying jobs by pipeline."""
        # Create jobs for different pipelines
        for pipeline in ["pipeline_a", "pipeline_a", "pipeline_b"]:
            job = Job.create(
                pipeline=pipeline,
                environment="dev",
                namespace="default",
                logic_version="v1",
                request={},
            )
            pg_backend.create_job(job)

        results = pg_backend.query_jobs(JobQuery(pipeline="pipeline_a"))
        assert len(results) == 2

    def test_query_jobs_by_environment(self, pg_backend: PostgreSQLRegistryBackend):
        """Test querying jobs by environment."""
        for env in ["dev", "staging", "dev"]:
            job = Job.create(
                pipeline="test",
                environment=env,
                namespace="default",
                logic_version="v1",
                request={},
            )
            pg_backend.create_job(job)

        results = pg_backend.query_jobs(JobQuery(environment="dev"))
        assert len(results) == 2


@pytest.mark.integration
class TestPostgreSQLRegistrySteps:
    """Test PostgreSQL registry step operations."""

    def test_add_step(self, pg_backend: PostgreSQLRegistryBackend):
        """Test adding a step to a job."""
        job = Job.create(
            pipeline="step_test",
            environment="dev",
            namespace="default",
            logic_version="v1",
            request={},
        )
        pg_backend.create_job(job)

        step = JobStep.create(
            job_id=job.job_id,
            step_name="build_staging",
            handler_type="transform",
            action="build",
            correlation_id="corr-123",
        )
        created = pg_backend.add_step(step)

        assert created.step_id == step.step_id
        assert created.job_id == job.job_id
        assert created.status == "pending"

    def test_update_step_with_lineage(self, pg_backend: PostgreSQLRegistryBackend):
        """Test updating step with inputs, outputs, executions."""
        job = Job.create(
            pipeline="lineage_test",
            environment="dev",
            namespace="default",
            logic_version="v1",
            request={},
        )
        pg_backend.create_job(job)

        step = JobStep.create(
            job_id=job.job_id,
            step_name="transform_data",
            handler_type="transform",
            action="build",
            correlation_id="corr-456",
        )
        pg_backend.add_step(step)

        # Update with lineage
        inputs = [
            DataReference(ref_type="table", location="raw.users"),
            DataReference(ref_type="table", location="raw.orders"),
        ]
        outputs = [
            DataReference(ref_type="table", location="main.dim_users"),
        ]
        executions = [
            ExecutionRecord.dbt(command="build", models=["dim_users"]),
        ]

        updated = pg_backend.update_step(
            step_id=step.step_id,
            status="success",
            duration_ms=1500,
            inputs=inputs,
            outputs=outputs,
            executions=executions,
        )

        assert updated is not None
        assert updated.status == "success"
        assert updated.duration_ms == 1500
        assert len(updated.inputs) == 2
        assert len(updated.outputs) == 1
        assert len(updated.executions) == 1

    def test_get_job_steps(self, pg_backend: PostgreSQLRegistryBackend):
        """Test getting all steps for a job."""
        job = Job.create(
            pipeline="multi_step_test",
            environment="dev",
            namespace="default",
            logic_version="v1",
            request={},
        )
        pg_backend.create_job(job)

        # Add multiple steps
        for name in ["seed", "build_staging", "build_marts"]:
            step = JobStep.create(
                job_id=job.job_id,
                step_name=name,
                handler_type="transform",
                action="build",
                correlation_id=f"corr-{name}",
            )
            pg_backend.add_step(step)

        steps = pg_backend.get_job_steps(job.job_id)
        assert len(steps) == 3


@pytest.mark.integration
class TestPostgreSQLRegistryLineage:
    """Test PostgreSQL registry lineage queries."""

    def test_get_lineage_for_output(self, pg_backend: PostgreSQLRegistryBackend):
        """Test finding producers of an output."""
        job = Job.create(
            pipeline="lineage_producer",
            environment="dev",
            namespace="default",
            logic_version="v1.0.0",
            request={},
        )
        pg_backend.create_job(job)

        step = JobStep.create(
            job_id=job.job_id,
            step_name="build_summary",
            handler_type="transform",
            action="build",
            correlation_id="corr-789",
        )
        pg_backend.add_step(step)

        pg_backend.update_step(
            step_id=step.step_id,
            status="success",
            outputs=[DataReference(ref_type="table", location="main.summary_report")],
            executions=[ExecutionRecord.sql(statements=["CREATE TABLE..."])],
        )

        # Query lineage
        producers = pg_backend.get_lineage_for_output("main.summary_report")

        assert len(producers) == 1
        assert producers[0]["step_name"] == "build_summary"
        assert producers[0]["logic_version"] == "v1.0.0"

    def test_get_lineage_for_input(self, pg_backend: PostgreSQLRegistryBackend):
        """Test finding consumers of an input."""
        job = Job.create(
            pipeline="lineage_consumer",
            environment="dev",
            namespace="default",
            logic_version="v2.0.0",
            request={},
        )
        pg_backend.create_job(job)

        step = JobStep.create(
            job_id=job.job_id,
            step_name="aggregate",
            handler_type="query",
            action="execute",
            correlation_id="corr-agg",
        )
        pg_backend.add_step(step)

        pg_backend.update_step(
            step_id=step.step_id,
            status="success",
            inputs=[DataReference(ref_type="table", location="main.raw_events")],
        )

        # Query consumers
        consumers = pg_backend.get_lineage_for_input("main.raw_events")

        assert len(consumers) == 1
        assert consumers[0]["step_name"] == "aggregate"
        assert consumers[0]["logic_version"] == "v2.0.0"
