"""Tests for the results consumer lineage persistence."""

import pytest

from pudato.backends.registry import InMemoryRegistryBackend
from pudato.handlers.registry import RegistryHandler
from pudato.protocol import Command, DataReference, ExecutionRecord, Result
from pudato.runtime.results_consumer import handle, process_result


@pytest.fixture
def registry() -> RegistryHandler:
    """Create a registry handler with in-memory backend."""
    backend = InMemoryRegistryBackend()
    return RegistryHandler(backend=backend)


@pytest.fixture
def job_with_step(registry: RegistryHandler) -> tuple[str, str]:
    """Create a job with a pending step, return (job_id, step_id)."""
    # Create job
    create_job_cmd = Command(
        type="registry",
        action="create_job",
        payload={
            "pipeline": "test-pipeline",
            "environment": "test",
        },
    )
    job_result = registry.handle(create_job_cmd)
    assert job_result.status == "success"
    job_id = job_result.data["job_id"]

    # Add step
    add_step_cmd = Command(
        type="registry",
        action="add_step",
        payload={
            "job_id": job_id,
            "step_name": "extract-data",
            "handler_type": "storage",
            "action": "get_object",
        },
    )
    step_result = registry.handle(add_step_cmd)
    assert step_result.status == "success"
    step_id = step_result.data["step_id"]

    return job_id, step_id


class TestProcessResult:
    """Tests for process_result function."""

    def test_result_without_step_id_is_skipped(self, registry: RegistryHandler) -> None:
        """Results without step_id should be skipped."""
        result = Result.success(
            correlation_id="test-corr-1",
            data={"message": "completed"},
        )

        outcome = process_result(result, registry)

        assert outcome["action"] == "skipped"
        assert outcome["reason"] == "no_step_id"

    def test_result_with_step_id_updates_registry(
        self, registry: RegistryHandler, job_with_step: tuple[str, str]
    ) -> None:
        """Results with step_id should update the registry step."""
        job_id, step_id = job_with_step

        result = Result.success(
            correlation_id="test-corr-2",
            job_id=job_id,
            step_id=step_id,
            duration_ms=150,
        )

        outcome = process_result(result, registry)

        assert outcome["action"] == "updated"
        assert outcome["status"] == "success"
        assert outcome["step_id"] == step_id

        # Verify step was updated
        get_step_cmd = Command(
            type="registry",
            action="get_step",
            payload={"step_id": step_id},
        )
        step_result = registry.handle(get_step_cmd)
        assert step_result.data["status"] == "success"
        assert step_result.data["duration_ms"] == 150

    def test_result_with_lineage_persists_inputs_outputs(
        self, registry: RegistryHandler, job_with_step: tuple[str, str]
    ) -> None:
        """Lineage data should be persisted to the step."""
        job_id, step_id = job_with_step

        result = Result.success(
            correlation_id="test-corr-3",
            job_id=job_id,
            step_id=step_id,
            inputs=[
                DataReference(
                    ref_type="file",
                    location="s3://bucket/input.csv",
                    format="csv",
                    metadata={"rows": 100},
                )
            ],
            outputs=[
                DataReference(
                    ref_type="table",
                    location="main.stg_data",
                    format="parquet",
                    metadata={"rows": 100},
                )
            ],
        )

        outcome = process_result(result, registry)
        assert outcome["status"] == "success"

        # Verify lineage was persisted
        get_step_cmd = Command(
            type="registry",
            action="get_step",
            payload={"step_id": step_id},
        )
        step_result = registry.handle(get_step_cmd)

        assert len(step_result.data["inputs"]) == 1
        assert step_result.data["inputs"][0]["location"] == "s3://bucket/input.csv"

        assert len(step_result.data["outputs"]) == 1
        assert step_result.data["outputs"][0]["location"] == "main.stg_data"

    def test_result_with_executions_persists(
        self, registry: RegistryHandler, job_with_step: tuple[str, str]
    ) -> None:
        """Execution records should be persisted to the step."""
        job_id, step_id = job_with_step

        result = Result.success(
            correlation_id="test-corr-4",
            job_id=job_id,
            step_id=step_id,
            executions=[
                ExecutionRecord.sql(
                    statements=["INSERT INTO t SELECT * FROM s"],
                    dialect="duckdb",
                )
            ],
        )

        outcome = process_result(result, registry)
        assert outcome["status"] == "success"

        # Verify execution was persisted
        get_step_cmd = Command(
            type="registry",
            action="get_step",
            payload={"step_id": step_id},
        )
        step_result = registry.handle(get_step_cmd)

        assert len(step_result.data["executions"]) == 1
        assert step_result.data["executions"][0]["execution_type"] == "sql"
        assert "INSERT INTO t" in step_result.data["executions"][0]["details"]["statements"][0]

    def test_error_result_updates_step_as_failed(
        self, registry: RegistryHandler, job_with_step: tuple[str, str]
    ) -> None:
        """Error results should mark step as failed with error message."""
        job_id, step_id = job_with_step

        result = Result.error(
            correlation_id="test-corr-5",
            job_id=job_id,
            step_id=step_id,
            errors=["Connection timeout", "Retry failed"],
            duration_ms=5000,
        )

        outcome = process_result(result, registry)
        assert outcome["status"] == "success"  # Consumer processed successfully

        # Verify step was marked as failed
        get_step_cmd = Command(
            type="registry",
            action="get_step",
            payload={"step_id": step_id},
        )
        step_result = registry.handle(get_step_cmd)

        assert step_result.data["status"] == "failed"
        assert "Connection timeout" in step_result.data["error"]
        assert "Retry failed" in step_result.data["error"]


class TestHandleLambdaEvent:
    """Tests for the Lambda handler entry point."""

    def test_handle_processes_sqs_event(
        self,
        registry: RegistryHandler,
        job_with_step: tuple[str, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Lambda handler should process SQS events with Results."""
        job_id, step_id = job_with_step

        # Monkeypatch the registry handler factory
        monkeypatch.setattr(
            "pudato.runtime.results_consumer._create_registry_handler",
            lambda: registry,
        )

        result = Result.success(
            correlation_id="lambda-test-1",
            job_id=job_id,
            step_id=step_id,
            outputs=[
                DataReference(ref_type="table", location="main.output_table"),
            ],
        )

        # Simulate SQS event format
        event = {
            "Records": [
                {"body": result.to_json()},
            ]
        }

        response = handle(event)

        assert response["processed"] == 1
        assert response["outcomes"][0]["action"] == "updated"

        # Verify step was updated
        get_step_cmd = Command(
            type="registry",
            action="get_step",
            payload={"step_id": step_id},
        )
        step_result = registry.handle(get_step_cmd)
        assert step_result.data["status"] == "success"
        assert len(step_result.data["outputs"]) == 1

    def test_handle_processes_sns_wrapped_event(
        self,
        registry: RegistryHandler,
        job_with_step: tuple[str, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Lambda handler should handle SNS-wrapped messages."""
        job_id, step_id = job_with_step

        monkeypatch.setattr(
            "pudato.runtime.results_consumer._create_registry_handler",
            lambda: registry,
        )

        result = Result.success(
            correlation_id="sns-test-1",
            job_id=job_id,
            step_id=step_id,
        )

        # SNS wraps the message in a notification envelope
        import json

        sns_notification = json.dumps({"Message": result.to_json()})

        event = {
            "Records": [
                {"body": sns_notification},
            ]
        }

        response = handle(event)

        assert response["processed"] == 1
        assert response["outcomes"][0]["action"] == "updated"

    def test_handle_skips_results_without_step_id(
        self, registry: RegistryHandler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Results without step_id should be skipped but counted."""
        monkeypatch.setattr(
            "pudato.runtime.results_consumer._create_registry_handler",
            lambda: registry,
        )

        result = Result.success(
            correlation_id="no-step-1",
            data={"info": "standalone result"},
        )

        event = {
            "Records": [
                {"body": result.to_json()},
            ]
        }

        response = handle(event)

        assert response["processed"] == 1
        assert response["outcomes"][0]["action"] == "skipped"


class TestLineageQuery:
    """Tests verifying lineage is queryable after results are processed."""

    def test_lineage_queryable_after_result_processed(
        self, registry: RegistryHandler, job_with_step: tuple[str, str]
    ) -> None:
        """After processing a result, lineage should be queryable."""
        job_id, step_id = job_with_step

        # Process a result with output
        result = Result.success(
            correlation_id="lineage-test-1",
            job_id=job_id,
            step_id=step_id,
            outputs=[
                DataReference(
                    ref_type="table",
                    location="main.fact_orders",
                    format="parquet",
                )
            ],
            executions=[
                ExecutionRecord.sql(
                    statements=["INSERT INTO main.fact_orders SELECT ..."],
                    dialect="duckdb",
                )
            ],
        )

        outcome = process_result(result, registry)
        assert outcome["status"] == "success"

        # Query lineage for the output location
        lineage_cmd = Command(
            type="registry",
            action="get_lineage",
            payload={
                "location": "main.fact_orders",
                "direction": "producers",
            },
        )
        lineage_result = registry.handle(lineage_cmd)

        assert lineage_result.status == "success"
        assert len(lineage_result.data["producers"]) == 1
        assert lineage_result.data["producers"][0]["step_name"] == "extract-data"
