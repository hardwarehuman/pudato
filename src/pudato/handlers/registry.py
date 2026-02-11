"""Registry handler for job tracking and lineage operations."""

from __future__ import annotations

from datetime import UTC, datetime

from pudato.backends.registry import (
    InMemoryRegistryBackend,
    Job,
    JobQuery,
    JobStep,
    RegistryBackend,
)
from pudato.handlers.base import BaseHandler
from pudato.protocol import Command, DataReference, ExecutionRecord, Result


class RegistryHandler(BaseHandler):
    """Handler for job registry operations.

    Tracks job execution history for lineage, reproducibility, and auditing.

    Supported actions:
    - create_job: Create a new job record
    - get_job: Get job by ID
    - update_job: Update job status/metadata
    - query_jobs: Search jobs by criteria
    - add_step: Add a step to a job
    - get_step: Get step by ID
    - get_job_steps: Get all steps for a job
    - update_step: Update step with results/lineage
    - get_lineage: Get lineage for a data location
    """

    service_type = "registry"

    def __init__(self, backend: RegistryBackend) -> None:
        super().__init__()
        self._backend = backend

    def supported_actions(self) -> list[str]:
        """Return list of supported registry actions."""
        return [
            "create_job",
            "get_job",
            "update_job",
            "query_jobs",
            "add_step",
            "get_step",
            "get_job_steps",
            "update_step",
            "get_lineage",
        ]

    def _handle(self, command: Command) -> Result:
        """Route command to appropriate registry operation."""
        match command.action:
            case "create_job":
                return self._create_job(command)
            case "get_job":
                return self._get_job(command)
            case "update_job":
                return self._update_job(command)
            case "query_jobs":
                return self._query_jobs(command)
            case "add_step":
                return self._add_step(command)
            case "get_step":
                return self._get_step(command)
            case "get_job_steps":
                return self._get_job_steps(command)
            case "update_step":
                return self._update_step(command)
            case "get_lineage":
                return self._get_lineage(command)
            case _:
                return self._error(command, [f"Unsupported action: {command.action}"])

    def _create_job(self, command: Command) -> Result:
        """Handle create_job command."""
        payload = command.payload

        pipeline = payload.get("pipeline")
        if not pipeline:
            return self._error(command, ["Missing required field: pipeline"])

        environment = payload.get("environment", "dev")
        namespace = payload.get("namespace", environment)
        logic_version = payload.get("logic_version", "unknown")

        try:
            job = Job.create(
                pipeline=pipeline,
                environment=environment,
                namespace=namespace,
                logic_version=logic_version,
                request=payload.get("request", {}),
                parameters=payload.get("parameters", {}),
                dag_run_id=payload.get("dag_run_id"),
                job_id=payload.get("job_id"),
                metadata=payload.get("metadata", {}),
            )

            created = self._backend.create_job(job)

            return self._success(
                command,
                data={
                    "job_id": created.job_id,
                    "pipeline": created.pipeline,
                    "environment": created.environment,
                    "namespace": created.namespace,
                    "status": created.status,
                    "created_at": created.created_at,
                },
            )
        except Exception as e:
            return self._error(command, [f"Failed to create job: {e!s}"])

    def _get_job(self, command: Command) -> Result:
        """Handle get_job command."""
        payload = command.payload

        job_id = payload.get("job_id")
        if not job_id:
            return self._error(command, ["Missing required field: job_id"])

        try:
            job = self._backend.get_job(job_id)

            if not job:
                return self._success(command, data={"job_id": job_id, "found": False})

            return self._success(
                command,
                data={
                    "job_id": job.job_id,
                    "found": True,
                    "pipeline": job.pipeline,
                    "environment": job.environment,
                    "namespace": job.namespace,
                    "status": job.status,
                    "logic_version": job.logic_version,
                    "request": job.request,
                    "parameters": job.parameters,
                    "dag_run_id": job.dag_run_id,
                    "created_at": job.created_at,
                    "started_at": job.started_at,
                    "completed_at": job.completed_at,
                    "error": job.error,
                    "metadata": job.metadata,
                },
            )
        except Exception as e:
            return self._error(command, [f"Failed to get job: {e!s}"])

    def _update_job(self, command: Command) -> Result:
        """Handle update_job command."""
        payload = command.payload

        job_id = payload.get("job_id")
        if not job_id:
            return self._error(command, ["Missing required field: job_id"])

        try:
            # Handle status transitions
            status = payload.get("status")
            started_at = payload.get("started_at")
            completed_at = payload.get("completed_at")

            # Auto-set timestamps based on status
            now = datetime.now(UTC).isoformat()
            if status == "running" and not started_at:
                started_at = now
            if status in ("success", "failed", "cancelled") and not completed_at:
                completed_at = now

            job = self._backend.update_job(
                job_id=job_id,
                status=status,
                started_at=started_at,
                completed_at=completed_at,
                error=payload.get("error"),
                metadata=payload.get("metadata"),
            )

            if not job:
                return self._error(command, [f"Job not found: {job_id}"])

            return self._success(
                command,
                data={
                    "job_id": job.job_id,
                    "status": job.status,
                    "updated": True,
                },
            )
        except Exception as e:
            return self._error(command, [f"Failed to update job: {e!s}"])

    def _query_jobs(self, command: Command) -> Result:
        """Handle query_jobs command."""
        payload = command.payload

        try:
            query = JobQuery(
                pipeline=payload.get("pipeline"),
                environment=payload.get("environment"),
                namespace=payload.get("namespace"),
                status=payload.get("status"),
                logic_version=payload.get("logic_version"),
                created_after=payload.get("created_after"),
                created_before=payload.get("created_before"),
                limit=payload.get("limit", 100),
                offset=payload.get("offset", 0),
            )

            jobs = self._backend.query_jobs(query)

            return self._success(
                command,
                data={
                    "count": len(jobs),
                    "jobs": [
                        {
                            "job_id": j.job_id,
                            "pipeline": j.pipeline,
                            "environment": j.environment,
                            "namespace": j.namespace,
                            "status": j.status,
                            "logic_version": j.logic_version,
                            "created_at": j.created_at,
                            "completed_at": j.completed_at,
                        }
                        for j in jobs
                    ],
                },
            )
        except Exception as e:
            return self._error(command, [f"Failed to query jobs: {e!s}"])

    def _add_step(self, command: Command) -> Result:
        """Handle add_step command."""
        payload = command.payload

        job_id = payload.get("job_id")
        if not job_id:
            return self._error(command, ["Missing required field: job_id"])

        step_name = payload.get("step_name")
        if not step_name:
            return self._error(command, ["Missing required field: step_name"])

        handler_type = payload.get("handler_type")
        if not handler_type:
            return self._error(command, ["Missing required field: handler_type"])

        action = payload.get("action")
        if not action:
            return self._error(command, ["Missing required field: action"])

        try:
            step = JobStep.create(
                job_id=job_id,
                step_name=step_name,
                handler_type=handler_type,
                action=action,
                correlation_id=payload.get("correlation_id", command.correlation_id),
                step_id=payload.get("step_id"),
                metadata=payload.get("metadata", {}),
            )

            created = self._backend.add_step(step)

            return self._success(
                command,
                data={
                    "step_id": created.step_id,
                    "job_id": created.job_id,
                    "step_name": created.step_name,
                    "status": created.status,
                },
            )
        except Exception as e:
            return self._error(command, [f"Failed to add step: {e!s}"])

    def _get_step(self, command: Command) -> Result:
        """Handle get_step command."""
        payload = command.payload

        step_id = payload.get("step_id")
        if not step_id:
            return self._error(command, ["Missing required field: step_id"])

        try:
            step = self._backend.get_step(step_id)

            if not step:
                return self._success(command, data={"step_id": step_id, "found": False})

            return self._success(
                command,
                data={
                    "step_id": step.step_id,
                    "found": True,
                    "job_id": step.job_id,
                    "step_name": step.step_name,
                    "handler_type": step.handler_type,
                    "action": step.action,
                    "status": step.status,
                    "correlation_id": step.correlation_id,
                    "inputs": [i.model_dump() for i in step.inputs],
                    "outputs": [o.model_dump() for o in step.outputs],
                    "executions": [e.model_dump() for e in step.executions],
                    "started_at": step.started_at,
                    "completed_at": step.completed_at,
                    "duration_ms": step.duration_ms,
                    "error": step.error,
                },
            )
        except Exception as e:
            return self._error(command, [f"Failed to get step: {e!s}"])

    def _get_job_steps(self, command: Command) -> Result:
        """Handle get_job_steps command."""
        payload = command.payload

        job_id = payload.get("job_id")
        if not job_id:
            return self._error(command, ["Missing required field: job_id"])

        try:
            steps = self._backend.get_job_steps(job_id)

            return self._success(
                command,
                data={
                    "job_id": job_id,
                    "count": len(steps),
                    "steps": [
                        {
                            "step_id": s.step_id,
                            "step_name": s.step_name,
                            "handler_type": s.handler_type,
                            "action": s.action,
                            "status": s.status,
                            "started_at": s.started_at,
                            "completed_at": s.completed_at,
                            "duration_ms": s.duration_ms,
                            "input_count": len(s.inputs),
                            "output_count": len(s.outputs),
                        }
                        for s in steps
                    ],
                },
            )
        except Exception as e:
            return self._error(command, [f"Failed to get job steps: {e!s}"])

    def _update_step(self, command: Command) -> Result:
        """Handle update_step command."""
        payload = command.payload

        step_id = payload.get("step_id")
        if not step_id:
            return self._error(command, ["Missing required field: step_id"])

        try:
            # Parse inputs/outputs/executions if provided
            inputs = None
            if "inputs" in payload:
                inputs = [DataReference(**i) for i in payload["inputs"]]

            outputs = None
            if "outputs" in payload:
                outputs = [DataReference(**o) for o in payload["outputs"]]

            executions = None
            if "executions" in payload:
                executions = [ExecutionRecord(**e) for e in payload["executions"]]

            # Handle status transitions
            status = payload.get("status")
            started_at = payload.get("started_at")
            completed_at = payload.get("completed_at")

            now = datetime.now(UTC).isoformat()
            if status == "running" and not started_at:
                started_at = now
            if status in ("success", "failed", "skipped") and not completed_at:
                completed_at = now

            step = self._backend.update_step(
                step_id=step_id,
                status=status,
                started_at=started_at,
                completed_at=completed_at,
                duration_ms=payload.get("duration_ms"),
                inputs=inputs,
                outputs=outputs,
                executions=executions,
                error=payload.get("error"),
            )

            if not step:
                return self._error(command, [f"Step not found: {step_id}"])

            return self._success(
                command,
                data={
                    "step_id": step.step_id,
                    "status": step.status,
                    "updated": True,
                },
            )
        except Exception as e:
            return self._error(command, [f"Failed to update step: {e!s}"])

    def _get_lineage(self, command: Command) -> Result:
        """Handle get_lineage command."""
        payload = command.payload

        location = payload.get("location")
        if not location:
            return self._error(command, ["Missing required field: location"])

        direction = payload.get("direction", "both")  # "producers", "consumers", "both"
        environment = payload.get("environment")

        try:
            result_data: dict = {"location": location}  # type: ignore[type-arg]

            if direction in ("producers", "both"):
                producers = self._backend.get_lineage_for_output(location, environment)
                result_data["producers"] = producers

            if direction in ("consumers", "both"):
                consumers = self._backend.get_lineage_for_input(location, environment)
                result_data["consumers"] = consumers

            return self._success(command, data=result_data)
        except Exception as e:
            return self._error(command, [f"Failed to get lineage: {e!s}"])


def create_memory_registry_handler() -> RegistryHandler:
    """Factory function to create RegistryHandler with in-memory backend.

    Returns:
        RegistryHandler configured with in-memory backend.
    """
    backend = InMemoryRegistryBackend()
    return RegistryHandler(backend=backend)
