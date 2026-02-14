"""Job Registry backend protocol and implementations.

The Job Registry tracks execution history for lineage, reproducibility, and auditing.
It stores:
- Jobs: top-level execution requests (pipeline runs)
- JobSteps: individual handler invocations within a job
- Data references: inputs and outputs for lineage tracking
- Execution records: what was actually executed (SQL, Python, etc.)
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal, Protocol

import structlog

from pudato.protocol import DataReference, ExecutionRecord

if TYPE_CHECKING:
    from psycopg import Connection

logger = structlog.get_logger()


@dataclass
class Job:
    """A job represents a single pipeline execution request."""

    job_id: str
    pipeline: str  # pipeline/DAG name
    environment: str  # "dev", "staging", "prod"
    namespace: str  # schema/prefix for isolation (e.g., "dev_alice")
    status: Literal["pending", "running", "success", "failed", "cancelled"]
    logic_version: str  # git commit hash or version tag
    request: dict[str, Any]  # original request payload
    parameters: dict[str, Any]  # resolved/interpolated parameters
    dag_run_id: str | None = None  # Airflow correlation
    created_at: str = ""
    started_at: str | None = None
    completed_at: str | None = None
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.created_at:
            self.created_at = datetime.now(UTC).isoformat()

    @classmethod
    def create(
        cls,
        pipeline: str,
        environment: str,
        namespace: str,
        logic_version: str,
        request: dict[str, Any],
        parameters: dict[str, Any] | None = None,
        dag_run_id: str | None = None,
        job_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Job:
        """Factory to create a new pending job."""
        return cls(
            job_id=job_id or f"job-{uuid.uuid4().hex[:12]}",
            pipeline=pipeline,
            environment=environment,
            namespace=namespace,
            status="pending",
            logic_version=logic_version,
            request=request,
            parameters=parameters or {},
            dag_run_id=dag_run_id,
            metadata=metadata or {},
        )


@dataclass
class JobStep:
    """A step within a job - represents a single handler invocation."""

    step_id: str
    job_id: str
    step_name: str  # descriptive name (e.g., "build_staging_models")
    handler_type: str  # "transform", "query", "storage", etc.
    action: str  # "build", "execute", "put_object", etc.
    status: Literal["pending", "running", "success", "failed", "skipped"]
    correlation_id: str  # links to Command/Result
    inputs: list[DataReference] = field(default_factory=list)
    outputs: list[DataReference] = field(default_factory=list)
    executions: list[ExecutionRecord] = field(default_factory=list)
    started_at: str | None = None
    completed_at: str | None = None
    duration_ms: int = 0
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        job_id: str,
        step_name: str,
        handler_type: str,
        action: str,
        correlation_id: str,
        step_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> JobStep:
        """Factory to create a new pending step."""
        return cls(
            step_id=step_id or f"step-{uuid.uuid4().hex[:12]}",
            job_id=job_id,
            step_name=step_name,
            handler_type=handler_type,
            action=action,
            status="pending",
            correlation_id=correlation_id,
            metadata=metadata or {},
        )


@dataclass
class JobQuery:
    """Query parameters for searching jobs."""

    pipeline: str | None = None
    environment: str | None = None
    namespace: str | None = None
    status: str | None = None
    logic_version: str | None = None
    created_after: str | None = None
    created_before: str | None = None
    limit: int = 100
    offset: int = 0


class RegistryBackend(Protocol):
    """Protocol for job registry operations.

    Stores and retrieves job execution history for lineage and auditing.
    Implementations: InMemory (testing), PostgreSQL (production).
    """

    # Job operations
    def create_job(self, job: Job) -> Job:
        """Create a new job record."""
        ...

    def get_job(self, job_id: str) -> Job | None:
        """Get a job by ID."""
        ...

    def update_job(
        self,
        job_id: str,
        status: str | None = None,
        started_at: str | None = None,
        completed_at: str | None = None,
        error: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Job | None:
        """Update job fields. Returns updated job or None if not found."""
        ...

    def query_jobs(self, query: JobQuery) -> list[Job]:
        """Query jobs matching criteria."""
        ...

    # Step operations
    def add_step(self, step: JobStep) -> JobStep:
        """Add a step to a job."""
        ...

    def get_step(self, step_id: str) -> JobStep | None:
        """Get a step by ID."""
        ...

    def get_job_steps(self, job_id: str) -> list[JobStep]:
        """Get all steps for a job, ordered by started_at."""
        ...

    def update_step(
        self,
        step_id: str,
        status: str | None = None,
        started_at: str | None = None,
        completed_at: str | None = None,
        duration_ms: int | None = None,
        inputs: list[DataReference] | None = None,
        outputs: list[DataReference] | None = None,
        executions: list[ExecutionRecord] | None = None,
        error: str | None = None,
    ) -> JobStep | None:
        """Update step fields. Returns updated step or None if not found."""
        ...

    # Lineage queries
    def get_lineage_for_output(
        self,
        location: str,
        environment: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get jobs/steps that produced a given output location."""
        ...

    def get_lineage_for_input(
        self,
        location: str,
        environment: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get jobs/steps that consumed a given input location."""
        ...


class InMemoryRegistryBackend:
    """In-memory registry backend for testing.

    Not suitable for production - data is lost when process exits.
    """

    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}
        self._steps: dict[str, JobStep] = {}
        self._log = logger.bind(backend="registry_memory")

    def create_job(self, job: Job) -> Job:
        """Create a new job record."""
        self._jobs[job.job_id] = job
        self._log.debug("job_created", job_id=job.job_id, pipeline=job.pipeline)
        return job

    def get_job(self, job_id: str) -> Job | None:
        """Get a job by ID."""
        return self._jobs.get(job_id)

    def update_job(
        self,
        job_id: str,
        status: str | None = None,
        started_at: str | None = None,
        completed_at: str | None = None,
        error: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Job | None:
        """Update job fields."""
        job = self._jobs.get(job_id)
        if not job:
            return None

        if status:
            job.status = status  # type: ignore
        if started_at:
            job.started_at = started_at
        if completed_at:
            job.completed_at = completed_at
        if error is not None:
            job.error = error
        if metadata:
            job.metadata.update(metadata)

        self._log.debug("job_updated", job_id=job_id, status=status)
        return job

    def query_jobs(self, query: JobQuery) -> list[Job]:
        """Query jobs matching criteria."""
        results = []

        for job in self._jobs.values():
            if query.pipeline and job.pipeline != query.pipeline:
                continue
            if query.environment and job.environment != query.environment:
                continue
            if query.namespace and job.namespace != query.namespace:
                continue
            if query.status and job.status != query.status:
                continue
            if query.logic_version and job.logic_version != query.logic_version:
                continue
            if query.created_after and job.created_at < query.created_after:
                continue
            if query.created_before and job.created_at > query.created_before:
                continue
            results.append(job)

        # Sort by created_at descending
        results.sort(key=lambda j: j.created_at, reverse=True)

        # Apply offset and limit
        return results[query.offset : query.offset + query.limit]

    def add_step(self, step: JobStep) -> JobStep:
        """Add a step to a job."""
        self._steps[step.step_id] = step
        self._log.debug(
            "step_added",
            step_id=step.step_id,
            job_id=step.job_id,
            handler=step.handler_type,
        )
        return step

    def get_step(self, step_id: str) -> JobStep | None:
        """Get a step by ID."""
        return self._steps.get(step_id)

    def get_job_steps(self, job_id: str) -> list[JobStep]:
        """Get all steps for a job, ordered by started_at."""
        steps = [s for s in self._steps.values() if s.job_id == job_id]
        # Sort by started_at (None values last)
        steps.sort(key=lambda s: s.started_at or "9999")
        return steps

    def update_step(
        self,
        step_id: str,
        status: str | None = None,
        started_at: str | None = None,
        completed_at: str | None = None,
        duration_ms: int | None = None,
        inputs: list[DataReference] | None = None,
        outputs: list[DataReference] | None = None,
        executions: list[ExecutionRecord] | None = None,
        error: str | None = None,
    ) -> JobStep | None:
        """Update step fields."""
        step = self._steps.get(step_id)
        if not step:
            return None

        if status:
            step.status = status  # type: ignore
        if started_at:
            step.started_at = started_at
        if completed_at:
            step.completed_at = completed_at
        if duration_ms is not None:
            step.duration_ms = duration_ms
        if inputs is not None:
            step.inputs = inputs
        if outputs is not None:
            step.outputs = outputs
        if executions is not None:
            step.executions = executions
        if error is not None:
            step.error = error

        self._log.debug("step_updated", step_id=step_id, status=status)
        return step

    def get_lineage_for_output(
        self,
        location: str,
        environment: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get jobs/steps that produced a given output location."""
        results = []

        for step in self._steps.values():
            for output in step.outputs:
                if output.location == location:
                    job = self._jobs.get(step.job_id)
                    if environment and job and job.environment != environment:
                        continue
                    results.append(
                        {
                            "job_id": step.job_id,
                            "step_id": step.step_id,
                            "step_name": step.step_name,
                            "handler_type": step.handler_type,
                            "logic_version": job.logic_version if job else None,
                            "completed_at": step.completed_at,
                            "executions": [e.model_dump() for e in step.executions],
                        }
                    )

        # Sort by completed_at descending
        results.sort(key=lambda r: r.get("completed_at") or "", reverse=True)
        return results

    def get_lineage_for_input(
        self,
        location: str,
        environment: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get jobs/steps that consumed a given input location."""
        results = []

        for step in self._steps.values():
            for inp in step.inputs:
                if inp.location == location:
                    job = self._jobs.get(step.job_id)
                    if environment and job and job.environment != environment:
                        continue
                    results.append(
                        {
                            "job_id": step.job_id,
                            "step_id": step.step_id,
                            "step_name": step.step_name,
                            "handler_type": step.handler_type,
                            "logic_version": job.logic_version if job else None,
                            "started_at": step.started_at,
                        }
                    )

        results.sort(key=lambda r: r.get("started_at") or "", reverse=True)
        return results


# SQL schema for PostgreSQL backend
_POSTGRES_SCHEMA = """
CREATE TABLE IF NOT EXISTS pudato_jobs (
    job_id TEXT PRIMARY KEY,
    pipeline TEXT NOT NULL,
    environment TEXT NOT NULL DEFAULT 'dev',
    namespace TEXT NOT NULL DEFAULT 'default',
    status TEXT NOT NULL DEFAULT 'pending',
    logic_version TEXT NOT NULL,
    request JSONB NOT NULL DEFAULT '{}',
    parameters JSONB NOT NULL DEFAULT '{}',
    dag_run_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_jobs_pipeline ON pudato_jobs(pipeline);
CREATE INDEX IF NOT EXISTS idx_jobs_environment ON pudato_jobs(environment);
CREATE INDEX IF NOT EXISTS idx_jobs_namespace ON pudato_jobs(namespace);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON pudato_jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON pudato_jobs(created_at);

CREATE TABLE IF NOT EXISTS pudato_job_steps (
    step_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES pudato_jobs(job_id) ON DELETE CASCADE,
    step_name TEXT NOT NULL,
    handler_type TEXT NOT NULL,
    action TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    correlation_id TEXT NOT NULL,
    inputs JSONB NOT NULL DEFAULT '[]',
    outputs JSONB NOT NULL DEFAULT '[]',
    executions JSONB NOT NULL DEFAULT '[]',
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    duration_ms INTEGER,
    error TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_steps_job_id ON pudato_job_steps(job_id);
CREATE INDEX IF NOT EXISTS idx_steps_status ON pudato_job_steps(status);
CREATE INDEX IF NOT EXISTS idx_steps_started_at ON pudato_job_steps(started_at);
"""


class PostgreSQLRegistryBackend:
    """PostgreSQL-backed registry for production use.

    Provides durable storage with ACID guarantees for job execution history.
    Requires psycopg 3.x.
    """

    def __init__(self, conn: Connection[Any]) -> None:
        """Initialize with a psycopg connection.

        Args:
            conn: An open psycopg Connection. Caller is responsible for
                  connection lifecycle management.
        """
        self._conn = conn
        self._log = logger.bind(backend="registry_postgres")

    def initialize_schema(self) -> None:
        """Create tables and indexes if they don't exist."""
        with self._conn.cursor() as cur:
            cur.execute(_POSTGRES_SCHEMA)
        self._conn.commit()
        self._log.info("schema_initialized")

    def create_job(self, job: Job) -> Job:
        """Create a new job record."""
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pudato_jobs (
                    job_id, pipeline, environment, namespace, status,
                    logic_version, request, parameters, dag_run_id,
                    created_at, started_at, completed_at, error, metadata
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    job.job_id,
                    job.pipeline,
                    job.environment,
                    job.namespace,
                    job.status,
                    job.logic_version,
                    json.dumps(job.request),
                    json.dumps(job.parameters),
                    job.dag_run_id,
                    job.created_at,
                    job.started_at,
                    job.completed_at,
                    job.error,
                    json.dumps(job.metadata),
                ),
            )
        self._conn.commit()
        self._log.debug("job_created", job_id=job.job_id, pipeline=job.pipeline)
        return job

    def get_job(self, job_id: str) -> Job | None:
        """Get a job by ID."""
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT job_id, pipeline, environment, namespace, status,
                       logic_version, request, parameters, dag_run_id,
                       created_at, started_at, completed_at, error, metadata
                FROM pudato_jobs WHERE job_id = %s
                """,
                (job_id,),
            )
            row = cur.fetchone()

        if not row:
            return None

        return Job(
            job_id=row[0],
            pipeline=row[1],
            environment=row[2],
            namespace=row[3],
            status=row[4],
            logic_version=row[5],
            request=row[6] if isinstance(row[6], dict) else json.loads(row[6] or "{}"),
            parameters=row[7] if isinstance(row[7], dict) else json.loads(row[7] or "{}"),
            dag_run_id=row[8],
            created_at=row[9].isoformat() if hasattr(row[9], "isoformat") else row[9],
            started_at=row[10].isoformat()
            if row[10] and hasattr(row[10], "isoformat")
            else row[10],
            completed_at=row[11].isoformat()
            if row[11] and hasattr(row[11], "isoformat")
            else row[11],
            error=row[12],
            metadata=row[13] if isinstance(row[13], dict) else json.loads(row[13] or "{}"),
        )

    def update_job(
        self,
        job_id: str,
        status: str | None = None,
        started_at: str | None = None,
        completed_at: str | None = None,
        error: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Job | None:
        """Update job fields."""
        # Build dynamic UPDATE query
        updates = []
        params: list[Any] = []

        if status is not None:
            updates.append("status = %s")
            params.append(status)
        if started_at is not None:
            updates.append("started_at = %s")
            params.append(started_at)
        if completed_at is not None:
            updates.append("completed_at = %s")
            params.append(completed_at)
        if error is not None:
            updates.append("error = %s")
            params.append(error)
        if metadata is not None:
            updates.append("metadata = metadata || %s")
            params.append(json.dumps(metadata))

        if not updates:
            return self.get_job(job_id)

        params.append(job_id)

        with self._conn.cursor() as cur:
            cur.execute(
                f"UPDATE pudato_jobs SET {', '.join(updates)} WHERE job_id = %s",
                params,
            )
            if cur.rowcount == 0:
                return None

        self._conn.commit()
        self._log.debug("job_updated", job_id=job_id, status=status)
        return self.get_job(job_id)

    def query_jobs(self, query: JobQuery) -> list[Job]:
        """Query jobs matching criteria."""
        conditions = []
        params: list[Any] = []

        if query.pipeline:
            conditions.append("pipeline = %s")
            params.append(query.pipeline)
        if query.environment:
            conditions.append("environment = %s")
            params.append(query.environment)
        if query.namespace:
            conditions.append("namespace = %s")
            params.append(query.namespace)
        if query.status:
            conditions.append("status = %s")
            params.append(query.status)
        if query.logic_version:
            conditions.append("logic_version = %s")
            params.append(query.logic_version)
        if query.created_after:
            conditions.append("created_at >= %s")
            params.append(query.created_after)
        if query.created_before:
            conditions.append("created_at <= %s")
            params.append(query.created_before)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        params.extend([query.limit, query.offset])

        with self._conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT job_id, pipeline, environment, namespace, status,
                       logic_version, request, parameters, dag_run_id,
                       created_at, started_at, completed_at, error, metadata
                FROM pudato_jobs
                {where_clause}
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                params,
            )
            rows = cur.fetchall()

        return [
            Job(
                job_id=row[0],
                pipeline=row[1],
                environment=row[2],
                namespace=row[3],
                status=row[4],
                logic_version=row[5],
                request=row[6] if isinstance(row[6], dict) else json.loads(row[6] or "{}"),
                parameters=row[7] if isinstance(row[7], dict) else json.loads(row[7] or "{}"),
                dag_run_id=row[8],
                created_at=row[9].isoformat() if hasattr(row[9], "isoformat") else row[9],
                started_at=row[10].isoformat()
                if row[10] and hasattr(row[10], "isoformat")
                else row[10],
                completed_at=row[11].isoformat()
                if row[11] and hasattr(row[11], "isoformat")
                else row[11],
                error=row[12],
                metadata=row[13] if isinstance(row[13], dict) else json.loads(row[13] or "{}"),
            )
            for row in rows
        ]

    def add_step(self, step: JobStep) -> JobStep:
        """Add a step to a job."""
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pudato_job_steps (
                    step_id, job_id, step_name, handler_type, action, status,
                    correlation_id, inputs, outputs, executions,
                    started_at, completed_at, duration_ms, error, metadata
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    step.step_id,
                    step.job_id,
                    step.step_name,
                    step.handler_type,
                    step.action,
                    step.status,
                    step.correlation_id,
                    json.dumps([i.model_dump() for i in step.inputs]),
                    json.dumps([o.model_dump() for o in step.outputs]),
                    json.dumps([e.model_dump() for e in step.executions]),
                    step.started_at,
                    step.completed_at,
                    step.duration_ms if step.duration_ms else None,
                    step.error,
                    json.dumps(step.metadata),
                ),
            )
        self._conn.commit()
        self._log.debug(
            "step_added",
            step_id=step.step_id,
            job_id=step.job_id,
            handler=step.handler_type,
        )
        return step

    def get_step(self, step_id: str) -> JobStep | None:
        """Get a step by ID."""
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT step_id, job_id, step_name, handler_type, action, status,
                       correlation_id, inputs, outputs, executions,
                       started_at, completed_at, duration_ms, error, metadata
                FROM pudato_job_steps WHERE step_id = %s
                """,
                (step_id,),
            )
            row = cur.fetchone()

        if not row:
            return None

        return self._row_to_step(row)

    def get_job_steps(self, job_id: str) -> list[JobStep]:
        """Get all steps for a job, ordered by started_at."""
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT step_id, job_id, step_name, handler_type, action, status,
                       correlation_id, inputs, outputs, executions,
                       started_at, completed_at, duration_ms, error, metadata
                FROM pudato_job_steps
                WHERE job_id = %s
                ORDER BY COALESCE(started_at, '9999-12-31'::timestamptz)
                """,
                (job_id,),
            )
            rows = cur.fetchall()

        return [self._row_to_step(row) for row in rows]

    def update_step(
        self,
        step_id: str,
        status: str | None = None,
        started_at: str | None = None,
        completed_at: str | None = None,
        duration_ms: int | None = None,
        inputs: list[DataReference] | None = None,
        outputs: list[DataReference] | None = None,
        executions: list[ExecutionRecord] | None = None,
        error: str | None = None,
    ) -> JobStep | None:
        """Update step fields."""
        updates = []
        params: list[Any] = []

        if status is not None:
            updates.append("status = %s")
            params.append(status)
        if started_at is not None:
            updates.append("started_at = %s")
            params.append(started_at)
        if completed_at is not None:
            updates.append("completed_at = %s")
            params.append(completed_at)
        if duration_ms is not None:
            updates.append("duration_ms = %s")
            params.append(duration_ms)
        if inputs is not None:
            updates.append("inputs = %s")
            params.append(json.dumps([i.model_dump() for i in inputs]))
        if outputs is not None:
            updates.append("outputs = %s")
            params.append(json.dumps([o.model_dump() for o in outputs]))
        if executions is not None:
            updates.append("executions = %s")
            params.append(json.dumps([e.model_dump() for e in executions]))
        if error is not None:
            updates.append("error = %s")
            params.append(error)

        if not updates:
            return self.get_step(step_id)

        params.append(step_id)

        with self._conn.cursor() as cur:
            cur.execute(
                f"UPDATE pudato_job_steps SET {', '.join(updates)} WHERE step_id = %s",
                params,
            )
            if cur.rowcount == 0:
                return None

        self._conn.commit()
        self._log.debug("step_updated", step_id=step_id, status=status)
        return self.get_step(step_id)

    def get_lineage_for_output(
        self,
        location: str,
        environment: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get jobs/steps that produced a given output location."""
        params: list[Any] = [f'%"location": "{location}"%']

        env_filter = ""
        if environment:
            env_filter = "AND j.environment = %s"
            params.append(environment)

        with self._conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT s.job_id, s.step_id, s.step_name, s.handler_type,
                       j.logic_version, s.completed_at, s.executions
                FROM pudato_job_steps s
                JOIN pudato_jobs j ON s.job_id = j.job_id
                WHERE s.outputs::text LIKE %s {env_filter}
                ORDER BY s.completed_at DESC NULLS LAST
                """,
                params,
            )
            rows = cur.fetchall()

        return [
            {
                "job_id": row[0],
                "step_id": row[1],
                "step_name": row[2],
                "handler_type": row[3],
                "logic_version": row[4],
                "completed_at": row[5].isoformat()
                if row[5] and hasattr(row[5], "isoformat")
                else row[5],
                "executions": row[6] if isinstance(row[6], list) else json.loads(row[6] or "[]"),
            }
            for row in rows
        ]

    def get_lineage_for_input(
        self,
        location: str,
        environment: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get jobs/steps that consumed a given input location."""
        params: list[Any] = [f'%"location": "{location}"%']

        env_filter = ""
        if environment:
            env_filter = "AND j.environment = %s"
            params.append(environment)

        with self._conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT s.job_id, s.step_id, s.step_name, s.handler_type,
                       j.logic_version, s.started_at
                FROM pudato_job_steps s
                JOIN pudato_jobs j ON s.job_id = j.job_id
                WHERE s.inputs::text LIKE %s {env_filter}
                ORDER BY s.started_at DESC NULLS LAST
                """,
                params,
            )
            rows = cur.fetchall()

        return [
            {
                "job_id": row[0],
                "step_id": row[1],
                "step_name": row[2],
                "handler_type": row[3],
                "logic_version": row[4],
                "started_at": row[5].isoformat()
                if row[5] and hasattr(row[5], "isoformat")
                else row[5],
            }
            for row in rows
        ]

    def _row_to_step(self, row: tuple[Any, ...]) -> JobStep:
        """Convert a database row to a JobStep."""
        inputs_raw = row[7] if isinstance(row[7], list) else json.loads(row[7] or "[]")
        outputs_raw = row[8] if isinstance(row[8], list) else json.loads(row[8] or "[]")
        executions_raw = row[9] if isinstance(row[9], list) else json.loads(row[9] or "[]")

        return JobStep(
            step_id=row[0],
            job_id=row[1],
            step_name=row[2],
            handler_type=row[3],
            action=row[4],
            status=row[5],
            correlation_id=row[6],
            inputs=[DataReference(**i) for i in inputs_raw],
            outputs=[DataReference(**o) for o in outputs_raw],
            executions=[ExecutionRecord(**e) for e in executions_raw],
            started_at=row[10].isoformat()
            if row[10] and hasattr(row[10], "isoformat")
            else row[10],
            completed_at=row[11].isoformat()
            if row[11] and hasattr(row[11], "isoformat")
            else row[11],
            duration_ms=row[12] or 0,
            error=row[13],
            metadata=row[14] if isinstance(row[14], dict) else json.loads(row[14] or "{}"),
        )


def create_postgres_registry_backend(connection_string: str) -> PostgreSQLRegistryBackend:
    """Factory to create a PostgreSQL registry backend.

    Args:
        connection_string: PostgreSQL connection string (e.g., "postgresql://user:pass@host/db")

    Returns:
        Initialized PostgreSQLRegistryBackend with schema created.
    """
    import psycopg

    conn = psycopg.connect(connection_string)
    backend = PostgreSQLRegistryBackend(conn)
    backend.initialize_schema()
    return backend
