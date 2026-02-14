"""Standardized message types for platform communication."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class DataReference(BaseModel):
    """Reference to a data asset involved in an operation.

    Used to track inputs and outputs for lineage.
    """

    ref_type: str = Field(description="Type of reference: table, file, stream, model, api, etc.")
    location: str = Field(
        description="Location identifier: 'main.stg_departments', 's3://bucket/path', 'kafka://topic'"
    )
    format: str | None = Field(
        default=None,
        description="Data format: parquet, csv, json, iceberg, avro, etc.",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional metadata: row_count, bytes, schema, partition, etc.",
    )


class ExecutionRecord(BaseModel):
    """Record of what was actually executed in a step.

    Extensible by execution_type to support SQL, Python, Spark, ML, streaming, etc.
    """

    execution_type: str = Field(
        description="Type of execution: sql, python, spark, ml_training, ml_inference, stream, api_call"
    )
    details: dict[str, Any] = Field(
        default_factory=dict,
        description="Type-specific execution details",
    )

    @classmethod
    def sql(
        cls,
        statements: list[str],
        dialect: str = "duckdb",
        **extra: Any,
    ) -> ExecutionRecord:
        """Factory for SQL execution records."""
        return cls(
            execution_type="sql",
            details={"dialect": dialect, "statements": statements, **extra},
        )

    @classmethod
    def python(
        cls,
        module: str,
        function: str,
        args: dict[str, Any] | None = None,
        code_hash: str | None = None,
        **extra: Any,
    ) -> ExecutionRecord:
        """Factory for Python execution records."""
        return cls(
            execution_type="python",
            details={
                "module": module,
                "function": function,
                "args": args or {},
                "code_hash": code_hash,
                **extra,
            },
        )

    @classmethod
    def dbt(
        cls,
        command: str,
        models: list[str] | None = None,
        compiled_sql: list[str] | None = None,
        project_dir: str | None = None,
        target: str | None = None,
        **extra: Any,
    ) -> ExecutionRecord:
        """Factory for dbt execution records."""
        return cls(
            execution_type="dbt",
            details={
                "command": command,
                "models": models or [],
                "compiled_sql": compiled_sql or [],
                "project_dir": project_dir,
                "target": target,
                **extra,
            },
        )


class Command(BaseModel):
    """Inbound request to a service handler.

    Commands are published to service-specific SNS topics (e.g., pudato-storage-commands)
    and consumed by the appropriate Lambda handler.
    """

    type: str = Field(description="Service type: storage, query, transform, catalog, etc.")
    action: str = Field(description="Action to perform: put_object, execute_sql, run_model, etc.")
    payload: dict[str, Any] = Field(default_factory=dict, description="Action-specific parameters")
    correlation_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique ID for tracing/lineage across services",
    )
    job_id: str | None = Field(
        default=None,
        description="Job ID if this command is part of a tracked job",
    )
    step_id: str | None = Field(
        default=None,
        description="Step ID if this command corresponds to a pre-created step",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Optional context: user, source, timestamps, etc.",
    )
    timestamp: str = Field(
        default_factory=lambda: datetime.now(UTC).isoformat(),
        description="ISO8601 timestamp when command was created",
    )

    def to_json(self) -> str:
        """Serialize to JSON for SNS/SQS publishing."""
        return self.model_dump_json()

    @classmethod
    def from_json(cls, data: str) -> Command:
        """Deserialize from JSON (from SNS/SQS message body)."""
        return cls.model_validate_json(data)

    @classmethod
    def from_sns_message(cls, message: dict[str, Any]) -> Command:
        """Parse from SNS notification format.

        SNS wraps the message in a notification envelope. This method
        handles both direct messages and SNS notification format.
        """
        if "Message" in message:
            # SNS notification format
            return cls.from_json(message["Message"])
        return cls.model_validate(message)


class Result(BaseModel):
    """Standardized response from any handler.

    Results are published to the pudato-results topic for consumers
    to track command completion. Includes lineage tracking for inputs,
    outputs, and execution details.
    """

    status: Literal["success", "error", "pending"] = Field(
        description="Outcome of the command execution"
    )
    data: dict[str, Any] | None = Field(
        default=None,
        description="Result payload on success",
    )
    errors: list[str] = Field(
        default_factory=list,
        description="Error messages if status is 'error'",
    )
    correlation_id: str = Field(description="Matches the originating command's correlation_id")
    job_id: str | None = Field(
        default=None,
        description="Job ID if this result is part of a tracked job",
    )
    step_id: str | None = Field(
        default=None,
        description="Step ID if this result corresponds to a pre-created step",
    )
    duration_ms: int = Field(
        default=0,
        description="Execution time in milliseconds",
    )
    timestamp: str = Field(
        default_factory=lambda: datetime.now(UTC).isoformat(),
        description="ISO8601 timestamp when result was created",
    )
    handler: str | None = Field(
        default=None,
        description="Handler that processed the command",
    )
    # Lineage tracking
    inputs: list[DataReference] = Field(
        default_factory=list,
        description="Data assets read during execution",
    )
    outputs: list[DataReference] = Field(
        default_factory=list,
        description="Data assets written during execution",
    )
    executions: list[ExecutionRecord] = Field(
        default_factory=list,
        description="Records of what was executed (SQL, Python, etc.)",
    )

    def to_json(self) -> str:
        """Serialize to JSON for SNS/SQS publishing."""
        return self.model_dump_json()

    @classmethod
    def from_json(cls, data: str) -> Result:
        """Deserialize from JSON."""
        return cls.model_validate_json(data)

    @classmethod
    def success(
        cls,
        correlation_id: str,
        data: dict[str, Any] | None = None,
        duration_ms: int = 0,
        handler: str | None = None,
        inputs: list[DataReference] | None = None,
        outputs: list[DataReference] | None = None,
        executions: list[ExecutionRecord] | None = None,
        job_id: str | None = None,
        step_id: str | None = None,
    ) -> Result:
        """Factory for successful result."""
        return cls(
            status="success",
            data=data,
            correlation_id=correlation_id,
            job_id=job_id,
            step_id=step_id,
            duration_ms=duration_ms,
            handler=handler,
            inputs=inputs or [],
            outputs=outputs or [],
            executions=executions or [],
        )

    @classmethod
    def error(
        cls,
        correlation_id: str,
        errors: list[str],
        duration_ms: int = 0,
        handler: str | None = None,
        job_id: str | None = None,
        step_id: str | None = None,
    ) -> Result:
        """Factory for error result."""
        return cls(
            status="error",
            errors=errors,
            correlation_id=correlation_id,
            job_id=job_id,
            step_id=step_id,
            duration_ms=duration_ms,
            handler=handler,
        )

    @classmethod
    def pending(
        cls,
        correlation_id: str,
        data: dict[str, Any] | None = None,
        handler: str | None = None,
        job_id: str | None = None,
        step_id: str | None = None,
    ) -> Result:
        """Factory for pending/async result."""
        return cls(
            status="pending",
            data=data,
            correlation_id=correlation_id,
            job_id=job_id,
            step_id=step_id,
            handler=handler,
        )


class Event(BaseModel):
    """Notification event for cross-service communication.

    Events are published to the pudato-events topic. Other services
    can subscribe to react to platform events (e.g., catalog updating
    metadata when a new object is stored).
    """

    type: str = Field(
        description="Event type: storage.object_created, transform.model_completed, etc."
    )
    payload: dict[str, Any] = Field(
        default_factory=dict,
        description="Event-specific data",
    )
    correlation_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Correlation ID for tracing (often from originating command)",
    )
    timestamp: str = Field(
        default_factory=lambda: datetime.now(UTC).isoformat(),
        description="ISO8601 timestamp when event occurred",
    )
    source: str | None = Field(
        default=None,
        description="Service that emitted the event",
    )

    def to_json(self) -> str:
        """Serialize to JSON for SNS/SQS publishing."""
        return self.model_dump_json()

    @classmethod
    def from_json(cls, data: str) -> Event:
        """Deserialize from JSON."""
        return cls.model_validate_json(data)

    @classmethod
    def from_sns_message(cls, message: dict[str, Any]) -> Event:
        """Parse from SNS notification format."""
        if "Message" in message:
            return cls.from_json(message["Message"])
        return cls.model_validate(message)
