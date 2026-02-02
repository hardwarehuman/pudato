"""Results consumer for persisting lineage to the registry.

This consumer subscribes to the results SNS topic (via SQS) and:
1. Receives Results from handler executions
2. For Results with a step_id, updates the registry step with lineage data
3. Ignores Results without step_id (not part of a tracked job)

The consumer can run as:
- A Lambda function (handle() entry point)
- A local process for development (run_local())
"""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, Any

import boto3
import structlog

from pudato.config import get_settings
from pudato.protocol import Result

if TYPE_CHECKING:
    from pudato.handlers.registry import RegistryHandler

logger = structlog.get_logger()


def _create_registry_handler() -> RegistryHandler:
    """Create registry handler based on environment config."""
    from pudato.backends.registry import RegistryBackend
    from pudato.handlers.registry import RegistryHandler

    database_url = os.environ.get("REGISTRY_DATABASE_URL")

    backend: RegistryBackend
    if database_url:
        from pudato.backends.registry import create_postgres_registry_backend

        backend = create_postgres_registry_backend(database_url)
    else:
        from pudato.backends.registry import InMemoryRegistryBackend

        backend = InMemoryRegistryBackend()

    return RegistryHandler(backend=backend)


def process_result(result: Result, registry: RegistryHandler) -> dict[str, Any]:
    """Process a single result and persist lineage if applicable.

    Args:
        result: The Result to process
        registry: Registry handler for persisting lineage

    Returns:
        Processing outcome with step_id and status
    """
    log = logger.bind(
        correlation_id=result.correlation_id,
        job_id=result.job_id,
        step_id=result.step_id,
    )

    # Skip results without step_id - not part of a tracked job
    if not result.step_id:
        log.debug("skipping_result_no_step_id")
        return {
            "correlation_id": result.correlation_id,
            "action": "skipped",
            "reason": "no_step_id",
        }

    log.info("processing_result_for_lineage")

    # Build update_step command payload
    from pudato.protocol import Command

    # Map result status to step status
    step_status = "success" if result.status == "success" else "failed"

    update_payload: dict[str, Any] = {
        "step_id": result.step_id,
        "status": step_status,
        "duration_ms": result.duration_ms,
    }

    # Include lineage data if present
    if result.inputs:
        update_payload["inputs"] = [i.model_dump() for i in result.inputs]

    if result.outputs:
        update_payload["outputs"] = [o.model_dump() for o in result.outputs]

    if result.executions:
        update_payload["executions"] = [e.model_dump() for e in result.executions]

    # Include error if present
    if result.errors:
        update_payload["error"] = "; ".join(result.errors)

    # Create and execute update_step command
    command = Command(
        type="registry",
        action="update_step",
        payload=update_payload,
        correlation_id=result.correlation_id,
        job_id=result.job_id,
        step_id=result.step_id,
    )

    update_result = registry.handle(command)

    if update_result.status == "success":
        log.info("lineage_persisted", step_id=result.step_id)
        return {
            "correlation_id": result.correlation_id,
            "step_id": result.step_id,
            "action": "updated",
            "status": "success",
        }
    else:
        log.error(
            "lineage_persist_failed",
            step_id=result.step_id,
            errors=update_result.errors,
        )
        return {
            "correlation_id": result.correlation_id,
            "step_id": result.step_id,
            "action": "updated",
            "status": "error",
            "errors": update_result.errors,
        }


def handle(event: dict[str, Any], context: Any = None) -> dict[str, Any]:  # noqa: ARG001
    """Lambda entry point for results consumer.

    Args:
        event: SQS event with Records containing Results
        context: Lambda context (unused)

    Returns:
        Processing summary
    """
    log = logger.bind(consumer="results")
    log.info("results_consumer_invoked", record_count=len(event.get("Records", [])))

    registry = _create_registry_handler()

    outcomes = []
    for record in event.get("Records", []):
        try:
            # Parse SQS message (may be wrapped in SNS notification)
            body = json.loads(record["body"])
            message_str = body.get("Message", record["body"])
            result = Result.from_json(message_str)

            outcome = process_result(result, registry)
            outcomes.append(outcome)

        except Exception as e:
            log.exception("result_processing_failed", error=str(e))
            outcomes.append({"status": "error", "error": str(e)})

    return {"processed": len(outcomes), "outcomes": outcomes}


def run_local(poll_interval: float = 1.0, max_messages: int = 10) -> None:
    """Run results consumer locally, polling SQS.

    Args:
        poll_interval: Seconds between polls when queue is empty
        max_messages: Max messages to receive per poll
    """
    import time

    settings = get_settings()
    log = logger.bind(consumer="results", mode="local")

    if not settings.results_queue_url:
        raise ValueError("RESULTS_QUEUE_URL must be set for local consumer")

    log.info("starting_local_results_consumer", queue_url=settings.results_queue_url)

    sqs = boto3.client(
        "sqs",
        endpoint_url=settings.endpoint_url,
        region_name=settings.aws_region,
    )

    registry = _create_registry_handler()

    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=settings.results_queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=int(poll_interval),
            )

            messages = response.get("Messages", [])
            if not messages:
                continue

            for message in messages:
                try:
                    body = json.loads(message["Body"])
                    message_str = body.get("Message", message["Body"])
                    result = Result.from_json(message_str)

                    outcome = process_result(result, registry)
                    log.info("processed_result", **outcome)

                    # Delete message after successful processing
                    sqs.delete_message(
                        QueueUrl=settings.results_queue_url,
                        ReceiptHandle=message["ReceiptHandle"],
                    )

                except Exception as e:
                    log.exception("message_processing_failed", error=str(e))

        except KeyboardInterrupt:
            log.info("shutting_down")
            break
        except Exception as e:
            log.exception("poll_failed", error=str(e))
            time.sleep(poll_interval)
