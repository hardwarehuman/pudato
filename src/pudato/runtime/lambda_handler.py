"""AWS Lambda entry point for handler invocation.

This is a thin wrapper that:
1. Parses the SQS event
2. Creates the appropriate handler
3. Invokes the handler
4. Publishes the result

Handler type is configured via HANDLER_TYPE environment variable.
"""

from __future__ import annotations

import json
import os
from typing import Any

import boto3
import structlog

from pudato.config import get_settings
from pudato.protocol import Command

logger = structlog.get_logger()

# Handler type -> factory function mapping
# Extended as new handlers are added
_HANDLER_FACTORIES: dict[str, Any] = {}


def register_handler_factory(handler_type: str, factory: Any) -> None:
    """Register a factory function for a handler type."""
    _HANDLER_FACTORIES[handler_type] = factory


def _create_storage_handler() -> Any:
    """Factory for storage handler."""
    from pudato.backends.storage import S3Backend
    from pudato.handlers.storage import StorageHandler

    settings = get_settings()
    s3_client = boto3.client(
        "s3",
        endpoint_url=settings.endpoint_url,
        region_name=settings.aws_region,
    )
    return StorageHandler(backend=S3Backend(client=s3_client))


def _create_transform_handler() -> Any:
    """Factory for transform handler."""
    from pathlib import Path

    from pudato.backends.logic_repo import resolve_logic_dir
    from pudato.handlers.transform import TransformHandler

    settings = get_settings()

    # DBT_PROJECT_DIR overrides everything (for direct local testing)
    explicit_project_dir = os.environ.get("DBT_PROJECT_DIR")

    if explicit_project_dir:
        project_dir = Path(explicit_project_dir)
        logic_version = None
    else:
        # Resolve via logic repo config (clone external repo or fall back to dbt/)
        project_dir, logic_version = resolve_logic_dir(
            repo_url=settings.logic_repo_url,
            clone_dir=Path(settings.logic_repo_clone_dir),
            branch=settings.logic_repo_branch,
            fallback_dir=Path("dbt"),
        )
        if settings.logic_repo_subdir:
            project_dir = project_dir / settings.logic_repo_subdir

    profiles_dir = os.environ.get("DBT_PROFILES_DIR")
    target = os.environ.get("DBT_TARGET")

    return TransformHandler(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        target=target,
        default_logic_version=logic_version,
    )


def _create_query_handler() -> Any:
    """Factory for query handler."""
    from pathlib import Path

    from pudato.handlers.query import create_duckdb_handler

    # Database path from env var; None for in-memory
    db_path = os.environ.get("DUCKDB_PATH")
    read_only = os.environ.get("DUCKDB_READ_ONLY", "false").lower() == "true"

    return create_duckdb_handler(
        database_path=Path(db_path) if db_path else None,
        read_only=read_only,
    )


def _create_table_handler() -> Any:
    """Factory for table handler."""
    from pathlib import Path

    from pudato.handlers.table import create_duckdb_table_handler

    # Database path from env var; None for in-memory
    db_path = os.environ.get("DUCKDB_PATH")
    read_only = os.environ.get("DUCKDB_READ_ONLY", "false").lower() == "true"

    return create_duckdb_table_handler(
        database_path=Path(db_path) if db_path else None,
        read_only=read_only,
    )


def _create_catalog_handler() -> Any:
    """Factory for catalog handler."""
    from pathlib import Path

    from pudato.handlers.catalog import create_file_catalog_handler, create_memory_catalog_handler

    # Catalog path from env var; None for in-memory
    catalog_path = os.environ.get("CATALOG_PATH")

    if catalog_path:
        return create_file_catalog_handler(catalog_path=Path(catalog_path))
    else:
        return create_memory_catalog_handler()


def _create_registry_handler() -> Any:
    """Factory for registry handler."""
    from pudato.handlers.registry import RegistryHandler

    database_url = os.environ.get("REGISTRY_DATABASE_URL")

    if database_url:
        from pudato.backends.registry import create_postgres_registry_backend

        backend = create_postgres_registry_backend(database_url)
    else:
        from pudato.backends.registry import InMemoryRegistryBackend

        backend = InMemoryRegistryBackend()  # type: ignore[assignment]

    return RegistryHandler(backend=backend)


# Register built-in handlers
register_handler_factory("storage", _create_storage_handler)
register_handler_factory("transform", _create_transform_handler)
register_handler_factory("query", _create_query_handler)
register_handler_factory("table", _create_table_handler)
register_handler_factory("catalog", _create_catalog_handler)
register_handler_factory("registry", _create_registry_handler)


def handle(event: dict[str, Any], context: Any = None) -> dict[str, Any]:  # noqa: ARG001
    """Lambda entry point.

    Args:
        event: SQS event with Records
        context: Lambda context (unused)

    Returns:
        Processing summary
    """
    handler_type = os.environ.get("HANDLER_TYPE", "storage")
    settings = get_settings()

    log = logger.bind(handler_type=handler_type)
    log.info("lambda_invoked", record_count=len(event.get("Records", [])))

    # Create handler
    factory = _HANDLER_FACTORIES.get(handler_type)
    if not factory:
        raise ValueError(f"Unknown handler type: {handler_type}")
    handler = factory()

    # SNS client for publishing results
    sns = boto3.client(
        "sns",
        endpoint_url=settings.endpoint_url,
        region_name=settings.aws_region,
    )

    results = []
    for record in event.get("Records", []):
        try:
            # Parse SQS message (may be wrapped in SNS notification)
            body = json.loads(record["body"])
            message_str = body.get("Message", record["body"])
            command = Command.from_json(message_str)

            # Process command
            result = handler.handle(command)

            results.append(
                {
                    "correlation_id": result.correlation_id,
                    "status": result.status,
                }
            )

            # Publish result (failure here shouldn't discard handler result)
            try:
                sns.publish(
                    TopicArn=settings.results_topic_arn,
                    Message=result.to_json(),
                )
            except Exception as pub_err:
                log.error(
                    "result_publish_failed",
                    error=str(pub_err),
                    correlation_id=result.correlation_id,
                )

        except Exception as e:
            log.exception("processing_failed", error=str(e))
            results.append({"status": "error", "error": str(e)})

    return {"processed": len(results), "results": results}
