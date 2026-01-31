"""Storage handler for object storage operations."""

from __future__ import annotations

import base64

from pudato.backends.storage import StorageBackend
from pudato.handlers.base import BaseHandler
from pudato.protocol import Command, DataReference, Result


class StorageHandler(BaseHandler):
    """Handler for storage operations.

    Translates standardized Command messages into storage backend operations.
    Uses generic terminology that maps to any cloud provider:

    - container: S3 bucket, Azure container, GCS bucket
    - path: S3 key, Azure blob name, GCS object name

    Supported actions:
    - put_object: Store an object
    - get_object: Retrieve an object
    - delete_object: Delete an object
    - list_objects: List objects in a container
    - head_object: Get object metadata
    """

    service_type = "storage"

    def __init__(self, backend: StorageBackend) -> None:
        super().__init__()
        self._backend = backend

    def supported_actions(self) -> list[str]:
        """Return list of supported storage actions."""
        return [
            "put_object",
            "get_object",
            "delete_object",
            "list_objects",
            "head_object",
        ]

    def _handle(self, command: Command) -> Result:
        """Route command to appropriate storage operation."""
        match command.action:
            case "put_object":
                return self._put_object(command)
            case "get_object":
                return self._get_object(command)
            case "delete_object":
                return self._delete_object(command)
            case "list_objects":
                return self._list_objects(command)
            case "head_object":
                return self._head_object(command)
            case _:
                return self._error(command, [f"Unknown action: {command.action}"])

    def _put_object(self, command: Command) -> Result:
        """Handle put_object command."""
        payload = command.payload

        # Validate required fields
        container = payload.get("container")
        path = payload.get("path")
        if not container or not path:
            return self._error(command, ["Missing required fields: container, path"])

        # Get body - support both raw string and base64-encoded binary
        body: bytes | str
        if "body" in payload:
            body = payload["body"]
        elif "body_base64" in payload:
            body = base64.b64decode(payload["body_base64"])
        else:
            return self._error(command, ["Missing required field: body or body_base64"])

        result = self._backend.put_object(
            container=container,
            path=path,
            body=body,
            content_type=payload.get("content_type"),
            metadata=payload.get("metadata"),
        )

        # Track output for lineage
        output_ref = DataReference(
            ref_type="file",
            location=f"{container}/{path}",
            format=payload.get("content_type"),
        )

        return self._success(command, data=result, outputs=[output_ref])

    def _get_object(self, command: Command) -> Result:
        """Handle get_object command."""
        payload = command.payload

        container = payload.get("container")
        path = payload.get("path")
        if not container or not path:
            return self._error(command, ["Missing required fields: container, path"])

        result = self._backend.get_object(container=container, path=path)

        # Encode binary body as base64 for JSON serialization
        body = result.pop("body")
        result["body_base64"] = base64.b64encode(body).decode("ascii")
        result["size"] = len(body)

        # Convert datetime to ISO string if present
        if result.get("last_modified"):
            result["last_modified"] = result["last_modified"].isoformat()

        # Track input for lineage
        input_ref = DataReference(
            ref_type="file",
            location=f"{container}/{path}",
            format=result.get("content_type"),
        )

        return self._success(command, data=result, inputs=[input_ref])

    def _delete_object(self, command: Command) -> Result:
        """Handle delete_object command."""
        payload = command.payload

        container = payload.get("container")
        path = payload.get("path")
        if not container or not path:
            return self._error(command, ["Missing required fields: container, path"])

        result = self._backend.delete_object(container=container, path=path)
        return self._success(command, data=result)

    def _list_objects(self, command: Command) -> Result:
        """Handle list_objects command."""
        payload = command.payload

        container = payload.get("container")
        if not container:
            return self._error(command, ["Missing required field: container"])

        result = self._backend.list_objects(
            container=container,
            prefix=payload.get("prefix"),
            max_keys=payload.get("max_keys", 1000),
        )

        # Convert datetime objects in the response
        for obj in result.get("objects", []):
            if obj.get("last_modified"):
                obj["last_modified"] = obj["last_modified"].isoformat()

        return self._success(command, data=result)

    def _head_object(self, command: Command) -> Result:
        """Handle head_object command."""
        payload = command.payload

        container = payload.get("container")
        path = payload.get("path")
        if not container or not path:
            return self._error(command, ["Missing required fields: container, path"])

        result = self._backend.head_object(container=container, path=path)

        # Convert datetime to ISO string if present
        if result.get("last_modified"):
            result["last_modified"] = result["last_modified"].isoformat()

        return self._success(command, data=result)
