"""Storage backend protocol and implementations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

import structlog

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

logger = structlog.get_logger()


class StorageBackend(Protocol):
    """Protocol for storage operations.

    Uses generic terminology that maps to any cloud provider:
    - container: S3 bucket, Azure container, GCS bucket
    - path: S3 key, Azure blob name, GCS object name

    Implementations handle the actual storage operations and translate
    generic terms to service-specific concepts.
    """

    def put_object(
        self,
        container: str,
        path: str,
        body: bytes | str,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Store an object.

        Returns:
            Dict with storage metadata (e.g., etag, version_id)
        """
        ...

    def get_object(
        self,
        container: str,
        path: str,
    ) -> dict[str, Any]:
        """Retrieve an object.

        Returns:
            Dict with 'body' (bytes), 'content_type', 'metadata', etc.
        """
        ...

    def delete_object(
        self,
        container: str,
        path: str,
    ) -> dict[str, Any]:
        """Delete an object.

        Returns:
            Dict with deletion confirmation.
        """
        ...

    def list_objects(
        self,
        container: str,
        prefix: str | None = None,
        max_keys: int = 1000,
    ) -> dict[str, Any]:
        """List objects in a container.

        Returns:
            Dict with 'objects' list and pagination info.
        """
        ...

    def head_object(
        self,
        container: str,
        path: str,
    ) -> dict[str, Any]:
        """Get object metadata without retrieving the body.

        Returns:
            Dict with object metadata.
        """
        ...


class S3Backend:
    """S3 storage backend implementation.

    Works with both real AWS S3 and LocalStack.
    Translates generic terms: container → bucket, path → key.
    """

    def __init__(self, client: S3Client) -> None:
        self._client = client
        self._log = logger.bind(backend="s3")

    def put_object(
        self,
        container: str,
        path: str,
        body: bytes | str,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Store an object in S3."""
        if isinstance(body, str):
            body = body.encode("utf-8")

        kwargs: dict[str, Any] = {
            "Bucket": container,
            "Key": path,
            "Body": body,
        }
        if content_type:
            kwargs["ContentType"] = content_type
        if metadata:
            kwargs["Metadata"] = metadata

        self._log.debug("put_object", container=container, path=path, size=len(body))
        response = self._client.put_object(**kwargs)

        return {
            "etag": response.get("ETag", "").strip('"'),
            "version_id": response.get("VersionId"),
        }

    def get_object(
        self,
        container: str,
        path: str,
    ) -> dict[str, Any]:
        """Retrieve an object from S3."""
        self._log.debug("get_object", container=container, path=path)
        response = self._client.get_object(Bucket=container, Key=path)

        body = response["Body"].read()

        return {
            "body": body,
            "content_type": response.get("ContentType"),
            "content_length": response.get("ContentLength"),
            "etag": response.get("ETag", "").strip('"'),
            "metadata": response.get("Metadata", {}),
            "last_modified": response.get("LastModified"),
        }

    def delete_object(
        self,
        container: str,
        path: str,
    ) -> dict[str, Any]:
        """Delete an object from S3."""
        self._log.debug("delete_object", container=container, path=path)
        response = self._client.delete_object(Bucket=container, Key=path)

        return {
            "deleted": True,
            "version_id": response.get("VersionId"),
        }

    def list_objects(
        self,
        container: str,
        prefix: str | None = None,
        max_keys: int = 1000,
    ) -> dict[str, Any]:
        """List objects in an S3 bucket."""
        kwargs: dict[str, Any] = {
            "Bucket": container,
            "MaxKeys": max_keys,
        }
        if prefix:
            kwargs["Prefix"] = prefix

        self._log.debug("list_objects", container=container, prefix=prefix)
        response = self._client.list_objects_v2(**kwargs)

        objects = [
            {
                "path": obj["Key"],
                "size": obj["Size"],
                "etag": obj.get("ETag", "").strip('"'),
                "last_modified": obj.get("LastModified"),
            }
            for obj in response.get("Contents", [])
        ]

        return {
            "objects": objects,
            "is_truncated": response.get("IsTruncated", False),
            "continuation_token": response.get("NextContinuationToken"),
            "count": response.get("KeyCount", 0),
        }

    def head_object(
        self,
        container: str,
        path: str,
    ) -> dict[str, Any]:
        """Get object metadata from S3."""
        self._log.debug("head_object", container=container, path=path)
        response = self._client.head_object(Bucket=container, Key=path)

        return {
            "content_type": response.get("ContentType"),
            "content_length": response.get("ContentLength"),
            "etag": response.get("ETag", "").strip('"'),
            "metadata": response.get("Metadata", {}),
            "last_modified": response.get("LastModified"),
        }
