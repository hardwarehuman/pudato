"""Integration tests for storage handler via LocalStack."""

import base64
import contextlib
import json
import os

import boto3
import pytest

from pudato.backends.storage import S3Backend
from pudato.handlers.storage import StorageHandler
from pudato.protocol import Command
from pudato.runtime.lambda_handler import handle


@pytest.fixture
def s3_client(localstack_endpoint: str):
    """Create S3 client configured for LocalStack."""
    return boto3.client(
        "s3",
        endpoint_url=localstack_endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


@pytest.fixture
def test_container(s3_client):
    """Create a test S3 bucket (container)."""
    container_name = "test-storage-container"
    with contextlib.suppress(s3_client.exceptions.BucketAlreadyOwnedByYou):
        s3_client.create_bucket(Bucket=container_name)
    yield container_name
    # Cleanup: delete all objects then bucket
    try:
        response = s3_client.list_objects_v2(Bucket=container_name)
        for obj in response.get("Contents", []):
            s3_client.delete_object(Bucket=container_name, Key=obj["Key"])
        s3_client.delete_bucket(Bucket=container_name)
    except Exception:
        pass


@pytest.fixture
def storage_handler(s3_client) -> StorageHandler:
    """Create StorageHandler with S3 backend."""
    backend = S3Backend(client=s3_client)
    return StorageHandler(backend=backend)


@pytest.mark.integration
class TestStorageHandler:
    """Test StorageHandler directly."""

    def test_put_object(self, storage_handler, test_container):
        """Test storing an object."""
        command = Command(
            type="storage",
            action="put_object",
            payload={
                "container": test_container,
                "path": "test/file.txt",
                "body": "Hello, World!",
                "content_type": "text/plain",
            },
        )

        result = storage_handler.handle(command)

        assert result.status == "success"
        assert result.correlation_id == command.correlation_id
        assert result.handler == "storage"
        assert result.data is not None
        assert "etag" in result.data

    def test_put_object_base64(self, storage_handler, test_container):
        """Test storing binary data via base64."""
        binary_data = b"\x00\x01\x02\x03\xff"
        command = Command(
            type="storage",
            action="put_object",
            payload={
                "container": test_container,
                "path": "test/binary.bin",
                "body_base64": base64.b64encode(binary_data).decode("ascii"),
            },
        )

        result = storage_handler.handle(command)

        assert result.status == "success"

    def test_get_object(self, storage_handler, test_container, s3_client):
        """Test retrieving an object."""
        # First put an object
        test_content = "Test content for get"
        s3_client.put_object(
            Bucket=test_container,
            Key="test/get-me.txt",
            Body=test_content.encode(),
        )

        command = Command(
            type="storage",
            action="get_object",
            payload={
                "container": test_container,
                "path": "test/get-me.txt",
            },
        )

        result = storage_handler.handle(command)

        assert result.status == "success"
        assert result.data is not None
        assert "body_base64" in result.data
        retrieved = base64.b64decode(result.data["body_base64"])
        assert retrieved.decode() == test_content

    def test_delete_object(self, storage_handler, test_container, s3_client):
        """Test deleting an object."""
        # First put an object
        s3_client.put_object(
            Bucket=test_container,
            Key="test/delete-me.txt",
            Body=b"delete me",
        )

        command = Command(
            type="storage",
            action="delete_object",
            payload={
                "container": test_container,
                "path": "test/delete-me.txt",
            },
        )

        result = storage_handler.handle(command)

        assert result.status == "success"
        assert result.data is not None
        assert result.data["deleted"] is True

        # Verify object is gone
        with pytest.raises(s3_client.exceptions.NoSuchKey):
            s3_client.get_object(Bucket=test_container, Key="test/delete-me.txt")

    def test_list_objects(self, storage_handler, test_container, s3_client):
        """Test listing objects."""
        # Put some objects
        for i in range(3):
            s3_client.put_object(
                Bucket=test_container,
                Key=f"list-test/file{i}.txt",
                Body=f"content {i}".encode(),
            )

        command = Command(
            type="storage",
            action="list_objects",
            payload={
                "container": test_container,
                "prefix": "list-test/",
            },
        )

        result = storage_handler.handle(command)

        assert result.status == "success"
        assert result.data is not None
        assert len(result.data["objects"]) == 3
        paths = [obj["path"] for obj in result.data["objects"]]
        assert "list-test/file0.txt" in paths

    def test_head_object(self, storage_handler, test_container, s3_client):
        """Test getting object metadata."""
        s3_client.put_object(
            Bucket=test_container,
            Key="test/head-me.txt",
            Body=b"head test content",
            ContentType="text/plain",
        )

        command = Command(
            type="storage",
            action="head_object",
            payload={
                "container": test_container,
                "path": "test/head-me.txt",
            },
        )

        result = storage_handler.handle(command)

        assert result.status == "success"
        assert result.data is not None
        assert result.data["content_length"] == len(b"head test content")

    def test_unsupported_action(self, storage_handler, test_container):
        """Test error handling for unsupported action."""
        command = Command(
            type="storage",
            action="unsupported_action",
            payload={"container": test_container},
        )

        result = storage_handler.handle(command)

        assert result.status == "error"
        assert "Unsupported action" in result.errors[0]

    def test_missing_required_fields(self, storage_handler):
        """Test error handling for missing fields."""
        command = Command(
            type="storage",
            action="put_object",
            payload={},  # Missing container, path, body
        )

        result = storage_handler.handle(command)

        assert result.status == "error"
        assert "Missing required fields" in result.errors[0]


@pytest.mark.integration
class TestLambdaHandler:
    """Test the Lambda entry point."""

    def test_lambda_handler_processes_sqs_event(
        self,
        s3_client,
        test_container,
        localstack_endpoint,
    ):
        """Test Lambda handler processes SQS events correctly."""
        # Set up environment
        os.environ["HANDLER_TYPE"] = "storage"
        os.environ["LOCALSTACK_ENDPOINT"] = localstack_endpoint

        command = Command(
            type="storage",
            action="put_object",
            payload={
                "container": test_container,
                "path": "lambda-test/file.txt",
                "body": "Lambda test content",
            },
        )

        # Simulate SQS event (as Lambda would receive)
        event = {
            "Records": [
                {
                    "messageId": "test-123",
                    "body": json.dumps({"Message": command.to_json()}),
                    "receiptHandle": "test-handle",
                }
            ]
        }

        result = handle(event)

        assert result["processed"] == 1
        assert result["results"][0]["status"] == "success"

        # Verify object was created in S3
        response = s3_client.get_object(
            Bucket=test_container,
            Key="lambda-test/file.txt",
        )
        assert response["Body"].read() == b"Lambda test content"
