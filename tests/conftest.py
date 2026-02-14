"""Pytest configuration and fixtures."""

import os

import boto3
import pytest


def pytest_configure(config):  # noqa: ARG001
    """Set environment variables before test collection."""
    os.environ.setdefault("PUDATO_ENV", "local")
    os.environ.setdefault("LOCALSTACK_ENDPOINT", "http://localhost:4566")
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    os.environ.setdefault(
        "REGISTRY_DATABASE_URL",
        "postgresql://pudato:pudato@localhost:5433/pudato",
    )


@pytest.fixture
def localstack_endpoint() -> str:
    """LocalStack endpoint URL."""
    return "http://localhost:4566"


@pytest.fixture
def sns_client(localstack_endpoint: str):
    """Create SNS client configured for LocalStack."""
    return boto3.client(
        "sns",
        endpoint_url=localstack_endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


@pytest.fixture
def sqs_client(localstack_endpoint: str):
    """Create SQS client configured for LocalStack."""
    return boto3.client(
        "sqs",
        endpoint_url=localstack_endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
