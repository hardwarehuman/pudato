"""Configuration management for Pudato platform."""

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Platform configuration loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_prefix="PUDATO_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Environment
    env: Literal["local", "aws"] = Field(default="local", description="Deployment environment")

    # AWS/LocalStack
    localstack_endpoint: str = Field(
        default="http://localhost:4566",
        alias="LOCALSTACK_ENDPOINT",
        description="LocalStack endpoint URL",
    )
    aws_region: str = Field(
        default="us-east-1",
        alias="AWS_DEFAULT_REGION",
        description="AWS region",
    )

    # SNS Topics
    storage_topic_arn: str = Field(
        default="arn:aws:sns:us-east-1:000000000000:pudato-storage-commands",
        description="Storage commands SNS topic ARN",
    )
    query_topic_arn: str = Field(
        default="arn:aws:sns:us-east-1:000000000000:pudato-query-commands",
        description="Query commands SNS topic ARN",
    )
    transform_topic_arn: str = Field(
        default="arn:aws:sns:us-east-1:000000000000:pudato-transform-commands",
        description="Transform commands SNS topic ARN",
    )
    results_topic_arn: str = Field(
        default="arn:aws:sns:us-east-1:000000000000:pudato-results",
        description="Results SNS topic ARN",
    )
    events_topic_arn: str = Field(
        default="arn:aws:sns:us-east-1:000000000000:pudato-events",
        description="Events SNS topic ARN",
    )

    # Storage
    data_container: str = Field(
        default="pudato-data-lake",
        description="Main data lake storage container (S3 bucket, Azure container, etc.)",
    )
    metadata_table: str = Field(
        default="pudato-metadata",
        description="DynamoDB metadata table name",
    )

    # Logic Repo (external data logic)
    logic_repo_url: str | None = Field(
        default=None,
        description="Git URL for external data logic repo (if None, uses local dbt/ directory)",
    )
    logic_repo_branch: str = Field(
        default="main",
        description="Branch to track in the logic repo",
    )
    logic_repo_clone_dir: str = Field(
        default="/tmp/pudato-logic-repo",
        description="Local directory for cloned logic repo",
    )
    logic_repo_subdir: str = Field(
        default="",
        description="Subdirectory within the logic repo containing the dbt project",
    )

    # Iceberg
    iceberg_catalog_path: str = Field(
        default="./data/iceberg-catalog",
        description="Local Iceberg catalog path",
    )

    @property
    def is_local(self) -> bool:
        """Check if running in local environment."""
        return self.env == "local"

    @property
    def endpoint_url(self) -> str | None:
        """Get endpoint URL for boto3 clients."""
        return self.localstack_endpoint if self.is_local else None


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
