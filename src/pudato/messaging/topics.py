"""Topic and queue name constants and utilities."""

from dataclasses import dataclass

from pudato.config import get_settings


@dataclass(frozen=True)
class TopicConfig:
    """Configuration for a single SNS topic."""

    name: str
    arn: str


class Topics:
    """Registry of platform SNS topics.

    Topics are configured via Terraform and their ARNs are passed
    through environment variables.
    """

    @classmethod
    def storage_commands(cls) -> TopicConfig:
        """Storage service command topic."""
        settings = get_settings()
        return TopicConfig(
            name="pudato-storage-commands",
            arn=settings.storage_topic_arn,
        )

    @classmethod
    def query_commands(cls) -> TopicConfig:
        """Query service command topic."""
        settings = get_settings()
        return TopicConfig(
            name="pudato-query-commands",
            arn=settings.query_topic_arn,
        )

    @classmethod
    def transform_commands(cls) -> TopicConfig:
        """Transform service command topic."""
        settings = get_settings()
        return TopicConfig(
            name="pudato-transform-commands",
            arn=settings.transform_topic_arn,
        )

    @classmethod
    def results(cls) -> TopicConfig:
        """Results topic for command completion notifications."""
        settings = get_settings()
        return TopicConfig(
            name="pudato-results",
            arn=settings.results_topic_arn,
        )

    @classmethod
    def events(cls) -> TopicConfig:
        """Events topic for cross-service notifications."""
        settings = get_settings()
        return TopicConfig(
            name="pudato-events",
            arn=settings.events_topic_arn,
        )

    @classmethod
    def for_service(cls, service_type: str) -> TopicConfig:
        """Get the command topic for a service type."""
        topics = {
            "storage": cls.storage_commands,
            "query": cls.query_commands,
            "transform": cls.transform_commands,
        }
        if service_type not in topics:
            raise ValueError(f"Unknown service type: {service_type}")
        return topics[service_type]()
