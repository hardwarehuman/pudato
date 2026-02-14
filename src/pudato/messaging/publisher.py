"""SNS publisher for platform messages."""

from typing import Any

import boto3
from mypy_boto3_sns import SNSClient

from pudato.config import get_settings
from pudato.protocol.messages import Command, Event, Result


class Publisher:
    """Publishes messages to SNS topics.

    Uses boto3 SNS client with endpoint URL configured for LocalStack
    in local development.
    """

    def __init__(self, client: SNSClient | None = None) -> None:
        """Initialize publisher with optional custom client.

        Args:
            client: Optional SNS client. If not provided, creates one
                   using platform configuration.
        """
        if client is not None:
            self._client = client
        else:
            settings = get_settings()
            self._client = boto3.client(
                "sns",
                endpoint_url=settings.endpoint_url,
                region_name=settings.aws_region,
            )

    def publish_command(self, topic_arn: str, command: Command) -> dict[str, Any]:
        """Publish a command to a service topic.

        Args:
            topic_arn: ARN of the SNS topic to publish to.
            command: The command to publish.

        Returns:
            SNS publish response containing MessageId.
        """
        response = self._client.publish(
            TopicArn=topic_arn,
            Message=command.to_json(),
            MessageAttributes={
                "type": {"DataType": "String", "StringValue": command.type},
                "action": {"DataType": "String", "StringValue": command.action},
                "correlation_id": {"DataType": "String", "StringValue": command.correlation_id},
            },
        )
        return response  # type: ignore[return-value]

    def publish_result(self, topic_arn: str, result: Result) -> dict[str, Any]:
        """Publish a result to the results topic.

        Args:
            topic_arn: ARN of the results SNS topic.
            result: The result to publish.

        Returns:
            SNS publish response containing MessageId.
        """
        response = self._client.publish(
            TopicArn=topic_arn,
            Message=result.to_json(),
            MessageAttributes={
                "status": {"DataType": "String", "StringValue": result.status},
                "correlation_id": {"DataType": "String", "StringValue": result.correlation_id},
            },
        )
        return response  # type: ignore[return-value]

    def publish_event(self, topic_arn: str, event: Event) -> dict[str, Any]:
        """Publish an event to the events topic.

        Args:
            topic_arn: ARN of the events SNS topic.
            event: The event to publish.

        Returns:
            SNS publish response containing MessageId.
        """
        response = self._client.publish(
            TopicArn=topic_arn,
            Message=event.to_json(),
            MessageAttributes={
                "type": {"DataType": "String", "StringValue": event.type},
                "correlation_id": {"DataType": "String", "StringValue": event.correlation_id},
            },
        )
        return response  # type: ignore[return-value]
