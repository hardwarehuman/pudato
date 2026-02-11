"""SQS consumer utilities for platform messages."""

import json
from collections.abc import Generator
from typing import Any

import boto3
from mypy_boto3_sqs import SQSClient

from pudato.config import get_settings
from pudato.protocol.messages import Command, Event, Result


class Consumer:
    """Consumes messages from SQS queues.

    Provides utilities for polling queues and parsing messages.
    In production, Lambda functions are triggered by SQS events,
    but this consumer is useful for local development and testing.
    """

    def __init__(self, client: SQSClient | None = None) -> None:
        """Initialize consumer with optional custom client.

        Args:
            client: Optional SQS client. If not provided, creates one
                   using platform configuration.
        """
        if client is not None:
            self._client = client
        else:
            settings = get_settings()
            self._client = boto3.client(
                "sqs",
                endpoint_url=settings.endpoint_url,
                region_name=settings.aws_region,
            )

    def receive_messages(
        self,
        queue_url: str,
        max_messages: int = 10,
        wait_time_seconds: int = 20,
    ) -> list[dict[str, Any]]:
        """Receive messages from an SQS queue.

        Args:
            queue_url: URL of the SQS queue.
            max_messages: Maximum number of messages to receive (1-10).
            wait_time_seconds: Long polling wait time (0-20 seconds).

        Returns:
            List of SQS message dictionaries.
        """
        response = self._client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time_seconds,
            MessageAttributeNames=["All"],
        )
        return response.get("Messages", [])  # type: ignore[return-value]

    def delete_message(self, queue_url: str, receipt_handle: str) -> None:
        """Delete a message from the queue after processing.

        Args:
            queue_url: URL of the SQS queue.
            receipt_handle: Receipt handle of the message to delete.
        """
        self._client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle,
        )

    def poll_commands(
        self,
        queue_url: str,
        wait_time_seconds: int = 20,
    ) -> Generator[tuple[Command, str], None, None]:
        """Poll queue for commands, yielding parsed Command objects.

        Args:
            queue_url: URL of the SQS queue to poll.
            wait_time_seconds: Long polling wait time.

        Yields:
            Tuple of (Command, receipt_handle) for each message.
        """
        messages = self.receive_messages(
            queue_url=queue_url,
            max_messages=10,
            wait_time_seconds=wait_time_seconds,
        )
        for message in messages:
            body = message.get("Body", "{}")
            # Handle SNS notification wrapper
            try:
                parsed = json.loads(body)
                if "Message" in parsed:
                    # SNS notification format
                    command = Command.from_json(parsed["Message"])
                else:
                    command = Command.model_validate(parsed)
            except Exception:
                # Try direct JSON parsing
                command = Command.from_json(body)

            yield command, message["ReceiptHandle"]

    def poll_results(
        self,
        queue_url: str,
        wait_time_seconds: int = 20,
    ) -> Generator[tuple[Result, str], None, None]:
        """Poll queue for results, yielding parsed Result objects.

        Args:
            queue_url: URL of the SQS queue to poll.
            wait_time_seconds: Long polling wait time.

        Yields:
            Tuple of (Result, receipt_handle) for each message.
        """
        messages = self.receive_messages(
            queue_url=queue_url,
            max_messages=10,
            wait_time_seconds=wait_time_seconds,
        )
        for message in messages:
            body = message.get("Body", "{}")
            try:
                parsed = json.loads(body)
                if "Message" in parsed:
                    result = Result.from_json(parsed["Message"])
                else:
                    result = Result.model_validate(parsed)
            except Exception:
                result = Result.from_json(body)

            yield result, message["ReceiptHandle"]

    def poll_events(
        self,
        queue_url: str,
        wait_time_seconds: int = 20,
    ) -> Generator[tuple[Event, str], None, None]:
        """Poll queue for events, yielding parsed Event objects.

        Args:
            queue_url: URL of the SQS queue to poll.
            wait_time_seconds: Long polling wait time.

        Yields:
            Tuple of (Event, receipt_handle) for each message.
        """
        messages = self.receive_messages(
            queue_url=queue_url,
            max_messages=10,
            wait_time_seconds=wait_time_seconds,
        )
        for message in messages:
            body = message.get("Body", "{}")
            try:
                parsed = json.loads(body)
                if "Message" in parsed:
                    event = Event.from_json(parsed["Message"])
                else:
                    event = Event.model_validate(parsed)
            except Exception:
                event = Event.from_json(body)

            yield event, message["ReceiptHandle"]
