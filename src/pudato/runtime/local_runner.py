"""Local runner that simulates Lambda invocation for development.

Polls an SQS queue and invokes the handler, simulating how Lambda
would be triggered in production.

Usage:
    python -m pudato.runtime.local_runner --handler storage --queue-url <url>
"""

from __future__ import annotations

import argparse
import signal
import sys
import time

import boto3
import structlog

from pudato.config import get_settings
from pudato.runtime.lambda_handler import handle

logger = structlog.get_logger()


class LocalRunner:
    """Simulates Lambda by polling SQS and invoking handlers."""

    def __init__(
        self,
        handler_type: str,
        queue_url: str,
        poll_interval: float = 1.0,
    ) -> None:
        self._handler_type = handler_type
        self._queue_url = queue_url
        self._poll_interval = poll_interval
        self._running = False

        settings = get_settings()
        self._sqs = boto3.client(
            "sqs",
            endpoint_url=settings.endpoint_url,
            region_name=settings.aws_region,
        )
        self._log = logger.bind(
            component="local_runner",
            handler_type=handler_type,
        )

    def start(self) -> None:
        """Start polling the queue."""
        self._running = True
        self._log.info("starting", queue_url=self._queue_url)

        while self._running:
            try:
                self._poll_once()
            except KeyboardInterrupt:
                self._log.info("interrupted")
                break
            except Exception as e:
                self._log.exception("poll_error", error=str(e))
                time.sleep(self._poll_interval)

    def stop(self) -> None:
        """Stop polling."""
        self._running = False
        self._log.info("stopping")

    def _poll_once(self) -> None:
        """Poll for messages and process them."""
        response = self._sqs.receive_message(
            QueueUrl=self._queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=5,
        )

        messages = response.get("Messages", [])
        if not messages:
            return

        self._log.debug("received_messages", count=len(messages))

        # Build Lambda-style event
        event = {
            "Records": [
                {
                    "messageId": msg["MessageId"],
                    "body": msg["Body"],
                    "receiptHandle": msg["ReceiptHandle"],
                }
                for msg in messages
            ]
        }

        # Set handler type for lambda_handler
        import os

        os.environ["HANDLER_TYPE"] = self._handler_type

        # Invoke handler
        result = handle(event)
        self._log.info("processed", result=result)

        # Delete processed messages
        for msg in messages:
            self._sqs.delete_message(
                QueueUrl=self._queue_url,
                ReceiptHandle=msg["ReceiptHandle"],
            )


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Local runner for Pudato handlers")
    parser.add_argument(
        "--handler",
        required=True,
        choices=["storage"],  # Extend as handlers are added
        help="Handler type to run",
    )
    parser.add_argument(
        "--queue-url",
        required=True,
        help="SQS queue URL to poll",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        help="Seconds between polls (default: 1.0)",
    )

    args = parser.parse_args()

    runner = LocalRunner(
        handler_type=args.handler,
        queue_url=args.queue_url,
        poll_interval=args.poll_interval,
    )

    # Handle graceful shutdown
    def shutdown(signum: int, frame: object) -> None:  # noqa: ARG001
        runner.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    runner.start()


if __name__ == "__main__":
    main()
