"""Integration tests for SNS/SQS messaging via LocalStack."""

import json
import time

import boto3
import pytest

from pudato.protocol.messages import Command, Event, Result


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


@pytest.fixture
def test_topic(sns_client):
    """Create a test SNS topic."""
    response = sns_client.create_topic(Name="test-commands")
    topic_arn = response["TopicArn"]
    yield topic_arn
    # Cleanup
    sns_client.delete_topic(TopicArn=topic_arn)


@pytest.fixture
def test_queue(sqs_client):
    """Create a test SQS queue."""
    response = sqs_client.create_queue(QueueName="test-queue")
    queue_url = response["QueueUrl"]
    yield queue_url
    # Cleanup
    sqs_client.delete_queue(QueueUrl=queue_url)


@pytest.fixture
def subscribed_queue(sns_client, sqs_client, test_topic, test_queue):
    """Create an SQS queue subscribed to an SNS topic."""
    # Get queue ARN
    queue_attrs = sqs_client.get_queue_attributes(
        QueueUrl=test_queue, AttributeNames=["QueueArn"]
    )
    queue_arn = queue_attrs["Attributes"]["QueueArn"]

    # Subscribe queue to topic
    sns_client.subscribe(
        TopicArn=test_topic,
        Protocol="sqs",
        Endpoint=queue_arn,
    )

    return {"topic_arn": test_topic, "queue_url": test_queue, "queue_arn": queue_arn}


@pytest.mark.integration
class TestMessaging:
    """Test SNS/SQS messaging patterns."""

    def test_publish_command_to_sns(self, sns_client, test_topic):
        """Test publishing a Command to SNS topic."""
        command = Command(
            type="storage",
            action="put_object",
            payload={"bucket": "test-bucket", "key": "test-key"},
        )

        response = sns_client.publish(
            TopicArn=test_topic,
            Message=command.to_json(),
            MessageAttributes={
                "type": {"DataType": "String", "StringValue": command.type},
                "action": {"DataType": "String", "StringValue": command.action},
            },
        )

        assert "MessageId" in response
        assert response["MessageId"] is not None

    def test_command_flows_through_sns_to_sqs(self, sns_client, sqs_client, subscribed_queue):
        """Test that a command published to SNS arrives in subscribed SQS queue."""
        topic_arn = subscribed_queue["topic_arn"]
        queue_url = subscribed_queue["queue_url"]

        # Create and publish command
        command = Command(
            type="query",
            action="execute_sql",
            payload={"sql": "SELECT * FROM test"},
            correlation_id="test-correlation-123",
        )

        sns_client.publish(
            TopicArn=topic_arn,
            Message=command.to_json(),
        )

        # Wait a moment for message to propagate
        time.sleep(0.5)

        # Receive message from SQS
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5,
        )

        assert "Messages" in response
        assert len(response["Messages"]) == 1

        # Parse the SNS notification wrapper
        message_body = json.loads(response["Messages"][0]["Body"])
        assert "Message" in message_body  # SNS wraps the message

        # Parse the actual command
        received_command = Command.from_json(message_body["Message"])
        assert received_command.type == "query"
        assert received_command.action == "execute_sql"
        assert received_command.correlation_id == "test-correlation-123"
        assert received_command.payload["sql"] == "SELECT * FROM test"

    def test_result_serialization(self):
        """Test Result serialization and deserialization."""
        result = Result.success(
            correlation_id="test-123",
            data={"rows_affected": 10},
            duration_ms=42,
            handler="query",
        )

        json_str = result.to_json()
        parsed = Result.from_json(json_str)

        assert parsed.status == "success"
        assert parsed.correlation_id == "test-123"
        assert parsed.data == {"rows_affected": 10}
        assert parsed.duration_ms == 42
        assert parsed.handler == "query"

    def test_event_serialization(self):
        """Test Event serialization and deserialization."""
        event = Event(
            type="storage.object_created",
            payload={"bucket": "data-lake", "key": "raw/file.parquet"},
            source="storage-handler",
        )

        json_str = event.to_json()
        parsed = Event.from_json(json_str)

        assert parsed.type == "storage.object_created"
        assert parsed.payload["bucket"] == "data-lake"
        assert parsed.source == "storage-handler"


@pytest.mark.integration
class TestPublisherConsumer:
    """Test the Publisher and Consumer utilities."""

    def test_publisher_publishes_command(self, sns_client, sqs_client, subscribed_queue):
        """Test Publisher.publish_command()."""
        from pudato.messaging.publisher import Publisher

        topic_arn = subscribed_queue["topic_arn"]
        queue_url = subscribed_queue["queue_url"]

        publisher = Publisher(client=sns_client)
        command = Command(
            type="transform",
            action="run_model",
            payload={"model": "customers"},
        )

        response = publisher.publish_command(topic_arn, command)
        assert "MessageId" in response

        # Verify message arrived in queue
        time.sleep(0.5)
        messages = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5,
        )

        assert len(messages.get("Messages", [])) == 1

    def test_consumer_polls_commands(self, sns_client, sqs_client, subscribed_queue):
        """Test Consumer.poll_commands()."""
        from pudato.messaging.consumer import Consumer
        from pudato.messaging.publisher import Publisher

        topic_arn = subscribed_queue["topic_arn"]
        queue_url = subscribed_queue["queue_url"]

        # Publish a command
        publisher = Publisher(client=sns_client)
        original_command = Command(
            type="catalog",
            action="register",
            payload={"table": "users"},
            correlation_id="poll-test-456",
        )
        publisher.publish_command(topic_arn, original_command)

        time.sleep(0.5)

        # Poll for commands
        consumer = Consumer(client=sqs_client)
        commands = list(consumer.poll_commands(queue_url, wait_time_seconds=5))

        assert len(commands) == 1
        received_command, receipt_handle = commands[0]
        assert received_command.type == "catalog"
        assert received_command.action == "register"
        assert received_command.correlation_id == "poll-test-456"

        # Delete the message
        consumer.delete_message(queue_url, receipt_handle)
