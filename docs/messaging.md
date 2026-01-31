# Messaging

SNS/SQS is the **initial** native message fabric. We use AWS services directly (LocalStack locally), not a custom message bus.

However, messaging is ultimately another swappable service. The abstraction should remain cloud-agnostic so we can eventually swap to Azure Service Bus or GCP Pub/Sub via Terraform configuration (not a near-term goal, but the design should not preclude it).

**Source**: `src/pudato/messaging/`, `src/pudato/protocol/messages.py`

## Architecture

```
SNS Topics → SQS Queues → Lambda Handlers → Backend Services
```

Each handler has a dedicated topic/queue pair. Terraform wires them together.

**Portability note**: The message protocol (`Command`, `Result`, `Event`) is cloud-agnostic. Only `src/pudato/messaging/` contains AWS-specific code (boto3 SNS/SQS calls). A future Azure or GCP implementation would provide alternative publisher/consumer modules.

## Message Types

### Command

Inbound request to a handler:

```python
from pudato.protocol import Command

command = Command(
    type="storage",                    # Handler type
    action="put_object",               # Action to perform
    payload={"container": "data-lake", "path": "data.parquet", "data": "..."},
    correlation_id="req-123",          # For tracing
    metadata={"source": "ingestion"},  # Optional context
)
```

### Result

Response from a handler:

```python
from pudato.protocol import Result

# Success
result = Result.success(
    correlation_id="req-123",
    data={"etag": "abc123"},
    duration_ms=45,
)

# Error
result = Result.error(
    correlation_id="req-123",
    errors=["File not found"],
)

# Pending (for async operations)
result = Result.pending(
    correlation_id="req-123",
    data={"task_id": "async-456"},
)
```

### Event

Cross-service notification:

```python
from pudato.protocol import Event

event = Event(
    type="storage.object_created",
    payload={"container": "data-lake", "path": "data.parquet"},
    correlation_id="req-123",
    source="storage-handler",
)
```

## SNS Topics

| Topic | Purpose |
|-------|---------|
| `pudato-storage-commands` | Storage operations (S3) |
| `pudato-query-commands` | Query operations (DuckDB/Athena) |
| `pudato-transform-commands` | dbt transformations |
| `pudato-results` | Handler responses |
| `pudato-events` | Cross-service events |

## Publishing

```python
from pudato.messaging import SNSPublisher
from pudato.config import Settings

settings = Settings()
publisher = SNSPublisher(settings)

# Publish command to handler
publisher.publish_command(command, topic_arn=settings.storage_topic_arn)

# Publish result
publisher.publish_result(result, topic_arn=settings.results_topic_arn)

# Publish event
publisher.publish_event(event, topic_arn=settings.events_topic_arn)
```

## Consuming

```python
from pudato.messaging import SQSConsumer

consumer = SQSConsumer(settings)

# Poll for messages
messages = consumer.receive_messages(queue_url=settings.storage_queue_url)
for msg in messages:
    command = Command.model_validate_json(msg.body)
    # process...
    consumer.delete_message(queue_url, msg.receipt_handle)
```

## Local Development

The local runner (`src/pudato/runtime/local_runner.py`) polls SQS and invokes handlers, simulating Lambda behavior:

```bash
# Run local handler
python -m pudato.runtime.local_runner --handler storage
```

## Correlation IDs

Every message carries a `correlation_id` for tracing requests across services. Generate at the entry point, propagate through all subsequent messages.

```python
import uuid
correlation_id = str(uuid.uuid4())
```
