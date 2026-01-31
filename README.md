# Pudato - Public Data Operations Platform

A modular, open-source platform for data operations with public sector data. Built with service abstractions enabling local development with AWS mocks and production deployment flexibility.

The organizing intention is to create a platform optimized for the sort of analyst-led data engineering crucial to acheiving speed, flexibility, and maintainability when working with the sort of [incorrigible data common in public-sector contexts](https://adhoc.team/2022/07/12/gaining-reliable-insights-with-incorrigible-data/). A key design principle is the separation of *platform infrastructure* from *data logic*: dbt models and SQL transforms live in a configurable external repository that data analysts iterate at their own operational cadence, independent of platform or other infrastructure deployments. Though the MVP targets AWS services, the modular nature provides easy adaptability to almost any existing cloud-based infrastructure.

## Project Status

**Current Phase**: Phase 4 In Progress (Data Logic Repo & Lineage Wiring)

### What's Built

| Component | Status | Description |
|-----------|--------|-------------|
| Message Protocol | ✅ | `Command`, `Result`, `Event` with lineage support |
| SNS/SQS Messaging | ✅ | Publisher, Consumer utilities via boto3 |
| Storage Handler | ✅ | S3 backend for object storage |
| Query Handler | ✅ | DuckDB backend for SQL queries |
| Transform Handler | ✅ | dbt backend with version tracking |
| Table Handler | ✅ | DuckDB backend for table operations |
| Catalog Handler | ✅ | In-memory + file-backed metadata |
| Registry Handler | ✅ | PostgreSQL backend for job/lineage tracking |
| Lambda Runtime | ✅ | Entry point + local runner |
| LocalStack Setup | ✅ | Docker Compose with SNS, SQS, S3, PostgreSQL |
| Terraform (local) | ✅ | Topics, queues, subscriptions |
| Integration Tests | ✅ | 110 passing tests |

### What's Next (Phase 4 Remaining)

- [ ] Wire handler Results into registry (persist lineage)
- [ ] Cross-handler lineage (connect steps into traceable Jobs)
- [ ] dbt manifest parsing for table-level I/O tracking

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│              SNS Topics / SQS Queues (via LocalStack)           │
│   pudato-storage-commands    pudato-query-commands              │
│   pudato-transform-commands  pudato-results    pudato-events    │
└─────────────────────────────────────────────────────────────────┘
        │                    │                    │
        │ (Terraform-configured subscriptions)    │
        ▼                    ▼                    ▼
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│ Storage       │   │ Query         │   │ Transform     │
│ Handler       │   │ Handler       │   │ Handler       │
│ (translator)  │   │ (translator)  │   │ (translator)  │
└───────────────┘   └───────────────┘   └───────────────┘
        │                    │                    │
        ▼                    ▼                    ▼
       S3               DuckDB/Athena           dbt
```

Each handler is a "translator" that:
1. Receives standardized messages from SNS/SQS
2. Translates to service-specific commands
3. Returns status to results topic

**Key benefit**: Swap backends by changing Terraform config + providing alternative backend module.

**External Logic Repo**: The Transform handler can fetch data logic (dbt models, SQL) from a configurable git repository at runtime. This decouples platform releases from data logic updates, enabling analysts to iterate on transforms via standard git workflows while the platform tracks each execution's exact logic version (commit hash).

## Quick Start

### Prerequisites

- Docker Desktop
- Terraform >= 1.0
- Python 3.11, 3.12, or 3.13

### Setup

```bash
# 1. Start LocalStack and PostgreSQL
docker compose --profile postgres up -d

# 2. Apply Terraform
cd terraform/environments/local
terraform init
terraform apply

# 3. Create S3 bucket (Terraform has timeout issues with LocalStack)
AWS_CONFIG_FILE=../../.aws/config AWS_SHARED_CREDENTIALS_FILE=../../.aws/credentials \
  aws --endpoint-url=http://localhost:4566 s3 mb s3://pudato-data-lake

# 4. Setup Python environment
cd ../../..
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# 5. Run tests
pytest tests/integration -v -m integration
```

### Project-Scoped AWS Credentials

This project uses local AWS credentials in `.aws/` (not global `~/.aws/`).
When using AWS CLI:

```bash
AWS_CONFIG_FILE=.aws/config AWS_SHARED_CREDENTIALS_FILE=.aws/credentials aws ...
```

## Project Structure

```
pudato/
├── src/pudato/
│   ├── config.py               # Configuration (pydantic-settings)
│   ├── protocol/messages.py    # Command, Result, Event
│   ├── messaging/              # SNS publisher, SQS consumer
│   ├── handlers/               # Storage, Query, Transform, Table, Catalog, Registry
│   ├── backends/               # S3, DuckDB, dbt, PostgreSQL, Logic Repo implementations
│   └── runtime/                # Lambda entry point, local runner
│
├── dbt/                        # Sample dbt project
│   ├── models/staging/         # Staging models
│   ├── models/marts/           # Mart models
│   └── seeds/                  # Sample data
│
├── terraform/
│   ├── environments/local/     # LocalStack configuration
│   └── modules/messaging/      # SNS topics, SQS queues
│
├── tests/integration/          # Integration tests (110 passing)
├── docker-compose.yml          # LocalStack + PostgreSQL
└── pyproject.toml              # Python package config
```

## Message Protocol

### Command

```python
from pudato.protocol import Command

command = Command(
    type="storage",
    action="put_object",
    payload={"container": "data-lake", "path": "data.parquet"},
    correlation_id="req-123",
    metadata={"source": "ingestion"},
)
```

### Result

```python
from pudato.protocol import Result

# Success with lineage
result = Result.success(
    correlation_id="req-123",
    data={"etag": "abc123"},
    duration_ms=45,
    inputs=[DataReference(ref_type="file", location="s3://bucket/input.csv")],
    outputs=[DataReference(ref_type="file", location="s3://bucket/output.parquet")],
)
```

### Event

```python
from pudato.protocol import Event

event = Event(
    type="storage.object_created",
    payload={"container": "data-lake", "path": "data.parquet"},
    correlation_id="req-123",
    source="storage-handler",
)
```

## Technology Stack

| Layer | Component | Purpose |
|-------|-----------|---------|
| **Language** | Python 3.11-3.13 | Platform code |
| **Messaging** | SNS/SQS (LocalStack) | Event-driven architecture |
| **Storage** | S3 (LocalStack) | Data lake |
| **Query Engine** | DuckDB (local) / Athena (AWS) | SQL queries |
| **Transformations** | dbt-core | SQL transforms, lineage |
| **Logic Repo** | Git (external repo) | Fetch/sync external data logic, version tracking |
| **Registry** | PostgreSQL | Job/step tracking, lineage |
| **Orchestration** | Apache Airflow | DAG scheduling (Phase 5) |
| **IaC** | Terraform | Infrastructure management |

## Development

```bash
source .venv/bin/activate

# Run integration tests
pytest tests/integration -v -m integration

# Linting
ruff check src tests

# Type checking
mypy src
```

## Documentation

- [Architecture](docs/architecture.md) - Design decisions, phases, principles
- [Handlers](docs/handlers.md) - Handler patterns, adding new handlers
- [Messaging](docs/messaging.md) - Message protocol, SNS/SQS patterns
- [Registry Schema](docs/schema.md) - Job/lineage database schema
- [Terraform](terraform/README.md) - Infrastructure setup

## Pronunciation
pu-day-to, pu-dah-to, it's all the same.

## License

MIT
