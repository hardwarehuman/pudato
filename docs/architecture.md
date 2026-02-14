# Pudato - Architecture Plan

## Overview

A modular, open-source platform for data operations with public sector data. Built with service abstractions enabling local development with AWS mocks and production deployment flexibility.

## Key Objectives

### 1. Separate Infrastructure from Data Logic

The platform enables data analysts to manage data operations with minimal help from data engineers, while treating data logic as a proper codebase with engineering controls.

- **Infrastructure repo** (this one): Pudato platform code, handlers, Terraform, managed by engineers
- **Data logic repo** (separate): dbt models, SQL transforms, managed by data analysts/architects
- Data analysts work primarily in SQL and Python
- Standard PR/code review workflow: analyst submits transform change → peer review → merge to main = production

### 2. Version-Tracked Reproducibility

Every flow execution must be traceable to the exact data logic version used.

- Track commit hash (or equivalent) for each flow execution
- Enable re-running historical flows with identical logic, even if production has moved on
- Pudato provides version configuration to Airflow/dbt at execution time
- Supports both: analyst testing/development AND automated production flows

### 3. Discover Airflow/dbt Responsibilities

Airflow and dbt have overlapping capabilities (DAGs, dependencies, scheduling). Part of development is exploring the right division of labor.

- **dbt strengths**: SQL-first, data models, built-in testing, git integration, lineage
- **Airflow strengths**: Broad orchestration, UI/visualization, external triggers, cross-system workflows
- The boundary between them will emerge through implementation

---

## Core Assumptions

### 1. Cloud Service Portability

Every major cloud has equivalent services (SNS/SQS ↔ Azure Service Bus ↔ GCP Pub/Sub, Lambda ↔ Azure Functions, etc.). As long as we don't lean too heavily into provider-specific details, we can swap one service for its counterpart transparently via Terraform configuration.

### 2. Don't Reimplement Cloud Services

Designing and maintaining even minimal versions of messaging, storage, or compute services would be a significant effort. We use robust existing cloud services, not build inferior versions. LocalStack provides the local development story.

### 3. dbt and Airflow Are Core Platform Components

These define the platform's paradigm (declarative SQL transforms, DAG orchestration), not just swappable backends. Modularization means giving them a place to run (ECS/ACI/K8s), not planning for alternatives. dbt is invoked via subprocess (`dbt run`) for vendor neutrality.

### 4. MVP First, Expand Via Real Use Cases

The initial goal is a complete end-to-end ELT orchestration system using Airflow and dbt on AWS, working in batch DAGs to a target data model. Streaming, unstructured data, MLOps, and multi-cloud support come later, driven by real-world use cases where Pudato provides majority capability and we add what's missing.

---

## Core Decisions

- **Language**: Python 3.11 - 3.13 (pinned for dbt/Airflow/Great Expectations compatibility; TODO: recheck Feb 2026)
- **AWS Mocking**: LocalStack (Docker-based)
- **Orchestration**: Apache Airflow (core, not swappable)
- **Transformations & Lineage**: dbt-core (core, not swappable)
- **Local Query Engine**: DuckDB (with Athena abstraction for AWS)
- **Data Format**: Apache Iceberg (ACID, vendor-neutral, time travel)
- **API Layer**: Deferred (batch pipelines first)
- **Target AWS Services**: S3, DynamoDB, Lambda, ECS, SQS, SNS, EventBridge, Glue, Athena, Redshift

---

## Architecture Principles

### 1. SNS/SQS as Native Message Fabric

The platform uses **SNS/SQS as the native message bus** (LocalStack locally, real AWS in production). No custom message bus implementation - we define how to use the robust existing cloud services.

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
│ Lambda        │   │ Lambda        │   │ Lambda        │
│ (translator)  │   │ (translator)  │   │ (translator)  │
└───────────────┘   └───────────────┘   └───────────────┘
        │                    │                    │
        ▼                    ▼                    ▼
   S3/DynamoDB       Redshift/Athena/         dbt/Spark
                      Snowflake
```

### 2. Lambda as Service Translator

Each Lambda knows how to:

1. **Receive** standardized messages from SNS/SQS
2. **Translate** messages into service-specific commands (e.g., Redshift SQL API)
3. **Execute** against its configured backend service
4. **Return** status to results topic, OR schedule follow-up Lambda for async operations

**Key benefit**: To swap Redshift → Snowflake, change Terraform config to point to Snowflake Lambda. Platform works transparently.

### 3. Standardized Message Protocol

Platform-agnostic message format for all inter-service communication (see `src/pudato/protocol/messages.py`):

```python
class Command(BaseModel):
    """Inbound request to a service handler."""
    type: str              # e.g., "storage", "query", "transform"
    action: str            # e.g., "put_object", "execute_sql", "run_model"
    payload: dict          # Action-specific parameters
    correlation_id: str    # For tracing/lineage
    job_id: str | None     # For lineage: links to registry job
    step_id: str | None    # For lineage: links to registry step
    metadata: dict         # Optional context (user, source, etc.)

class Result(BaseModel):
    """Standardized response from any handler."""
    status: Literal["success", "error", "pending"]
    data: dict | None      # Result payload
    errors: list[str]      # Error messages if any
    correlation_id: str    # Matches originating command
    job_id: str | None     # Copied from command for lineage
    step_id: str | None    # Copied from command for lineage
    duration_ms: int       # Execution time

class Event(BaseModel):
    """Cross-service notification."""
    type: str              # e.g., "storage.object_created"
    payload: dict          # Event-specific data
    correlation_id: str    # For tracing
    source: str            # Emitting service
```

### 4. Handler Abstraction

Each service domain has a handler that abstracts away implementation differences:

```python
class Handler(Protocol):
    """Base protocol for all service handlers."""
    def handle(self, command: Command) -> Result: ...
    def supported_actions(self) -> list[str]: ...

class StorageHandler(Handler):
    """Handles storage operations across S3, local filesystem, etc."""
    def __init__(self, backend: StorageBackend):
        self.backend = backend  # S3Backend or LocalFilesystemBackend

    def handle(self, command: Command) -> Result:
        match command.action:
            case "put_object": return self._put_object(command.payload)
            case "get_object": return self._get_object(command.payload)
            case _: return Result(status="error", errors=["Unknown action"])
```

### 5. Service Registry

Expected services that the platform provides:

| Service       | Commands                             | Responsibility           |
| ------------- | ------------------------------------ | ------------------------ |
| `storage`     | put_object, get_object, list, delete | Object/file storage      |
| `table`       | create, insert, query, compact       | Iceberg table operations |
| `query`       | execute_sql, execute_async           | Ad-hoc SQL queries       |
| `transform`   | run_model, test_model                | dbt transformations      |
| `catalog`     | register, discover, lineage          | Metadata management      |
| `orchestrate` | trigger_dag, get_status              | Airflow DAG control      |
| `notify`      | publish, subscribe                   | Event notifications      |

### 6. Configuration-Driven Backend Selection (Terraform)

Environment determines which backend implements each handler:

- `PUDATO_ENV=local` → LocalStack, DuckDB, local filesystem
- `PUDATO_ENV=aws` → S3, Athena, Lambda, real AWS services
- Future: `PUDATO_ENV=azure`, `PUDATO_ENV=gcp`

---

## Project Structure

```
pudato/
├── pyproject.toml              # Project config, dependencies
├── docker-compose.yml          # LocalStack + Airflow
├── .env                        # Environment variables
│
├── terraform/                  # Infrastructure as Code
│   ├── environments/
│   │   ├── local/              # LocalStack configuration
│   │   └── aws/                # Production AWS configuration (future)
│   └── modules/
│       ├── messaging/          # SNS topics, SQS queues
│       ├── lambdas/            # Lambda function deployments (future)
│       └── storage/            # S3 buckets, DynamoDB tables (future)
│
├── src/pudato/
│   ├── config.py               # Configuration management (pydantic-settings)
│   ├── protocol/               # Platform-agnostic message protocol
│   │   ├── messages.py         # Command, Result, Event
│   │   └── errors.py           # Standard error types
│   ├── messaging/              # SNS/SQS integration (boto3)
│   │   ├── publisher.py        # Publish commands to SNS topics
│   │   ├── consumer.py         # SQS consumer utilities
│   │   └── topics.py           # Topic/queue name constants
│   ├── handlers/               # Service handlers (Phase 2+)
│   ├── backends/               # Cloud-specific implementations (Phase 2+)
│   └── runtime/                # Execution environments (Phase 2+)
│
├── dbt/                        # dbt project (Phase 3+)
├── airflow/                    # Airflow DAGs (Phase 4+)
├── tests/
│   └── integration/            # Tests against LocalStack
└── docs/
    ├── architecture.md         # This file
    └── schema.md               # Registry database ER diagram
```

---

## Component Stack

| Layer               | Component                     | Purpose                                          |
| ------------------- | ----------------------------- | ------------------------------------------------ |
| **Orchestration**   | Apache Airflow                | DAG scheduling, workflow management              |
| **Transformations** | dbt-core                      | SQL transformations, lineage, docs               |
| **Table Format**    | Apache Iceberg                | ACID transactions, time travel, schema evolution |
| **Storage**         | S3 (LocalStack)               | Data lake object storage                         |
| **Database**        | DynamoDB (LocalStack)         | Metadata, state management                       |
| **Messaging**       | SQS/SNS (LocalStack)          | Async processing, notifications                  |
| **Query Engine**    | DuckDB (local) / Athena (AWS) | SQL queries over Iceberg tables                  |
| **Catalog**         | dbt docs + Iceberg catalog    | Schema registry, lineage                         |

---

## Implementation Phases

### Phase 1: Infrastructure & Messaging Foundation ✅ COMPLETE

- [x] Project scaffolding (pyproject.toml, structure)
- [x] Docker Compose with LocalStack
- [x] Terraform modules: SNS topics, SQS queues (local environment)
- [x] Define message protocol (`Command`, `Result`, `Event` dataclasses)
- [x] SNS/SQS publisher/consumer utilities (boto3)
- [x] Basic test: publish message → receive message via LocalStack (6 tests passing)

### Phase 2: First Lambda Handler (Storage) ✅ COMPLETE

- [x] Handler base class with standard lifecycle (`src/pudato/handlers/base.py`)
- [x] Storage handler with S3 backend (`src/pudato/handlers/storage.py`, `src/pudato/backends/storage.py`)
- [x] Lambda entry point (`src/pudato/runtime/lambda_handler.py`)
- [x] Lambda local runner for development/testing (`src/pudato/runtime/local_runner.py`)
- [x] Integration tests: 9 tests for storage handler operations (15 total)

**Note**: Local development uses `local_runner.py` to simulate Lambda (polls SQS, invokes handler). Actual AWS Lambda deployment via Terraform is deferred to Phase 5+.

### Phase 3: Query & Transform Handlers ✅ COMPLETE

- [x] dbt project structure with sample models (`dbt/`)
- [x] Sample models: staging + mart with Pudato version tracking columns
- [x] dbt-core and dbt-duckdb installed
- [x] Python version pinned to 3.11-3.13 for dbt/Airflow/Great Expectations compatibility
  - TODO(Feb 2026): Recheck if Python 3.14 support has improved across ecosystem
- [x] Transform handler with dbt backend (`src/pudato/handlers/transform.py`, `src/pudato/backends/dbt.py`)
- [x] Version tracking columns in dbt output (`_pudato_logic_version`, `_pudato_execution_id`)
- [x] Query handler with DuckDB backend (`src/pudato/handlers/query.py`, `src/pudato/backends/query.py`)
- [x] End-to-end: ingest → transform → query via messages (`tests/integration/test_end_to_end.py`)

### Phase 4: Data Logic Repo & Lineage Wiring (IN PROGRESS)

**Completed:**

- [x] Table handler with DuckDB backend (`src/pudato/handlers/table.py`, `src/pudato/backends/table.py`)
- [x] Catalog handler with file-backed + in-memory backends (`src/pudato/handlers/catalog.py`, `src/pudato/backends/catalog.py`)
- [x] Registry handler with PostgreSQL + in-memory backends (`src/pudato/handlers/registry.py`, `src/pudato/backends/registry.py`)
- [x] Lineage data structures: DataReference, ExecutionRecord on Result objects (`src/pudato/protocol/messages.py`)
- [x] dbt project with sample models and version tracking
- [x] Lambda handler integration for all handler types
- [x] External data logic repo integration (clone/fetch repo, extract commit hash as logic_version)
- [x] Explicit `job_id` and `step_id` on Command/Result for lineage tracking
- [x] Results consumer (`src/pudato/runtime/results_consumer.py`) - persists lineage from Results to registry
- [x] Cross-handler lineage (connect steps into Jobs so transform → query chains are traceable)
- [x] End-to-end lineage tests (143 tests passing)

**Remaining:**

- [ ] Terraform: Results SQS queue subscribed to results SNS topic
- [ ] dbt manifest parsing for actual table-level I/O tracking

### Phase 5: Orchestration & Production

- [ ] Orchestrate handler (Airflow DAG triggering)
- [ ] Sample pipeline with real public sector data
- [ ] Async pattern: Lambda schedules follow-up for long operations
- [ ] EventBridge patterns for scheduled/event-driven workflows
- [ ] Terraform AWS Lambda deployment (SQS → Lambda triggers)
- [ ] Terraform AWS environment (production config)

### Phase 6+: To Be Defined

Potential future areas:

- Multi-cloud support (Azure, GCP)
- Streaming data
- Unstructured data handling
- MLOps integration

---

## Key Dependencies

```toml
[project]
dependencies = [
    "boto3>=1.34",
    "pydantic>=2.0",
    "pydantic-settings>=2.0",
    "structlog>=24.0",
    "pyiceberg>=0.6",
    "duckdb>=1.0",
    "pyarrow>=15.0",
]

[project.optional-dependencies]
airflow = ["apache-airflow>=2.8"]
dbt = ["dbt-core>=1.7", "dbt-duckdb>=1.7"]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.23",
    "moto>=5.0",
    "localstack-client>=2.0",
    "ruff>=0.3",
    "mypy>=1.8",
    "boto3-stubs[sns,sqs,s3,lambda]>=1.34",
]
```

---

## Remaining Considerations

1. **Authentication**: IAM handled via LocalStack locally; real IAM in AWS. Abstract via config.
2. **Iceberg Catalog**: Use SQLite-based catalog locally, Glue Catalog in AWS.
3. **dbt + Iceberg**: dbt-duckdb adapter supports Iceberg; evaluate dbt-athena for AWS parity.
