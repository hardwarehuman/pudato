# CLAUDE.md - Pudato Project Guide

## Project Summary

Pudato is a modular platform for public sector data operations. Uses AWS services (LocalStack locally) with SNS/SQS as the message fabric. Lambda handlers translate messages to service-specific operations (S3, DuckDB, dbt).

**Current Phase**: 4 (Data Logic Repo & Lineage Wiring)
**Tests**: 143 passing

## Which Doc to Read

| Task | Read |
|------|------|
| Understanding architecture, design decisions, phases | `docs/architecture.md` |
| Working on handlers or backends | `docs/handlers.md` |
| Working on messaging, SNS/SQS, message protocol | `docs/messaging.md` |
| Working on registry, lineage, Job/Step persistence | `docs/schema.md` |
| Working on Terraform infrastructure | `terraform/README.md` |
| Resuming from previous session | `SESSION.md` |

## Quick Reference

```
src/pudato/
├── config.py              # Pydantic settings
├── protocol/messages.py   # Command, Result, Event
├── messaging/             # SNS publisher, SQS consumer
├── handlers/              # Service handlers
├── backends/              # Cloud-specific implementations
└── runtime/               # Lambda entry point, local runner
```

## Setup

```bash
# 1. Start LocalStack + PostgreSQL (Docker Desktop must be running)
docker compose --profile postgres up -d

# 2. Terraform
cd terraform/environments/local && terraform init && terraform apply

# 3. Create S3 bucket
AWS_CONFIG_FILE=../../.aws/config AWS_SHARED_CREDENTIALS_FILE=../../.aws/credentials \
  aws --endpoint-url=http://localhost:4566 s3 mb s3://pudato-data-lake

# 4. Python
cd ../../.. && source .venv/bin/activate && pip install -e ".[dev]"

# 5. Tests
pytest tests/integration -v -m integration
```

**AWS CLI**: Use project-scoped credentials:
```bash
AWS_CONFIG_FILE=.aws/config AWS_SHARED_CREDENTIALS_FILE=.aws/credentials aws ...
```

## Code Style

- **Lint**: `ruff check src tests`
- **Types**: `mypy src`
- **Test**: `pytest tests/integration -v -m integration`

## Session Continuity (SESSION.md)

At the **start of every session**, read `SESSION.md` for context from the previous session.

**Staleness detection protocol:**
1. After reading SESSION.md, check for `<!-- SESSION_READ -->` marker at top
2. If marker is **present**: the file is **stale** — warn the user that the previous session didn't update it, and context may be outdated
3. If marker is **absent**: file is fresh from the previous session's update
4. After reading (regardless of staleness), **add the marker** to SESSION.md
5. At **end of session**, write new SESSION.md **without** the marker

**SESSION.md format** (end of session):
```markdown
# Session Notes — YYYY-MM-DD

## What We Did Today
- Bullet points of completed work

## Current Test Status
- Test counts

## To Resume Development
- Commands to restore working state

## Next Steps
- What to work on next
```

## Key Rules

1. **SNS/SQS is the message bus** — don't implement a custom one
2. **Design for cloud portability** — AWS is initial target, but avoid tight coupling; Azure/GCP swap should be possible via Terraform + alternative backend modules
3. **Use `container`/`path`** not `bucket`/`key` in handler interfaces
4. **Use `correlation_id`** for tracing across services
5. **All handlers publish to results topic**
6. **Use `job_id`/`step_id`** for lineage tracking — pre-create steps in registry, pass IDs in commands
7. **Python 3.11-3.13** (not 3.14 — dbt/Airflow compatibility)
8. **Single-line commit messages** — keep commit messages concise and on one line
