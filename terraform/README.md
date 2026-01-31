# Pudato Terraform Infrastructure

Infrastructure as Code for the Pudato platform.

## Environments

- **local/** - LocalStack configuration for local development
- **aws/** - Production AWS configuration (Phase 5)

## Modules

- **messaging/** - SNS topics and SQS queues for platform communication
- **storage/** - S3 buckets and DynamoDB tables (TBD)
- **lambdas/** - Lambda function deployments (Phase 2)

## Local Development Setup

### Prerequisites

1. Docker and Docker Compose
2. Terraform >= 1.0
3. LocalStack running via Docker Compose

### Quick Start

```bash
# Start LocalStack
docker compose up -d localstack

# Wait for LocalStack to be healthy
docker compose exec localstack curl -f http://localhost:4566/_localstack/health

# Initialize Terraform
cd terraform/environments/local
terraform init

# Apply infrastructure
terraform apply

# Generate .env file from outputs
terraform output -raw env_file_content > ../../../.env
```

### Terraform Commands

```bash
# Plan changes
terraform plan

# Apply changes
terraform apply

# Destroy infrastructure
terraform destroy

# View outputs
terraform output
```

## Architecture

```
SNS Topics                          SQS Queues              Handlers
─────────────────────────────────────────────────────────────────────
pudato-storage-commands  ──────►  pudato-storage-queue  ──► Storage Lambda
pudato-query-commands    ──────►  pudato-query-queue    ──► Query Lambda
pudato-transform-commands ─────►  pudato-transform-queue ─► Transform Lambda
pudato-results           ──────►  pudato-results-queue  ──► Result consumers
pudato-events            ──────►  pudato-events-queue   ──► Event subscribers
```
