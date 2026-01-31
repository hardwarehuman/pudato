#!/bin/bash
# Setup local development environment for Pudato
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "==> Setting up Pudato local development environment"

# Check prerequisites
command -v docker >/dev/null 2>&1 || { echo "Docker is required but not installed. Aborting."; exit 1; }
command -v terraform >/dev/null 2>&1 || { echo "Terraform is required but not installed. Aborting."; exit 1; }

# Create data directories
echo "==> Creating data directories..."
mkdir -p "$PROJECT_ROOT/data/localstack"
mkdir -p "$PROJECT_ROOT/data/postgres"
mkdir -p "$PROJECT_ROOT/data/iceberg-catalog"

# Start LocalStack
echo "==> Starting LocalStack..."
cd "$PROJECT_ROOT"
docker compose up -d localstack

# Wait for LocalStack to be healthy
echo "==> Waiting for LocalStack to be healthy..."
max_attempts=30
attempt=0
while ! docker compose exec -T localstack curl -sf http://localhost:4566/_localstack/health >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ $attempt -ge $max_attempts ]; then
        echo "LocalStack failed to start after $max_attempts attempts"
        exit 1
    fi
    echo "Waiting for LocalStack... (attempt $attempt/$max_attempts)"
    sleep 2
done
echo "LocalStack is healthy!"

# Initialize and apply Terraform
echo "==> Applying Terraform configuration..."
cd "$PROJECT_ROOT/terraform/environments/local"
terraform init -input=false
terraform apply -auto-approve

# Generate .env file
echo "==> Generating .env file..."
terraform output -raw env_file_content > "$PROJECT_ROOT/.env"

echo ""
echo "==> Local environment setup complete!"
echo ""
echo "Your .env file has been created with the following configuration:"
cat "$PROJECT_ROOT/.env"
echo ""
echo "Next steps:"
echo "  1. Install Python dependencies: pip install -e '.[dev]'"
echo "  2. Run tests: pytest"
echo ""
