# Pudato Local Environment (LocalStack)
# Terraform configuration for local development using LocalStack

terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Configure AWS provider to use LocalStack
provider "aws" {
  region                      = var.aws_region
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    s3       = var.localstack_endpoint
    sqs      = var.localstack_endpoint
    sns      = var.localstack_endpoint
    lambda   = var.localstack_endpoint
    dynamodb = var.localstack_endpoint
    iam      = var.localstack_endpoint
  }
}

# Messaging infrastructure (SNS topics, SQS queues)
module "messaging" {
  source = "../../modules/messaging"

  prefix = var.prefix
  tags   = local.tags
}

# S3 bucket for data lake
resource "aws_s3_bucket" "data_lake" {
  bucket = "${var.prefix}-data-lake"

  tags = local.tags
}

# DynamoDB table for metadata
resource "aws_dynamodb_table" "metadata" {
  name         = "${var.prefix}-metadata"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  tags = local.tags
}

locals {
  tags = merge(var.tags, {
    Environment = "local"
    ManagedBy   = "terraform"
    Project     = "pudato"
  })
}
