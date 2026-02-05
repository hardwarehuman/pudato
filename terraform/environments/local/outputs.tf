# Messaging outputs
output "storage_topic_arn" {
  description = "ARN of the storage commands SNS topic"
  value       = module.messaging.storage_topic_arn
}

output "query_topic_arn" {
  description = "ARN of the query commands SNS topic"
  value       = module.messaging.query_topic_arn
}

output "transform_topic_arn" {
  description = "ARN of the transform commands SNS topic"
  value       = module.messaging.transform_topic_arn
}

output "results_topic_arn" {
  description = "ARN of the results SNS topic"
  value       = module.messaging.results_topic_arn
}

output "events_topic_arn" {
  description = "ARN of the events SNS topic"
  value       = module.messaging.events_topic_arn
}

output "storage_queue_url" {
  description = "URL of the storage handler SQS queue"
  value       = module.messaging.storage_queue_url
}

output "results_queue_url" {
  description = "URL of the results consumer SQS queue"
  value       = module.messaging.results_queue_url
}

output "events_queue_url" {
  description = "URL of the events consumer SQS queue"
  value       = module.messaging.events_queue_url
}

output "results_queue_arn" {
  description = "ARN of the results consumer SQS queue"
  value       = module.messaging.results_queue_arn
}

output "events_queue_arn" {
  description = "ARN of the events consumer SQS queue"
  value       = module.messaging.events_queue_arn
}

# Storage outputs
output "data_lake_bucket" {
  description = "Name of the data lake S3 bucket"
  value       = aws_s3_bucket.data_lake.id
}

output "metadata_table" {
  description = "Name of the metadata DynamoDB table"
  value       = aws_dynamodb_table.metadata.id
}

# Environment file content for easy .env setup
output "env_file_content" {
  description = "Content for .env file"
  value       = <<-EOT
    PUDATO_ENV=local
    LOCALSTACK_ENDPOINT=http://localhost:4566
    AWS_ACCESS_KEY_ID=test
    AWS_SECRET_ACCESS_KEY=test
    AWS_DEFAULT_REGION=us-east-1
    PUDATO_STORAGE_TOPIC_ARN=${module.messaging.storage_topic_arn}
    PUDATO_QUERY_TOPIC_ARN=${module.messaging.query_topic_arn}
    PUDATO_TRANSFORM_TOPIC_ARN=${module.messaging.transform_topic_arn}
    PUDATO_RESULTS_TOPIC_ARN=${module.messaging.results_topic_arn}
    PUDATO_EVENTS_TOPIC_ARN=${module.messaging.events_topic_arn}
    PUDATO_RESULTS_QUEUE_URL=${module.messaging.results_queue_url}
    PUDATO_DATA_BUCKET=${aws_s3_bucket.data_lake.id}
    PUDATO_METADATA_TABLE=${aws_dynamodb_table.metadata.id}
  EOT
}
