# SNS Topic ARNs
output "storage_topic_arn" {
  description = "ARN of the storage commands SNS topic"
  value       = aws_sns_topic.storage_commands.arn
}

output "query_topic_arn" {
  description = "ARN of the query commands SNS topic"
  value       = aws_sns_topic.query_commands.arn
}

output "transform_topic_arn" {
  description = "ARN of the transform commands SNS topic"
  value       = aws_sns_topic.transform_commands.arn
}

output "results_topic_arn" {
  description = "ARN of the results SNS topic"
  value       = aws_sns_topic.results.arn
}

output "events_topic_arn" {
  description = "ARN of the events SNS topic"
  value       = aws_sns_topic.events.arn
}

# SQS Queue URLs
output "storage_queue_url" {
  description = "URL of the storage handler SQS queue"
  value       = aws_sqs_queue.storage_queue.url
}

output "query_queue_url" {
  description = "URL of the query handler SQS queue"
  value       = aws_sqs_queue.query_queue.url
}

output "transform_queue_url" {
  description = "URL of the transform handler SQS queue"
  value       = aws_sqs_queue.transform_queue.url
}

output "results_queue_url" {
  description = "URL of the results consumer SQS queue"
  value       = aws_sqs_queue.results_queue.url
}

output "events_queue_url" {
  description = "URL of the events consumer SQS queue"
  value       = aws_sqs_queue.events_queue.url
}

# SQS Queue ARNs (for Lambda triggers)
output "storage_queue_arn" {
  description = "ARN of the storage handler SQS queue"
  value       = aws_sqs_queue.storage_queue.arn
}

output "query_queue_arn" {
  description = "ARN of the query handler SQS queue"
  value       = aws_sqs_queue.query_queue.arn
}

output "transform_queue_arn" {
  description = "ARN of the transform handler SQS queue"
  value       = aws_sqs_queue.transform_queue.arn
}
