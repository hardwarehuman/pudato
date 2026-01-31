# Pudato Messaging Infrastructure
# SNS Topics and SQS Queues for platform communication

# ==============================================================================
# SNS Topics
# ==============================================================================

# Storage commands topic
resource "aws_sns_topic" "storage_commands" {
  name = "${var.prefix}-storage-commands"

  tags = var.tags
}

# Query commands topic
resource "aws_sns_topic" "query_commands" {
  name = "${var.prefix}-query-commands"

  tags = var.tags
}

# Transform commands topic
resource "aws_sns_topic" "transform_commands" {
  name = "${var.prefix}-transform-commands"

  tags = var.tags
}

# Results topic (all handlers publish results here)
resource "aws_sns_topic" "results" {
  name = "${var.prefix}-results"

  tags = var.tags
}

# Events topic (cross-service notifications)
resource "aws_sns_topic" "events" {
  name = "${var.prefix}-events"

  tags = var.tags
}

# ==============================================================================
# SQS Queues (for Lambda triggers)
# ==============================================================================

# Storage handler queue
resource "aws_sqs_queue" "storage_queue" {
  name                       = "${var.prefix}-storage-queue"
  visibility_timeout_seconds = 300  # 5 minutes for handler processing
  message_retention_seconds  = 86400  # 1 day
  receive_wait_time_seconds  = 20  # Long polling

  tags = var.tags
}

# Query handler queue
resource "aws_sqs_queue" "query_queue" {
  name                       = "${var.prefix}-query-queue"
  visibility_timeout_seconds = 300
  message_retention_seconds  = 86400
  receive_wait_time_seconds  = 20

  tags = var.tags
}

# Transform handler queue
resource "aws_sqs_queue" "transform_queue" {
  name                       = "${var.prefix}-transform-queue"
  visibility_timeout_seconds = 900  # 15 minutes for dbt transforms
  message_retention_seconds  = 86400
  receive_wait_time_seconds  = 20

  tags = var.tags
}

# Results consumer queue (for clients awaiting results)
resource "aws_sqs_queue" "results_queue" {
  name                       = "${var.prefix}-results-queue"
  visibility_timeout_seconds = 60
  message_retention_seconds  = 86400
  receive_wait_time_seconds  = 20

  tags = var.tags
}

# Events consumer queue (for catalog and other subscribers)
resource "aws_sqs_queue" "events_queue" {
  name                       = "${var.prefix}-events-queue"
  visibility_timeout_seconds = 60
  message_retention_seconds  = 86400
  receive_wait_time_seconds  = 20

  tags = var.tags
}

# ==============================================================================
# SNS to SQS Subscriptions
# ==============================================================================

# Subscribe storage queue to storage commands topic
resource "aws_sns_topic_subscription" "storage_subscription" {
  topic_arn = aws_sns_topic.storage_commands.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.storage_queue.arn
}

# Subscribe query queue to query commands topic
resource "aws_sns_topic_subscription" "query_subscription" {
  topic_arn = aws_sns_topic.query_commands.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.query_queue.arn
}

# Subscribe transform queue to transform commands topic
resource "aws_sns_topic_subscription" "transform_subscription" {
  topic_arn = aws_sns_topic.transform_commands.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.transform_queue.arn
}

# Subscribe results queue to results topic
resource "aws_sns_topic_subscription" "results_subscription" {
  topic_arn = aws_sns_topic.results.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.results_queue.arn
}

# Subscribe events queue to events topic
resource "aws_sns_topic_subscription" "events_subscription" {
  topic_arn = aws_sns_topic.events.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.events_queue.arn
}

# ==============================================================================
# SQS Queue Policies (allow SNS to send messages)
# ==============================================================================

resource "aws_sqs_queue_policy" "storage_queue_policy" {
  queue_url = aws_sqs_queue.storage_queue.id
  policy    = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = "*"
        Action    = "sqs:SendMessage"
        Resource  = aws_sqs_queue.storage_queue.arn
        Condition = {
          ArnEquals = {
            "aws:SourceArn" = aws_sns_topic.storage_commands.arn
          }
        }
      }
    ]
  })
}

resource "aws_sqs_queue_policy" "query_queue_policy" {
  queue_url = aws_sqs_queue.query_queue.id
  policy    = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = "*"
        Action    = "sqs:SendMessage"
        Resource  = aws_sqs_queue.query_queue.arn
        Condition = {
          ArnEquals = {
            "aws:SourceArn" = aws_sns_topic.query_commands.arn
          }
        }
      }
    ]
  })
}

resource "aws_sqs_queue_policy" "transform_queue_policy" {
  queue_url = aws_sqs_queue.transform_queue.id
  policy    = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = "*"
        Action    = "sqs:SendMessage"
        Resource  = aws_sqs_queue.transform_queue.arn
        Condition = {
          ArnEquals = {
            "aws:SourceArn" = aws_sns_topic.transform_commands.arn
          }
        }
      }
    ]
  })
}

resource "aws_sqs_queue_policy" "results_queue_policy" {
  queue_url = aws_sqs_queue.results_queue.id
  policy    = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = "*"
        Action    = "sqs:SendMessage"
        Resource  = aws_sqs_queue.results_queue.arn
        Condition = {
          ArnEquals = {
            "aws:SourceArn" = aws_sns_topic.results.arn
          }
        }
      }
    ]
  })
}

resource "aws_sqs_queue_policy" "events_queue_policy" {
  queue_url = aws_sqs_queue.events_queue.id
  policy    = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = "*"
        Action    = "sqs:SendMessage"
        Resource  = aws_sqs_queue.events_queue.arn
        Condition = {
          ArnEquals = {
            "aws:SourceArn" = aws_sns_topic.events.arn
          }
        }
      }
    ]
  })
}
