###############################################################################
# CloudWatch - logs, alarms, dashboard
#
# The alarm set is deliberately small. Each one maps to an entry in
# docs/RUNBOOK.md with a concrete first action; an alarm nobody knows how to
# action is just noise that teaches people to ignore the channel.
###############################################################################

resource "aws_sns_topic" "alarms" {
  name = "${local.name_prefix}-alarms"
  tags = local.tags
}

resource "aws_sns_topic_subscription" "alarms_email" {
  count = var.alarm_email != "" ? 1 : 0

  topic_arn = aws_sns_topic.alarms.arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

###############################################################################
# Log groups
###############################################################################

resource "aws_cloudwatch_log_group" "service" {
  for_each = var.services

  name              = "/ecs/${local.name_prefix}/${each.key}"
  retention_in_days = var.log_retention_days
  tags              = merge(local.tags, { Service = each.key })
}

resource "aws_cloudwatch_log_group" "lambda_stream_processor" {
  name              = "/aws/lambda/${local.name_prefix}-order-events-stream-processor"
  retention_in_days = var.log_retention_days
  tags              = local.tags
}

resource "aws_cloudwatch_log_group" "lambda_status_api" {
  name              = "/aws/lambda/${local.name_prefix}-order-status-api"
  retention_in_days = var.log_retention_days
  tags              = local.tags
}

resource "aws_cloudwatch_log_group" "lambda_dlq_reprocessor" {
  name              = "/aws/lambda/${local.name_prefix}-kafka-dlq-reprocessor"
  retention_in_days = var.log_retention_days
  tags              = local.tags
}

resource "aws_cloudwatch_log_group" "api_gateway" {
  name              = "/aws/apigateway/${local.name_prefix}"
  retention_in_days = var.log_retention_days
  tags              = local.tags
}

###############################################################################
# Alarms
###############################################################################

# 1. Replay queue depth. Anything above the threshold means events are failing faster than they
#    are being replayed. Runbook: "DLQ depth rising".
resource "aws_cloudwatch_metric_alarm" "dlq_replay_depth" {
  alarm_name          = "${local.name_prefix}-dlq-replay-depth"
  alarm_description   = "Messages waiting in the DLQ replay queue. See docs/RUNBOOK.md#dlq-depth-rising."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 3
  threshold           = var.dlq_depth_alarm_threshold
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.dlq_replay.name
  }

  alarm_actions = [aws_sns_topic.alarms.arn]
  ok_actions    = [aws_sns_topic.alarms.arn]
  tags          = local.tags
}

# 2. Parking queue is non-empty. By definition these events could not be replayed automatically,
#    so a single message is worth waking someone for during business hours.
resource "aws_cloudwatch_metric_alarm" "dlq_parking_not_empty" {
  alarm_name          = "${local.name_prefix}-dlq-parking-not-empty"
  alarm_description   = "Events that exhausted automatic replay and need a human decision."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.dlq_parking.name
  }

  alarm_actions = [aws_sns_topic.alarms.arn]
  tags          = local.tags
}

# 3. Lambda errors, one alarm per function.
resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  for_each = {
    stream_processor = aws_lambda_function.order_events_stream_processor.function_name
    status_api       = aws_lambda_function.order_status_api.function_name
    dlq_reprocessor  = aws_lambda_function.kafka_dlq_reprocessor.function_name
  }

  alarm_name          = "${local.name_prefix}-lambda-errors-${each.key}"
  alarm_description   = "Unhandled errors in ${each.value}. See docs/RUNBOOK.md#lambda-error-rate."
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 5
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = each.value
  }

  alarm_actions = [aws_sns_topic.alarms.arn]
  tags          = local.tags
}

# 4. Stream iterator age. A growing age means the projection is falling behind the write model,
#    so "where is my order" starts answering with stale data.
resource "aws_cloudwatch_metric_alarm" "stream_iterator_age" {
  alarm_name          = "${local.name_prefix}-stream-iterator-age"
  alarm_description   = "orders stream processing lag. See docs/RUNBOOK.md#read-model-lag."
  namespace           = "AWS/Lambda"
  metric_name         = "IteratorAge"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 5
  threshold           = 60000 # 60 s of lag
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.order_events_stream_processor.function_name
  }

  alarm_actions = [aws_sns_topic.alarms.arn]
  tags          = local.tags
}

# 5. API 5xx rate at the edge: the single number that says "customers are seeing failures".
resource "aws_cloudwatch_metric_alarm" "api_5xx" {
  alarm_name          = "${local.name_prefix}-api-5xx"
  alarm_description   = "Server errors returned by the public API."
  namespace           = "AWS/ApiGateway"
  metric_name         = "5xx"
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 3
  threshold           = 10
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    ApiId = aws_apigatewayv2_api.http.id
    Stage = aws_apigatewayv2_stage.default.name
  }

  alarm_actions = [aws_sns_topic.alarms.arn]
  tags          = local.tags
}

# 6. DynamoDB throttles on the hot tables: the earliest signal that the saga is about to slow down.
resource "aws_cloudwatch_metric_alarm" "dynamodb_throttles" {
  for_each = {
    orders = aws_dynamodb_table.orders.name
    outbox = aws_dynamodb_table.outbox.name
  }

  alarm_name          = "${local.name_prefix}-ddb-throttles-${each.key}"
  alarm_description   = "Throttled requests on ${each.value}. See docs/RUNBOOK.md#dynamodb-throttling."
  namespace           = "AWS/DynamoDB"
  metric_name         = "ThrottledRequests"
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 3
  threshold           = 5
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    TableName = each.value
  }

  alarm_actions = [aws_sns_topic.alarms.arn]
  tags          = local.tags
}

###############################################################################
# Dashboard - one screen that answers "is the saga healthy?"
###############################################################################

resource "aws_cloudwatch_dashboard" "saga" {
  dashboard_name = "${local.name_prefix}-saga"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        width  = 12
        height = 6
        properties = {
          title  = "API requests and errors"
          region = var.aws_region
          view   = "timeSeries"
          metrics = [
            ["AWS/ApiGateway", "Count", "ApiId", aws_apigatewayv2_api.http.id, { stat = "Sum" }],
            ["AWS/ApiGateway", "4xx", "ApiId", aws_apigatewayv2_api.http.id, { stat = "Sum" }],
            ["AWS/ApiGateway", "5xx", "ApiId", aws_apigatewayv2_api.http.id, { stat = "Sum" }],
          ]
        }
      },
      {
        type   = "metric"
        width  = 12
        height = 6
        properties = {
          title  = "Saga latency, end to end"
          region = var.aws_region
          view   = "timeSeries"
          metrics = [
            ["OrderProcessing/Saga", "SagaDurationMillis", "Status", "CONFIRMED", { stat = "p50" }],
            ["OrderProcessing/Saga", "SagaDurationMillis", "Status", "CONFIRMED", { stat = "p99" }],
            ["OrderProcessing/Saga", "SagaDurationMillis", "Status", "REJECTED", { stat = "p99" }],
          ]
        }
      },
      {
        type   = "metric"
        width  = 12
        height = 6
        properties = {
          title  = "Dead letters and replay"
          region = var.aws_region
          view   = "timeSeries"
          metrics = [
            ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.dlq_replay.name],
            ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.dlq_parking.name],
          ]
        }
      },
      {
        type   = "metric"
        width  = 12
        height = 6
        properties = {
          title  = "Read-model lag"
          region = var.aws_region
          view   = "timeSeries"
          metrics = [
            ["AWS/Lambda", "IteratorAge", "FunctionName", aws_lambda_function.order_events_stream_processor.function_name, { stat = "Maximum" }],
          ]
        }
      },
    ]
  })
}
