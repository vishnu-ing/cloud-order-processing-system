###############################################################################
# Lambda - the serverless slice
#
#   OrderEventsStreamProcessor  orders stream -> order_read_model + saga metrics
#   OrderStatusApiHandler       API Gateway  -> order_read_model (read-only)
#   KafkaDlqReprocessor         SQS          -> live Kafka topic (write-only)
#
# All three share one shaded jar; the handler class selects the behaviour, which
# keeps the build to a single artifact and the cold-start profile identical.
###############################################################################

locals {
  lambda_runtime = "java21"
  lambda_jar     = var.lambda_artifact_path
}

resource "aws_security_group" "lambda" {
  name        = "${local.name_prefix}-lambda"
  description = "VPC-attached Lambda functions (DLQ reprocessor)"
  vpc_id      = module.vpc.vpc_id

  tags = merge(local.tags, { Name = "${local.name_prefix}-lambda" })
}

resource "aws_vpc_security_group_egress_rule" "lambda_all" {
  security_group_id = aws_security_group.lambda.id
  description       = "Lambda egress to MSK and AWS APIs"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

###############################################################################
# 1. DynamoDB Streams -> read model
###############################################################################

resource "aws_lambda_function" "order_events_stream_processor" {
  function_name = "${local.name_prefix}-order-events-stream-processor"
  role          = aws_iam_role.lambda_stream_processor.arn
  handler       = "io.orderflow.lambda.OrderEventsStreamProcessor::handleRequest"
  runtime       = local.lambda_runtime
  architectures = ["arm64"]

  filename         = local.lambda_jar
  source_code_hash = filebase64sha256(local.lambda_jar)

  memory_size = var.lambda_memory_mb
  timeout     = var.lambda_timeout_seconds

  environment {
    variables = {
      READ_MODEL_TABLE = local.table_names.order_read_model
      # Disable the SDK's own JMX/metrics plumbing: it only adds cold-start work here.
      AWS_CSM_ENABLED   = "false"
      JAVA_TOOL_OPTIONS = "-XX:+TieredCompilation -XX:TieredStopAtLevel=1"
    }
  }

  # SnapStart restores a pre-initialised JVM snapshot, which is what makes a Java handler viable
  # on a latency-sensitive path: cold starts drop from seconds to the low hundreds of ms.
  snap_start {
    apply_on = "PublishedVersions"
  }

  publish = true

  tracing_config {
    mode = "Active"
  }

  tags = merge(local.tags, { Function = "order-events-stream-processor" })
}

resource "aws_lambda_event_source_mapping" "orders_stream" {
  event_source_arn  = aws_dynamodb_table.orders.stream_arn
  function_name     = aws_lambda_function.order_events_stream_processor.arn
  starting_position = "LATEST"

  batch_size                         = 100
  maximum_batching_window_in_seconds = 2
  parallelization_factor             = 2

  # Bisect on error isolates the one poison record instead of retrying a batch of 100 forever;
  # after three attempts the remainder is sent to the parking queue for a human.
  bisect_batch_on_function_error = true
  maximum_retry_attempts         = 3
  maximum_record_age_in_seconds  = 3600

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.dlq_parking.arn
    }
  }

  # Only order changes matter; a stream record for a table-level change is filtered out before
  # it is billed as an invocation.
  filter_criteria {
    filter {
      pattern = jsonencode({
        eventName = ["INSERT", "MODIFY", "REMOVE"]
      })
    }
  }
}

###############################################################################
# 2. API Gateway -> read model
###############################################################################

resource "aws_lambda_function" "order_status_api" {
  function_name = "${local.name_prefix}-order-status-api"
  role          = aws_iam_role.lambda_status_api.arn
  handler       = "io.orderflow.lambda.OrderStatusApiHandler::handleRequest"
  runtime       = local.lambda_runtime
  architectures = ["arm64"]

  filename         = local.lambda_jar
  source_code_hash = filebase64sha256(local.lambda_jar)

  memory_size = var.lambda_memory_mb
  timeout     = 15 # a projection read that takes longer than this is a failure, not slow

  environment {
    variables = {
      READ_MODEL_TABLE  = local.table_names.order_read_model
      JAVA_TOOL_OPTIONS = "-XX:+TieredCompilation -XX:TieredStopAtLevel=1"
    }
  }

  snap_start {
    apply_on = "PublishedVersions"
  }

  publish = true

  tracing_config {
    mode = "Active"
  }

  tags = merge(local.tags, { Function = "order-status-api" })
}

# Provisioned concurrency on the customer-facing handler only, and only in prod: it removes the
# tail latency that SnapStart alone still leaves during a traffic ramp.
resource "aws_lambda_provisioned_concurrency_config" "order_status_api" {
  count = var.environment == "prod" ? 1 : 0

  function_name                     = aws_lambda_function.order_status_api.function_name
  qualifier                         = aws_lambda_function.order_status_api.version
  provisioned_concurrent_executions = 2
}

resource "aws_lambda_permission" "order_status_api_invoke" {
  statement_id  = "AllowApiGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.order_status_api.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.http.execution_arn}/*/*"
}

###############################################################################
# 3. SQS -> Kafka replay
###############################################################################

resource "aws_lambda_function" "kafka_dlq_reprocessor" {
  function_name = "${local.name_prefix}-kafka-dlq-reprocessor"
  role          = aws_iam_role.lambda_dlq_reprocessor.arn
  handler       = "io.orderflow.lambda.KafkaDlqReprocessor::handleRequest"
  runtime       = local.lambda_runtime
  architectures = ["arm64"]

  filename         = local.lambda_jar
  source_code_hash = filebase64sha256(local.lambda_jar)

  memory_size = 1536 # a Kafka producer inside a JVM wants headroom
  timeout     = var.lambda_timeout_seconds

  # In the VPC because MSK has no public endpoint.
  vpc_config {
    subnet_ids         = module.vpc.private_subnets
    security_group_ids = [aws_security_group.lambda.id]
  }

  environment {
    variables = {
      KAFKA_BOOTSTRAP_SERVERS = aws_msk_serverless_cluster.orders.bootstrap_brokers_sasl_iam
      KAFKA_SECURITY_PROTOCOL = "SASL_SSL"
      KAFKA_SASL_MECHANISM    = "AWS_MSK_IAM"
      MAX_REPLAY_ATTEMPTS     = "3"
      SEND_TIMEOUT_SECONDS    = "10"
    }
  }

  tracing_config {
    mode = "Active"
  }

  tags = merge(local.tags, { Function = "kafka-dlq-reprocessor" })
}

resource "aws_lambda_event_source_mapping" "dlq_replay" {
  event_source_arn = aws_sqs_queue.dlq_replay.arn
  function_name    = aws_lambda_function.kafka_dlq_reprocessor.arn

  batch_size                         = 10
  maximum_batching_window_in_seconds = 5

  # Partial batch failure: only the messages the handler reports stay on the queue.
  function_response_types = ["ReportBatchItemFailures"]

  scaling_config {
    # Replays are deliberately slow: a burst of re-published events must not overwhelm the very
    # consumers that were struggling when the records were dead-lettered.
    maximum_concurrency = 2
  }
}
