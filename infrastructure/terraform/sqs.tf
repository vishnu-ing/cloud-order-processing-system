###############################################################################
# SQS - dead-letter replay path
#
# Kafka's <topic>.DLT holds the failed records; SQS is what gives an operator a
# controlled way to replay them:
#
#   <topic>.DLT  --(mirror)-->  dlq-replay  --(Lambda)-->  live topic
#                                    |
#                                    +-- after 3 receives --> dlq-parking
#
# The parking queue is the end of the line: anything there needs a human, and its
# depth is an alarm rather than a retry.
###############################################################################

resource "aws_sqs_queue" "dlq_parking" {
  name                       = "${local.name_prefix}-dlq-parking"
  message_retention_seconds  = 1209600 # 14 days: a full replay window for an operator
  visibility_timeout_seconds = 60
  sqs_managed_sse_enabled    = true

  tags = merge(local.tags, { Name = "${local.name_prefix}-dlq-parking" })
}

resource "aws_sqs_queue" "dlq_replay" {
  name = "${local.name_prefix}-dlq-replay"

  # Six times the Lambda timeout: a batch must never become visible again while it is still
  # being processed, or the same events get replayed twice for no reason.
  visibility_timeout_seconds = var.lambda_timeout_seconds * 6
  message_retention_seconds  = 1209600
  receive_wait_time_seconds  = 20 # long polling: fewer empty receives, lower cost
  sqs_managed_sse_enabled    = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq_parking.arn
    maxReceiveCount     = 3
  })

  tags = merge(local.tags, { Name = "${local.name_prefix}-dlq-replay" })
}

resource "aws_sqs_queue_redrive_allow_policy" "parking" {
  queue_url = aws_sqs_queue.dlq_parking.id

  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.dlq_replay.arn]
  })
}
