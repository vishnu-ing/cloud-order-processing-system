###############################################################################
# IAM - least privilege, per workload
#
# Each service gets its own task role whose DynamoDB statement is generated from
# `var.services[*].tables`. payment-service cannot read the inventory table;
# notification-service cannot write to the outbox. The policy is derived from the
# same list that documents the ownership boundary, so the two cannot drift.
###############################################################################

locals {
  table_arns = {
    for key, name in local.table_names :
    key => "arn:aws:dynamodb:${var.aws_region}:${local.account_id}:table/${name}"
  }

  # Table ARN plus its index ARNs: a Query against a GSI is authorised on the index, not the table.
  service_table_arns = {
    for name, config in var.services :
    name => flatten([
      for table in config.tables : [
        local.table_arns[table],
        "${local.table_arns[table]}/index/*",
      ]
    ])
  }
}

data "aws_iam_policy_document" "ecs_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

###############################################################################
# ECS execution role - pulls images and writes the container's stdout to logs.
# Shared by every service: it grants nothing about application data.
###############################################################################

resource "aws_iam_role" "ecs_task_execution" {
  name               = "${local.name_prefix}-ecs-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume_role.json
  tags               = local.tags
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution" {
  role       = aws_iam_role.ecs_task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

###############################################################################
# Per-service task roles
###############################################################################

resource "aws_iam_role" "ecs_task" {
  for_each = var.services

  name               = "${local.name_prefix}-${each.key}-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume_role.json
  tags               = merge(local.tags, { Service = each.key })
}

data "aws_iam_policy_document" "service_permissions" {
  for_each = var.services

  # DynamoDB: only the tables this service owns, and only the verbs it uses.
  dynamic "statement" {
    for_each = length(each.value.tables) > 0 ? [1] : []

    content {
      sid    = "DynamoDbDataAccess"
      effect = "Allow"
      actions = [
        "dynamodb:GetItem",
        "dynamodb:BatchGetItem",
        "dynamodb:Query",
        "dynamodb:PutItem",
        "dynamodb:UpdateItem",
        "dynamodb:DeleteItem",
        "dynamodb:TransactGetItems",
        "dynamodb:TransactWriteItems",
        "dynamodb:ConditionCheckItem",
      ]
      resources = local.service_table_arns[each.key]
    }
  }

  # Kafka over IAM: connect to the cluster, describe groups, read and write the saga topics.
  # Note there is no kafka-cluster:*Topic (create/delete) permission: topics are provisioned by
  # the platform, not by application code.
  dynamic "statement" {
    for_each = each.key == "api-gateway" ? [] : [1]

    content {
      sid       = "MskClusterConnect"
      effect    = "Allow"
      actions   = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster"]
      resources = [aws_msk_serverless_cluster.orders.arn]
    }
  }

  dynamic "statement" {
    for_each = each.key == "api-gateway" ? [] : [1]

    content {
      sid    = "MskTopicAccess"
      effect = "Allow"
      actions = [
        "kafka-cluster:DescribeTopic",
        "kafka-cluster:ReadData",
        "kafka-cluster:WriteData",
      ]
      resources = local.msk_topic_arns
    }
  }

  dynamic "statement" {
    for_each = each.key == "api-gateway" ? [] : [1]

    content {
      sid    = "MskGroupAccess"
      effect = "Allow"
      actions = [
        "kafka-cluster:AlterGroup",
        "kafka-cluster:DescribeGroup",
      ]
      resources = [local.msk_group_arn_wildcard]
    }
  }

  # notification-service sends the rendered messages; nobody else may.
  dynamic "statement" {
    for_each = each.key == "notification-service" ? [1] : []

    content {
      sid       = "SesSend"
      effect    = "Allow"
      actions   = ["ses:SendEmail", "ses:SendRawEmail"]
      resources = ["*"]

      condition {
        test     = "StringEquals"
        variable = "ses:FromAddress"
        values   = ["no-reply@${var.project}.example.com"]
      }
    }
  }

  statement {
    sid    = "Observability"
    effect = "Allow"
    actions = [
      "cloudwatch:PutMetricData",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "service_permissions" {
  for_each = var.services

  name   = "${local.name_prefix}-${each.key}-permissions"
  role   = aws_iam_role.ecs_task[each.key].id
  policy = data.aws_iam_policy_document.service_permissions[each.key].json
}

###############################################################################
# Lambda roles
###############################################################################

resource "aws_iam_role" "lambda_stream_processor" {
  name               = "${local.name_prefix}-stream-processor"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = local.tags
}

data "aws_iam_policy_document" "lambda_stream_processor" {
  statement {
    sid    = "ReadOrdersStream"
    effect = "Allow"
    actions = [
      "dynamodb:DescribeStream",
      "dynamodb:GetRecords",
      "dynamodb:GetShardIterator",
      "dynamodb:ListStreams",
    ]
    resources = [aws_dynamodb_table.orders.stream_arn]
  }

  statement {
    sid    = "WriteProjection"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:DeleteItem",
      "dynamodb:Query",
    ]
    resources = [
      aws_dynamodb_table.order_read_model.arn,
      "${aws_dynamodb_table.order_read_model.arn}/index/*",
    ]
  }

  statement {
    sid       = "Logs"
    effect    = "Allow"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.lambda_stream_processor.arn}:*"]
  }
}

resource "aws_iam_role_policy" "lambda_stream_processor" {
  name   = "${local.name_prefix}-stream-processor-permissions"
  role   = aws_iam_role.lambda_stream_processor.id
  policy = data.aws_iam_policy_document.lambda_stream_processor.json
}

resource "aws_iam_role" "lambda_status_api" {
  name               = "${local.name_prefix}-status-api"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = local.tags
}

data "aws_iam_policy_document" "lambda_status_api" {
  # Read-only on the projection: the serverless read path cannot mutate anything, by construction.
  statement {
    sid     = "ReadProjection"
    effect  = "Allow"
    actions = ["dynamodb:GetItem", "dynamodb:Query"]
    resources = [
      aws_dynamodb_table.order_read_model.arn,
      "${aws_dynamodb_table.order_read_model.arn}/index/*",
    ]
  }

  statement {
    sid       = "Logs"
    effect    = "Allow"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.lambda_status_api.arn}:*"]
  }
}

resource "aws_iam_role_policy" "lambda_status_api" {
  name   = "${local.name_prefix}-status-api-permissions"
  role   = aws_iam_role.lambda_status_api.id
  policy = data.aws_iam_policy_document.lambda_status_api.json
}

resource "aws_iam_role" "lambda_dlq_reprocessor" {
  name               = "${local.name_prefix}-dlq-reprocessor"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = local.tags
}

# The reprocessor runs inside the VPC to reach MSK, so it needs ENI management.
resource "aws_iam_role_policy_attachment" "lambda_dlq_vpc" {
  role       = aws_iam_role.lambda_dlq_reprocessor.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

data "aws_iam_policy_document" "lambda_dlq_reprocessor" {
  statement {
    sid    = "ConsumeReplayQueue"
    effect = "Allow"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
    ]
    resources = [aws_sqs_queue.dlq_replay.arn]
  }

  statement {
    sid       = "MskConnect"
    effect    = "Allow"
    actions   = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster"]
    resources = [aws_msk_serverless_cluster.orders.arn]
  }

  # Write-only on the live topics: a replay must never be able to consume, only re-publish.
  statement {
    sid       = "MskReplay"
    effect    = "Allow"
    actions   = ["kafka-cluster:DescribeTopic", "kafka-cluster:WriteData"]
    resources = local.msk_topic_arns
  }

  statement {
    sid       = "Logs"
    effect    = "Allow"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.lambda_dlq_reprocessor.arn}:*"]
  }
}

resource "aws_iam_role_policy" "lambda_dlq_reprocessor" {
  name   = "${local.name_prefix}-dlq-reprocessor-permissions"
  role   = aws_iam_role.lambda_dlq_reprocessor.id
  policy = data.aws_iam_policy_document.lambda_dlq_reprocessor.json
}
