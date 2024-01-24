###############################################################################
# Managed Streaming for Apache Kafka - Serverless
#
# Serverless rather than provisioned because this platform's load is bursty and
# partition-light: there is no broker sizing to get wrong, storage and throughput
# scale on their own, and IAM replaces a certificate story. The trade-offs are
# explicit: no custom broker configs, a 20 MiB/s per-partition ingress ceiling,
# and IAM-only auth (which is why the services authenticate with SASL/IAM).
#
# Migration note: switching to a provisioned cluster is a bootstrap-string change
# for the apps plus a different auth block here. See docs/ARCHITECTURE.md.
###############################################################################

resource "aws_security_group" "msk" {
  name        = "${local.name_prefix}-msk"
  description = "MSK Serverless access from ECS tasks and Lambda only"
  vpc_id      = module.vpc.vpc_id

  tags = merge(local.tags, { Name = "${local.name_prefix}-msk" })
}

resource "aws_vpc_security_group_ingress_rule" "msk_from_tasks" {
  security_group_id            = aws_security_group.msk.id
  description                  = "Kafka TLS/IAM from the ECS service security group"
  from_port                    = 9098
  to_port                      = 9098
  ip_protocol                  = "tcp"
  referenced_security_group_id = aws_security_group.ecs_tasks.id
}

resource "aws_vpc_security_group_ingress_rule" "msk_from_lambda" {
  security_group_id            = aws_security_group.msk.id
  description                  = "Kafka TLS/IAM from the DLQ reprocessor"
  from_port                    = 9098
  to_port                      = 9098
  ip_protocol                  = "tcp"
  referenced_security_group_id = aws_security_group.lambda.id
}

resource "aws_vpc_security_group_egress_rule" "msk_all" {
  security_group_id = aws_security_group.msk.id
  description       = "Cluster egress"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_msk_serverless_cluster" "orders" {
  cluster_name = "${local.name_prefix}-kafka"

  vpc_config {
    subnet_ids         = module.vpc.private_subnets
    security_group_ids = [aws_security_group.msk.id]
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }

  tags = merge(local.tags, { Name = "${local.name_prefix}-kafka" })
}

# Topic ARNs for the IAM policies. MSK Serverless creates topics on first use, so the ARNs are
# constructed rather than read from a resource; the cluster uuid is the last ARN segment.
locals {
  msk_cluster_uuid = element(split("/", aws_msk_serverless_cluster.orders.arn), 2)

  msk_topic_arns = [
    for topic in concat(var.kafka_topics, [for t in var.kafka_topics : "${t}.DLT"]) :
    "arn:aws:kafka:${var.aws_region}:${local.account_id}:topic/${aws_msk_serverless_cluster.orders.cluster_name}/${local.msk_cluster_uuid}/${topic}"
  ]

  msk_group_arn_wildcard = "arn:aws:kafka:${var.aws_region}:${local.account_id}:group/${aws_msk_serverless_cluster.orders.cluster_name}/${local.msk_cluster_uuid}/*"
}
