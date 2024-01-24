###############################################################################
# Cloud-Based Order Processing System - AWS topology
#
# Layout:
#   main.tf         providers, VPC, shared locals
#   dynamodb.tf     nine tables, GSIs, streams, PITR, TTL
#   msk.tf          MSK Serverless + security group
#   ecs.tf          Fargate services behind an internal ALB
#   lambda.tf       three functions + event source mappings
#   apigateway.tf   HTTP API v2, JWT authorizer, VPC link, throttling
#   sqs.tf          DLQ replay queue + parking queue
#   iam.tf          least-privilege task and function roles
#   cloudwatch.tf   log groups, alarms, dashboard
###############################################################################

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }

  # Remote state is intentionally left to the caller: `terraform init -backend-config=...`.
  # A committed backend block is the fastest way to have two environments fight over one state file.
  backend "s3" {}
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = local.tags
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_caller_identity" "current" {}

locals {
  name_prefix = "${var.project}-${var.environment}"

  tags = merge({
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "terraform"
    Repository  = "cloud-order-processing-system"
  }, var.tags)

  azs = slice(data.aws_availability_zones.available.names, 0, var.availability_zone_count)

  account_id = data.aws_caller_identity.current.account_id

  # Every DynamoDB table this platform owns, and the logical-to-physical name mapping used by
  # the ECS task definitions and the IAM policies.
  table_names = {
    orders                 = "${local.name_prefix}-orders"
    order_items            = "${local.name_prefix}-order-items"
    inventory              = "${local.name_prefix}-inventory"
    inventory_reservations = "${local.name_prefix}-inventory-reservations"
    payments               = "${local.name_prefix}-payments"
    outbox                 = "${local.name_prefix}-outbox"
    processed_events       = "${local.name_prefix}-processed-events"
    notifications          = "${local.name_prefix}-notifications"
    order_read_model       = "${local.name_prefix}-order-read-model"
  }

  container_registry = var.ecr_registry != "" ? var.ecr_registry : "${local.account_id}.dkr.ecr.${var.aws_region}.amazonaws.com"
}

###############################################################################
# Networking
#
# Three AZs, private subnets for every workload, NAT for egress. Nothing that
# processes an order is reachable from the internet: the only public surface is
# API Gateway, which reaches the internal ALB through a VPC link.
###############################################################################

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.13"

  name = "${local.name_prefix}-vpc"
  cidr = var.vpc_cidr

  azs             = local.azs
  private_subnets = [for index in range(var.availability_zone_count) : cidrsubnet(var.vpc_cidr, 4, index)]
  public_subnets  = [for index in range(var.availability_zone_count) : cidrsubnet(var.vpc_cidr, 4, index + 8)]

  enable_nat_gateway   = var.enable_nat_gateway
  single_nat_gateway   = var.single_nat_gateway
  enable_dns_hostnames = true
  enable_dns_support   = true

  # Flow logs to CloudWatch: the only way to answer "did the task reach DynamoDB" after the fact.
  enable_flow_log                                 = true
  create_flow_log_cloudwatch_log_group            = true
  create_flow_log_cloudwatch_iam_role             = true
  flow_log_cloudwatch_log_group_retention_in_days = var.log_retention_days

  tags = local.tags
}

# DynamoDB through a gateway endpoint: saga traffic is chatty, and keeping it off the NAT gateway
# removes both a per-GB charge and a shared failure domain.
resource "aws_vpc_endpoint" "dynamodb" {
  vpc_id            = module.vpc.vpc_id
  service_name      = "com.amazonaws.${var.aws_region}.dynamodb"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = module.vpc.private_route_table_ids

  tags = merge(local.tags, { Name = "${local.name_prefix}-dynamodb-endpoint" })
}
