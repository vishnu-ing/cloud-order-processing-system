output "api_endpoint" {
  description = "Public base URL of the HTTP API. Append /api/v1/orders."
  value       = aws_apigatewayv2_stage.default.invoke_url
}

output "vpc_id" {
  description = "VPC that holds the ALB, the Fargate tasks and MSK."
  value       = module.vpc.vpc_id
}

output "private_subnet_ids" {
  description = "Private subnets used by ECS, MSK and the VPC-attached Lambda."
  value       = module.vpc.private_subnets
}

output "alb_dns_name" {
  description = "Internal ALB DNS name. Not reachable from the internet."
  value       = aws_lb.internal.dns_name
}

output "ecs_cluster_name" {
  description = "ECS cluster name, for `aws ecs execute-command` and deploys."
  value       = aws_ecs_cluster.main.name
}

output "ecs_service_names" {
  description = "Deployed Fargate service names, keyed by module."
  value       = { for name, service in aws_ecs_service.service : name => service.name }
}

output "kafka_bootstrap_brokers_sasl_iam" {
  description = "MSK Serverless bootstrap string (SASL/IAM). Injected into the services as KAFKA_BOOTSTRAP_SERVERS."
  value       = aws_msk_serverless_cluster.orders.bootstrap_brokers_sasl_iam
}

output "kafka_cluster_arn" {
  description = "MSK Serverless cluster ARN."
  value       = aws_msk_serverless_cluster.orders.arn
}

output "dynamodb_tables" {
  description = "Logical table name to physical table name."
  value       = local.table_names
}

output "orders_stream_arn" {
  description = "Stream ARN consumed by the read-model projection Lambda."
  value       = aws_dynamodb_table.orders.stream_arn
}

output "lambda_function_names" {
  description = "The three deployed functions."
  value = {
    stream_processor = aws_lambda_function.order_events_stream_processor.function_name
    status_api       = aws_lambda_function.order_status_api.function_name
    dlq_reprocessor  = aws_lambda_function.kafka_dlq_reprocessor.function_name
  }
}

output "dlq_queue_urls" {
  description = "Replay and parking queue URLs used by the runbook."
  value = {
    replay  = aws_sqs_queue.dlq_replay.url
    parking = aws_sqs_queue.dlq_parking.url
  }
}

output "alarm_topic_arn" {
  description = "SNS topic every CloudWatch alarm publishes to."
  value       = aws_sns_topic.alarms.arn
}

output "dashboard_url" {
  description = "Direct link to the saga dashboard."
  value       = "https://${var.aws_region}.console.aws.amazon.com/cloudwatch/home?region=${var.aws_region}#dashboards:name=${aws_cloudwatch_dashboard.saga.dashboard_name}"
}
