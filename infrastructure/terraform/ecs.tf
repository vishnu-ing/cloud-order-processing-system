###############################################################################
# ECS Fargate
#
# Five services, one task definition each, all behind one internal ALB. Fargate
# rather than EKS because five stateless JVM services do not justify a control
# plane to operate; the migration path to EKS is in docs/ARCHITECTURE.md.
#
# Nothing here has a public IP. API Gateway reaches the ALB through a VPC link,
# which keeps the only internet-facing component a managed one.
###############################################################################

resource "aws_ecs_cluster" "main" {
  name = "${local.name_prefix}-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = local.tags
}

resource "aws_ecs_cluster_capacity_providers" "main" {
  cluster_name = aws_ecs_cluster.main.name

  capacity_providers = ["FARGATE", "FARGATE_SPOT"]

  # Baseline on-demand, burst on spot. Consumers are interruption-tolerant: a killed task
  # rebalances its partitions and the idempotency guard makes the redelivery harmless.
  default_capacity_provider_strategy {
    capacity_provider = "FARGATE"
    base              = 1
    weight            = 1
  }
}

###############################################################################
# Security groups
###############################################################################

resource "aws_security_group" "alb" {
  name        = "${local.name_prefix}-alb"
  description = "Internal ALB: reachable from the API Gateway VPC link only"
  vpc_id      = module.vpc.vpc_id

  tags = merge(local.tags, { Name = "${local.name_prefix}-alb" })
}

resource "aws_security_group" "vpc_link" {
  name        = "${local.name_prefix}-vpc-link"
  description = "ENIs used by the API Gateway VPC link"
  vpc_id      = module.vpc.vpc_id

  tags = merge(local.tags, { Name = "${local.name_prefix}-vpc-link" })
}

resource "aws_security_group" "ecs_tasks" {
  name        = "${local.name_prefix}-ecs-tasks"
  description = "Fargate tasks: inbound from the ALB only"
  vpc_id      = module.vpc.vpc_id

  tags = merge(local.tags, { Name = "${local.name_prefix}-ecs-tasks" })
}

resource "aws_vpc_security_group_ingress_rule" "alb_from_vpc_link" {
  security_group_id            = aws_security_group.alb.id
  description                  = "HTTP from the API Gateway VPC link"
  from_port                    = 80
  to_port                      = 80
  ip_protocol                  = "tcp"
  referenced_security_group_id = aws_security_group.vpc_link.id
}

resource "aws_vpc_security_group_egress_rule" "alb_to_tasks" {
  security_group_id            = aws_security_group.alb.id
  description                  = "ALB to task ports"
  from_port                    = 8080
  to_port                      = 8090
  ip_protocol                  = "tcp"
  referenced_security_group_id = aws_security_group.ecs_tasks.id
}

resource "aws_vpc_security_group_egress_rule" "vpc_link_to_alb" {
  security_group_id            = aws_security_group.vpc_link.id
  description                  = "VPC link to the internal ALB"
  from_port                    = 80
  to_port                      = 80
  ip_protocol                  = "tcp"
  referenced_security_group_id = aws_security_group.alb.id
}

resource "aws_vpc_security_group_ingress_rule" "tasks_from_alb" {
  security_group_id            = aws_security_group.ecs_tasks.id
  description                  = "Task ports from the ALB"
  from_port                    = 8080
  to_port                      = 8090
  ip_protocol                  = "tcp"
  referenced_security_group_id = aws_security_group.alb.id
}

# Egress is open because tasks need MSK, DynamoDB (via the gateway endpoint), ECR and the PSP.
# Tightening this to prefix lists is the next hardening step, not a day-one requirement.
resource "aws_vpc_security_group_egress_rule" "tasks_all" {
  security_group_id = aws_security_group.ecs_tasks.id
  description       = "Task egress"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

###############################################################################
# Internal ALB
###############################################################################

resource "aws_lb" "internal" {
  name               = "${local.name_prefix}-alb"
  internal           = true
  load_balancer_type = "application"
  subnets            = module.vpc.private_subnets
  security_groups    = [aws_security_group.alb.id]

  idle_timeout               = 65 # just above the gateway's 8 s response timeout plus retries
  enable_deletion_protection = var.environment == "prod"
  drop_invalid_header_fields = true

  tags = merge(local.tags, { Name = "${local.name_prefix}-alb" })
}

resource "aws_lb_target_group" "service" {
  for_each = var.services

  name        = substr("${local.name_prefix}-${each.key}", 0, 32)
  port        = each.value.port
  protocol    = "HTTP"
  target_type = "ip"
  vpc_id      = module.vpc.vpc_id

  # Readiness, not liveness: a task that is up but not ready to serve must not receive traffic.
  health_check {
    path                = "/actuator/health/readiness"
    healthy_threshold   = 2
    unhealthy_threshold = 3
    timeout             = 5
    interval            = 15
    matcher             = "200"
  }

  # Let in-flight requests finish before a task is removed during a deploy.
  deregistration_delay = 20

  tags = merge(local.tags, { Service = each.key })
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.internal.arn
  port              = 80
  protocol          = "HTTP"

  # Default to the gateway: it owns routing decisions for anything not matched below.
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.service["api-gateway"].arn
  }
}

# Direct routes to the internal services, used by the gateway's /internal/* passthrough and by
# operators. They are not reachable from API Gateway, which only forwards /api/*.
resource "aws_lb_listener_rule" "service" {
  for_each = { for name, config in var.services : name => config if !config.public }

  listener_arn = aws_lb_listener.http.arn
  priority     = 100 + index(keys(var.services), each.key)

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.service[each.key].arn
  }

  condition {
    path_pattern {
      values = [each.value.path_prefix]
    }
  }
}

###############################################################################
# Task definitions and services
###############################################################################

locals {
  # Environment shared by every service. Per-service table names are appended below.
  common_environment = [
    { name = "SPRING_PROFILES_ACTIVE", value = var.environment },
    { name = "AWS_REGION", value = var.aws_region },
    { name = "KAFKA_BOOTSTRAP_SERVERS", value = aws_msk_serverless_cluster.orders.bootstrap_brokers_sasl_iam },
    { name = "KAFKA_SECURITY_PROTOCOL", value = "SASL_SSL" },
    { name = "KAFKA_SASL_MECHANISM", value = "AWS_MSK_IAM" },
    { name = "ORDERS_TABLE", value = local.table_names.orders },
    { name = "ORDER_ITEMS_TABLE", value = local.table_names.order_items },
    { name = "INVENTORY_TABLE", value = local.table_names.inventory },
    { name = "RESERVATIONS_TABLE", value = local.table_names.inventory_reservations },
    { name = "PAYMENTS_TABLE", value = local.table_names.payments },
    { name = "OUTBOX_TABLE", value = local.table_names.outbox },
    { name = "PROCESSED_EVENTS_TABLE", value = local.table_names.processed_events },
    { name = "NOTIFICATIONS_TABLE", value = local.table_names.notifications },
  ]

  gateway_environment = [
    { name = "ORDER_SERVICE_URI", value = "http://${aws_lb.internal.dns_name}/internal/order" },
    { name = "INVENTORY_SERVICE_URI", value = "http://${aws_lb.internal.dns_name}/internal/inventory" },
    { name = "PAYMENT_SERVICE_URI", value = "http://${aws_lb.internal.dns_name}/internal/payment" },
    { name = "NOTIFICATION_SERVICE_URI", value = "http://${aws_lb.internal.dns_name}/internal/notification" },
    { name = "APP_SECURITY_JWT_ENABLED", value = "true" },
    { name = "JWT_ISSUER_URI", value = var.jwt_issuer },
  ]
}

resource "aws_ecs_task_definition" "service" {
  for_each = var.services

  family                   = "${local.name_prefix}-${each.key}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = each.value.cpu
  memory                   = each.value.memory
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task[each.key].arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64" # Graviton: ~20% cheaper for the same JVM throughput
  }

  container_definitions = jsonencode([
    {
      name      = each.key
      image     = "${local.container_registry}/${var.project}/${each.key}:${var.image_tag}"
      essential = true

      portMappings = [
        {
          containerPort = each.value.port
          protocol      = "tcp"
        }
      ]

      environment = concat(
        local.common_environment,
        [{ name = "SERVER_PORT", value = tostring(each.value.port) }],
        each.key == "api-gateway" ? local.gateway_environment : [],
      )

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.service[each.key].name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }

      # Container-level check in addition to the target group: a task that fails this is replaced
      # even when it is not currently receiving traffic.
      healthCheck = {
        command     = ["CMD-SHELL", "curl -fsS http://localhost:${each.value.port}/actuator/health/liveness || exit 1"]
        interval    = 30
        timeout     = 5
        retries     = 3
        startPeriod = 60
      }

      # SIGTERM handling: Spring's graceful shutdown drains listeners inside this window.
      stopTimeout = 30
    }
  ])

  tags = merge(local.tags, { Service = each.key })
}

resource "aws_ecs_service" "service" {
  for_each = var.services

  name            = "${local.name_prefix}-${each.key}"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.service[each.key].arn
  desired_count   = each.value.desired_count
  propagate_tags  = "SERVICE"

  # Rolling deploy that never drops below the current capacity.
  deployment_minimum_healthy_percent = 100
  deployment_maximum_percent         = 200
  health_check_grace_period_seconds  = 90
  enable_execute_command             = var.environment != "prod"

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  network_configuration {
    subnets          = module.vpc.private_subnets
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.service[each.key].arn
    container_name   = each.key
    container_port   = each.value.port
  }

  depends_on = [aws_lb_listener.http]

  tags = merge(local.tags, { Service = each.key })
}

###############################################################################
# Autoscaling
#
# CPU-based for the gateway (request bound) and the consumers (partition bound).
# The ceiling for a consumer service is its topic's partition count: more tasks
# than partitions means idle consumers, so max_capacity stays at 6.
###############################################################################

resource "aws_appautoscaling_target" "service" {
  for_each = var.services

  service_namespace  = "ecs"
  resource_id        = "service/${aws_ecs_cluster.main.name}/${aws_ecs_service.service[each.key].name}"
  scalable_dimension = "ecs:service:DesiredCount"
  min_capacity       = each.value.desired_count
  max_capacity       = 6
}

resource "aws_appautoscaling_policy" "service_cpu" {
  for_each = var.services

  name               = "${local.name_prefix}-${each.key}-cpu"
  policy_type        = "TargetTrackingScaling"
  service_namespace  = aws_appautoscaling_target.service[each.key].service_namespace
  resource_id        = aws_appautoscaling_target.service[each.key].resource_id
  scalable_dimension = aws_appautoscaling_target.service[each.key].scalable_dimension

  target_tracking_scaling_policy_configuration {
    target_value       = 65
    scale_in_cooldown  = 300 # slow in: a rebalance storm is worse than a warm task
    scale_out_cooldown = 60  # fast out

    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageCPUUtilization"
    }
  }
}
