###############################################################################
# API Gateway HTTP API (v2)
#
# One public front door with two kinds of integration:
#
#   /api/v1/orders*              -> VPC link -> internal ALB -> api-gateway service
#   /api/v1/orders/{id}/status   -> Lambda (read model, scales to zero)
#   /api/v1/customers/{id}/orders-> Lambda (read model)
#
# HTTP API rather than REST API: the JWT authorizer is built in, latency is lower
# and it costs roughly a third as much. The features REST API has that we would
# miss (request validation, WAF-per-method) are handled at the gateway service.
###############################################################################

resource "aws_apigatewayv2_api" "http" {
  name          = "${local.name_prefix}-api"
  protocol_type = "HTTP"
  description   = "Public API for the order processing platform"

  cors_configuration {
    allow_origins  = ["*"]
    allow_methods  = ["GET", "POST", "OPTIONS"]
    allow_headers  = ["authorization", "content-type", "idempotency-key", "x-correlation-id"]
    expose_headers = ["x-correlation-id"]
    max_age        = 300
  }

  tags = local.tags
}

# Private integration path to the ALB. The link's ENIs live in the private subnets, so traffic
# never leaves the VPC between API Gateway and the services.
resource "aws_apigatewayv2_vpc_link" "internal" {
  name               = "${local.name_prefix}-vpc-link"
  subnet_ids         = module.vpc.private_subnets
  security_group_ids = [aws_security_group.vpc_link.id]

  tags = local.tags
}

resource "aws_apigatewayv2_authorizer" "jwt" {
  count = var.jwt_issuer != "" ? 1 : 0

  api_id           = aws_apigatewayv2_api.http.id
  authorizer_type  = "JWT"
  identity_sources = ["$request.header.Authorization"]
  name             = "${local.name_prefix}-jwt"

  jwt_configuration {
    issuer   = var.jwt_issuer
    audience = var.jwt_audience
  }
}

###############################################################################
# Integrations
###############################################################################

resource "aws_apigatewayv2_integration" "alb" {
  api_id             = aws_apigatewayv2_api.http.id
  integration_type   = "HTTP_PROXY"
  integration_method = "ANY"
  integration_uri    = aws_lb_listener.http.arn
  connection_type    = "VPC_LINK"
  connection_id      = aws_apigatewayv2_vpc_link.internal.id

  # 29 s is the API Gateway ceiling; the write path is fronted by a circuit breaker that trips
  # long before this, so the timeout only guards against a truly stuck backend.
  timeout_milliseconds = 29000

  request_parameters = {
    "overwrite:header.X-Forwarded-Prefix" = "/api"
  }
}

resource "aws_apigatewayv2_integration" "order_status_lambda" {
  api_id                 = aws_apigatewayv2_api.http.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.order_status_api.invoke_arn
  payload_format_version = "2.0"
  timeout_milliseconds   = 15000
}

###############################################################################
# Routes
###############################################################################

locals {
  authorizer_id   = var.jwt_issuer != "" ? aws_apigatewayv2_authorizer.jwt[0].id : null
  authorizer_type = var.jwt_issuer != "" ? "JWT" : "NONE"
}

# Serverless read path, declared before the catch-all so it wins the more specific match.
resource "aws_apigatewayv2_route" "order_status" {
  api_id             = aws_apigatewayv2_api.http.id
  route_key          = "GET /api/v1/orders/{orderId}/status"
  target             = "integrations/${aws_apigatewayv2_integration.order_status_lambda.id}"
  authorization_type = local.authorizer_type
  authorizer_id      = local.authorizer_id
}

resource "aws_apigatewayv2_route" "customer_orders" {
  api_id             = aws_apigatewayv2_api.http.id
  route_key          = "GET /api/v1/customers/{customerId}/orders"
  target             = "integrations/${aws_apigatewayv2_integration.order_status_lambda.id}"
  authorization_type = local.authorizer_type
  authorizer_id      = local.authorizer_id
}

# Everything else on /api/* goes to the containerised gateway.
resource "aws_apigatewayv2_route" "api_proxy" {
  api_id             = aws_apigatewayv2_api.http.id
  route_key          = "ANY /api/{proxy+}"
  target             = "integrations/${aws_apigatewayv2_integration.alb.id}"
  authorization_type = local.authorizer_type
  authorizer_id      = local.authorizer_id
}

###############################################################################
# Stage
###############################################################################

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.http.id
  name        = "$default"
  auto_deploy = true

  # The authoritative, global rate limit. The in-process limiter in the gateway service is a
  # second line of defence, not the contract.
  default_route_settings {
    throttling_burst_limit   = var.api_throttle_burst_limit
    throttling_rate_limit    = var.api_throttle_rate_limit
    detailed_metrics_enabled = true
  }

  # Order creation is the expensive route: give it a tighter budget of its own.
  route_settings {
    route_key              = "ANY /api/{proxy+}"
    throttling_burst_limit = var.api_throttle_burst_limit
    throttling_rate_limit  = var.api_throttle_rate_limit
  }

  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.api_gateway.arn

    # Correlation id is logged so an access-log line can be joined to the saga it started.
    format = jsonencode({
      requestId          = "$context.requestId"
      correlationId      = "$context.request.header.x-correlation-id"
      ip                 = "$context.identity.sourceIp"
      requestTime        = "$context.requestTime"
      httpMethod         = "$context.httpMethod"
      routeKey           = "$context.routeKey"
      path               = "$context.path"
      status             = "$context.status"
      integrationError   = "$context.integrationErrorMessage"
      responseLatency    = "$context.responseLatency"
      integrationLatency = "$context.integrationLatency"
      userSub            = "$context.authorizer.claims.sub"
    })
  }

  tags = local.tags
}
