variable "project" {
  description = "Project name, used as the prefix for every resource name."
  type        = string
  default     = "order-processing"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod). Part of every resource name."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of dev, staging, prod."
  }
}

variable "aws_region" {
  description = "AWS region for every resource in this stack."
  type        = string
  default     = "us-east-1"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC."
  type        = string
  default     = "10.40.0.0/16"
}

variable "availability_zone_count" {
  description = "Number of AZs to spread subnets across. Three is the minimum for MSK and for an ALB that survives an AZ loss."
  type        = number
  default     = 3

  validation {
    condition     = var.availability_zone_count >= 2 && var.availability_zone_count <= 4
    error_message = "availability_zone_count must be between 2 and 4."
  }
}

variable "enable_nat_gateway" {
  description = "NAT for private subnets. Required for pulling images from ECR Public and reaching external PSPs."
  type        = bool
  default     = true
}

variable "single_nat_gateway" {
  description = "Use one NAT gateway instead of one per AZ. Cheaper, but a single-AZ dependency: dev only."
  type        = bool
  default     = true
}

variable "ecr_registry" {
  description = "Registry that holds the six service images, e.g. 123456789012.dkr.ecr.us-east-1.amazonaws.com."
  type        = string
  default     = ""
}

variable "image_tag" {
  description = "Immutable image tag deployed by CI (a git SHA, never 'latest')."
  type        = string
  default     = "main"
}

variable "services" {
  description = <<-EOT
    The five Spring services. `port` is the container port, `path_prefix` the ALB routing rule,
    `desired_count` the Fargate task count, and `tables` the DynamoDB tables the task role is
    allowed to touch — the list is the least-privilege policy, so keep it tight.
  EOT
  type = map(object({
    port          = number
    path_prefix   = string
    desired_count = number
    cpu           = number
    memory        = number
    tables        = list(string)
    public        = bool
  }))

  default = {
    api-gateway = {
      port          = 8080
      path_prefix   = "/api/*"
      desired_count = 2
      cpu           = 512
      memory        = 1024
      tables        = []
      public        = true
    }
    order-service = {
      port          = 8081
      path_prefix   = "/internal/order/*"
      desired_count = 3
      cpu           = 1024
      memory        = 2048
      tables        = ["orders", "order_items", "outbox", "processed_events"]
      public        = false
    }
    inventory-service = {
      port          = 8082
      path_prefix   = "/internal/inventory/*"
      desired_count = 2
      cpu           = 512
      memory        = 1024
      tables        = ["inventory", "inventory_reservations", "outbox", "processed_events"]
      public        = false
    }
    payment-service = {
      port          = 8083
      path_prefix   = "/internal/payment/*"
      desired_count = 2
      cpu           = 512
      memory        = 1024
      tables        = ["payments", "outbox", "processed_events"]
      public        = false
    }
    notification-service = {
      port          = 8084
      path_prefix   = "/internal/notification/*"
      desired_count = 2
      cpu           = 512
      memory        = 1024
      tables        = ["notifications", "processed_events"]
      public        = false
    }
  }
}

variable "kafka_topics" {
  description = "Saga topics. MSK Serverless auto-creates topics, but the list drives IAM resource ARNs."
  type        = list(string)
  default = [
    "orders.created",
    "inventory.reserved",
    "inventory.rejected",
    "payments.captured",
    "payments.failed",
    "orders.confirmed",
    "orders.cancelled",
  ]
}

variable "lambda_artifact_path" {
  description = "Path to the shaded lambda-functions jar produced by `mvn -pl lambda-functions package`."
  type        = string
  default     = "../../lambda-functions/target/lambda-functions.jar"
}

variable "lambda_memory_mb" {
  description = "Lambda memory. CPU scales with memory, so 1024 MB is the sweet spot for a JVM handler."
  type        = number
  default     = 1024
}

variable "lambda_timeout_seconds" {
  description = "Lambda timeout. Stream and SQS handlers are batch-bounded, so 60 s is generous."
  type        = number
  default     = 60
}

variable "jwt_issuer" {
  description = "OIDC issuer URL for the API Gateway JWT authorizer (e.g. a Cognito user pool)."
  type        = string
  default     = ""
}

variable "jwt_audience" {
  description = "Accepted audiences for the JWT authorizer."
  type        = list(string)
  default     = []
}

variable "api_throttle_burst_limit" {
  description = "API Gateway stage burst limit. The authoritative, global rate limit."
  type        = number
  default     = 200
}

variable "api_throttle_rate_limit" {
  description = "API Gateway stage steady-state requests per second."
  type        = number
  default     = 100
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention. 30 days for app logs keeps cost sane while covering a sprint of incidents."
  type        = number
  default     = 30
}

variable "alarm_email" {
  description = "Address subscribed to the alarm SNS topic. Empty means no subscription is created."
  type        = string
  default     = ""
}

variable "dlq_depth_alarm_threshold" {
  description = "Messages in the replay queue before the DLQ-depth alarm fires."
  type        = number
  default     = 10
}

variable "tags" {
  description = "Extra tags merged into the default tag set."
  type        = map(string)
  default     = {}
}
