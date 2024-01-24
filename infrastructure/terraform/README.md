# Terraform — AWS topology

Provisions the whole platform: network, DynamoDB, MSK Serverless, five Fargate services behind an
internal ALB, the three Lambda functions, the public HTTP API, the DLQ replay path and the
observability that goes with it.

## Layout

| File | What it owns |
| --- | --- |
| `main.tf` | Providers, VPC (3 AZs, private subnets, NAT, flow logs), DynamoDB gateway endpoint, shared locals |
| `variables.tf` | Every input, including the `services` map that drives task definitions *and* IAM |
| `dynamodb.tf` | Nine tables with GSIs, streams, PITR and TTL |
| `msk.tf` | MSK Serverless cluster, security group, topic ARNs for IAM |
| `ecs.tf` | Cluster, task definitions, services, internal ALB, target groups, autoscaling |
| `lambda.tf` | Three functions, SnapStart, event source mappings (Streams + SQS) |
| `apigateway.tf` | HTTP API v2, JWT authorizer, VPC link, routes, stage throttling, access logs |
| `sqs.tf` | Replay queue and parking queue with a redrive policy |
| `iam.tf` | Per-service task roles, per-function execution roles |
| `cloudwatch.tf` | Log groups, six alarms, SNS topic, saga dashboard |
| `outputs.tf` | API URL, bootstrap brokers, table names, queue URLs, dashboard link |

## Prerequisites

* Terraform >= 1.6
* AWS credentials with permission to create the above
* The Lambda artifact built first — Terraform uploads a real jar:

```bash
mvn -q -pl lambda-functions -am package
ls -lh ../../lambda-functions/target/lambda-functions.jar
```

* The five container images pushed to ECR under `<registry>/<project>/<service>:<tag>`.

## Usage

```bash
# Remote state is intentionally not committed: pass it at init time.
terraform init \
  -backend-config="bucket=my-tfstate" \
  -backend-config="key=order-processing/dev/terraform.tfstate" \
  -backend-config="region=us-east-1" \
  -backend-config="dynamodb_table=my-tfstate-locks"

terraform plan  -var-file=dev.tfvars -out=plan.out
terraform apply plan.out

terraform output api_endpoint
```

A minimal `dev.tfvars`:

```hcl
environment  = "dev"
aws_region   = "us-east-1"
ecr_registry = "123456789012.dkr.ecr.us-east-1.amazonaws.com"
image_tag    = "a1b2c3d"          # a git SHA, never "latest"
alarm_email  = "oncall@example.com"

# Leave jwt_issuer empty to deploy the API without an authorizer (dev only).
jwt_issuer   = "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_ABC123"
jwt_audience = ["order-api"]

single_nat_gateway = true          # cost over resilience in dev
```

## Design decisions worth knowing

**On-demand DynamoDB everywhere.** Order traffic is spiky; provisioned capacity would either be
over-provisioned most of the day or throttle during a campaign. Once the daily curve is
predictable, `orders`, `outbox` and `processed_events` are the three worth moving to provisioned
with autoscaling — they carry the bulk of the writes.

**MSK Serverless, not provisioned.** No broker sizing, no certificate rotation, IAM auth that
matches the task roles we already have. Accepted limits: no custom broker configs and a 20 MiB/s
per-partition ingress cap. `docs/ARCHITECTURE.md` covers the switch to provisioned.

**Nothing has a public IP.** API Gateway is the only internet-facing component; it reaches the
internal ALB through a VPC link. Tasks sit in private subnets with egress via NAT.

**ARM64/Graviton on both ECS and Lambda** — same JVM throughput, roughly 20% cheaper.

**IAM is generated from the service map.** `var.services[*].tables` is both the documentation of
who owns what and the input to the policy documents, so the two cannot drift. payment-service
genuinely cannot read the inventory table.

**Alarms map to runbook entries.** Every alarm description points at a section of
`docs/RUNBOOK.md`. An alarm without a first action is noise.

## Teardown

```bash
terraform destroy -var-file=dev.tfvars
```

`deletion_protection_enabled` is on for the business tables in `prod`, so a production destroy
fails until it is explicitly turned off — which is the intent.
