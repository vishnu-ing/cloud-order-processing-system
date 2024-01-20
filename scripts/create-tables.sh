#!/usr/bin/env bash
#
# Creates every DynamoDB table the platform needs, with the same keys, indexes, streams and TTL
# settings that infrastructure/terraform/dynamodb.tf provisions in AWS. Keeping the two in step is
# what makes "works locally" mean something.
#
# Runs automatically as a LocalStack ready.d hook (see docker-compose.yml), and can be re-run by
# hand against LocalStack or a real account:
#
#   ./scripts/create-tables.sh                                  # LocalStack on :4566
#   AWS_ENDPOINT_URL= AWS_REGION=us-east-1 ./scripts/create-tables.sh   # real AWS
#
set -euo pipefail

AWS_REGION="${AWS_REGION:-us-east-1}"
AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL-http://localhost:4566}"
SEED_INVENTORY="${SEED_INVENTORY:-true}"

# Prefer awslocal inside the LocalStack container; fall back to aws with an explicit endpoint.
if command -v awslocal >/dev/null 2>&1; then
  aws_cmd() { awslocal "$@"; }
elif [[ -n "${AWS_ENDPOINT_URL}" ]]; then
  aws_cmd() { aws --endpoint-url "${AWS_ENDPOINT_URL}" --region "${AWS_REGION}" "$@"; }
else
  aws_cmd() { aws --region "${AWS_REGION}" "$@"; }
fi

log() { printf '[create-tables] %s\n' "$*"; }

table_exists() {
  aws_cmd dynamodb describe-table --table-name "$1" >/dev/null 2>&1
}

create_table() {
  local name="$1"
  shift
  if table_exists "${name}"; then
    log "${name} already exists, skipping"
    return 0
  fi
  log "creating ${name}"
  aws_cmd dynamodb create-table --table-name "${name}" --billing-mode PAY_PER_REQUEST "$@" >/dev/null
  aws_cmd dynamodb wait table-exists --table-name "${name}"
}

# ---------------------------------------------------------------------------- orders
# PK orderId. Two GSIs: customer history and an operational sweep by status.
# The stream feeds OrderEventsStreamProcessor, which maintains order_read_model.
create_table orders \
  --attribute-definitions \
      AttributeName=orderId,AttributeType=S \
      AttributeName=customerId,AttributeType=S \
      AttributeName=status,AttributeType=S \
      AttributeName=createdAt,AttributeType=S \
  --key-schema AttributeName=orderId,KeyType=HASH \
  --global-secondary-indexes '[
    {
      "IndexName": "customerId-createdAt-index",
      "KeySchema": [
        {"AttributeName": "customerId", "KeyType": "HASH"},
        {"AttributeName": "createdAt", "KeyType": "RANGE"}
      ],
      "Projection": {"ProjectionType": "ALL"}
    },
    {
      "IndexName": "status-createdAt-index",
      "KeySchema": [
        {"AttributeName": "status", "KeyType": "HASH"},
        {"AttributeName": "createdAt", "KeyType": "RANGE"}
      ],
      "Projection": {
        "ProjectionType": "INCLUDE",
        "NonKeyAttributes": ["customerId", "totalAmount", "currency", "updatedAt"]
      }
    }
  ]' \
  --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES

# ---------------------------------------------------------------------------- order_items
# PK orderId + SK sku: one query returns an order's lines, one GetItem answers "is SKU X on it".
create_table order_items \
  --attribute-definitions \
      AttributeName=orderId,AttributeType=S \
      AttributeName=sku,AttributeType=S \
  --key-schema \
      AttributeName=orderId,KeyType=HASH \
      AttributeName=sku,KeyType=RANGE

# ---------------------------------------------------------------------------- inventory
create_table inventory \
  --attribute-definitions AttributeName=sku,AttributeType=S \
  --key-schema AttributeName=sku,KeyType=HASH

# ---------------------------------------------------------------------------- inventory_reservations
# GSI orderId-index is what compensation uses to find the hold from a cancellation event.
create_table inventory_reservations \
  --attribute-definitions \
      AttributeName=reservationId,AttributeType=S \
      AttributeName=orderId,AttributeType=S \
  --key-schema AttributeName=reservationId,KeyType=HASH \
  --global-secondary-indexes '[
    {
      "IndexName": "orderId-index",
      "KeySchema": [{"AttributeName": "orderId", "KeyType": "HASH"}],
      "Projection": {"ProjectionType": "ALL"}
    }
  ]'

# ---------------------------------------------------------------------------- payments
create_table payments \
  --attribute-definitions \
      AttributeName=paymentId,AttributeType=S \
      AttributeName=orderId,AttributeType=S \
  --key-schema AttributeName=paymentId,KeyType=HASH \
  --global-secondary-indexes '[
    {
      "IndexName": "orderId-index",
      "KeySchema": [{"AttributeName": "orderId", "KeyType": "HASH"}],
      "Projection": {"ProjectionType": "ALL"}
    }
  ]'

# ---------------------------------------------------------------------------- outbox
# GSI status-createdAt-index lets the publisher read only the pending tail, oldest first.
create_table outbox \
  --attribute-definitions \
      AttributeName=recordId,AttributeType=S \
      AttributeName=status,AttributeType=S \
      AttributeName=createdAt,AttributeType=S \
  --key-schema AttributeName=recordId,KeyType=HASH \
  --global-secondary-indexes '[
    {
      "IndexName": "status-createdAt-index",
      "KeySchema": [
        {"AttributeName": "status", "KeyType": "HASH"},
        {"AttributeName": "createdAt", "KeyType": "RANGE"}
      ],
      "Projection": {"ProjectionType": "ALL"}
    }
  ]'

# ---------------------------------------------------------------------------- processed_events
# PK consumerGroup + SK eventId: the conditional put that makes consumers idempotent.
create_table processed_events \
  --attribute-definitions \
      AttributeName=consumerGroup,AttributeType=S \
      AttributeName=eventId,AttributeType=S \
  --key-schema \
      AttributeName=consumerGroup,KeyType=HASH \
      AttributeName=eventId,KeyType=RANGE

log "enabling TTL on processed_events.expiresAt"
aws_cmd dynamodb update-time-to-live \
  --table-name processed_events \
  --time-to-live-specification "Enabled=true,AttributeName=expiresAt" >/dev/null 2>&1 \
  || log "TTL already configured"

# ---------------------------------------------------------------------------- notifications
create_table notifications \
  --attribute-definitions AttributeName=notificationId,AttributeType=S \
  --key-schema AttributeName=notificationId,KeyType=HASH

# ---------------------------------------------------------------------------- order_read_model
# Projection maintained by Lambda from the orders stream; served by OrderStatusApiHandler.
create_table order_read_model \
  --attribute-definitions \
      AttributeName=orderId,AttributeType=S \
      AttributeName=customerId,AttributeType=S \
      AttributeName=updatedAt,AttributeType=S \
  --key-schema AttributeName=orderId,KeyType=HASH \
  --global-secondary-indexes '[
    {
      "IndexName": "customerId-updatedAt-index",
      "KeySchema": [
        {"AttributeName": "customerId", "KeyType": "HASH"},
        {"AttributeName": "updatedAt", "KeyType": "RANGE"}
      ],
      "Projection": {"ProjectionType": "ALL"}
    }
  ]'

# ---------------------------------------------------------------------------- demo stock
if [[ "${SEED_INVENTORY}" == "true" ]]; then
  log "seeding demo inventory"
  seed_sku() {
    aws_cmd dynamodb put-item --table-name inventory --item "{
      \"sku\": {\"S\": \"$1\"},
      \"description\": {\"S\": \"$2\"},
      \"stock\": {\"N\": \"$3\"},
      \"reserved\": {\"N\": \"0\"},
      \"version\": {\"N\": \"1\"},
      \"updatedAt\": {\"S\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}
    }" >/dev/null
  }
  seed_sku "SKU-BOOK-1"    "Designing Data-Intensive Applications" 250
  seed_sku "SKU-LAPTOP-1"  "14-inch developer laptop"              12
  seed_sku "SKU-HEADSET-1" "Noise cancelling headset"              64
  # Deliberately tiny so the InventoryRejected path is easy to demo.
  seed_sku "SKU-LIMITED-1" "Limited edition mechanical keyboard"    2
fi

log "done: $(aws_cmd dynamodb list-tables --query 'TableNames' --output text)"
