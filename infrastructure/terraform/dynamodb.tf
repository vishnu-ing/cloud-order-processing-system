###############################################################################
# DynamoDB
#
# On-demand billing everywhere: saga load is spiky (a marketing email doubles
# order intake for ten minutes) and on-demand removes capacity planning as a
# source of 4xx throttles. PITR is on for every table that holds business data.
###############################################################################

resource "aws_dynamodb_table" "orders" {
  name         = local.table_names.orders
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "orderId"

  attribute {
    name = "orderId"
    type = "S"
  }
  attribute {
    name = "customerId"
    type = "S"
  }
  attribute {
    name = "status"
    type = "S"
  }
  attribute {
    name = "createdAt"
    type = "S"
  }

  # "My orders, newest first" — the customer-facing list endpoint.
  global_secondary_index {
    name            = "customerId-createdAt-index"
    hash_key        = "customerId"
    range_key       = "createdAt"
    projection_type = "ALL"
  }

  # Operational sweeps: find sagas stuck in PENDING. Projection is narrow on purpose — the
  # sweep needs identifiers and amounts, not the embedded timeline, and a narrower projection
  # is a smaller write amplification on every order update.
  global_secondary_index {
    name               = "status-createdAt-index"
    hash_key           = "status"
    range_key          = "createdAt"
    projection_type    = "INCLUDE"
    non_key_attributes = ["customerId", "totalAmount", "currency", "updatedAt"]
  }

  # Feeds OrderEventsStreamProcessor, which maintains the read model.
  stream_enabled   = true
  stream_view_type = "NEW_AND_OLD_IMAGES"

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }

  deletion_protection_enabled = var.environment == "prod"

  tags = merge(local.tags, { Name = local.table_names.orders })
}

resource "aws_dynamodb_table" "order_items" {
  name         = local.table_names.order_items
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "orderId"
  range_key    = "sku"

  attribute {
    name = "orderId"
    type = "S"
  }
  attribute {
    name = "sku"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }

  deletion_protection_enabled = var.environment == "prod"

  tags = merge(local.tags, { Name = local.table_names.order_items })
}

resource "aws_dynamodb_table" "inventory" {
  name         = local.table_names.inventory
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "sku"

  attribute {
    name = "sku"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }

  deletion_protection_enabled = var.environment == "prod"

  tags = merge(local.tags, { Name = local.table_names.inventory })
}

resource "aws_dynamodb_table" "inventory_reservations" {
  name         = local.table_names.inventory_reservations
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "reservationId"

  attribute {
    name = "reservationId"
    type = "S"
  }
  attribute {
    name = "orderId"
    type = "S"
  }

  # Compensation path: resolve the hold from the order alone.
  global_secondary_index {
    name            = "orderId-index"
    hash_key        = "orderId"
    projection_type = "ALL"
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }

  tags = merge(local.tags, { Name = local.table_names.inventory_reservations })
}

resource "aws_dynamodb_table" "payments" {
  name         = local.table_names.payments
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "paymentId"

  attribute {
    name = "paymentId"
    type = "S"
  }
  attribute {
    name = "orderId"
    type = "S"
  }

  # Pre-capture duplicate check.
  global_secondary_index {
    name            = "orderId-index"
    hash_key        = "orderId"
    projection_type = "ALL"
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }

  deletion_protection_enabled = var.environment == "prod"

  tags = merge(local.tags, { Name = local.table_names.payments })
}

resource "aws_dynamodb_table" "outbox" {
  name         = local.table_names.outbox
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "recordId"

  attribute {
    name = "recordId"
    type = "S"
  }
  attribute {
    name = "status"
    type = "S"
  }
  attribute {
    name = "createdAt"
    type = "S"
  }

  # The publisher's only query: PENDING, oldest first.
  global_secondary_index {
    name            = "status-createdAt-index"
    hash_key        = "status"
    range_key       = "createdAt"
    projection_type = "ALL"
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }

  tags = merge(local.tags, { Name = local.table_names.outbox })
}

resource "aws_dynamodb_table" "processed_events" {
  name         = local.table_names.processed_events
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "consumerGroup"
  range_key    = "eventId"

  attribute {
    name = "consumerGroup"
    type = "S"
  }
  attribute {
    name = "eventId"
    type = "S"
  }

  # Dedupe markers are only useful while a redelivery is plausible; TTL keeps the table bounded
  # and the deletes are free (no write capacity is consumed by TTL expiry).
  ttl {
    attribute_name = "expiresAt"
    enabled        = true
  }

  server_side_encryption {
    enabled = true
  }

  tags = merge(local.tags, { Name = local.table_names.processed_events })
}

resource "aws_dynamodb_table" "notifications" {
  name         = local.table_names.notifications
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "notificationId"

  attribute {
    name = "notificationId"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }

  tags = merge(local.tags, { Name = local.table_names.notifications })
}

resource "aws_dynamodb_table" "order_read_model" {
  name         = local.table_names.order_read_model
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "orderId"

  attribute {
    name = "orderId"
    type = "S"
  }
  attribute {
    name = "customerId"
    type = "S"
  }
  attribute {
    name = "updatedAt"
    type = "S"
  }

  global_secondary_index {
    name            = "customerId-updatedAt-index"
    hash_key        = "customerId"
    range_key       = "updatedAt"
    projection_type = "ALL"
  }

  server_side_encryption {
    enabled = true
  }

  # No PITR: the projection is rebuildable from the orders table at any time, so paying to back
  # it up would be paying twice for the same data.
  tags = merge(local.tags, { Name = local.table_names.order_read_model })
}
