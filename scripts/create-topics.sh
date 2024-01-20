#!/usr/bin/env bash
#
# Creates the saga topics and their dead-letter counterparts.
#
# Partition-key choice: every event is keyed by `orderId`. Kafka guarantees ordering within a
# partition, so keying on the aggregate id gives us exactly the ordering the saga needs — all
# events for one order are strictly ordered — while different orders spread across partitions and
# scale independently. Keying by customerId instead would create hot partitions for bulk buyers;
# keying by nothing (round-robin) would let InventoryReserved overtake OrderCreated.
#
# Partition counts are sized for the expected fan-out, not uniformly: orders.created and
# inventory.reserved carry every order, while orders.cancelled only carries failures.
# Dead-letter topics get 1 partition: they are drained by an operator, not by a consumer group,
# and the DeadLetterPublishingRecoverer therefore targets partition -1 (broker's choice).
#
# Local run (from the host):   ./scripts/create-topics.sh
# Compose runs it automatically in the `kafka-setup` service.
#
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
REPLICATION="${KAFKA_REPLICATION_FACTOR:-1}"   # 3 in MSK across three AZs
MIN_INSYNC="${KAFKA_MIN_INSYNC_REPLICAS:-1}"   # 2 in MSK, so a single AZ loss is survivable
RETENTION_MS="${KAFKA_RETENTION_MS:-604800000}"      # 7 days
DLT_RETENTION_MS="${KAFKA_DLT_RETENTION_MS:-1209600000}"  # 14 days: replay window for operators

log() { printf '[create-topics] %s\n' "$*"; }

# Confluent images ship `kafka-topics`; an Apache Kafka download ships `kafka-topics.sh`.
if command -v kafka-topics >/dev/null 2>&1; then
  KAFKA_TOPICS=kafka-topics
elif command -v kafka-topics.sh >/dev/null 2>&1; then
  KAFKA_TOPICS=kafka-topics.sh
else
  log "neither kafka-topics nor kafka-topics.sh is on PATH"
  exit 1
fi

wait_for_broker() {
  log "waiting for ${BOOTSTRAP}"
  for _ in $(seq 1 30); do
    if "${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP}" --list >/dev/null 2>&1; then
      log "broker is up"
      return 0
    fi
    sleep 2
  done
  log "broker did not become available"
  exit 1
}

create_topic() {
  local topic="$1" partitions="$2" retention="$3"
  if "${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP}" --list | grep -qx "${topic}"; then
    log "${topic} exists"
    return 0
  fi
  log "creating ${topic} (partitions=${partitions}, rf=${REPLICATION})"
  "${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP}" \
    --create \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor "${REPLICATION}" \
    --config "min.insync.replicas=${MIN_INSYNC}" \
    --config "retention.ms=${retention}" \
    --config "cleanup.policy=delete" \
    --config "compression.type=producer" >/dev/null
}

wait_for_broker

# topic                     partitions  retention
create_topic orders.created        6  "${RETENTION_MS}"
create_topic inventory.reserved    6  "${RETENTION_MS}"
create_topic inventory.rejected    3  "${RETENTION_MS}"
create_topic payments.captured     6  "${RETENTION_MS}"
create_topic payments.failed       3  "${RETENTION_MS}"
create_topic orders.confirmed      6  "${RETENTION_MS}"
create_topic orders.cancelled      3  "${RETENTION_MS}"

# Dead-letter topics: one partition each, longer retention.
for topic in orders.created inventory.reserved inventory.rejected \
             payments.captured payments.failed orders.confirmed orders.cancelled; do
  create_topic "${topic}.DLT" 1 "${DLT_RETENTION_MS}"
done

log "topics now present:"
"${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP}" --list | sort
