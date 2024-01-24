# Runbook

Operational procedures for the order processing platform. Every CloudWatch alarm in
`infrastructure/terraform/cloudwatch.tf` points at a section here.

Conventions: `$CLUSTER` is the MSK bootstrap string, `$PREFIX` is `order-processing-<env>`.
Locally, substitute `localhost:9092` and the plain table names.

---

## Quick reference

| Symptom | Section |
| --- | --- |
| `*-dlq-replay-depth` alarm | [DLQ depth rising](#dlq-depth-rising) |
| `*-dlq-parking-not-empty` alarm | [Parking queue](#parking-queue) |
| `*-stream-iterator-age` alarm | [Read-model lag](#read-model-lag) |
| `*-lambda-errors-*` alarm | [Lambda error rate](#lambda-error-rate) |
| `*-ddb-throttles-*` alarm | [DynamoDB throttling](#dynamodb-throttling) |
| `*-api-5xx` alarm | [API 5xx](#api-5xx) |
| "My order is stuck" | [Stuck saga](#stuck-saga) |

---

## Inspecting dead letters

Every topic has a `<topic>.DLT` sibling. Start by finding out which one is growing.

```bash
# What is in the dead-letter topics at all?
kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server "$CLUSTER" --topic-list "$(kafka-topics --bootstrap-server "$CLUSTER" \
  --list | grep '\.DLT$' | paste -sd, -)"

# Read the failures with the headers Spring attached: exception class, message, original offset.
kafka-console-consumer \
  --bootstrap-server "$CLUSTER" \
  --topic inventory.reserved.DLT \
  --from-beginning --max-messages 20 \
  --property print.headers=true \
  --property print.key=true
```

The headers that matter:

| Header | What it tells you |
| --- | --- |
| `kafka_dlt-exception-fqcn` | `NonRetryableException` means poison; anything else exhausted its retries |
| `kafka_dlt-exception-message` | Usually the whole story |
| `kafka_dlt-original-topic` / `-partition` / `-offset` | Where to look in the live topic |
| `X-Correlation-Id` | Join to logs across every service |

Then pull the full picture for that saga:

```bash
aws logs start-query \
  --log-group-names "/ecs/$PREFIX/order-service" "/ecs/$PREFIX/inventory-service" \
                    "/ecs/$PREFIX/payment-service" "/ecs/$PREFIX/notification-service" \
  --start-time "$(date -u -d '2 hours ago' +%s)" --end-time "$(date -u +%s)" \
  --query-string 'fields @timestamp, @message | filter @message like /CORRELATION_ID/ | sort @timestamp asc'
```

---

## Replaying events

Replay is safe: every consumer claims `(consumerGroup, eventId)` before processing, so an event
that was already applied is suppressed rather than double-applied.

**Single event.** Put the record on the replay queue; `KafkaDlqReprocessor` publishes it back onto
the live topic derived from the DLT name.

```bash
aws sqs send-message \
  --queue-url "$(terraform -chdir=infrastructure/terraform output -json dlq_queue_urls | jq -r .replay)" \
  --message-body '{
    "topic": "inventory.reserved.DLT",
    "key": "0f6c1d7e-...",
    "envelope": { "eventId": "...", "eventType": "INVENTORY_RESERVED", "schemaVersion": 1, "...": "..." }
  }'
```

**A batch.** Consume the DLT to a file, then fan it into SQS:

```bash
kafka-console-consumer --bootstrap-server "$CLUSTER" --topic payments.failed.DLT \
  --from-beginning --timeout-ms 10000 > /tmp/dlt.ndjson

while read -r line; do
  jq -c --arg t payments.failed.DLT '{topic: $t, key: .payload.orderId, envelope: .}' <<<"$line" \
  | xargs -0 -I{} aws sqs send-message --queue-url "$REPLAY_QUEUE_URL" --message-body {}
done < /tmp/dlt.ndjson
```

Replays are throttled to `maximum_concurrency = 2` on purpose: a burst of re-published events must
not overwhelm the very consumers that were struggling when the records were dead-lettered.

**Before replaying, ask whether the bug is fixed.** Replaying into the same defect just refills the
DLT — with a `receiveCount` now closer to the parking threshold.

---

## Checking consumer lag

```bash
# Per-group lag, per partition. LAG is the number that matters.
kafka-consumer-groups --bootstrap-server "$CLUSTER" --describe --group order-service
kafka-consumer-groups --bootstrap-server "$CLUSTER" --describe --group inventory-service
kafka-consumer-groups --bootstrap-server "$CLUSTER" --describe --group payment-service
kafka-consumer-groups --bootstrap-server "$CLUSTER" --describe --group notification-service

# All groups at once, sorted by the worst offender.
for g in order-service inventory-service payment-service notification-service; do
  echo "== $g"
  kafka-consumer-groups --bootstrap-server "$CLUSTER" --describe --group "$g" \
    | awk 'NR>1 {sum+=$6} END {print "total lag:", sum}'
done
```

Reading the output:

* **Lag rising on every partition** — the service is undersized or wedged. Check CPU, then
  `saga_step_failures_total` in Prometheus.
* **Lag on one partition only** — one hot aggregate, or a record being retried. Find it with the
  DLT headers or the per-partition offsets.
* **`CONSUMER-ID` empty** — no live consumer. The service is down or failing its readiness probe.
* **Lag flat but nonzero** — normal during a rebalance; it should clear within a minute.

Never reset offsets on a live group to "fix" lag. Skipping forward drops saga steps silently, which
turns a slow system into an inconsistent one.

---

## Alarms

### DLQ depth rising

**Means:** events are failing faster than they are being replayed.

1. Identify the topic: the DLT with the newest records.
2. Read 10 messages with headers and classify the exception (poison vs transient).
3. If transient and the cause is resolved — replay.
4. If poison — fix the producer or the contract first; the events keep in the DLT for 14 days.
5. If the depth is growing *while* replay is running, the replay is feeding the same defect. Pause
   it by setting the event source mapping's `maximum_concurrency` to 0 rather than deleting it.

### Parking queue

**Means:** events exhausted three automatic replay attempts. Nothing more will happen without a
human.

1. `aws sqs receive-message --queue-url "$PARKING_QUEUE_URL" --max-number-of-messages 10`
2. For each: decide *replay* (after a fix — send back to the replay queue), *repair* (correct the
   aggregate directly and drop the event), or *discard* (record why in the incident notes).
3. The original record is still in the DLT topic, so discarding from SQS loses nothing.

### Read-model lag

**Means:** `IteratorAge` on `OrderEventsStreamProcessor` is above 60 s, so `GET /orders/{id}/status`
is answering with stale data. The write model is unaffected.

1. Check the function's error rate — a failing batch is retried and blocks its shard.
2. Check for a throttle on `order_read_model`.
3. If a single record is poisoning the batch, `bisect_batch_on_function_error` will isolate it
   within three attempts and route it to the parking queue; confirm that is happening rather than
   the whole batch failing.
4. Mitigation while investigating: point the status endpoint at order-service (`/orders/{id}`),
   which reads the write model directly.

### Lambda error rate

1. `aws logs tail "/aws/lambda/$PREFIX-<function>" --since 15m --follow`
2. Stream processor: almost always a malformed image or a projection throttle.
3. Status API: a 500 here is a read failure; check IAM and the table name env var.
4. DLQ reprocessor: check that `KAFKA_BOOTSTRAP_SERVERS` is set and the security group still allows
   9098 to MSK.

### DynamoDB throttling

**Means:** requests are being rejected on `orders` or `outbox`.

1. Check `ConsumedWriteCapacityUnits` against the on-demand ramp — a 10x spike within 30 minutes
   can outrun on-demand's adaptive scaling.
2. Look for a hot partition: one `orderId` receiving a storm of retries, or one popular SKU.
3. Short term: reduce listener concurrency to shed write pressure.
4. Longer term: pre-warm the table by requesting a provisioned baseline, or shard the hot SKU.

### API 5xx

1. Check the gateway's circuit-breaker state: `GET /actuator/metrics/resilience4j.circuitbreaker.state`.
2. An open breaker means order-service is unhealthy — go to its readiness probe and logs.
3. 503s with a `transient-failure` problem type are DynamoDB pressure; see above.
4. Clients retrying with the same `Idempotency-Key` are safe, and that is worth saying explicitly in
   any status-page update.

### Stuck saga

**Means:** an order has sat in a non-terminal state far longer than the ~1 s the saga normally
takes. Usually reported by a customer rather than by an alarm.

```bash
# Everything created in PENDING before a cutoff.
aws dynamodb query \
  --table-name "$PREFIX-orders" \
  --index-name status-createdAt-index \
  --key-condition-expression '#s = :status AND createdAt < :cutoff' \
  --expression-attribute-names '{"#s":"status"}' \
  --expression-attribute-values '{":status":{"S":"PENDING"},":cutoff":{"S":"2026-02-01T10:00:00Z"}}'
```

Then, for one order:

1. `GET /api/v1/orders/{id}/timeline` — the last entry names the last event that landed.
2. Check the outbox for an undelivered record:
   `aws dynamodb query --table-name "$PREFIX-outbox" --index-name status-createdAt-index \
    --key-condition-expression '#s = :s' --expression-attribute-names '{"#s":"status"}' \
    --expression-attribute-values '{":s":{"S":"PENDING"}}'`
   Records stuck in `PENDING` with a rising `attempts` mean the publisher cannot reach Kafka;
   `FAILED` means it gave up after 10 attempts and needs a manual re-publish.
3. If the event was published but never consumed, check that consumer group's lag.
4. If the event was consumed and dropped, it is in that topic's DLT — replay it.
5. Last resort: drive the aggregate forward by hand, then record it. `FAILED` exists precisely so an
   operator can mark an order as needing reconciliation rather than leaving it ambiguous.

---

## Routine checks

```bash
# Health of every service through the gateway
for s in inventory payment notification; do
  curl -fsS "http://localhost:8080/internal/$s/actuator/health" | jq -c '{service:"'$s'", status}'
done

# Saga throughput and failures
curl -s localhost:8091/actuator/prometheus | grep -E 'saga_(events|step|terminal)'

# Outbox backlog — should be near zero
aws dynamodb scan --table-name "$PREFIX-outbox" \
  --filter-expression '#s = :s' --expression-attribute-names '{"#s":"status"}' \
  --expression-attribute-values '{":s":{"S":"PENDING"}}' --select COUNT

# Dedupe table size, a proxy for redelivery volume
aws dynamodb describe-table --table-name "$PREFIX-processed-events" \
  --query 'Table.ItemCount'
```

---

## Escalation

| Situation | Action |
| --- | --- |
| Payment captured but order `FAILED` | Do not automate. Reconcile against the `payments` row and raise a refund through the PSP console; record the `paymentId` and `authorizationCode`. |
| Stock counters look wrong | Compare `inventory.reserved` against the sum of `HELD` rows in `inventory_reservations`; the reservations table is authoritative. |
| A whole consumer group is down | Scale the ECS service to 0 and back to force fresh tasks; Kafka retains events, so nothing is lost while it is gone. |
| Suspected data loss | Stop replaying. PITR is on for every business table; restore to a new table and diff before touching production data. |
