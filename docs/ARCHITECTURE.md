# Architecture

This document is the "why" behind the code. The README shows what the system does; this explains
the decisions, what they cost, and what would make me change them.

---

## 1. Service boundaries

Each service owns its data outright. No service reads another's tables, and there are no
synchronous calls between them — the only runtime coupling is the event contract in
`common-events`.

| Service | Owns | Publishes | Consumes |
| --- | --- | --- | --- |
| **order-service** | `orders`, `order_items` | `orders.created`, `orders.confirmed`, `orders.cancelled` | `inventory.reserved`, `inventory.rejected`, `payments.captured`, `payments.failed` |
| **inventory-service** | `inventory`, `inventory_reservations` | `inventory.reserved`, `inventory.rejected` | `orders.created`, `orders.cancelled` |
| **payment-service** | `payments` | `payments.captured`, `payments.failed` | `inventory.reserved` |
| **notification-service** | `notifications` | — | `orders.confirmed`, `orders.cancelled`, `inventory.rejected`, `payments.failed` |
| **api-gateway** | — | — | — (HTTP only) |
| **lambda-functions** | `order_read_model` | — | DynamoDB Streams, SQS |

Two boundaries are worth defending explicitly:

**Only order-service may change `OrderStatus`.** Inventory and payment report facts about their own
domain ("stock is held", "payment captured"); order-service decides what those facts mean for the
order. If inventory-service could write `orders`, the state machine would be enforced in three
places and eventually in none.

**payment-service never sees `orders.created`.** It waits for `inventory.reserved`. That ordering is
the whole point of the saga: the reversible step (a stock hold) runs before the expensive one (a
capture), so the common failure — out of stock — costs a release rather than a refund.

`common-events` also carries the reliability building blocks (outbox, idempotency guard,
correlation, saga metrics). Those are infrastructure, not domain, and duplicating them four times
would guarantee four subtly different behaviours.

---

## 2. Why choreography, not orchestration

An orchestrated saga has a coordinator that calls each participant and tracks the workflow
(Step Functions, Temporal, or a hand-rolled process manager). A choreographed saga has each
participant react to events and publish its own.

I chose choreography here because:

* **The workflow is short and linear.** Three steps with one compensating action. The state a
  coordinator would hold fits comfortably in the order aggregate, which already exists.
* **No coordinator means no coordinator outage.** With orchestration, the coordinator is on the
  critical path of every order. Here, if payment-service is down, its events queue up in Kafka and
  drain when it returns; nothing else notices.
* **Adding a participant is additive.** Fraud screening becomes a new consumer of
  `inventory.reserved` plus a new event order-service reacts to. With an orchestrator, every new
  step is a change to the central workflow definition — a single file every team has to touch.

What choreography costs, and how it is paid for here:

| Cost | Mitigation in this codebase |
| --- | --- |
| No single place shows "where is this saga?" | The order aggregate carries an embedded timeline with `causationId` links; `GET /orders/{id}/timeline` reconstructs the whole chain from one `GetItem`. |
| Cyclic dependencies are easy to create by accident | The event catalog in the README fixes exactly one producer per topic, and the sealed `DomainEvent` hierarchy makes the full set visible in one file. |
| Compensation logic is spread across services | Each service owns its own undo (`InventoryService.release`), which is also the only place that knows what it actually did. |
| No built-in timeout for a stalled saga | The `status-createdAt-index` GSI supports a sweep for orders stuck in a non-terminal state — see the RUNBOOK. This is the one thing an orchestrator would give for free. |

**When I would switch.** If the flow grew past roughly six steps, or gained human approval,
scheduled waits, or branch conditions that depend on several participants at once, I would move the
coordination into Step Functions and keep the services as they are. Those are the cases where
"which step are we on" genuinely needs first-class representation.

---

## 3. Consistency model

There is no distributed transaction anywhere. The guarantees are:

**Within a service: ACID, via DynamoDB.** Every state change that produces an event is written with
`TransactWriteItems` covering the aggregate and the outbox row. Either both land or neither does.
This closes the dual-write hole where a service commits state and then dies before reaching the
broker.

The `outbox` table itself is shared by the three producing services, which keeps one table
definition, one GSI and one runbook query for the whole platform. Each row records the `source`
service that wrote it, and each service's publisher drains only its own rows: without that filter
every producer would also publish every other producer's events, tripling Kafka traffic for no
benefit.

**Across services: eventual consistency with at-least-once delivery.** The outbox publisher sends
to Kafka *before* marking the record published, so a crash in between causes a redelivery rather
than a lost event. Losing an event would strand a saga forever — a duplicate is merely work we
already did.

**Effectively-once processing** is reconstructed at the consumer:

1. `IdempotencyGuard` claims `(consumerGroup, eventId)` with a conditional `PutItem` before the
   handler runs; a duplicate short-circuits.
2. If the handler throws, the marker is released so the Kafka retry can genuinely re-run.
3. Handlers are *also* written to be naturally idempotent — conditional writes, state-machine
   guards, a pre-capture payment lookup. Defence in depth, because the marker has one uncovered
   window: a crash between a successful handler and a released claim.

**Ordering** is per aggregate, not global. Every event is keyed by `orderId`, so all events for one
order land in the same partition and are consumed in order. Two different orders have no ordering
relationship, which is correct — they share no state except SKU counters, and those are protected
by optimistic concurrency rather than by ordering.

**Optimistic concurrency, not locks.** `orders` and `inventory` both carry
`@DynamoDbVersionAttribute`. A conflicting write fails its condition and is retried against fresh
state with jittered back-off. Pessimistic locking would need a lock table, a lease, and a story for
what happens when a lock holder dies mid-saga; a version number needs none of that.

**The read model is explicitly stale.** `order_read_model` is maintained by a Lambda from the
`orders` stream, typically under a second behind. That is documented in the API (`Cache-Control`,
and an OpenAPI note) rather than hidden, because a status page that is one second stale is fine and
a status page that competes with order intake for capacity is not.

---

## 4. Failure taxonomy

Every failure in the system falls into one of five classes, and each has one designed response.

**1. Transient infrastructure faults** — DynamoDB throttling, a broker leader election, a gateway
timeout. Modelled as `RetryableException`. Response: retry in-process with exponential back-off
(500 ms, x2, capped at 10 s, 4 attempts), then dead-letter. The SDK's own retries use full jitter,
because unjittered retries from a dozen consumer threads turn a brief throttle into a synchronised
stampede.

**2. Poison messages** — malformed JSON, an unsupported `schemaVersion`, an event on the wrong
topic. Modelled as `NonRetryableException`. Response: skip the back-off entirely and publish to
`<topic>.DLT` on the first attempt. Retrying cannot change the outcome, and a poison pill that is
retried 4x with back-off blocks its partition for ~16 s per record.

**3. Business rejections** — out of stock, card declined. Not failures at all: they are saga
outcomes. They produce events (`InventoryRejected`, `PaymentFailed`), drive the order to a terminal
state, and notify the customer. Nothing is retried and nothing is dead-lettered.

**4. Logic errors** — an event asking for a transition the state machine forbids. Modelled as
`IllegalStateTransitionException extends NonRetryableException`, so it dead-letters immediately with
the failure headers Spring attaches. In a choreographed saga this usually means two events arrived
in an order the design did not anticipate, which is exactly the thing you want sitting in a DLQ
with its payload, not swallowed.

**5. Stalled sagas** — nothing failed loudly; an order simply never progressed (a consumer was down
long enough for a record to age out, or an event was dropped during a manual intervention).
Response: the `status-createdAt-index` sweep plus the read-model `IteratorAge` alarm. This is the
class that silent systems hide, so it gets an explicit detection mechanism.

A sixth, deliberately unhandled class: **a captured payment for an order that then fails
downstream.** The compensating action for a capture is a refund, which is a business decision with
an audit trail, not something to automate on day one. The order goes to `FAILED`, the payment row
carries the authorization, and the RUNBOOK tells the operator what to reconcile.

---

## 5. Capacity planning

Sizing is driven by the partition count, because that is the hard ceiling on consumer parallelism.

**Topics.** `orders.created`, `inventory.reserved`, `payments.captured` and `orders.confirmed` carry
every order and have 6 partitions. `inventory.rejected`, `payments.failed` and `orders.cancelled`
carry only failures and have 3. Dead-letter topics have 1: they are drained by an operator, not by
a consumer group.

**Consumers.** Listener concurrency is 3 per instance with 2-3 instances, so a service has 6-9
consumer threads against 6 partitions. Anything past 6 idles — `max_capacity = 6` in the ECS
autoscaling target is that ceiling, not a coincidence. Scaling past it means adding partitions
first, and partitions can be added but never removed, so the growth path is: 6 → 12 → 24.

**Throughput per order.** One order costs 4 Kafka records, 4 outbox rows, roughly 12 DynamoDB writes
(including index updates) and 8 reads. At 100 orders/second that is ~1,200 WCU and ~800 RCU —
comfortably inside on-demand's per-table default of 40,000 RCU/WCU, and two orders of magnitude
below the 1,000 WCU-per-partition physical limit given `orderId` spreads uniformly.

**Hot partitions.** The one real risk is `inventory`: a single popular SKU is a single DynamoDB
partition *and* a single point of optimistic-lock contention. The mitigation when it bites is write
sharding — `SKU#1..SKU#N` sub-items summed on read — which trades read cost for write throughput.
It is not implemented here because it is a 10x-traffic problem, and premature sharding makes the
availability check harder to reason about.

**Latency budget.** With the `local` profile (250 ms outbox poll) on a laptop, against a single
Kafka broker and DynamoDB Local, six consecutive warm orders went from `PENDING` to `CONFIRMED` in
400-480 ms, measured from the first and last `occurredAt` in each order's timeline; the first order
after start-up took about 2 s while the JVMs and connections warmed up. Each of the three hops
between those two states waits on an outbox poll, so the poll interval dominates: the default
500 ms should roughly double the figure, and dropping it to 100 ms, or publishing directly to Kafka after a successful commit with the outbox
as a fallback sweeper, would roughly halve it. I kept the simpler pure-outbox path because half a
second is invisible behind an async API.

---

## 6. Cost notes

Rough monthly figures for a steady 50 orders/second in `us-east-1` — useful for relative weight,
not for a procurement spreadsheet.

| Component | Driver | Approximate cost |
| --- | --- | --- |
| MSK Serverless | ~$0.75/hour base + partition-hours + GB in/out | $600-800 |
| ECS Fargate (5 services, Graviton) | 11 tasks, 7 vCPU / 14 GB in total | ~$200 |
| DynamoDB on-demand | ~155 M writes + ~104 M reads | $200-280 |
| NAT Gateway | hourly + per-GB (why the DynamoDB endpoint exists) | $35-90 |
| Lambda | ~130 M stream records, ARM, SnapStart | $20-40 |
| CloudWatch | logs + custom metrics + dashboards | $40-80 |
| ALB + API Gateway | LCUs + $1/million requests | $30-60 |

Three observations that actually change decisions:

1. **MSK is the biggest line item and the least elastic.** At low volume, SQS + SNS would be a
   fraction of the cost. Kafka earns its keep when you need replay, multiple independent consumer
   groups over the same stream, and ordering — all three of which this design uses.
2. **The DynamoDB gateway endpoint pays for itself immediately.** Saga traffic is chatty; routing it
   through NAT would add per-GB charges *and* a shared failure domain.
3. **On-demand DynamoDB is roughly 6x the cost of well-utilised provisioned capacity.** It is the
   right default while the traffic curve is unknown; `orders`, `outbox` and `processed_events` are
   the three worth converting once it is predictable.

---

## 7. Migration path

### 7.1 MSK Serverless → provisioned MSK

Serverless is the right starting point: no broker sizing, no certificate rotation, IAM auth that
reuses the task roles. It becomes the wrong choice when any of these is true:

* sustained ingress approaches the 20 MiB/s per-partition ceiling;
* a broker-level config is needed (log compaction for a state topic, tiered storage, a different
  `min.insync.replicas` per topic);
* the cost curve inverts — above roughly 150-200 MB/s sustained, provisioned brokers are cheaper.

The migration is smaller than it sounds because nothing in the application code knows which it is
talking to:

1. Provision a 3-broker cluster (one per AZ), `replication.factor=3`, `min.insync.replicas=2`,
   IAM auth, in the same subnets.
2. Mirror the topics with MirrorMaker 2 and let it catch up.
3. Roll the services with the new bootstrap string; consumer groups start from `earliest` and the
   idempotency markers absorb the overlap. This is exactly the property at-least-once plus
   idempotency buys: a migration that re-delivers is a non-event.
4. Decommission the serverless cluster once lag is zero and the DLTs are empty.

The one code change: `min.insync.replicas` and `acks=all` now mean a real quorum, so a single-AZ
failure degrades throughput instead of stopping writes.

### 7.2 ECS Fargate → EKS

Fargate is right for six stateless JVM services: no nodes to patch, no cluster autoscaler, no
control plane to operate. EKS starts to pay off when:

* the service count grows past ~15-20 and per-service Terraform becomes the bottleneck;
* workloads need features Fargate lacks — DaemonSets, custom schedulers, GPU, sidecar-based service
  mesh with mTLS;
* the organisation already runs EKS and this is the odd one out (operational consistency is a real
  argument, not a technical one).

The path:

1. Helm chart per service — the container is already 12-factor: config from env, logs to stdout,
   `/actuator/health/{liveness,readiness}` for probes, graceful SIGTERM handling.
2. IRSA replaces the ECS task role, one service account per service. The per-service policies in
   `iam.tf` transfer unchanged, which is the payoff for having generated them from the service map.
3. Keep the ALB: the AWS Load Balancer Controller reuses the same target-group model.
4. Migrate one consumer service first (inventory-service), run both fleets against the same
   consumer group, and let Kafka rebalance partitions across them. That is a genuinely zero-downtime
   cutover, and it only works because the consumers are idempotent.
5. HPA on CPU to start; KEDA on Kafka consumer lag once there is a reason, since lag is the metric
   that actually reflects consumer pressure.

What I would not migrate: the three Lambda functions. The stream projection and the status read path
are the workloads where scale-to-zero and per-request billing are genuinely the right model, and
putting them on a cluster would only add cost and an idle deployment to maintain.
