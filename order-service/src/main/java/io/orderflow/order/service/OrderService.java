package io.orderflow.order.service;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import io.orderflow.common.event.DomainEvent;
import io.orderflow.common.event.EventEnvelope;
import io.orderflow.common.idempotency.ProcessedEvent;
import io.orderflow.common.idempotency.ProcessedEventRepository;
import io.orderflow.common.observability.CorrelationContext;
import io.orderflow.common.observability.SagaMetrics;
import io.orderflow.common.outbox.OutboxRecord;
import io.orderflow.order.domain.OrderNotFoundException;
import io.orderflow.order.domain.OrderStateMachine;
import io.orderflow.order.domain.OrderStatus;
import io.orderflow.order.model.OrderEntity;
import io.orderflow.order.model.OrderItemEntity;
import io.orderflow.order.model.OrderTimelineEntry;
import io.orderflow.order.repository.OrderRepository;
import io.orderflow.order.web.OrderDtos.CreateOrderRequest;
import io.orderflow.order.web.OrderDtos.OrderPageResponse;
import io.orderflow.order.web.OrderDtos.OrderResponse;
import io.orderflow.order.web.OrderDtos.OrderSummaryResponse;
import io.orderflow.order.web.OrderDtos.OrderTimelineResponse;

/**
 * Application service for the order aggregate. Holds the saga's decision logic:
 *
 * <ul>
 *   <li>{@code InventoryReserved} → hold recorded, payment-service takes over;</li>
 *   <li>{@code InventoryRejected} → order {@code REJECTED}, nothing to compensate;</li>
 *   <li>{@code PaymentCaptured} → {@code PAYMENT_CAPTURED} then {@code CONFIRMED} + emit
 *       {@code OrderConfirmed};</li>
 *   <li>{@code PaymentFailed} → order {@code REJECTED} + emit {@code OrderCancelled}, which is what
 *       releases the stock hold (the compensating action).</li>
 * </ul>
 *
 * <p>Every handler is written to be safe under redelivery: a terminal order ignores further saga
 * events, and an already-applied transition is a no-op rather than an error.
 */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    /** Namespace for HTTP-level idempotency keys inside {@code processed_events}. */
    private static final String API_IDEMPOTENCY_GROUP = "order-api";
    private static final String SOURCE = "order-service";

    private final OrderRepository orderRepository;
    private final OrderStateMachine stateMachine;
    private final ProcessedEventRepository processedEvents;
    private final SagaMetrics metrics;
    private final Duration idempotencyRetention;
    private final int maxPageSize;

    public OrderService(OrderRepository orderRepository,
                        OrderStateMachine stateMachine,
                        ProcessedEventRepository processedEvents,
                        SagaMetrics metrics,
                        @Value("${app.orders.idempotency-retention-hours:24}") int idempotencyRetentionHours,
                        @Value("${app.orders.max-page-size:100}") int maxPageSize) {
        this.orderRepository = orderRepository;
        this.stateMachine = stateMachine;
        this.processedEvents = processedEvents;
        this.metrics = metrics;
        this.idempotencyRetention = Duration.ofHours(idempotencyRetentionHours);
        this.maxPageSize = maxPageSize;
    }

    /** {@code created == false} means the response replays an earlier, identical request. */
    public record CreatedOrder(OrderResponse order, boolean created) {
    }

    // ------------------------------------------------------------------ write path (HTTP)

    public CreatedOrder createOrder(CreateOrderRequest request, String idempotencyKey, String correlationId) {
        String key = (idempotencyKey == null || idempotencyKey.isBlank())
                ? UUID.randomUUID().toString()
                : idempotencyKey;

        Optional<OrderResponse> replay = replayIfDuplicate(key);
        if (replay.isPresent()) {
            return new CreatedOrder(replay.get(), false);
        }

        String orderId = UUID.randomUUID().toString();
        ProcessedEvent marker = ProcessedEvent.of(
                API_IDEMPOTENCY_GROUP, key, "ORDER_API_REQUEST", orderId, idempotencyRetention);

        if (!processedEvents.claim(marker)) {
            // Lost a race with a concurrent identical request: serve whatever the winner created.
            return new CreatedOrder(
                    replayIfDuplicate(key).orElseThrow(() -> new OrderNotFoundException(orderId)), false);
        }

        try {
            OrderEntity order = newOrder(orderId, request, key, correlationId);
            List<OrderItemEntity> items = request.lines().stream()
                    .map(line -> OrderItemEntity.from(orderId, line.toOrderLine()))
                    .toList();
            order.setLineCount(items.size());

            DomainEvent.OrderCreated payload = new DomainEvent.OrderCreated(
                    orderId,
                    request.customerId(),
                    request.customerEmail(),
                    request.lines().stream().map(line -> line.toOrderLine()).toList(),
                    request.totalAmount(),
                    request.currency());
            EventEnvelope<DomainEvent.OrderCreated> envelope =
                    EventEnvelope.start(payload, SOURCE, correlationId);

            order.applyStatus(OrderStatus.PENDING, OrderTimelineEntry.of(
                    OrderStatus.PENDING.name(),
                    envelope.eventType().name(),
                    envelope.eventId(),
                    null,
                    envelope.correlationId(),
                    "Order accepted with " + items.size() + " line(s)"));

            orderRepository.create(order, items, OutboxRecord.pending(envelope));
            metrics.eventConsumed(SOURCE, "ORDER_API_CREATE");
            log.info("Created order {} for customer {} total={} {} correlationId={}",
                    orderId, request.customerId(), request.totalAmount(), request.currency(),
                    envelope.correlationId());

            return new CreatedOrder(OrderResponse.from(order, items), true);
        } catch (RuntimeException e) {
            // The order was not written, so the idempotency marker must not survive either,
            // otherwise the client's retry would be answered with a 404 forever.
            processedEvents.release(API_IDEMPOTENCY_GROUP, key);
            throw e;
        }
    }

    public OrderResponse cancelOrder(String orderId, String reason, String correlationId) {
        OrderEntity updated = orderRepository.mutate(orderId, order -> {
            OrderStatus current = order.orderStatus();
            stateMachine.transition(orderId, current, OrderStatus.CANCELLED);

            DomainEvent.OrderCancelled payload = new DomainEvent.OrderCancelled(
                    orderId,
                    order.getCustomerId(),
                    order.getCustomerEmail(),
                    order.getReservationId(),
                    reason);
            EventEnvelope<DomainEvent.OrderCancelled> envelope = EventEnvelope.start(
                    payload, SOURCE, CorrelationContext.orNew(correlationId));

            order.setFailureCode("CANCELLED_BY_CUSTOMER");
            order.setFailureReason(reason);
            order.applyStatus(OrderStatus.CANCELLED, OrderTimelineEntry.of(
                    OrderStatus.CANCELLED.name(),
                    envelope.eventType().name(),
                    envelope.eventId(),
                    null,
                    envelope.correlationId(),
                    reason));
            return Optional.of(OutboxRecord.pending(envelope));
        }).orElseThrow(() -> new OrderNotFoundException(orderId));

        metrics.sagaTerminal(OrderStatus.CANCELLED.name());
        log.info("Cancelled order {} reason={}", orderId, reason);
        return OrderResponse.from(updated, orderRepository.findItems(orderId));
    }

    // ------------------------------------------------------------------ saga handlers (Kafka)

    public void onInventoryReserved(EventEnvelope<DomainEvent.InventoryReserved> envelope) {
        DomainEvent.InventoryReserved payload = envelope.payload();
        applySagaStep(envelope, payload.orderId(), OrderStatus.INVENTORY_RESERVED, order -> {
            order.setReservationId(payload.reservationId());
            return Optional.empty();
        }, "Stock reserved, reservationId=" + payload.reservationId());
    }

    public void onInventoryRejected(EventEnvelope<DomainEvent.InventoryRejected> envelope) {
        DomainEvent.InventoryRejected payload = envelope.payload();
        applySagaStep(envelope, payload.orderId(), OrderStatus.REJECTED, order -> {
            order.setFailureCode("INVENTORY_REJECTED");
            order.setFailureReason(payload.reason());
            return Optional.empty();
        }, "Rejected: " + payload.reason());
        metrics.sagaTerminal(OrderStatus.REJECTED.name());
    }

    /**
     * Happy-path completion. Two transitions are applied under one optimistic-lock write so the
     * aggregate never rests in {@code PAYMENT_CAPTURED} — a state that would otherwise need its own
     * timeout sweep.
     */
    public void onPaymentCaptured(EventEnvelope<DomainEvent.PaymentCaptured> envelope) {
        DomainEvent.PaymentCaptured payload = envelope.payload();
        String orderId = payload.orderId();

        Optional<OrderEntity> result = orderRepository.mutate(orderId, order -> {
            if (isIgnorable(order, orderId, envelope)) {
                return Optional.empty();
            }
            OrderStatus current = order.orderStatus();
            if (current != OrderStatus.PAYMENT_CAPTURED) {
                stateMachine.transition(orderId, current, OrderStatus.PAYMENT_CAPTURED);
                order.setPaymentId(payload.paymentId());
                order.applyStatus(OrderStatus.PAYMENT_CAPTURED, timelineEntry(
                        envelope, OrderStatus.PAYMENT_CAPTURED,
                        "Payment captured, authorization=" + payload.authorizationCode()));
            }

            stateMachine.transition(orderId, OrderStatus.PAYMENT_CAPTURED, OrderStatus.CONFIRMED);
            DomainEvent.OrderConfirmed confirmed = new DomainEvent.OrderConfirmed(
                    orderId,
                    order.getCustomerId(),
                    order.getCustomerEmail(),
                    payload.paymentId(),
                    payload.amount(),
                    payload.currency());
            EventEnvelope<DomainEvent.OrderConfirmed> confirmation =
                    EventEnvelope.follow(confirmed, SOURCE, envelope);

            order.applyStatus(OrderStatus.CONFIRMED, timelineEntry(
                    confirmation, OrderStatus.CONFIRMED, "Order confirmed"));
            return Optional.of(OutboxRecord.pending(confirmation));
        });

        if (result.isEmpty()) {
            log.warn("PaymentCaptured for unknown order {} — dropping (eventId={})",
                    orderId, envelope.eventId());
            return;
        }
        metrics.sagaTerminal(OrderStatus.CONFIRMED.name());
    }

    /**
     * Compensation entry point. The order is rejected <em>and</em> an {@code OrderCancelled} event
     * is emitted, because the stock hold taken in the previous step must be given back — that
     * release is the saga's compensating action for {@code InventoryReserved}.
     */
    public void onPaymentFailed(EventEnvelope<DomainEvent.PaymentFailed> envelope) {
        DomainEvent.PaymentFailed payload = envelope.payload();
        String orderId = payload.orderId();

        Optional<OrderEntity> result = orderRepository.mutate(orderId, order -> {
            if (isIgnorable(order, orderId, envelope)) {
                return Optional.empty();
            }
            stateMachine.transition(orderId, order.orderStatus(), OrderStatus.REJECTED);

            DomainEvent.OrderCancelled cancellation = new DomainEvent.OrderCancelled(
                    orderId,
                    order.getCustomerId(),
                    order.getCustomerEmail(),
                    payload.reservationId() != null ? payload.reservationId() : order.getReservationId(),
                    "Payment failed: " + payload.failureCode());
            EventEnvelope<DomainEvent.OrderCancelled> compensation =
                    EventEnvelope.follow(cancellation, SOURCE, envelope);

            order.setFailureCode(payload.failureCode());
            order.setFailureReason(payload.failureReason());
            order.applyStatus(OrderStatus.REJECTED, timelineEntry(
                    envelope, OrderStatus.REJECTED,
                    "Payment failed: " + payload.failureReason() + " — releasing stock"));
            return Optional.of(OutboxRecord.pending(compensation));
        });

        if (result.isEmpty()) {
            log.warn("PaymentFailed for unknown order {} — dropping (eventId={})",
                    orderId, envelope.eventId());
            return;
        }
        metrics.sagaTerminal(OrderStatus.REJECTED.name());
    }

    // ------------------------------------------------------------------ read path

    public OrderResponse getOrder(String orderId) {
        OrderEntity order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
        return OrderResponse.from(order, orderRepository.findItems(orderId));
    }

    public OrderTimelineResponse getTimeline(String orderId) {
        return orderRepository.findById(orderId)
                .map(OrderTimelineResponse::from)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
    }

    /**
     * Lists orders by customer or by status.
     *
     * <p>Exactly one of the two must be supplied: both are GSI partition keys, and a query without
     * a partition key would degrade into a table scan — the kind of endpoint that works in a demo
     * and melts in production.
     */
    public OrderPageResponse listOrders(String customerId, String status, String cursor, int limit) {
        int effectiveLimit = Math.clamp(limit, 1, maxPageSize);
        OrderRepository.PageResult<OrderEntity> page;

        if (customerId != null && !customerId.isBlank()) {
            page = orderRepository.findByCustomer(customerId, cursor, effectiveLimit);
        } else if (status != null && !status.isBlank()) {
            page = orderRepository.findByStatus(OrderStatus.valueOf(status).name(), cursor, effectiveLimit);
        } else {
            throw new IllegalArgumentException("Either customerId or status must be provided");
        }

        List<OrderSummaryResponse> items = page.items().stream()
                // A customer-scoped query may additionally narrow by status client-side: the page is
                // already bounded by `limit`, so this filter is O(limit), never a scan.
                .filter(order -> status == null || status.isBlank()
                        || status.equalsIgnoreCase(order.getStatus()))
                .map(OrderSummaryResponse::from)
                .toList();

        return new OrderPageResponse(items, effectiveLimit, page.nextCursor());
    }

    // ------------------------------------------------------------------ internals

    private Optional<OrderResponse> replayIfDuplicate(String idempotencyKey) {
        return processedEvents.find(API_IDEMPOTENCY_GROUP, idempotencyKey)
                .map(ProcessedEvent::getOrderId)
                .flatMap(orderRepository::findById)
                .map(order -> {
                    log.info("Replaying idempotent create for key={} orderId={}",
                            idempotencyKey, order.getOrderId());
                    return OrderResponse.from(order, orderRepository.findItems(order.getOrderId()));
                });
    }

    private OrderEntity newOrder(String orderId,
                                 CreateOrderRequest request,
                                 String idempotencyKey,
                                 String correlationId) {
        OrderEntity order = new OrderEntity();
        order.setOrderId(orderId);
        order.setCustomerId(request.customerId());
        order.setCustomerEmail(request.customerEmail());
        order.setStatus(OrderStatus.PENDING.name());
        order.setTotalAmount(request.totalAmount());
        order.setCurrency(request.currency());
        order.setIdempotencyKey(idempotencyKey);
        order.setCorrelationId(CorrelationContext.orNew(correlationId));
        order.setCreatedAt(Instant.now());
        order.setUpdatedAt(Instant.now());
        return order;
    }

    /**
     * Shared body for the single-transition saga steps: guard against terminal/duplicate state,
     * apply the transition, append the timeline entry, optionally emit an event.
     */
    private void applySagaStep(EventEnvelope<? extends DomainEvent> envelope,
                               String orderId,
                               OrderStatus target,
                               java.util.function.Function<OrderEntity, Optional<OutboxRecord>> extra,
                               String detail) {
        Optional<OrderEntity> result = orderRepository.mutate(orderId, order -> {
            if (isIgnorable(order, orderId, envelope)) {
                return Optional.empty();
            }
            if (!stateMachine.transitionIfNeeded(orderId, order.orderStatus(), target)) {
                log.info("Order {} already in {} — event {} is a no-op",
                        orderId, target, envelope.eventId());
                return Optional.empty();
            }
            Optional<OutboxRecord> emitted = extra.apply(order);
            order.applyStatus(target, timelineEntry(envelope, target, detail));
            return emitted;
        });

        if (result.isEmpty()) {
            log.warn("{} for unknown order {} — dropping (eventId={})",
                    envelope.eventType(), orderId, envelope.eventId());
        }
    }

    /**
     * A saga event that arrives after the order already reached a terminal state is logged and
     * ignored rather than dead-lettered: this is the expected outcome of a customer cancelling
     * while a downstream step was still in flight, and the compensation path already handles it.
     */
    private boolean isIgnorable(OrderEntity order, String orderId, EventEnvelope<? extends DomainEvent> envelope) {
        OrderStatus current = order.orderStatus();
        if (current != null && current.isTerminal()) {
            log.info("Ignoring {} for order {} already terminal in {} (eventId={})",
                    envelope.eventType(), orderId, current, envelope.eventId());
            metrics.stepFailed(SOURCE, envelope.eventType().name(), "terminal_state");
            return true;
        }
        return false;
    }

    private OrderTimelineEntry timelineEntry(EventEnvelope<? extends DomainEvent> envelope,
                                             OrderStatus status,
                                             String detail) {
        return OrderTimelineEntry.of(
                status.name(),
                envelope.eventType().name(),
                envelope.eventId(),
                envelope.causationId(),
                envelope.correlationId(),
                detail);
    }
}
