package io.orderflow.inventory.listener;

import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import io.orderflow.common.event.DomainEvent;
import io.orderflow.common.event.EventEnvelope;
import io.orderflow.common.event.Topics;
import io.orderflow.common.idempotency.IdempotencyGuard;
import io.orderflow.common.observability.CorrelationContext;
import io.orderflow.common.observability.SagaMetrics;
import io.orderflow.inventory.service.InventoryService;

/**
 * Two subscriptions: the forward step and its compensation.
 *
 * <p>They deliberately share one consumer group so that both are processed by the same set of
 * partitions-to-consumer assignments — a cancellation for order X is handled by whichever instance
 * also handles X's creation, which keeps DynamoDB conditional-write contention on one aggregate
 * localised instead of spread across the fleet.
 */
@Component
public class InventoryEventListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventListener.class);

    public static final String CONSUMER_GROUP = "inventory-service";

    private final InventoryService inventoryService;
    private final IdempotencyGuard idempotencyGuard;
    private final SagaMetrics metrics;

    public InventoryEventListener(InventoryService inventoryService,
                                  IdempotencyGuard idempotencyGuard,
                                  SagaMetrics metrics) {
        this.inventoryService = inventoryService;
        this.idempotencyGuard = idempotencyGuard;
        this.metrics = metrics;
    }

    @KafkaListener(
            topics = Topics.ORDERS_CREATED,
            groupId = CONSUMER_GROUP,
            concurrency = "${app.kafka.listener.concurrency:3}")
    public void onOrderCreated(EventEnvelope<DomainEvent> envelope) {
        handle(envelope, DomainEvent.OrderCreated.class, inventoryService::reserve);
    }

    @KafkaListener(
            topics = Topics.ORDERS_CANCELLED,
            groupId = CONSUMER_GROUP,
            concurrency = "${app.kafka.listener.concurrency:3}")
    public void onOrderCancelled(EventEnvelope<DomainEvent> envelope) {
        handle(envelope, DomainEvent.OrderCancelled.class, inventoryService::release);
    }

    private <T extends DomainEvent> void handle(EventEnvelope<DomainEvent> envelope,
                                                Class<T> payloadType,
                                                Consumer<EventEnvelope<T>> handler) {
        CorrelationContext.put(envelope.correlationId(), envelope.eventId(), envelope.payload().orderId());
        try {
            metrics.eventConsumed(CONSUMER_GROUP, envelope.eventType().name());
            EventEnvelope<T> typed = envelope.as(payloadType);
            metrics.timeStep(CONSUMER_GROUP + "." + envelope.eventType().name(), () -> {
                idempotencyGuard.runOnce(CONSUMER_GROUP, typed, () -> handler.accept(typed));
                return null;
            });
        } catch (RuntimeException e) {
            metrics.stepFailed(CONSUMER_GROUP, envelope.eventType().name(), e.getClass().getSimpleName());
            log.error("Inventory step failed eventType={} eventId={} orderId={}",
                    envelope.eventType(), envelope.eventId(), envelope.payload().orderId(), e);
            throw e;
        } finally {
            CorrelationContext.clear();
        }
    }
}
