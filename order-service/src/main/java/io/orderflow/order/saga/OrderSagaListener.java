package io.orderflow.order.saga;

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
import io.orderflow.order.service.OrderService;

/**
 * The order-service half of the choreography: four listeners, one per inbound saga event.
 *
 * <p>Every listener follows the same three-step shape — restore the correlation context, claim the
 * event for this consumer group, delegate to the domain service. Anything the handler throws
 * propagates to the shared {@code DefaultErrorHandler}, which decides between back-off retry and
 * the dead-letter topic.
 *
 * <p>Concurrency is configured per listener container and must stay {@code <=} the partition count
 * of the topic; extra threads would idle. See the Performance Notes in the README.
 */
@Component
public class OrderSagaListener {

    private static final Logger log = LoggerFactory.getLogger(OrderSagaListener.class);

    /** Must match {@code spring.kafka.consumer.group-id}: it namespaces the dedupe markers. */
    public static final String CONSUMER_GROUP = "order-service";

    private final OrderService orderService;
    private final IdempotencyGuard idempotencyGuard;
    private final SagaMetrics metrics;

    public OrderSagaListener(OrderService orderService,
                             IdempotencyGuard idempotencyGuard,
                             SagaMetrics metrics) {
        this.orderService = orderService;
        this.idempotencyGuard = idempotencyGuard;
        this.metrics = metrics;
    }

    @KafkaListener(
            topics = Topics.INVENTORY_RESERVED,
            groupId = CONSUMER_GROUP,
            concurrency = "${app.kafka.listener.concurrency:3}")
    public void onInventoryReserved(EventEnvelope<DomainEvent> envelope) {
        handle(envelope, DomainEvent.InventoryReserved.class, orderService::onInventoryReserved);
    }

    @KafkaListener(
            topics = Topics.INVENTORY_REJECTED,
            groupId = CONSUMER_GROUP,
            concurrency = "${app.kafka.listener.concurrency:3}")
    public void onInventoryRejected(EventEnvelope<DomainEvent> envelope) {
        handle(envelope, DomainEvent.InventoryRejected.class, orderService::onInventoryRejected);
    }

    @KafkaListener(
            topics = Topics.PAYMENTS_CAPTURED,
            groupId = CONSUMER_GROUP,
            concurrency = "${app.kafka.listener.concurrency:3}")
    public void onPaymentCaptured(EventEnvelope<DomainEvent> envelope) {
        handle(envelope, DomainEvent.PaymentCaptured.class, orderService::onPaymentCaptured);
    }

    @KafkaListener(
            topics = Topics.PAYMENTS_FAILED,
            groupId = CONSUMER_GROUP,
            concurrency = "${app.kafka.listener.concurrency:3}")
    public void onPaymentFailed(EventEnvelope<DomainEvent> envelope) {
        handle(envelope, DomainEvent.PaymentFailed.class, orderService::onPaymentFailed);
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
            metrics.stepFailed(CONSUMER_GROUP, envelope.eventType().name(),
                    e.getClass().getSimpleName());
            log.error("Saga step failed group={} eventType={} eventId={}",
                    CONSUMER_GROUP, envelope.eventType(), envelope.eventId(), e);
            throw e;
        } finally {
            CorrelationContext.clear();
        }
    }
}
