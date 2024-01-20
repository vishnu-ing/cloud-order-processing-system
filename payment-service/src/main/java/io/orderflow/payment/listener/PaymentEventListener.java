package io.orderflow.payment.listener;

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
import io.orderflow.payment.service.PaymentService;

/**
 * Single subscription: {@code inventory.reserved}.
 *
 * <p>payment-service never listens to {@code orders.created}. Charging before stock is held would
 * make the common failure (out of stock) require a refund instead of a release — the whole reason
 * the saga orders the reversible step first.
 */
@Component
public class PaymentEventListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventListener.class);

    public static final String CONSUMER_GROUP = "payment-service";

    private final PaymentService paymentService;
    private final IdempotencyGuard idempotencyGuard;
    private final SagaMetrics metrics;

    public PaymentEventListener(PaymentService paymentService,
                                IdempotencyGuard idempotencyGuard,
                                SagaMetrics metrics) {
        this.paymentService = paymentService;
        this.idempotencyGuard = idempotencyGuard;
        this.metrics = metrics;
    }

    @KafkaListener(
            topics = Topics.INVENTORY_RESERVED,
            groupId = CONSUMER_GROUP,
            concurrency = "${app.kafka.listener.concurrency:3}")
    public void onInventoryReserved(EventEnvelope<DomainEvent> envelope) {
        CorrelationContext.put(envelope.correlationId(), envelope.eventId(), envelope.payload().orderId());
        try {
            metrics.eventConsumed(CONSUMER_GROUP, envelope.eventType().name());
            EventEnvelope<DomainEvent.InventoryReserved> typed =
                    envelope.as(DomainEvent.InventoryReserved.class);
            metrics.timeStep(CONSUMER_GROUP + ".CAPTURE", () -> {
                idempotencyGuard.runOnce(CONSUMER_GROUP, typed, () -> paymentService.capture(typed));
                return null;
            });
        } catch (RuntimeException e) {
            metrics.stepFailed(CONSUMER_GROUP, envelope.eventType().name(), e.getClass().getSimpleName());
            log.error("Capture failed eventId={} orderId={}",
                    envelope.eventId(), envelope.payload().orderId(), e);
            throw e;
        } finally {
            CorrelationContext.clear();
        }
    }
}
