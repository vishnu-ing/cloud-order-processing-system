package io.orderflow.notification.listener;

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
import io.orderflow.notification.service.NotificationService;

/**
 * Fan-in listener: one method, four topics, all of them saga endings.
 *
 * <p>Multiple topics on a single {@code @KafkaListener} is the right shape here because the handler
 * is polymorphic over the envelope and there is no per-topic behaviour to separate. The dedupe
 * marker is what keeps a duplicate confirmation email from ever being rendered twice.
 */
@Component
public class NotificationEventListener {

    private static final Logger log = LoggerFactory.getLogger(NotificationEventListener.class);

    public static final String CONSUMER_GROUP = "notification-service";

    private final NotificationService notificationService;
    private final IdempotencyGuard idempotencyGuard;
    private final SagaMetrics metrics;

    public NotificationEventListener(NotificationService notificationService,
                                     IdempotencyGuard idempotencyGuard,
                                     SagaMetrics metrics) {
        this.notificationService = notificationService;
        this.idempotencyGuard = idempotencyGuard;
        this.metrics = metrics;
    }

    @KafkaListener(
            topics = {
                    Topics.ORDERS_CONFIRMED,
                    Topics.ORDERS_CANCELLED,
                    Topics.INVENTORY_REJECTED,
                    Topics.PAYMENTS_FAILED
            },
            groupId = CONSUMER_GROUP,
            concurrency = "${app.kafka.listener.concurrency:2}")
    public void onTerminalEvent(EventEnvelope<DomainEvent> envelope) {
        CorrelationContext.put(envelope.correlationId(), envelope.eventId(), envelope.payload().orderId());
        try {
            metrics.eventConsumed(CONSUMER_GROUP, envelope.eventType().name());
            idempotencyGuard.runOnce(CONSUMER_GROUP, envelope,
                    () -> notificationService.handle(envelope));
        } catch (RuntimeException e) {
            metrics.stepFailed(CONSUMER_GROUP, envelope.eventType().name(), e.getClass().getSimpleName());
            log.error("Notification failed eventType={} eventId={} orderId={}",
                    envelope.eventType(), envelope.eventId(), envelope.payload().orderId(), e);
            throw e;
        } finally {
            CorrelationContext.clear();
        }
    }
}
