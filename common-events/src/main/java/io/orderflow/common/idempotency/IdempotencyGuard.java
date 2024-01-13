package io.orderflow.common.idempotency;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.orderflow.common.event.DomainEvent;
import io.orderflow.common.event.EventEnvelope;
import io.orderflow.common.observability.SagaMetrics;

/**
 * Turns Kafka's at-least-once delivery into effectively-once <em>processing</em>.
 *
 * <p>Usage in every listener:
 *
 * <pre>{@code
 * guard.runOnce(CONSUMER_GROUP, envelope, () -> inventoryService.reserve(envelope));
 * }</pre>
 *
 * <p>Ordering matters. The marker is claimed <em>before</em> the handler runs, so two concurrent
 * deliveries cannot both enter the handler. If the handler then fails, the marker is released so
 * the Kafka retry can legitimately try again. That leaves exactly one uncovered window — a crash
 * between a successful handler and the release path — which is why handlers are also written to be
 * naturally idempotent (conditional writes, state-machine guards) rather than relying on the
 * marker alone. Defence in depth, not a single point of truth.
 */
public class IdempotencyGuard {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyGuard.class);

    private final ProcessedEventRepository repository;
    private final SagaMetrics metrics;
    private final Duration retention;

    public IdempotencyGuard(ProcessedEventRepository repository, SagaMetrics metrics, Duration retention) {
        this.repository = repository;
        this.metrics = metrics;
        this.retention = retention;
    }

    /**
     * @return {@code true} if the handler ran, {@code false} if the event was a duplicate
     */
    public boolean runOnce(String consumerGroup,
                           EventEnvelope<? extends DomainEvent> envelope,
                           Runnable handler) {
        ProcessedEvent marker = ProcessedEvent.of(
                consumerGroup,
                envelope.eventId(),
                envelope.eventType().name(),
                envelope.payload().orderId(),
                retention);

        if (!repository.claim(marker)) {
            metrics.duplicateSuppressed(consumerGroup, envelope.eventType().name());
            log.info("Duplicate delivery suppressed group={} eventId={} eventType={} orderId={}",
                    consumerGroup, envelope.eventId(), envelope.eventType(), envelope.payload().orderId());
            return false;
        }

        try {
            handler.run();
            return true;
        } catch (RuntimeException e) {
            repository.release(consumerGroup, envelope.eventId());
            log.warn("Released idempotency marker after failure group={} eventId={}",
                    consumerGroup, envelope.eventId());
            throw e;
        }
    }

    public boolean wasProcessed(String consumerGroup, String eventId) {
        return repository.wasProcessed(consumerGroup, eventId);
    }
}
