package io.orderflow.common.event;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Transport envelope wrapping every {@link DomainEvent} published to Kafka.
 *
 * <p>The envelope carries everything a consumer needs that is <em>not</em> business data:
 *
 * <ul>
 *   <li>{@code eventId} — the deduplication key. Consumers record it in {@code processed_events}
 *       so at-least-once delivery becomes effectively-once processing.</li>
 *   <li>{@code schemaVersion} — lets a consumer reject or up-convert payloads it does not
 *       understand instead of guessing.</li>
 *   <li>{@code correlationId} — stable for the whole saga instance, mirrored into the MDC and the
 *       {@code X-Correlation-Id} HTTP header, so one grep spans six services.</li>
 *   <li>{@code causationId} — the {@code eventId} of the event that caused this one, which is what
 *       makes {@code GET /api/v1/orders/{id}/timeline} a real causal chain rather than a log dump.</li>
 * </ul>
 *
 * @param <T> the concrete event payload type
 */
public record EventEnvelope<T extends DomainEvent>(
        String eventId,
        EventType eventType,
        int schemaVersion,
        String correlationId,
        String causationId,
        Instant occurredAt,
        String source,
        T payload) {

    /** Current contract version. Bump on a breaking payload change and support both for a window. */
    public static final int CURRENT_SCHEMA_VERSION = 1;

    public EventEnvelope {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(eventType, "eventType");
        Objects.requireNonNull(occurredAt, "occurredAt");
        Objects.requireNonNull(payload, "payload");
    }

    /** Starts a new causal chain (an event with no upstream event, e.g. from a REST request). */
    public static <T extends DomainEvent> EventEnvelope<T> start(T payload, String source, String correlationId) {
        return new EventEnvelope<>(
                UUID.randomUUID().toString(),
                payload.eventType(),
                CURRENT_SCHEMA_VERSION,
                correlationId == null ? UUID.randomUUID().toString() : correlationId,
                null,
                Instant.now(),
                source,
                payload);
    }

    /** Continues an existing saga: inherits the correlation id, records the cause. */
    public static <T extends DomainEvent> EventEnvelope<T> follow(
            T payload, String source, EventEnvelope<? extends DomainEvent> cause) {
        return new EventEnvelope<>(
                UUID.randomUUID().toString(),
                payload.eventType(),
                CURRENT_SCHEMA_VERSION,
                cause.correlationId(),
                cause.eventId(),
                Instant.now(),
                source,
                payload);
    }

    /**
     * Narrows a wire envelope (deserialized as {@code EventEnvelope<DomainEvent>}) to the concrete
     * payload type a handler expects.
     *
     * <p>Keeps the single unchecked cast in the contract module, behind a runtime check, instead of
     * scattering {@code @SuppressWarnings} through every listener. A mismatch means a topic is
     * carrying an event it was never meant to, which is permanent — hence non-retryable.
     */
    @SuppressWarnings("unchecked")
    public <R extends DomainEvent> EventEnvelope<R> as(Class<R> payloadType) {
        if (!payloadType.isInstance(payload)) {
            throw new io.orderflow.common.error.NonRetryableException(
                    "Expected payload " + payloadType.getSimpleName() + " but received "
                            + payload.getClass().getSimpleName() + " for eventId " + eventId);
        }
        return (EventEnvelope<R>) this;
    }

    /** Kafka partition key: the aggregate id, so one order's events stay strictly ordered. */
    public String partitionKey() {
        return payload.orderId();
    }

    /** Derived from {@code schemaVersion}; ignored by Jackson so it never becomes a wire field. */
    @JsonIgnore
    public boolean isSchemaSupported() {
        return schemaVersion <= CURRENT_SCHEMA_VERSION;
    }
}
