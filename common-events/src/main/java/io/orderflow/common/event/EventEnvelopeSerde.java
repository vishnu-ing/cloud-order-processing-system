package io.orderflow.common.event;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import io.orderflow.common.error.NonRetryableException;

/**
 * Kafka serde pair for {@link EventEnvelope}.
 *
 * <p>The deserializer is intentionally strict: anything it cannot parse raises a
 * {@link NonRetryableException}. Wrapped in Spring's {@code ErrorHandlingDeserializer}, that turns
 * a poison pill into an immediate dead-letter publish instead of an infinite redelivery loop — the
 * failure mode that takes consumer groups down in production.
 */
public final class EventEnvelopeSerde {

    private EventEnvelopeSerde() {
    }

    public static class EnvelopeSerializer implements Serializer<EventEnvelope<? extends DomainEvent>> {

        @Override
        public byte[] serialize(String topic, EventEnvelope<? extends DomainEvent> data) {
            if (data == null) {
                return null;
            }
            return EventJson.toJson(data).getBytes(StandardCharsets.UTF_8);
        }
    }

    public static class EnvelopeDeserializer implements Deserializer<EventEnvelope<DomainEvent>> {

        @Override
        public EventEnvelope<DomainEvent> deserialize(String topic, byte[] data) {
            if (data == null || data.length == 0) {
                return null;
            }
            EventEnvelope<DomainEvent> envelope = EventJson.fromBytes(data);
            if (!envelope.isSchemaSupported()) {
                throw new NonRetryableException("Unsupported schemaVersion " + envelope.schemaVersion()
                        + " for " + envelope.eventType() + " on topic " + topic);
            }
            return envelope;
        }
    }
}
