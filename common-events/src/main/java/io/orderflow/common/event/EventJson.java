package io.orderflow.common.event;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.orderflow.common.error.NonRetryableException;

/**
 * Single JSON contract for everything that crosses a process boundary as an event.
 *
 * <p>Deliberate settings:
 * <ul>
 *   <li>unknown properties are ignored, so a producer can add a field without a lock-step deploy
 *       of every consumer (forward compatibility);</li>
 *   <li>timestamps are ISO-8601 strings, not epoch numbers, so topics stay human-readable in
 *       kafka-ui and in DLQ dumps;</li>
 *   <li>nulls are omitted to keep payloads small on high-volume topics.</li>
 * </ul>
 */
public final class EventJson {

    private static final TypeReference<EventEnvelope<DomainEvent>> ENVELOPE_TYPE =
            new TypeReference<>() {
            };

    private static final ObjectMapper MAPPER = createMapper();

    private EventJson() {
    }

    public static ObjectMapper createMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES);
    }

    public static ObjectMapper mapper() {
        return MAPPER;
    }

    public static String toJson(EventEnvelope<? extends DomainEvent> envelope) {
        try {
            return MAPPER.writeValueAsString(envelope);
        } catch (Exception e) {
            // A payload we cannot serialise will never serialise: never retry this.
            throw new NonRetryableException("Unable to serialise event " + envelope.eventId(), e);
        }
    }

    public static EventEnvelope<DomainEvent> fromJson(String json) {
        try {
            return MAPPER.readValue(json, ENVELOPE_TYPE);
        } catch (Exception e) {
            throw new NonRetryableException("Malformed event envelope: " + e.getMessage(), e);
        }
    }

    public static EventEnvelope<DomainEvent> fromBytes(byte[] json) {
        try {
            return MAPPER.readValue(json, ENVELOPE_TYPE);
        } catch (Exception e) {
            throw new NonRetryableException("Malformed event envelope: " + e.getMessage(), e);
        }
    }
}
