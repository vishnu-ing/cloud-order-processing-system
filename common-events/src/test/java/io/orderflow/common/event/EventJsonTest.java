package io.orderflow.common.event;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.orderflow.common.error.NonRetryableException;
import io.orderflow.common.outbox.OutboxRecord;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The wire contract every service shares. Polymorphic payloads are resolved from the
 * {@code eventType} property inside {@code payload}, so these tests pin down that the property is
 * actually written, that every event type survives a round trip through the Kafka serde, and that
 * derived accessors on the envelope stay off the wire.
 */
class EventJsonTest {

    private static final List<DomainEvent.OrderLine> LINES = List.of(
            new DomainEvent.OrderLine("SKU-BOOK-1", 2, new BigDecimal("42.50")),
            new DomainEvent.OrderLine("SKU-HEADSET-1", 1, new BigDecimal("189.00")));

    static Stream<DomainEvent> everyEventType() {
        return Stream.of(
                new DomainEvent.OrderCreated("order-1", "cust-1", "cust@example.com", LINES,
                        new BigDecimal("274.00"), "USD"),
                new DomainEvent.InventoryReserved("order-1", "res-1", "cust-1", LINES,
                        new BigDecimal("274.00"), "USD"),
                new DomainEvent.InventoryRejected("order-1", "Insufficient stock",
                        List.of("SKU-LIMITED-1")),
                new DomainEvent.PaymentCaptured("order-1", "pay-1", "res-1",
                        new BigDecimal("274.00"), "USD", "AUTH-1"),
                new DomainEvent.PaymentFailed("order-1", "pay-1", "res-1", "CARD_DECLINED",
                        "Issuer declined", false),
                new DomainEvent.OrderConfirmed("order-1", "cust-1", "cust@example.com", "pay-1",
                        new BigDecimal("274.00"), "USD"),
                new DomainEvent.OrderCancelled("order-1", "cust-1", "cust@example.com", "res-1",
                        "Payment failed: CARD_DECLINED"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("everyEventType")
    @DisplayName("every event type round-trips through the Kafka serde")
    void roundTripsThroughTheKafkaSerde(DomainEvent payload) {
        EventEnvelope<DomainEvent> sent = EventEnvelope.start(payload, "order-service", "corr-1");

        byte[] bytes = new EventEnvelopeSerde.EnvelopeSerializer().serialize("any-topic", sent);
        EventEnvelope<DomainEvent> received =
                new EventEnvelopeSerde.EnvelopeDeserializer().deserialize("any-topic", bytes);

        assertThat(received).isEqualTo(sent);
        assertThat(received.payload()).isInstanceOf(payload.getClass());
    }

    @Test
    @DisplayName("the payload carries its eventType discriminator; derived accessors stay off the wire")
    void payloadCarriesTheDiscriminator() throws Exception {
        EventEnvelope<DomainEvent> envelope = EventEnvelope.start(
                new DomainEvent.InventoryRejected("order-1", "Insufficient stock", List.of("SKU-1")),
                "inventory-service", "corr-1");

        JsonNode json = EventJson.mapper().readTree(EventJson.toJson(envelope));

        assertThat(json.path("payload").path("eventType").asText()).isEqualTo("INVENTORY_REJECTED");
        assertThat(json.path("eventType").asText()).isEqualTo("INVENTORY_REJECTED");
        assertThat(json.has("schemaSupported")).isFalse();
        assertThat(json.has("partitionKey")).isFalse();
    }

    @Test
    @DisplayName("a payload without a discriminator is a poison pill, not a retry")
    void missingDiscriminatorIsNonRetryable() {
        String json = """
                {"eventId":"e-1","eventType":"ORDER_CREATED","schemaVersion":1,
                 "correlationId":"c-1","occurredAt":"2026-02-01T10:00:00Z","source":"order-service",
                 "payload":{"orderId":"order-1","customerId":"cust-1","currency":"USD"}}""";

        assertThatThrownBy(() -> new EventEnvelopeSerde.EnvelopeDeserializer()
                .deserialize("orders.created", json.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(NonRetryableException.class);
    }

    @Test
    @DisplayName("a newer schemaVersion is rejected instead of guessed at")
    void newerSchemaVersionIsRejected() {
        EventEnvelope<DomainEvent> current = EventEnvelope.start(
                new DomainEvent.InventoryRejected("order-1", "Insufficient stock", List.of("SKU-1")),
                "inventory-service", "corr-1");
        String future = EventJson.toJson(current).replace("\"schemaVersion\":1", "\"schemaVersion\":2");

        assertThatThrownBy(() -> new EventEnvelopeSerde.EnvelopeDeserializer()
                .deserialize("inventory.rejected", future.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(NonRetryableException.class)
                .hasMessageContaining("schemaVersion 2");
    }

    @Test
    @DisplayName("an outbox row records the producing service, so each service drains only its own rows")
    void outboxRecordCarriesItsSource() {
        EventEnvelope<DomainEvent> envelope = EventEnvelope.start(
                new DomainEvent.OrderCreated("order-1", "cust-1", "cust@example.com", LINES,
                        new BigDecimal("274.00"), "USD"),
                "order-service", "corr-1");

        OutboxRecord record = OutboxRecord.pending(envelope);

        assertThat(record.getSource()).isEqualTo("order-service");
        assertThat(record.getTopic()).isEqualTo(Topics.ORDERS_CREATED);
        assertThat(record.getPartitionKey()).isEqualTo("order-1");
        assertThat(record.getStatus()).isEqualTo(OutboxRecord.STATUS_PENDING);
    }
}
