package io.orderflow.common.event;

import java.math.BigDecimal;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * The saga payload contract: a sealed hierarchy of immutable records, one per {@link EventType}.
 *
 * <p>Sealing it means every consumer can {@code switch} over the events exhaustively, so adding a
 * new saga step fails the build in each service that has to react to it rather than silently
 * dropping messages at runtime.
 *
 * <p>Polymorphism on the wire is driven by the already-present {@code eventType} property
 * ({@link JsonTypeInfo.As#EXISTING_PROPERTY}), so the JSON stays free of Java-specific
 * {@code @class} hints and remains readable by non-JVM consumers.
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "eventType",
        visible = false)
@JsonSubTypes({
        @JsonSubTypes.Type(value = DomainEvent.OrderCreated.class, name = "ORDER_CREATED"),
        @JsonSubTypes.Type(value = DomainEvent.InventoryReserved.class, name = "INVENTORY_RESERVED"),
        @JsonSubTypes.Type(value = DomainEvent.InventoryRejected.class, name = "INVENTORY_REJECTED"),
        @JsonSubTypes.Type(value = DomainEvent.PaymentCaptured.class, name = "PAYMENT_CAPTURED"),
        @JsonSubTypes.Type(value = DomainEvent.PaymentFailed.class, name = "PAYMENT_FAILED"),
        @JsonSubTypes.Type(value = DomainEvent.OrderConfirmed.class, name = "ORDER_CONFIRMED"),
        @JsonSubTypes.Type(value = DomainEvent.OrderCancelled.class, name = "ORDER_CANCELLED")
})
public sealed interface DomainEvent {

    /** Aggregate id. Doubles as the Kafka partition key for every topic in the saga. */
    String orderId();

    /**
     * The type discriminator. It is not a record component, so Jackson would not write it on its
     * own; {@code @JsonProperty} puts it on the wire, where {@code EXISTING_PROPERTY} expects it.
     */
    @JsonProperty("eventType")
    EventType eventType();

    /** A single requested line of an order. */
    record OrderLine(String sku, int quantity, BigDecimal unitPrice) {

        public BigDecimal lineTotal() {
            return unitPrice.multiply(BigDecimal.valueOf(quantity));
        }
    }

    /** Emitted by order-service once an order is durably persisted in {@code PENDING}. */
    record OrderCreated(
            String orderId,
            String customerId,
            String customerEmail,
            List<OrderLine> lines,
            BigDecimal totalAmount,
            String currency) implements DomainEvent {

        @Override
        public EventType eventType() {
            return EventType.ORDER_CREATED;
        }
    }

    /** Stock is held for the order; {@code reservationId} is the compensation handle. */
    record InventoryReserved(
            String orderId,
            String reservationId,
            String customerId,
            List<OrderLine> lines,
            BigDecimal totalAmount,
            String currency) implements DomainEvent {

        @Override
        public EventType eventType() {
            return EventType.INVENTORY_RESERVED;
        }
    }

    /** Stock could not be held. Nothing to compensate: no reservation was written. */
    record InventoryRejected(
            String orderId,
            String reason,
            List<String> unavailableSkus) implements DomainEvent {

        @Override
        public EventType eventType() {
            return EventType.INVENTORY_REJECTED;
        }
    }

    record PaymentCaptured(
            String orderId,
            String paymentId,
            String reservationId,
            BigDecimal amount,
            String currency,
            String authorizationCode) implements DomainEvent {

        @Override
        public EventType eventType() {
            return EventType.PAYMENT_CAPTURED;
        }
    }

    /**
     * Payment could not be captured. {@code retryable} distinguishes a gateway/infrastructure
     * fault (worth another attempt) from a business decline (terminal for this order).
     */
    record PaymentFailed(
            String orderId,
            String paymentId,
            String reservationId,
            String failureCode,
            String failureReason,
            boolean retryable) implements DomainEvent {

        @Override
        public EventType eventType() {
            return EventType.PAYMENT_FAILED;
        }
    }

    record OrderConfirmed(
            String orderId,
            String customerId,
            String customerEmail,
            String paymentId,
            BigDecimal totalAmount,
            String currency) implements DomainEvent {

        @Override
        public EventType eventType() {
            return EventType.ORDER_CONFIRMED;
        }
    }

    /**
     * Compensation trigger. Published by order-service both for customer-initiated cancellation
     * and for saga rollback after a payment failure; inventory-service releases the reservation.
     */
    record OrderCancelled(
            String orderId,
            String customerId,
            String customerEmail,
            String reservationId,
            String reason) implements DomainEvent {

        @Override
        public EventType eventType() {
            return EventType.ORDER_CANCELLED;
        }
    }
}
