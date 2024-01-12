package io.orderflow.common.event;

/**
 * The saga vocabulary. Each event type is bound to exactly one topic and one producing service,
 * which keeps the choreography auditable: given an event type you know who wrote it and where it
 * lives.
 */
public enum EventType {

    ORDER_CREATED(Topics.ORDERS_CREATED, "order-service"),
    INVENTORY_RESERVED(Topics.INVENTORY_RESERVED, "inventory-service"),
    INVENTORY_REJECTED(Topics.INVENTORY_REJECTED, "inventory-service"),
    PAYMENT_CAPTURED(Topics.PAYMENTS_CAPTURED, "payment-service"),
    PAYMENT_FAILED(Topics.PAYMENTS_FAILED, "payment-service"),
    ORDER_CONFIRMED(Topics.ORDERS_CONFIRMED, "order-service"),
    ORDER_CANCELLED(Topics.ORDERS_CANCELLED, "order-service");

    private final String topic;
    private final String producer;

    EventType(String topic, String producer) {
        this.topic = topic;
        this.producer = producer;
    }

    public String topic() {
        return topic;
    }

    public String producer() {
        return producer;
    }

    /** True for the events that end a saga instance and therefore trigger a notification. */
    public boolean isTerminal() {
        return this == ORDER_CONFIRMED || this == ORDER_CANCELLED || this == INVENTORY_REJECTED
                || this == PAYMENT_FAILED;
    }
}
