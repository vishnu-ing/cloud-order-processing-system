package io.orderflow.order.domain;

import io.orderflow.common.error.NonRetryableException;

/**
 * Thrown when an event asks the aggregate to make a transition the state machine forbids.
 *
 * <p>Extends {@link NonRetryableException} on purpose: replaying the same event against the same
 * state will always be illegal, so the Kafka error handler must dead-letter it immediately rather
 * than burn the retry budget. In a choreographed saga this is also the signal that two events
 * arrived out of the order the design assumed — exactly the thing you want in a DLQ with headers,
 * not swallowed.
 */
public class IllegalStateTransitionException extends NonRetryableException {

    private final OrderStatus from;
    private final OrderStatus to;

    public IllegalStateTransitionException(String orderId, OrderStatus from, OrderStatus to) {
        super("Order " + orderId + " cannot transition from " + from + " to " + to);
        this.from = from;
        this.to = to;
    }

    public OrderStatus from() {
        return from;
    }

    public OrderStatus to() {
        return to;
    }
}
