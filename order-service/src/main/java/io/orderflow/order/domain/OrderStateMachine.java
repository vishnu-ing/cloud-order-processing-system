package io.orderflow.order.domain;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.springframework.stereotype.Component;

/**
 * The single authority on order lifecycle transitions.
 *
 * <p>In a choreographed saga, events can arrive late, twice, or in an order the designer did not
 * anticipate (a retried {@code PaymentCaptured} after the customer already cancelled, for
 * instance). Encoding the legal graph explicitly — instead of scattering {@code if (status == ...)}
 * checks through handlers — means every such case is either an allowed transition or a loud,
 * dead-letterable error.
 *
 * <p>The graph:
 * <pre>
 * PENDING            -> INVENTORY_RESERVED | REJECTED | CANCELLED | FAILED
 * INVENTORY_RESERVED -> PAYMENT_CAPTURED   | REJECTED | CANCELLED | FAILED
 * PAYMENT_CAPTURED   -> CONFIRMED          | FAILED
 * CONFIRMED / REJECTED / CANCELLED / FAILED -> (terminal)
 * </pre>
 */
@Component
public class OrderStateMachine {

    private static final Map<OrderStatus, Set<OrderStatus>> ALLOWED = allowedTransitions();

    private static Map<OrderStatus, Set<OrderStatus>> allowedTransitions() {
        Map<OrderStatus, Set<OrderStatus>> transitions = new EnumMap<>(OrderStatus.class);
        transitions.put(OrderStatus.PENDING, EnumSet.of(
                OrderStatus.INVENTORY_RESERVED,
                OrderStatus.REJECTED,
                OrderStatus.CANCELLED,
                OrderStatus.FAILED));
        transitions.put(OrderStatus.INVENTORY_RESERVED, EnumSet.of(
                OrderStatus.PAYMENT_CAPTURED,
                OrderStatus.REJECTED,
                OrderStatus.CANCELLED,
                OrderStatus.FAILED));
        transitions.put(OrderStatus.PAYMENT_CAPTURED, EnumSet.of(
                OrderStatus.CONFIRMED,
                OrderStatus.FAILED));
        transitions.put(OrderStatus.CONFIRMED, EnumSet.noneOf(OrderStatus.class));
        transitions.put(OrderStatus.REJECTED, EnumSet.noneOf(OrderStatus.class));
        transitions.put(OrderStatus.CANCELLED, EnumSet.noneOf(OrderStatus.class));
        transitions.put(OrderStatus.FAILED, EnumSet.noneOf(OrderStatus.class));
        return Map.copyOf(transitions);
    }

    public Set<OrderStatus> allowedFrom(OrderStatus from) {
        return ALLOWED.getOrDefault(from, EnumSet.noneOf(OrderStatus.class));
    }

    public boolean canTransition(OrderStatus from, OrderStatus to) {
        return from != null && to != null && allowedFrom(from).contains(to);
    }

    /**
     * @return {@code to}, if the transition is legal
     * @throws IllegalStateTransitionException otherwise
     */
    public OrderStatus transition(String orderId, OrderStatus from, OrderStatus to) {
        if (!canTransition(from, to)) {
            throw new IllegalStateTransitionException(orderId, from, to);
        }
        return to;
    }

    /**
     * Idempotent variant used by event handlers.
     *
     * <p>A duplicate delivery that has already been applied ({@code from == to}) is a no-op rather
     * than an error, which keeps a redelivered event from dead-lettering a perfectly healthy order.
     */
    public boolean transitionIfNeeded(String orderId, OrderStatus from, OrderStatus to) {
        if (from == to) {
            return false;
        }
        transition(orderId, from, to);
        return true;
    }
}
