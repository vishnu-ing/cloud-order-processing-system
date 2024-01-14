package io.orderflow.order.domain;

/**
 * Lifecycle of an order aggregate.
 *
 * <p>The distinction between the three unhappy endings is deliberate and customer-visible:
 * <ul>
 *   <li>{@code REJECTED} — a business decision went against the order (no stock, card declined);</li>
 *   <li>{@code CANCELLED} — the customer (or the compensation path) withdrew it;</li>
 *   <li>{@code FAILED} — the platform could not decide; needs operator attention, see RUNBOOK.</li>
 * </ul>
 */
public enum OrderStatus {

    /** Accepted and durably stored; {@code OrderCreated} is in the outbox. */
    PENDING,

    /** inventory-service is holding stock for this order. */
    INVENTORY_RESERVED,

    /** Funds captured; awaiting final confirmation write. */
    PAYMENT_CAPTURED,

    /** Happy-path terminal state. */
    CONFIRMED,

    /** Terminal: rejected by inventory or payment. */
    REJECTED,

    /** Terminal: cancelled by the customer or by saga compensation. */
    CANCELLED,

    /** Terminal: unrecoverable platform error; the saga needs manual reconciliation. */
    FAILED;

    public boolean isTerminal() {
        return this == CONFIRMED || this == REJECTED || this == CANCELLED || this == FAILED;
    }

    /** True while inventory is still held and therefore must be released on cancellation. */
    public boolean holdsInventory() {
        return this == INVENTORY_RESERVED || this == PAYMENT_CAPTURED;
    }
}
