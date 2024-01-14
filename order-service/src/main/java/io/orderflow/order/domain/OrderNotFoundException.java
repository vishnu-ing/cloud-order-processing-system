package io.orderflow.order.domain;

/** Raised by the read paths when an order id does not exist; mapped to a 404 ProblemDetail. */
public class OrderNotFoundException extends RuntimeException {

    private final String orderId;

    public OrderNotFoundException(String orderId) {
        super("Order " + orderId + " was not found");
        this.orderId = orderId;
    }

    public String orderId() {
        return orderId;
    }
}
