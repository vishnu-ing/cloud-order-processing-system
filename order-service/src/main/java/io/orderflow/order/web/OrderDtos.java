package io.orderflow.order.web;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import io.orderflow.common.event.DomainEvent;
import io.orderflow.order.model.OrderEntity;
import io.orderflow.order.model.OrderItemEntity;
import io.orderflow.order.model.OrderTimelineEntry;

/**
 * Request and response contracts for the public API.
 *
 * <p>Records, not entities: the wire format is versioned by the OpenAPI document in
 * {@code docs/api/openapi.yaml} and must be free to diverge from the DynamoDB layout. Validation
 * is declarative so a bad request never reaches the aggregate — the 25-line cap, for example, is
 * what keeps an order write inside DynamoDB's 100-item transaction limit.
 */
public final class OrderDtos {

    private OrderDtos() {
    }

    public record OrderLineRequest(
            @NotBlank @Size(max = 64) String sku,
            @Min(1) @Max(1_000) int quantity,
            @NotNull @DecimalMin(value = "0.01") BigDecimal unitPrice) {

        public DomainEvent.OrderLine toOrderLine() {
            return new DomainEvent.OrderLine(sku, quantity, unitPrice);
        }
    }

    public record CreateOrderRequest(
            @NotBlank @Size(max = 64) String customerId,
            @NotBlank @Email @Size(max = 255) String customerEmail,
            @NotEmpty @Size(max = 25) @Valid List<OrderLineRequest> lines,
            @NotBlank @Pattern(regexp = "^[A-Z]{3}$", message = "must be a 3-letter ISO-4217 code")
            String currency) {

        public BigDecimal totalAmount() {
            return lines.stream()
                    .map(line -> line.unitPrice().multiply(BigDecimal.valueOf(line.quantity())))
                    .reduce(BigDecimal.ZERO, BigDecimal::add);
        }
    }

    public record CancelOrderRequest(@Size(max = 280) String reason) {

        public String reasonOrDefault() {
            return (reason == null || reason.isBlank()) ? "Cancelled by customer" : reason;
        }
    }

    public record OrderLineResponse(String sku, int quantity, BigDecimal unitPrice, BigDecimal lineTotal) {

        static OrderLineResponse from(OrderItemEntity entity) {
            return new OrderLineResponse(entity.getSku(), entity.getQuantity(),
                    entity.getUnitPrice(), entity.getLineTotal());
        }
    }

    public record OrderResponse(
            String orderId,
            String customerId,
            String customerEmail,
            String status,
            BigDecimal totalAmount,
            String currency,
            String reservationId,
            String paymentId,
            String failureCode,
            String failureReason,
            String correlationId,
            Instant createdAt,
            Instant updatedAt,
            List<OrderLineResponse> lines) {

        public static OrderResponse from(OrderEntity order, List<OrderItemEntity> items) {
            return new OrderResponse(
                    order.getOrderId(),
                    order.getCustomerId(),
                    order.getCustomerEmail(),
                    order.getStatus(),
                    order.getTotalAmount(),
                    order.getCurrency(),
                    order.getReservationId(),
                    order.getPaymentId(),
                    order.getFailureCode(),
                    order.getFailureReason(),
                    order.getCorrelationId(),
                    order.getCreatedAt(),
                    order.getUpdatedAt(),
                    items.stream().map(OrderLineResponse::from).toList());
        }
    }

    public record OrderSummaryResponse(
            String orderId,
            String customerId,
            String status,
            BigDecimal totalAmount,
            String currency,
            Instant createdAt) {

        public static OrderSummaryResponse from(OrderEntity order) {
            return new OrderSummaryResponse(
                    order.getOrderId(),
                    order.getCustomerId(),
                    order.getStatus(),
                    order.getTotalAmount(),
                    order.getCurrency(),
                    order.getCreatedAt());
        }
    }

    /** Cursor-paginated list. {@code nextCursor == null} means "no more pages". */
    public record OrderPageResponse(List<OrderSummaryResponse> items, int limit, String nextCursor) {
    }

    public record TimelineEventResponse(
            String status,
            String eventType,
            String eventId,
            String causationId,
            String correlationId,
            String detail,
            Instant occurredAt) {

        static TimelineEventResponse from(OrderTimelineEntry entry) {
            return new TimelineEventResponse(
                    entry.getStatus(),
                    entry.getEventType(),
                    entry.getEventId(),
                    entry.getCausationId(),
                    entry.getCorrelationId(),
                    entry.getDetail(),
                    entry.getOccurredAt());
        }
    }

    public record OrderTimelineResponse(String orderId, String status, List<TimelineEventResponse> events) {

        public static OrderTimelineResponse from(OrderEntity order) {
            return new OrderTimelineResponse(
                    order.getOrderId(),
                    order.getStatus(),
                    order.getTimeline().stream()
                            .sorted((left, right) -> left.getOccurredAt().compareTo(right.getOccurredAt()))
                            .map(TimelineEventResponse::from)
                            .toList());
        }
    }
}
