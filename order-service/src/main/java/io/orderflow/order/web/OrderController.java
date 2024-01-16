package io.orderflow.order.web;

import java.net.URI;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.orderflow.common.observability.CorrelationContext;
import io.orderflow.order.service.OrderService;
import io.orderflow.order.service.OrderService.CreatedOrder;
import io.orderflow.order.web.OrderDtos.CancelOrderRequest;
import io.orderflow.order.web.OrderDtos.CreateOrderRequest;
import io.orderflow.order.web.OrderDtos.OrderPageResponse;
import io.orderflow.order.web.OrderDtos.OrderResponse;
import io.orderflow.order.web.OrderDtos.OrderTimelineResponse;

/**
 * Public HTTP surface, fronted by Spring Cloud Gateway.
 *
 * <p>Two design notes worth calling out:
 *
 * <ul>
 *   <li>{@code POST /orders} is <strong>idempotent by contract</strong>. Supply
 *       {@code Idempotency-Key} and a retry after a timeout returns {@code 200} with the original
 *       order instead of creating a second one. Without that, an HTTP retry in a payment flow
 *       double-charges the customer.</li>
 *   <li>Creation returns {@code 202}-style semantics with a {@code 201}: the order exists, but it is
 *       {@code PENDING} and the saga is still running. Clients poll {@code /timeline} (or subscribe
 *       to their own webhook) rather than expecting a synchronous confirmation.</li>
 * </ul>
 */
@RestController
@Validated
@RequestMapping(path = "/api/v1/orders", produces = MediaType.APPLICATION_JSON_VALUE)
@Tag(name = "Orders", description = "Create, inspect and cancel orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Create an order and start the saga",
            description = "Returns 201 for a new order, 200 when replaying an Idempotency-Key.")
    public ResponseEntity<OrderResponse> create(
            @Valid @RequestBody CreateOrderRequest request,
            @RequestHeader(name = CorrelationContext.IDEMPOTENCY_KEY_HEADER, required = false)
            String idempotencyKey,
            @RequestHeader(name = CorrelationContext.CORRELATION_ID_HEADER, required = false)
            String correlationId) {

        CreatedOrder result = orderService.createOrder(request, idempotencyKey, correlationId);
        OrderResponse body = result.order();

        // X-Correlation-Id is already on the response (CorrelationIdFilter) and identifies this
        // request; the saga's correlation id travels in the body, which matters on a replay.
        URI location = URI.create("/api/v1/orders/" + body.orderId());
        return result.created()
                ? ResponseEntity.created(location).body(body)
                : ResponseEntity.ok().location(location).body(body);
    }

    @GetMapping("/{orderId}")
    @Operation(summary = "Fetch a single order with its lines")
    public OrderResponse get(@PathVariable String orderId) {
        return orderService.getOrder(orderId);
    }

    @GetMapping
    @Operation(summary = "List orders by customer or status, cursor-paginated")
    public OrderPageResponse list(
            @RequestParam(required = false) String customerId,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String cursor,
            @RequestParam(defaultValue = "20") @Min(1) @Max(100) int limit) {
        return orderService.listOrders(customerId, status, cursor, limit);
    }

    @PostMapping(path = "/{orderId}/cancel", consumes = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Cancel an order and trigger inventory compensation")
    public OrderResponse cancel(
            @PathVariable String orderId,
            @Valid @RequestBody(required = false) CancelOrderRequest request,
            @RequestHeader(name = CorrelationContext.CORRELATION_ID_HEADER, required = false)
            String correlationId) {
        String reason = request == null ? "Cancelled by customer" : request.reasonOrDefault();
        return orderService.cancelOrder(orderId, reason, correlationId);
    }

    @GetMapping("/{orderId}/timeline")
    @Operation(summary = "Saga event history for an order, oldest first")
    public OrderTimelineResponse timeline(@PathVariable String orderId) {
        return orderService.getTimeline(orderId);
    }
}
