package io.orderflow.lambda;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;

import io.orderflow.common.observability.CorrelationContext;
import io.orderflow.lambda.model.OrderReadModel;
import io.orderflow.lambda.repository.OrderProjectionRepository;
import io.orderflow.lambda.support.ApiGatewayResponseBuilder;

/**
 * Serverless read path: {@code GET /orders/{orderId}/status} and
 * {@code GET /orders?customerId=...} behind an HTTP API v2 with a JWT authorizer.
 *
 * <p>This is the traffic-shape argument for going serverless on one endpoint. Status polling is
 * bursty, read-only and needs no saga context, so pinning it to the always-on ECS fleet would mean
 * sizing that fleet for polling rather than for order intake. On Lambda it scales to zero between
 * bursts and cannot contend with the write path for capacity.
 */
public class OrderStatusApiHandler implements RequestHandler<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse> {

    private static final int DEFAULT_LIMIT = 20;
    private static final int MAX_LIMIT = 100;
    private static final int CACHE_SECONDS = 5;

    private final OrderProjectionRepository projections;

    public OrderStatusApiHandler() {
        this(new OrderProjectionRepository());
    }

    /** Test seam. */
    public OrderStatusApiHandler(OrderProjectionRepository projections) {
        this.projections = projections;
    }

    @Override
    public APIGatewayV2HTTPResponse handleRequest(APIGatewayV2HTTPEvent request, Context context) {
        String correlationId = CorrelationContext.orNew(header(request, CorrelationContext.CORRELATION_ID_HEADER));

        try {
            String orderId = pathParameter(request, "orderId");
            if (orderId != null && !orderId.isBlank()) {
                return projections.findById(orderId)
                        .map(projection -> ApiGatewayResponseBuilder
                                .okCacheable(projection, correlationId, CACHE_SECONDS))
                        .orElseGet(() -> ApiGatewayResponseBuilder
                                .notFound("No order with id " + orderId, correlationId));
            }

            String customerId = queryParameter(request, "customerId");
            if (customerId == null || customerId.isBlank()) {
                return ApiGatewayResponseBuilder.badRequest(
                        "Provide either a path orderId or a customerId query parameter",
                        correlationId);
            }

            int limit = parseLimit(queryParameter(request, "limit"));
            List<OrderReadModel> orders = projections.findByCustomer(customerId, limit);
            return ApiGatewayResponseBuilder.okCacheable(
                    Map.of("customerId", customerId, "count", orders.size(), "items", orders),
                    correlationId,
                    CACHE_SECONDS);

        } catch (NumberFormatException e) {
            return ApiGatewayResponseBuilder.badRequest("limit must be an integer", correlationId);
        } catch (RuntimeException e) {
            if (context != null && context.getLogger() != null) {
                context.getLogger().log("OrderStatusApiHandler failed correlationId=" + correlationId
                        + " cause=" + e + "\n");
            }
            return ApiGatewayResponseBuilder.serverError(
                    "Unable to read the order projection", correlationId);
        }
    }

    private static int parseLimit(String raw) {
        if (raw == null || raw.isBlank()) {
            return DEFAULT_LIMIT;
        }
        return Math.clamp(Integer.parseInt(raw), 1, MAX_LIMIT);
    }

    private static String pathParameter(APIGatewayV2HTTPEvent request, String name) {
        return Optional.ofNullable(request.getPathParameters())
                .map(parameters -> parameters.get(name))
                .orElse(null);
    }

    private static String queryParameter(APIGatewayV2HTTPEvent request, String name) {
        return Optional.ofNullable(request.getQueryStringParameters())
                .map(parameters -> parameters.get(name))
                .orElse(null);
    }

    private static String header(APIGatewayV2HTTPEvent request, String name) {
        Map<String, String> headers = request.getHeaders();
        if (headers == null) {
            return null;
        }
        // HTTP API lower-cases header names; check both spellings so local invokes behave the same.
        String value = headers.get(name);
        return value != null ? value : headers.get(name.toLowerCase(java.util.Locale.ROOT));
    }
}
