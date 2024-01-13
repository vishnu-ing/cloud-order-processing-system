package io.orderflow.lambda.support;

import java.util.LinkedHashMap;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.orderflow.common.event.EventJson;
import io.orderflow.common.observability.CorrelationContext;

/**
 * Small helper that keeps every Lambda HTTP response consistent with the Spring services:
 * JSON bodies, RFC-7807 problems for errors, and the correlation id echoed back.
 *
 * <p>Without something like this, handlers drift — one returns a bare string, another a different
 * error shape — and clients end up with two contracts for one API.
 */
public final class ApiGatewayResponseBuilder {

    private static final ObjectMapper MAPPER = EventJson.createMapper();

    private static final String CONTENT_TYPE = "Content-Type";
    private static final String JSON = "application/json";
    private static final String PROBLEM_JSON = "application/problem+json";
    private static final String CACHE_CONTROL = "Cache-Control";

    private ApiGatewayResponseBuilder() {
    }

    /** Serialized payload plus the status code, as handed back to API Gateway. */
    public record LambdaResponse(int statusCode, String body, Map<String, String> headers) {

        public APIGatewayV2HTTPResponse toApiGatewayResponse() {
            return APIGatewayV2HTTPResponse.builder()
                    .withStatusCode(statusCode)
                    .withHeaders(headers)
                    .withBody(body)
                    .withIsBase64Encoded(false)
                    .build();
        }
    }

    public static APIGatewayV2HTTPResponse ok(Object payload, String correlationId) {
        return json(200, payload, correlationId, "no-store");
    }

    /** Projections are eventually consistent, so a short shared cache is safe and cheap. */
    public static APIGatewayV2HTTPResponse okCacheable(Object payload, String correlationId, int maxAgeSeconds) {
        return json(200, payload, correlationId, "public, max-age=" + maxAgeSeconds);
    }

    public static APIGatewayV2HTTPResponse notFound(String detail, String correlationId) {
        return problem(404, "Order not found", detail, correlationId);
    }

    public static APIGatewayV2HTTPResponse badRequest(String detail, String correlationId) {
        return problem(400, "Invalid request", detail, correlationId);
    }

    public static APIGatewayV2HTTPResponse serverError(String detail, String correlationId) {
        return problem(500, "Internal server error", detail, correlationId);
    }

    private static APIGatewayV2HTTPResponse json(int status,
                                                 Object payload,
                                                 String correlationId,
                                                 String cacheControl) {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(CONTENT_TYPE, JSON);
        headers.put(CACHE_CONTROL, cacheControl);
        if (correlationId != null) {
            headers.put(CorrelationContext.CORRELATION_ID_HEADER, correlationId);
        }
        return new LambdaResponse(status, serialize(payload), headers).toApiGatewayResponse();
    }

    private static APIGatewayV2HTTPResponse problem(int status,
                                                    String title,
                                                    String detail,
                                                    String correlationId) {
        Map<String, Object> problem = new LinkedHashMap<>();
        problem.put("type", "urn:problem-type:orders:lambda-" + status);
        problem.put("title", title);
        problem.put("status", status);
        problem.put("detail", detail);
        if (correlationId != null) {
            problem.put("correlationId", correlationId);
        }

        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(CONTENT_TYPE, PROBLEM_JSON);
        headers.put(CACHE_CONTROL, "no-store");
        if (correlationId != null) {
            headers.put(CorrelationContext.CORRELATION_ID_HEADER, correlationId);
        }
        return new LambdaResponse(status, serialize(problem), headers).toApiGatewayResponse();
    }

    private static String serialize(Object payload) {
        try {
            return MAPPER.writeValueAsString(payload);
        } catch (Exception e) {
            // Never let a serialisation bug produce an empty 200: fail loudly with valid JSON.
            return "{\"title\":\"Serialisation failure\",\"status\":500}";
        }
    }
}
