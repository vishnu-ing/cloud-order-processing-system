package io.orderflow.gateway.web;

import java.net.URI;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;

import io.orderflow.common.observability.CorrelationContext;

/**
 * Circuit-breaker fallback.
 *
 * <p>When order-service is unhealthy the gateway answers here instead of hanging: a
 * {@code 503 application/problem+json} with the correlation id, so a client can retry safely with
 * the same {@code Idempotency-Key} and support can find the incident.
 *
 * <p>Both verbs are mapped because {@code POST /orders} and the read endpoints share the breaker —
 * and the message differs: a failed create may or may not have been accepted, which the client
 * needs to know.
 */
@RestController
@RequestMapping(path = "/fallback", produces = MediaType.APPLICATION_PROBLEM_JSON_VALUE)
public class FallbackController {

    private static final URI TYPE =
            URI.create("urn:problem-type:orders:upstream-unavailable");

    @GetMapping("/orders")
    public ResponseEntity<ProblemDetail> ordersReadFallback(ServerWebExchange exchange) {
        return build(exchange, "Order lookup is temporarily unavailable. Retry shortly.");
    }

    @PostMapping("/orders")
    public ResponseEntity<ProblemDetail> ordersWriteFallback(ServerWebExchange exchange) {
        return build(exchange, "The order could not be submitted. Retry with the same "
                + CorrelationContext.IDEMPOTENCY_KEY_HEADER + " — duplicates are suppressed.");
    }

    private ResponseEntity<ProblemDetail> build(ServerWebExchange exchange, String detail) {
        ProblemDetail problem =
                ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, detail);
        problem.setTitle("Upstream service unavailable");
        problem.setType(TYPE);
        String correlationId = exchange.getRequest().getHeaders()
                .getFirst(CorrelationContext.CORRELATION_ID_HEADER);
        if (correlationId != null) {
            problem.setProperty("correlationId", correlationId);
        }
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header("Retry-After", "5")
                .body(problem);
    }
}
