package io.orderflow.order.web;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

import jakarta.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import io.orderflow.common.error.RetryableException;
import io.orderflow.common.observability.CorrelationContext;
import io.orderflow.order.domain.IllegalStateTransitionException;
import io.orderflow.order.domain.OrderNotFoundException;

/**
 * Maps every exception the API can raise to an RFC-7807 {@code application/problem+json} body.
 *
 * <p>Each problem carries the request's {@code correlationId}, so a client-reported failure can be
 * traced straight to the saga instance in CloudWatch without asking the customer for a timestamp.
 *
 * <p>Status choices that matter:
 * <ul>
 *   <li>{@code 409} for an illegal state transition — the request was well-formed, the aggregate
 *       simply is not in a state where it applies (cancelling an already-confirmed order);</li>
 *   <li>{@code 503} with {@code Retry-After} semantics for transient DynamoDB/Kafka faults, so
 *       clients back off instead of hammering a throttled table.</li>
 * </ul>
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);
    private static final String PROBLEM_BASE = "urn:problem-type:orders:";

    @ExceptionHandler(OrderNotFoundException.class)
    public ProblemDetail handleNotFound(OrderNotFoundException e) {
        ProblemDetail problem = problem(HttpStatus.NOT_FOUND, "Order not found", e.getMessage(),
                "order-not-found");
        problem.setProperty("orderId", e.orderId());
        return problem;
    }

    @ExceptionHandler(IllegalStateTransitionException.class)
    public ProblemDetail handleIllegalTransition(IllegalStateTransitionException e) {
        ProblemDetail problem = problem(HttpStatus.CONFLICT, "Illegal order state transition",
                e.getMessage(), "illegal-state-transition");
        problem.setProperty("currentStatus", String.valueOf(e.from()));
        problem.setProperty("requestedStatus", String.valueOf(e.to()));
        return problem;
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail handleValidation(MethodArgumentNotValidException e) {
        Map<String, String> fieldErrors = new LinkedHashMap<>();
        e.getBindingResult().getFieldErrors()
                .forEach(error -> fieldErrors.put(error.getField(), error.getDefaultMessage()));
        ProblemDetail problem = problem(HttpStatus.BAD_REQUEST, "Request validation failed",
                "One or more fields are invalid", "validation-failed");
        problem.setProperty("errors", fieldErrors);
        return problem;
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ProblemDetail handleConstraintViolation(ConstraintViolationException e) {
        return problem(HttpStatus.BAD_REQUEST, "Request validation failed", e.getMessage(),
                "validation-failed");
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail handleBadRequest(IllegalArgumentException e) {
        return problem(HttpStatus.BAD_REQUEST, "Invalid request", e.getMessage(), "invalid-request");
    }

    @ExceptionHandler(RetryableException.class)
    public ProblemDetail handleTransient(RetryableException e) {
        log.warn("Transient failure serving request: {}", e.getMessage());
        return problem(HttpStatus.SERVICE_UNAVAILABLE, "Temporarily unavailable",
                "The order store is busy; please retry with the same Idempotency-Key.",
                "transient-failure");
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail handleUnexpected(Exception e) {
        log.error("Unhandled exception serving request", e);
        return problem(HttpStatus.INTERNAL_SERVER_ERROR, "Internal server error",
                "An unexpected error occurred. Quote the correlationId when reporting it.",
                "internal-error");
    }

    private ProblemDetail problem(HttpStatus status, String title, String detail, String slug) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setTitle(title);
        problem.setType(URI.create(PROBLEM_BASE + slug));
        String correlationId = CorrelationContext.currentCorrelationId();
        if (correlationId != null) {
            problem.setProperty("correlationId", correlationId);
        }
        return problem;
    }
}
