package io.orderflow.common.observability;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.MDC;

/**
 * Correlation-id plumbing shared by the HTTP edge, the Kafka listeners and the Lambda handlers.
 *
 * <p>A correlation id is minted at the gateway (or at {@code POST /api/v1/orders} if the caller did
 * not supply one), copied into the event envelope, re-published as a Kafka header, and pushed into
 * the SLF4J MDC on the consuming side. Every log line in the system therefore carries the id, which
 * is what makes a six-service saga debuggable from one CloudWatch Logs Insights query.
 */
public final class CorrelationContext {

    public static final String CORRELATION_ID_HEADER = "X-Correlation-Id";
    public static final String EVENT_ID_HEADER = "X-Event-Id";
    public static final String EVENT_TYPE_HEADER = "X-Event-Type";
    public static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";

    public static final String MDC_CORRELATION_ID = "correlationId";
    public static final String MDC_EVENT_ID = "eventId";
    public static final String MDC_ORDER_ID = "orderId";

    private CorrelationContext() {
    }

    public static String newCorrelationId() {
        return UUID.randomUUID().toString();
    }

    public static String orNew(String candidate) {
        return (candidate == null || candidate.isBlank()) ? newCorrelationId() : candidate;
    }

    public static void put(String correlationId, String eventId, String orderId) {
        if (correlationId != null) {
            MDC.put(MDC_CORRELATION_ID, correlationId);
        }
        if (eventId != null) {
            MDC.put(MDC_EVENT_ID, eventId);
        }
        if (orderId != null) {
            MDC.put(MDC_ORDER_ID, orderId);
        }
    }

    public static void clear() {
        MDC.remove(MDC_CORRELATION_ID);
        MDC.remove(MDC_EVENT_ID);
        MDC.remove(MDC_ORDER_ID);
    }

    public static String currentCorrelationId() {
        return MDC.get(MDC_CORRELATION_ID);
    }

    /** Reads the correlation id off an inbound Kafka record, if the producer set one. */
    public static String fromHeaders(Headers headers) {
        if (headers == null) {
            return null;
        }
        Header header = headers.lastHeader(CORRELATION_ID_HEADER);
        if (header == null || header.value() == null || header.value().length == 0) {
            return null;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
