package io.orderflow.gateway.filter;

import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import io.orderflow.common.observability.CorrelationContext;

/**
 * Mints the correlation id for the whole platform.
 *
 * <p>If the caller supplied {@code X-Correlation-Id} we keep it (so a client can tie its own trace
 * to ours); otherwise we generate one. The value is written onto the mutated request — which is
 * what downstream services, and eventually the Kafka event envelopes, carry — and echoed on the
 * response.
 *
 * <p>Runs first, ahead of rate limiting, so even a request that gets a {@code 429} is traceable.
 */
@Component
public class CorrelationIdWebFilter implements WebFilter, Ordered {

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String correlationId = CorrelationContext.orNew(
                exchange.getRequest().getHeaders().getFirst(CorrelationContext.CORRELATION_ID_HEADER));

        ServerWebExchange mutated = exchange.mutate()
                .request(builder -> builder.header(CorrelationContext.CORRELATION_ID_HEADER, correlationId))
                .build();
        mutated.getResponse().getHeaders()
                .set(CorrelationContext.CORRELATION_ID_HEADER, correlationId);

        // The MDC is only valid for the synchronous part of a reactive chain, so it is set for the
        // gateway's own log lines and cleared immediately; propagation downstream is via the header.
        MDC.put(CorrelationContext.MDC_CORRELATION_ID, correlationId);
        try {
            return chain.filter(mutated)
                    .contextWrite(context ->
                            context.put(CorrelationContext.MDC_CORRELATION_ID, correlationId));
        } finally {
            MDC.remove(CorrelationContext.MDC_CORRELATION_ID);
        }
    }
}
