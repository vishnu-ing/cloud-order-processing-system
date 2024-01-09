package io.orderflow.gateway.filter;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * Per-client token-bucket rate limiter.
 *
 * <p>Implemented in-process on purpose. Spring Cloud Gateway's built-in
 * {@code RequestRateLimiter} needs Redis, and adding a Redis dependency to make a local demo work
 * would be a worse trade than being explicit about the limitation:
 *
 * <ul>
 *   <li><strong>here</strong>, the limit is per gateway instance — with N replicas the effective
 *       global limit is N x {@code app.rate-limit.requests-per-minute};</li>
 *   <li><strong>in AWS</strong>, the authoritative limit is the API Gateway usage plan / stage
 *       throttle configured in {@code infrastructure/terraform/apigateway.tf}, which is global and
 *       enforced before a request ever reaches compute.</li>
 * </ul>
 *
 * <p>The client key is the authenticated subject when a JWT is present, otherwise the remote
 * address — so an authenticated tenant is not punished for sharing a NAT with someone noisy.
 */
@Component
public class RateLimitFilter implements GlobalFilter, Ordered {

    private final Map<String, Bucket> buckets = new ConcurrentHashMap<>();
    private final Counter throttled;
    private final int capacity;
    private final Duration window;

    public RateLimitFilter(MeterRegistry meterRegistry,
                           @Value("${app.rate-limit.requests-per-minute:120}") int requestsPerMinute) {
        this.capacity = requestsPerMinute;
        this.window = Duration.ofMinutes(1);
        this.throttled = Counter.builder("gateway.requests.throttled").register(meterRegistry);
    }

    @Override
    public int getOrder() {
        // After the correlation filter, before routing.
        return Ordered.HIGHEST_PRECEDENCE + 10;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String key = clientKey(exchange);
        Bucket bucket = buckets.computeIfAbsent(key, ignored -> new Bucket(capacity, window));

        if (bucket.tryConsume()) {
            exchange.getResponse().getHeaders()
                    .set("X-RateLimit-Remaining", Long.toString(bucket.remaining()));
            return chain.filter(exchange);
        }

        throttled.increment();
        exchange.getResponse().setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
        exchange.getResponse().getHeaders().set(HttpHeaders.RETRY_AFTER,
                Long.toString(window.toSeconds()));
        exchange.getResponse().getHeaders().set("X-RateLimit-Limit", Integer.toString(capacity));
        return exchange.getResponse().setComplete();
    }

    private String clientKey(ServerWebExchange exchange) {
        String subject = exchange.getRequest().getHeaders().getFirst("X-Auth-Subject");
        if (subject != null && !subject.isBlank()) {
            return "sub:" + subject;
        }
        String forwardedFor = exchange.getRequest().getHeaders().getFirst("X-Forwarded-For");
        if (forwardedFor != null && !forwardedFor.isBlank()) {
            return "ip:" + forwardedFor.split(",")[0].trim();
        }
        return exchange.getRequest().getRemoteAddress() == null
                ? "ip:unknown"
                : "ip:" + exchange.getRequest().getRemoteAddress().getAddress().getHostAddress();
    }

    /** Lock-free token bucket: refills the full capacity once per window. */
    private static final class Bucket {

        private final int capacity;
        private final long windowNanos;
        private final AtomicLong tokens;
        private final AtomicLong windowStart;

        private Bucket(int capacity, Duration window) {
            this.capacity = capacity;
            this.windowNanos = window.toNanos();
            this.tokens = new AtomicLong(capacity);
            this.windowStart = new AtomicLong(System.nanoTime());
        }

        private boolean tryConsume() {
            long now = System.nanoTime();
            long start = windowStart.get();
            if (now - start > windowNanos && windowStart.compareAndSet(start, now)) {
                tokens.set(capacity);
            }
            return tokens.getAndDecrement() > 0;
        }

        private long remaining() {
            return Math.max(0, tokens.get());
        }
    }
}
