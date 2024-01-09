package io.orderflow.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * api-gateway: the single public entry point.
 *
 * <p>Responsibilities, in the order a request meets them: mint/propagate the correlation id,
 * enforce the per-client rate limit, validate and relay the JWT, then route to the owning service
 * behind a circuit breaker with a fallback.
 *
 * <p>Deliberately reactive (WebFlux) — the edge is I/O bound and mostly idle-waiting on downstream
 * services, which is exactly where the event-loop model pays off over a thread-per-request pool.
 */
@SpringBootApplication
public class ApiGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApiGatewayApplication.class, args);
    }
}
