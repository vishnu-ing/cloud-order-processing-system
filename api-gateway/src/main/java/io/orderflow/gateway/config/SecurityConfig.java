package io.orderflow.gateway.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.security.web.server.SecurityWebFilterChain;
import org.springframework.web.server.WebFilter;
import reactor.core.publisher.Mono;

/**
 * Edge authentication.
 *
 * <p>When {@code app.security.jwt.enabled=true} (the deployed configuration) the gateway validates
 * the bearer token against the issuer's JWKS, rejects anything unsigned or expired, and
 * <em>relays</em> the original {@code Authorization} header downstream untouched — services can
 * therefore make their own authorisation decisions without a second round trip to the IdP.
 *
 * <p>It additionally injects {@code X-Auth-Subject}, which the rate limiter uses as its bucket key
 * and the services use for audit logging. Downstream services trust that header precisely because
 * they are only reachable through this gateway (private subnets + internal ALB in the Terraform
 * topology); the gateway overwrites any client-supplied value.
 *
 * <p>Locally the flag defaults to {@code false} so the README's {@code curl} examples work without
 * standing up an identity provider.
 */
@Configuration(proxyBeanMethods = false)
@EnableWebFluxSecurity
public class SecurityConfig {

    @Bean
    public SecurityWebFilterChain securityWebFilterChain(
            ServerHttpSecurity http,
            @Value("${app.security.jwt.enabled:false}") boolean jwtEnabled) {

        http.csrf(ServerHttpSecurity.CsrfSpec::disable)
                .httpBasic(ServerHttpSecurity.HttpBasicSpec::disable)
                .formLogin(ServerHttpSecurity.FormLoginSpec::disable);

        if (jwtEnabled) {
            http.authorizeExchange(exchanges -> exchanges
                            .pathMatchers("/actuator/health/**", "/fallback/**").permitAll()
                            .pathMatchers("/api/**").authenticated()
                            .anyExchange().denyAll())
                    .oauth2ResourceServer(oauth2 -> oauth2.jwt(Customizer.withDefaults()));
        } else {
            http.authorizeExchange(exchanges -> exchanges.anyExchange().permitAll());
        }
        return http.build();
    }

    /**
     * Copies the validated JWT subject onto the request as {@code X-Auth-Subject}.
     *
     * <p>Always strips an inbound value first: a client-supplied subject header must never be able
     * to impersonate a tenant or dodge its rate-limit bucket.
     */
    @Bean
    public WebFilter jwtSubjectRelayFilter() {
        return (exchange, chain) -> {
            var sanitized = exchange.mutate()
                    .request(builder -> builder.headers(headers -> headers.remove("X-Auth-Subject")))
                    .build();

            return ReactiveSecurityContextHolder.getContext()
                    .map(context -> context.getAuthentication())
                    .filter(JwtAuthenticationToken.class::isInstance)
                    .map(JwtAuthenticationToken.class::cast)
                    .map(JwtAuthenticationToken::getToken)
                    .map(Jwt::getSubject)
                    .flatMap(subject -> chain.filter(sanitized.mutate()
                            .request(builder -> builder.header("X-Auth-Subject", subject))
                            .build()))
                    .switchIfEmpty(Mono.defer(() -> chain.filter(sanitized)));
        };
    }
}
