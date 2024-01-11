package io.orderflow.common.config;

import java.time.Duration;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;

import io.orderflow.common.idempotency.IdempotencyGuard;
import io.orderflow.common.idempotency.ProcessedEventRepository;
import io.orderflow.common.observability.CorrelationIdFilter;
import io.orderflow.common.observability.SagaMetrics;

/**
 * Shared consumer-side infrastructure: idempotency, saga metrics, correlation propagation.
 *
 * <p>Imported explicitly with {@code @Import(CommonInfraConfig.class)} rather than exposed as an
 * auto-configuration, so each service opts in to exactly the shared machinery it needs and the
 * reactive gateway stays free of servlet types.
 */
@Configuration(proxyBeanMethods = false)
public class CommonInfraConfig {

    @Bean
    public SagaMetrics sagaMetrics(MeterRegistry meterRegistry) {
        return new SagaMetrics(meterRegistry);
    }

    @Bean
    public ProcessedEventRepository processedEventRepository(
            DynamoDbEnhancedClient enhancedClient,
            @Value("${app.dynamodb.tables.processed-events:processed_events}") String tableName) {
        return new ProcessedEventRepository(enhancedClient, tableName);
    }

    @Bean
    public IdempotencyGuard idempotencyGuard(
            ProcessedEventRepository processedEventRepository,
            SagaMetrics sagaMetrics,
            @Value("${app.idempotency.retention-days:7}") int retentionDays) {
        return new IdempotencyGuard(processedEventRepository, sagaMetrics, Duration.ofDays(retentionDays));
    }

    @Bean
    public CorrelationIdFilter correlationIdFilter() {
        return new CorrelationIdFilter();
    }
}
