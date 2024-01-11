package io.orderflow.common.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;

import io.orderflow.common.observability.SagaMetrics;
import io.orderflow.common.outbox.OutboxPublisher;
import io.orderflow.common.outbox.OutboxRepository;

/**
 * Producer-side infrastructure. Imported only by the three services that emit events
 * (order, inventory, payment); notification-service is a pure sink and never wires this in.
 */
@Configuration(proxyBeanMethods = false)
public class OutboxConfig {

    @Bean
    public OutboxRepository outboxRepository(
            DynamoDbEnhancedClient enhancedClient,
            @Value("${app.dynamodb.tables.outbox:outbox}") String tableName,
            @Value("${spring.application.name}") String serviceName) {
        return new OutboxRepository(enhancedClient, tableName, serviceName);
    }

    /**
     * The String/String template the outbox publishes with.
     *
     * <p>Declared explicitly because {@link KafkaErrorHandlingConfig} contributes a
     * {@code KafkaTemplate<Object, Object>} for dead-letter publishing, and Spring Boot's
     * auto-configured template backs off as soon as any {@code KafkaTemplate} bean exists. It reuses
     * the auto-configured producer factory, so the idempotent-producer settings under
     * {@code spring.kafka.producer} still apply.
     */
    @Bean
    public KafkaTemplate<String, String> outboxKafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public OutboxPublisher outboxPublisher(
            OutboxRepository outboxRepository,
            KafkaTemplate<String, String> kafkaTemplate,
            SagaMetrics sagaMetrics,
            @Value("${app.outbox.batch-size:50}") int batchSize,
            @Value("${app.outbox.max-attempts:10}") int maxAttempts,
            @Value("${app.outbox.send-timeout-seconds:5}") long sendTimeoutSeconds) {
        return new OutboxPublisher(outboxRepository, kafkaTemplate, sagaMetrics,
                batchSize, maxAttempts, sendTimeoutSeconds);
    }
}
