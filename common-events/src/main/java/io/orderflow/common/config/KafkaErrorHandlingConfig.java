package io.orderflow.common.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.ToStringSerializer;

import io.orderflow.common.error.NonRetryableException;
import io.orderflow.common.event.EventEnvelope;
import io.orderflow.common.event.EventEnvelopeSerde;
import io.orderflow.common.event.Topics;

/**
 * Consumer error policy shared by every listener in the platform.
 *
 * <p>Spring Boot picks up the {@link DefaultErrorHandler} bean and installs it on the
 * auto-configured listener container factory, so all four consuming services get identical
 * retry/dead-letter behaviour without copy-pasted configuration:
 *
 * <ol>
 *   <li>retry in-process with exponential back-off (defaults: 500 ms, x2, capped at 10 s, 4 retries
 *       — roughly 16 s of total retry budget, short enough not to block the partition for long);</li>
 *   <li>{@link NonRetryableException} and deserialization failures skip the back-off entirely and
 *       are dead-lettered on the first attempt (poison-pill containment);</li>
 *   <li>on exhaustion, publish the original record to {@code <topic>.DLT} with the failure headers
 *       Spring adds, then commit the offset so the partition keeps moving.</li>
 * </ol>
 *
 * <p>The DLT resolver targets partition {@code -1} (let the broker choose) because dead-letter
 * topics are provisioned with fewer partitions than their live counterparts.
 */
@Configuration(proxyBeanMethods = false)
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    /**
     * Dedicated template for dead-letter publishing.
     *
     * <p>It must be able to write both {@code byte[]} (a record whose value never deserialized —
     * {@code ErrorHandlingDeserializer} hands back the raw bytes) and a fully deserialized
     * {@link EventEnvelope} (a record that parsed but whose handler failed). A
     * {@link DelegatingByTypeSerializer} covers both without losing the original payload.
     */
    @Bean
    public KafkaTemplate<Object, Object> deadLetterKafkaTemplate(
            @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        ProducerFactory<Object, Object> factory = new DefaultKafkaProducerFactory<>(
                props,
                new ToStringSerializer(),
                new DelegatingByTypeSerializer(Map.of(
                        byte[].class, new ByteArraySerializer(),
                        EventEnvelope.class, new EventEnvelopeSerde.EnvelopeSerializer()), true));
        return new KafkaTemplate<>(factory);
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
            KafkaTemplate<Object, Object> deadLetterKafkaTemplate,
            @Value("${app.kafka.retry.initial-interval-ms:500}") long initialIntervalMs,
            @Value("${app.kafka.retry.multiplier:2.0}") double multiplier,
            @Value("${app.kafka.retry.max-interval-ms:10000}") long maxIntervalMs,
            @Value("${app.kafka.retry.max-retries:4}") int maxRetries) {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                deadLetterKafkaTemplate,
                (record, exception) -> {
                    log.error("Dead-lettering record topic={} partition={} offset={} key={} cause={}",
                            record.topic(), record.partition(), record.offset(), record.key(),
                            exception.getMessage());
                    return new TopicPartition(Topics.deadLetterFor(record.topic()), -1);
                });

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(maxRetries);
        backOff.setInitialInterval(initialIntervalMs);
        backOff.setMultiplier(multiplier);
        backOff.setMaxInterval(maxIntervalMs);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        // Business rejections and malformed payloads can never succeed on retry.
        errorHandler.addNotRetryableExceptions(
                NonRetryableException.class,
                IllegalArgumentException.class);
        errorHandler.setCommitRecovered(true);
        errorHandler.setLogLevel(KafkaException.Level.WARN);
        return errorHandler;
    }
}
