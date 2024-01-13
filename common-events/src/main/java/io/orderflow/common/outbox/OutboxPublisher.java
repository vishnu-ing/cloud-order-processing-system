package io.orderflow.common.outbox;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import io.orderflow.common.observability.CorrelationContext;
import io.orderflow.common.observability.SagaMetrics;

/**
 * Drains the {@code outbox} table to Kafka.
 *
 * <p>Delivery semantics are deliberately <strong>at-least-once</strong>. The publisher sends the
 * record first and only then flips its status, so a crash in between causes a redelivery rather
 * than a lost event — the trade we want, because every consumer is idempotent (see
 * {@code IdempotencyGuard}) while a lost event would silently strand a saga.
 *
 * <p>Runs on every replica: the conditional status update in
 * {@link OutboxRepository#markPublished(OutboxRecord)} keeps concurrent drains harmless.
 */
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final SagaMetrics metrics;
    private final int batchSize;
    private final int maxAttempts;
    private final long sendTimeoutSeconds;

    public OutboxPublisher(OutboxRepository outboxRepository,
                           KafkaTemplate<String, String> kafkaTemplate,
                           SagaMetrics metrics,
                           int batchSize,
                           int maxAttempts,
                           long sendTimeoutSeconds) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.metrics = metrics;
        this.batchSize = batchSize;
        this.maxAttempts = maxAttempts;
        this.sendTimeoutSeconds = sendTimeoutSeconds;
    }

    @Scheduled(
            initialDelayString = "${app.outbox.initial-delay-ms:2000}",
            fixedDelayString = "${app.outbox.poll-interval-ms:500}")
    public void drain() {
        List<OutboxRecord> pending = outboxRepository.findPending(batchSize);
        if (pending.isEmpty()) {
            return;
        }
        log.debug("Draining {} pending outbox record(s)", pending.size());
        for (OutboxRecord record : pending) {
            publish(record);
        }
    }

    private void publish(OutboxRecord record) {
        try {
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(record.getTopic(), record.getPartitionKey(), record.getPayload());
            producerRecord.headers()
                    .add(CorrelationContext.CORRELATION_ID_HEADER,
                            nullSafe(record.getCorrelationId()))
                    .add(CorrelationContext.EVENT_ID_HEADER, nullSafe(record.getEventId()))
                    .add(CorrelationContext.EVENT_TYPE_HEADER, nullSafe(record.getEventType()));

            kafkaTemplate.send(producerRecord).get(sendTimeoutSeconds, TimeUnit.SECONDS);

            if (outboxRepository.markPublished(record)) {
                metrics.eventPublished(record.getEventType());
                log.info("Published {} eventId={} to {} correlationId={}",
                        record.getEventType(), record.getEventId(), record.getTopic(),
                        record.getCorrelationId());
            } else {
                // Another replica already claimed this row; its consumer-side dedupe absorbs it.
                metrics.outboxRaceLost(record.getEventType());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Outbox drain interrupted for recordId={}", record.getRecordId());
        } catch (Exception e) {
            metrics.outboxPublishFailed(record.getEventType());
            log.error("Failed to publish outbox recordId={} eventType={} attempt={}",
                    record.getRecordId(), record.getEventType(), record.getAttempts() + 1, e);
            outboxRepository.markAttemptFailed(record, e.getMessage(), maxAttempts);
        }
    }

    private static byte[] nullSafe(String value) {
        return (value == null ? "" : value).getBytes(StandardCharsets.UTF_8);
    }
}
