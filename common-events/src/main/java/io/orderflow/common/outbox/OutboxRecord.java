package io.orderflow.common.outbox;

import java.time.Instant;
import java.util.UUID;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondaryPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondarySortKey;

import io.orderflow.common.event.DomainEvent;
import io.orderflow.common.event.EventEnvelope;
import io.orderflow.common.event.EventJson;

/**
 * One row of the {@code outbox} table: an event that has been <em>decided</em> but not yet
 * <em>published</em>.
 *
 * <p>The record is written inside the same DynamoDB {@code TransactWriteItems} call as the
 * aggregate mutation that produced it. Either both land or neither does, which removes the classic
 * dual-write hole where a service commits its state and then dies before reaching the broker.
 *
 * <p>Table: PK {@code recordId}; GSI {@code status-createdAt-index} (PK {@code status},
 * SK {@code createdAt}) so the publisher can scan only the pending tail in insertion order.
 */
@DynamoDbBean
public class OutboxRecord {

    public static final String STATUS_PENDING = "PENDING";
    public static final String STATUS_PUBLISHED = "PUBLISHED";
    public static final String STATUS_FAILED = "FAILED";

    public static final String STATUS_INDEX = "status-createdAt-index";

    private String recordId;
    private String aggregateId;
    private String eventId;
    private String eventType;
    private String topic;
    private String partitionKey;
    private String payload;
    private String status;
    private String correlationId;
    private String source;
    private Instant createdAt;
    private Instant publishedAt;
    private int attempts;
    private String lastError;

    public OutboxRecord() {
        // Required by the DynamoDB Enhanced Client.
    }

    /** Serialises an envelope into a pending outbox row. */
    public static OutboxRecord pending(EventEnvelope<? extends DomainEvent> envelope) {
        OutboxRecord record = new OutboxRecord();
        record.setRecordId(UUID.randomUUID().toString());
        record.setAggregateId(envelope.payload().orderId());
        record.setEventId(envelope.eventId());
        record.setEventType(envelope.eventType().name());
        record.setTopic(envelope.eventType().topic());
        record.setPartitionKey(envelope.partitionKey());
        record.setPayload(EventJson.toJson(envelope));
        record.setStatus(STATUS_PENDING);
        record.setCorrelationId(envelope.correlationId());
        record.setSource(envelope.source());
        record.setCreatedAt(envelope.occurredAt());
        record.setAttempts(0);
        return record;
    }

    @DynamoDbPartitionKey
    @DynamoDbAttribute("recordId")
    public String getRecordId() {
        return recordId;
    }

    public void setRecordId(String recordId) {
        this.recordId = recordId;
    }

    @DynamoDbAttribute("aggregateId")
    public String getAggregateId() {
        return aggregateId;
    }

    public void setAggregateId(String aggregateId) {
        this.aggregateId = aggregateId;
    }

    @DynamoDbAttribute("eventId")
    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    @DynamoDbAttribute("eventType")
    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    @DynamoDbAttribute("topic")
    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    @DynamoDbAttribute("partitionKey")
    public String getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    @DynamoDbAttribute("payload")
    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    @DynamoDbSecondaryPartitionKey(indexNames = STATUS_INDEX)
    @DynamoDbAttribute("status")
    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @DynamoDbAttribute("correlationId")
    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    /** The producing service. Each service's publisher drains only the rows it wrote. */
    @DynamoDbAttribute("source")
    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    @DynamoDbSecondarySortKey(indexNames = STATUS_INDEX)
    @DynamoDbAttribute("createdAt")
    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    @DynamoDbAttribute("publishedAt")
    public Instant getPublishedAt() {
        return publishedAt;
    }

    public void setPublishedAt(Instant publishedAt) {
        this.publishedAt = publishedAt;
    }

    @DynamoDbAttribute("attempts")
    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    @DynamoDbAttribute("lastError")
    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }

    public void markPublished() {
        this.status = STATUS_PUBLISHED;
        this.publishedAt = Instant.now();
        this.lastError = null;
    }

    public void markAttemptFailed(String error, int maxAttempts) {
        this.attempts = this.attempts + 1;
        this.lastError = error == null ? "unknown" : error.substring(0, Math.min(error.length(), 500));
        if (this.attempts >= maxAttempts) {
            this.status = STATUS_FAILED;
        }
    }
}
