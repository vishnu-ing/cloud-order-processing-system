package io.orderflow.common.idempotency;

import java.time.Duration;
import java.time.Instant;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

/**
 * Dedupe marker in the {@code processed_events} table: PK {@code consumerGroup}, SK {@code eventId}.
 *
 * <p>Keying on the consumer group (not just the event id) means each consumer group dedupes
 * independently — a replay for payment-service must not be suppressed because notification-service
 * already saw the event.
 *
 * <p>{@code expiresAt} is a DynamoDB TTL attribute (epoch seconds). Markers are only useful for as
 * long as a redelivery is plausible, so they self-expire after the retention window instead of
 * growing an unbounded table.
 */
@DynamoDbBean
public class ProcessedEvent {

    private String consumerGroup;
    private String eventId;
    private String eventType;
    private String orderId;
    private Instant processedAt;
    private Long expiresAt;

    public ProcessedEvent() {
        // Required by the DynamoDB Enhanced Client.
    }

    public static ProcessedEvent of(String consumerGroup,
                                    String eventId,
                                    String eventType,
                                    String orderId,
                                    Duration retention) {
        ProcessedEvent marker = new ProcessedEvent();
        Instant now = Instant.now();
        marker.setConsumerGroup(consumerGroup);
        marker.setEventId(eventId);
        marker.setEventType(eventType);
        marker.setOrderId(orderId);
        marker.setProcessedAt(now);
        marker.setExpiresAt(now.plus(retention).getEpochSecond());
        return marker;
    }

    @DynamoDbPartitionKey
    @DynamoDbAttribute("consumerGroup")
    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    @DynamoDbSortKey
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

    @DynamoDbAttribute("orderId")
    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    @DynamoDbAttribute("processedAt")
    public Instant getProcessedAt() {
        return processedAt;
    }

    public void setProcessedAt(Instant processedAt) {
        this.processedAt = processedAt;
    }

    @DynamoDbAttribute("expiresAt")
    public Long getExpiresAt() {
        return expiresAt;
    }

    public void setExpiresAt(Long expiresAt) {
        this.expiresAt = expiresAt;
    }
}
