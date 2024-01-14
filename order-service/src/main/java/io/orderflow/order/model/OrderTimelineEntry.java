package io.orderflow.order.model;

import java.time.Instant;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;

/**
 * One step of a saga instance, embedded as a list on the order item.
 *
 * <p>Keeping the timeline inside the aggregate (rather than in its own table) means
 * {@code GET /api/v1/orders/{id}/timeline} is a single-digit-millisecond {@code GetItem} with no
 * fan-out, and the history is written in the same conditional update that changed the status — so
 * the audit trail can never disagree with the state it describes.
 */
@DynamoDbBean
public class OrderTimelineEntry {

    private String status;
    private String eventType;
    private String eventId;
    private String causationId;
    private String correlationId;
    private String detail;
    private Instant occurredAt;

    public OrderTimelineEntry() {
        // Required by the DynamoDB Enhanced Client.
    }

    public static OrderTimelineEntry of(String status,
                                        String eventType,
                                        String eventId,
                                        String causationId,
                                        String correlationId,
                                        String detail) {
        OrderTimelineEntry entry = new OrderTimelineEntry();
        entry.setStatus(status);
        entry.setEventType(eventType);
        entry.setEventId(eventId);
        entry.setCausationId(causationId);
        entry.setCorrelationId(correlationId);
        entry.setDetail(detail);
        entry.setOccurredAt(Instant.now());
        return entry;
    }

    @DynamoDbAttribute("status")
    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @DynamoDbAttribute("eventType")
    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    @DynamoDbAttribute("eventId")
    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    @DynamoDbAttribute("causationId")
    public String getCausationId() {
        return causationId;
    }

    public void setCausationId(String causationId) {
        this.causationId = causationId;
    }

    @DynamoDbAttribute("correlationId")
    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    @DynamoDbAttribute("detail")
    public String getDetail() {
        return detail;
    }

    public void setDetail(String detail) {
        this.detail = detail;
    }

    @DynamoDbAttribute("occurredAt")
    public Instant getOccurredAt() {
        return occurredAt;
    }

    public void setOccurredAt(Instant occurredAt) {
        this.occurredAt = occurredAt;
    }
}
