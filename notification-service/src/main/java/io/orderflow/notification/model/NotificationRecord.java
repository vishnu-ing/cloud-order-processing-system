package io.orderflow.notification.model;

import java.time.Instant;
import java.util.UUID;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;

/**
 * A rendered, ready-to-send message in the {@code notifications} table: PK {@code notificationId}.
 *
 * <p>The table is a send outbox, not a log. Rendering and sending are separated so a transient SES
 * failure cannot lose a confirmation email: the record is durable first, the send is retried
 * against it, and {@code status} records the outcome.
 */
@DynamoDbBean
public class NotificationRecord {

    public static final String STATUS_PENDING = "PENDING";
    public static final String STATUS_SENT = "SENT";
    public static final String STATUS_FAILED = "FAILED";

    private String notificationId;
    private String orderId;
    private String customerId;
    private String recipient;
    private String channel;
    private String templateKey;
    private String subject;
    private String body;
    private String status;
    private String eventId;
    private String correlationId;
    private String failureReason;
    private Instant createdAt;
    private Instant sentAt;

    public NotificationRecord() {
        // Required by the DynamoDB Enhanced Client.
    }

    public static NotificationRecord pending(String orderId,
                                             String customerId,
                                             String recipient,
                                             String channel,
                                             String templateKey,
                                             String subject,
                                             String body,
                                             String eventId,
                                             String correlationId) {
        NotificationRecord record = new NotificationRecord();
        record.setNotificationId(UUID.randomUUID().toString());
        record.setOrderId(orderId);
        record.setCustomerId(customerId);
        record.setRecipient(recipient);
        record.setChannel(channel);
        record.setTemplateKey(templateKey);
        record.setSubject(subject);
        record.setBody(body);
        record.setStatus(STATUS_PENDING);
        record.setEventId(eventId);
        record.setCorrelationId(correlationId);
        record.setCreatedAt(Instant.now());
        return record;
    }

    @DynamoDbPartitionKey
    @DynamoDbAttribute("notificationId")
    public String getNotificationId() {
        return notificationId;
    }

    public void setNotificationId(String notificationId) {
        this.notificationId = notificationId;
    }

    @DynamoDbAttribute("orderId")
    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    @DynamoDbAttribute("customerId")
    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    @DynamoDbAttribute("recipient")
    public String getRecipient() {
        return recipient;
    }

    public void setRecipient(String recipient) {
        this.recipient = recipient;
    }

    @DynamoDbAttribute("channel")
    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    @DynamoDbAttribute("templateKey")
    public String getTemplateKey() {
        return templateKey;
    }

    public void setTemplateKey(String templateKey) {
        this.templateKey = templateKey;
    }

    @DynamoDbAttribute("subject")
    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    @DynamoDbAttribute("body")
    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    @DynamoDbAttribute("status")
    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @DynamoDbAttribute("eventId")
    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    @DynamoDbAttribute("correlationId")
    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    @DynamoDbAttribute("failureReason")
    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
    }

    @DynamoDbAttribute("createdAt")
    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    @DynamoDbAttribute("sentAt")
    public Instant getSentAt() {
        return sentAt;
    }

    public void setSentAt(Instant sentAt) {
        this.sentAt = sentAt;
    }

    public void markSent() {
        this.status = STATUS_SENT;
        this.sentAt = Instant.now();
        this.failureReason = null;
    }

    public void markFailed(String reason) {
        this.status = STATUS_FAILED;
        this.failureReason = reason;
    }
}
