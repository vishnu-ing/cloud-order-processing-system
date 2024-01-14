package io.orderflow.order.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import software.amazon.awssdk.enhanced.dynamodb.extensions.annotations.DynamoDbVersionAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbIgnore;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondaryPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondarySortKey;

import io.orderflow.order.domain.OrderStatus;

/**
 * The order aggregate as stored in the {@code orders} table.
 *
 * <p>Keys and indexes:
 * <ul>
 *   <li>PK {@code orderId} — the only access pattern that must be single-digit-ms.</li>
 *   <li>GSI {@code customerId-createdAt-index} — "my orders, newest first" with cursor pagination.</li>
 *   <li>GSI {@code status-createdAt-index} — operational sweeps ("everything stuck in PENDING for
 *       more than 10 minutes"), used by the runbook and the stuck-saga alarm.</li>
 * </ul>
 *
 * <p>{@code version} is a {@link DynamoDbVersionAttribute}: the enhanced client turns every write
 * into a conditional write on the version it read. Two saga events landing on the same order
 * concurrently cannot silently overwrite each other — the loser gets a
 * {@code ConditionalCheckFailedException} and is retried against fresh state by
 * {@code OrderRepository.mutate(...)}.
 */
@DynamoDbBean
public class OrderEntity {

    public static final String CUSTOMER_INDEX = "customerId-createdAt-index";
    public static final String STATUS_INDEX = "status-createdAt-index";

    private String orderId;
    private String customerId;
    private String customerEmail;
    private String status;
    private BigDecimal totalAmount;
    private String currency;
    private int lineCount;
    private String idempotencyKey;
    private String correlationId;
    private String reservationId;
    private String paymentId;
    private String failureCode;
    private String failureReason;
    private Instant createdAt;
    private Instant updatedAt;
    private Long version;
    private List<OrderTimelineEntry> timeline = new ArrayList<>();

    public OrderEntity() {
        // Required by the DynamoDB Enhanced Client.
    }

    @DynamoDbPartitionKey
    @DynamoDbAttribute("orderId")
    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    @DynamoDbSecondaryPartitionKey(indexNames = CUSTOMER_INDEX)
    @DynamoDbAttribute("customerId")
    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    @DynamoDbAttribute("customerEmail")
    public String getCustomerEmail() {
        return customerEmail;
    }

    public void setCustomerEmail(String customerEmail) {
        this.customerEmail = customerEmail;
    }

    @DynamoDbSecondaryPartitionKey(indexNames = STATUS_INDEX)
    @DynamoDbAttribute("status")
    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @DynamoDbAttribute("totalAmount")
    public BigDecimal getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(BigDecimal totalAmount) {
        this.totalAmount = totalAmount;
    }

    @DynamoDbAttribute("currency")
    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    /** Denormalised so the read-model projection never has to join {@code order_items}. */
    @DynamoDbAttribute("lineCount")
    public int getLineCount() {
        return lineCount;
    }

    public void setLineCount(int lineCount) {
        this.lineCount = lineCount;
    }

    @DynamoDbAttribute("idempotencyKey")
    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    @DynamoDbAttribute("correlationId")
    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    @DynamoDbAttribute("reservationId")
    public String getReservationId() {
        return reservationId;
    }

    public void setReservationId(String reservationId) {
        this.reservationId = reservationId;
    }

    @DynamoDbAttribute("paymentId")
    public String getPaymentId() {
        return paymentId;
    }

    public void setPaymentId(String paymentId) {
        this.paymentId = paymentId;
    }

    @DynamoDbAttribute("failureCode")
    public String getFailureCode() {
        return failureCode;
    }

    public void setFailureCode(String failureCode) {
        this.failureCode = failureCode;
    }

    @DynamoDbAttribute("failureReason")
    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
    }

    @DynamoDbSecondarySortKey(indexNames = {CUSTOMER_INDEX, STATUS_INDEX})
    @DynamoDbAttribute("createdAt")
    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    @DynamoDbAttribute("updatedAt")
    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    @DynamoDbVersionAttribute
    @DynamoDbAttribute("version")
    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    @DynamoDbAttribute("timeline")
    public List<OrderTimelineEntry> getTimeline() {
        return timeline;
    }

    public void setTimeline(List<OrderTimelineEntry> timeline) {
        this.timeline = timeline == null ? new ArrayList<>() : timeline;
    }

    @DynamoDbIgnore
    public OrderStatus orderStatus() {
        return status == null ? null : OrderStatus.valueOf(status);
    }

    /** Applies a validated status change and appends the matching timeline entry. */
    public void applyStatus(OrderStatus newStatus, OrderTimelineEntry entry) {
        this.status = newStatus.name();
        this.updatedAt = Instant.now();
        if (entry != null) {
            if (this.timeline == null) {
                this.timeline = new ArrayList<>();
            }
            this.timeline.add(entry);
        }
    }
}
