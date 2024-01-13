package io.orderflow.lambda.model;

import java.math.BigDecimal;
import java.time.Instant;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondaryPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondarySortKey;

/**
 * Denormalised projection in {@code order_read_model}: PK {@code orderId},
 * GSI {@code customerId-updatedAt-index}.
 *
 * <p>Why a second copy of data we already have: the write model is tuned for the saga (versioned,
 * transactional, embedded timeline) and every read against it competes with saga writes on the same
 * partitions. The projection is append-mostly, carries pre-computed fields a status page wants
 * (line count, saga duration), and is served by a Lambda behind API Gateway — so a traffic spike on
 * "where is my order" cannot slow down order intake.
 *
 * <p>It is eventually consistent by construction: DynamoDB Streams deliver within roughly a second,
 * which is well inside what a status page needs.
 */
@DynamoDbBean
public class OrderReadModel {

    public static final String CUSTOMER_INDEX = "customerId-updatedAt-index";

    private String orderId;
    private String customerId;
    private String status;
    private BigDecimal totalAmount;
    private String currency;
    private int lineCount;
    private String lastEventType;
    private String correlationId;
    private Instant createdAt;
    private Instant updatedAt;
    private Long sagaDurationMillis;
    private int revision;

    public OrderReadModel() {
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

    @DynamoDbAttribute("lineCount")
    public int getLineCount() {
        return lineCount;
    }

    public void setLineCount(int lineCount) {
        this.lineCount = lineCount;
    }

    @DynamoDbAttribute("lastEventType")
    public String getLastEventType() {
        return lastEventType;
    }

    public void setLastEventType(String lastEventType) {
        this.lastEventType = lastEventType;
    }

    @DynamoDbAttribute("correlationId")
    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    @DynamoDbAttribute("createdAt")
    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    @DynamoDbSecondarySortKey(indexNames = CUSTOMER_INDEX)
    @DynamoDbAttribute("updatedAt")
    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    /** End-to-end saga latency, populated once the order reaches a terminal state. */
    @DynamoDbAttribute("sagaDurationMillis")
    public Long getSagaDurationMillis() {
        return sagaDurationMillis;
    }

    public void setSagaDurationMillis(Long sagaDurationMillis) {
        this.sagaDurationMillis = sagaDurationMillis;
    }

    /** Number of stream records folded into this projection; useful when debugging replays. */
    @DynamoDbAttribute("revision")
    public int getRevision() {
        return revision;
    }

    public void setRevision(int revision) {
        this.revision = revision;
    }
}
