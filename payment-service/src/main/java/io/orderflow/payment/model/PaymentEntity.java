package io.orderflow.payment.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbIgnore;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondaryPartitionKey;

/**
 * A capture attempt in the {@code payments} table: PK {@code paymentId},
 * GSI {@code orderId-index}.
 *
 * <p>The row is written whether the capture succeeded or failed. A failed attempt is evidence, not
 * noise: it is what lets support answer "were we charged?" without calling the gateway, and it is
 * what the idempotent re-check reads before deciding to charge again.
 */
@DynamoDbBean
public class PaymentEntity {

    public static final String ORDER_INDEX = "orderId-index";

    public static final String STATUS_CAPTURED = "CAPTURED";
    public static final String STATUS_FAILED = "FAILED";

    private String paymentId;
    private String orderId;
    private String reservationId;
    private String status;
    private BigDecimal amount;
    private String currency;
    private String authorizationCode;
    private String failureCode;
    private String failureReason;
    private String gatewayIdempotencyKey;
    private String correlationId;
    private Instant createdAt;

    public PaymentEntity() {
        // Required by the DynamoDB Enhanced Client.
    }

    public static PaymentEntity captured(String orderId,
                                         String reservationId,
                                         BigDecimal amount,
                                         String currency,
                                         String authorizationCode,
                                         String gatewayIdempotencyKey,
                                         String correlationId) {
        PaymentEntity payment = base(orderId, reservationId, amount, currency,
                gatewayIdempotencyKey, correlationId);
        payment.setStatus(STATUS_CAPTURED);
        payment.setAuthorizationCode(authorizationCode);
        return payment;
    }

    public static PaymentEntity failed(String orderId,
                                       String reservationId,
                                       BigDecimal amount,
                                       String currency,
                                       String failureCode,
                                       String failureReason,
                                       String gatewayIdempotencyKey,
                                       String correlationId) {
        PaymentEntity payment = base(orderId, reservationId, amount, currency,
                gatewayIdempotencyKey, correlationId);
        payment.setStatus(STATUS_FAILED);
        payment.setFailureCode(failureCode);
        payment.setFailureReason(failureReason);
        return payment;
    }

    private static PaymentEntity base(String orderId,
                                      String reservationId,
                                      BigDecimal amount,
                                      String currency,
                                      String gatewayIdempotencyKey,
                                      String correlationId) {
        PaymentEntity payment = new PaymentEntity();
        payment.setPaymentId(UUID.randomUUID().toString());
        payment.setOrderId(orderId);
        payment.setReservationId(reservationId);
        payment.setAmount(amount);
        payment.setCurrency(currency);
        payment.setGatewayIdempotencyKey(gatewayIdempotencyKey);
        payment.setCorrelationId(correlationId);
        payment.setCreatedAt(Instant.now());
        return payment;
    }

    @DynamoDbPartitionKey
    @DynamoDbAttribute("paymentId")
    public String getPaymentId() {
        return paymentId;
    }

    public void setPaymentId(String paymentId) {
        this.paymentId = paymentId;
    }

    @DynamoDbSecondaryPartitionKey(indexNames = ORDER_INDEX)
    @DynamoDbAttribute("orderId")
    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    @DynamoDbAttribute("reservationId")
    public String getReservationId() {
        return reservationId;
    }

    public void setReservationId(String reservationId) {
        this.reservationId = reservationId;
    }

    @DynamoDbAttribute("status")
    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @DynamoDbAttribute("amount")
    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    @DynamoDbAttribute("currency")
    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    @DynamoDbAttribute("authorizationCode")
    public String getAuthorizationCode() {
        return authorizationCode;
    }

    public void setAuthorizationCode(String authorizationCode) {
        this.authorizationCode = authorizationCode;
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

    @DynamoDbAttribute("gatewayIdempotencyKey")
    public String getGatewayIdempotencyKey() {
        return gatewayIdempotencyKey;
    }

    public void setGatewayIdempotencyKey(String gatewayIdempotencyKey) {
        this.gatewayIdempotencyKey = gatewayIdempotencyKey;
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

    @DynamoDbIgnore
    public boolean isCaptured() {
        return STATUS_CAPTURED.equals(status);
    }
}
