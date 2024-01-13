package io.orderflow.inventory.model;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbIgnore;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondaryPartitionKey;

/**
 * A stock hold in {@code inventory_reservations}: PK {@code reservationId},
 * GSI {@code orderId-index} so compensation can find the hold from the order alone.
 *
 * <p>The reservation is the saga's undo record. Without it, releasing stock after a payment failure
 * would mean trusting the cancellation event to carry the exact quantities — and a replayed or
 * doctored event could then inflate inventory. Holding the authoritative quantities here means the
 * release is derived from what we actually reserved.
 */
@DynamoDbBean
public class Reservation {

    public static final String ORDER_INDEX = "orderId-index";

    public static final String STATUS_HELD = "HELD";
    public static final String STATUS_RELEASED = "RELEASED";

    private String reservationId;
    private String orderId;
    private String status;
    /** sku -> reserved quantity, the authoritative record for the release path. */
    private Map<String, Integer> quantities = new LinkedHashMap<>();
    private String correlationId;
    private Instant createdAt;
    private Instant releasedAt;

    public Reservation() {
        // Required by the DynamoDB Enhanced Client.
    }

    public static Reservation held(String orderId, Map<String, Integer> quantities, String correlationId) {
        Reservation reservation = new Reservation();
        reservation.setReservationId(UUID.randomUUID().toString());
        reservation.setOrderId(orderId);
        reservation.setStatus(STATUS_HELD);
        reservation.setQuantities(new LinkedHashMap<>(quantities));
        reservation.setCorrelationId(correlationId);
        reservation.setCreatedAt(Instant.now());
        return reservation;
    }

    @DynamoDbPartitionKey
    @DynamoDbAttribute("reservationId")
    public String getReservationId() {
        return reservationId;
    }

    public void setReservationId(String reservationId) {
        this.reservationId = reservationId;
    }

    @DynamoDbSecondaryPartitionKey(indexNames = ORDER_INDEX)
    @DynamoDbAttribute("orderId")
    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    @DynamoDbAttribute("status")
    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @DynamoDbAttribute("quantities")
    public Map<String, Integer> getQuantities() {
        return quantities;
    }

    public void setQuantities(Map<String, Integer> quantities) {
        this.quantities = quantities == null ? new LinkedHashMap<>() : quantities;
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

    @DynamoDbAttribute("releasedAt")
    public Instant getReleasedAt() {
        return releasedAt;
    }

    public void setReleasedAt(Instant releasedAt) {
        this.releasedAt = releasedAt;
    }

    @DynamoDbIgnore
    public boolean isHeld() {
        return STATUS_HELD.equals(status);
    }

    public void markReleased() {
        this.status = STATUS_RELEASED;
        this.releasedAt = Instant.now();
    }
}
