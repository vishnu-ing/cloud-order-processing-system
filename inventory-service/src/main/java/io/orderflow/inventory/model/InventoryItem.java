package io.orderflow.inventory.model;

import java.time.Instant;

import software.amazon.awssdk.enhanced.dynamodb.extensions.annotations.DynamoDbVersionAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbIgnore;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;

/**
 * Stock for one SKU in the {@code inventory} table: PK {@code sku}.
 *
 * <p>Two counters, not one. {@code stock} is what physically exists; {@code reserved} is what is
 * promised to in-flight orders. Sellable quantity is the difference. Keeping them apart means a
 * release only has to decrement {@code reserved} and can never invent physical stock, which makes
 * the compensating action safe to apply more than once.
 *
 * <p>{@code version} is a {@link DynamoDbVersionAttribute}, so two orders competing for the last
 * unit of a SKU serialise: the second write fails its condition and is retried against the value
 * the first one wrote. That is the difference between overselling and not.
 */
@DynamoDbBean
public class InventoryItem {

    private String sku;
    private String description;
    private int stock;
    private int reserved;
    private Instant updatedAt;
    private Long version;

    public InventoryItem() {
        // Required by the DynamoDB Enhanced Client.
    }

    @DynamoDbPartitionKey
    @DynamoDbAttribute("sku")
    public String getSku() {
        return sku;
    }

    public void setSku(String sku) {
        this.sku = sku;
    }

    @DynamoDbAttribute("description")
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @DynamoDbAttribute("stock")
    public int getStock() {
        return stock;
    }

    public void setStock(int stock) {
        this.stock = stock;
    }

    @DynamoDbAttribute("reserved")
    public int getReserved() {
        return reserved;
    }

    public void setReserved(int reserved) {
        this.reserved = reserved;
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

    @DynamoDbIgnore
    public int available() {
        return stock - reserved;
    }

    @DynamoDbIgnore
    public boolean canReserve(int quantity) {
        return available() >= quantity;
    }

    /** Holds {@code quantity} units for an order. Caller must have checked {@link #canReserve}. */
    public void reserve(int quantity) {
        this.reserved = this.reserved + quantity;
        this.updatedAt = Instant.now();
    }

    /** Compensating action. Clamped at zero so a double release can never go negative. */
    public void release(int quantity) {
        this.reserved = Math.max(0, this.reserved - quantity);
        this.updatedAt = Instant.now();
    }
}
