package io.orderflow.order.model;

import java.math.BigDecimal;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

import io.orderflow.common.event.DomainEvent;

/**
 * A line of an order in the {@code order_items} table: PK {@code orderId}, SK {@code sku}.
 *
 * <p>Lines live outside the order item for two reasons: a 400 KB DynamoDB item limit is a real
 * ceiling for bulk orders, and the composite key makes "does this order contain SKU X" a direct
 * {@code GetItem} rather than a filter over an embedded list.
 */
@DynamoDbBean
public class OrderItemEntity {

    private String orderId;
    private String sku;
    private int quantity;
    private BigDecimal unitPrice;
    private BigDecimal lineTotal;

    public OrderItemEntity() {
        // Required by the DynamoDB Enhanced Client.
    }

    public static OrderItemEntity from(String orderId, DomainEvent.OrderLine line) {
        OrderItemEntity item = new OrderItemEntity();
        item.setOrderId(orderId);
        item.setSku(line.sku());
        item.setQuantity(line.quantity());
        item.setUnitPrice(line.unitPrice());
        item.setLineTotal(line.lineTotal());
        return item;
    }

    public DomainEvent.OrderLine toOrderLine() {
        return new DomainEvent.OrderLine(sku, quantity, unitPrice);
    }

    @DynamoDbPartitionKey
    @DynamoDbAttribute("orderId")
    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    @DynamoDbSortKey
    @DynamoDbAttribute("sku")
    public String getSku() {
        return sku;
    }

    public void setSku(String sku) {
        this.sku = sku;
    }

    @DynamoDbAttribute("quantity")
    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    @DynamoDbAttribute("unitPrice")
    public BigDecimal getUnitPrice() {
        return unitPrice;
    }

    public void setUnitPrice(BigDecimal unitPrice) {
        this.unitPrice = unitPrice;
    }

    @DynamoDbAttribute("lineTotal")
    public BigDecimal getLineTotal() {
        return lineTotal;
    }

    public void setLineTotal(BigDecimal lineTotal) {
        this.lineTotal = lineTotal;
    }
}
