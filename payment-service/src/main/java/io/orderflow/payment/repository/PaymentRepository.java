package io.orderflow.payment.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbIndex;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.TransactWriteItemsEnhancedRequest;

import io.orderflow.common.outbox.OutboxRecord;
import io.orderflow.common.outbox.OutboxRepository;
import io.orderflow.payment.model.PaymentEntity;

/** Data access for {@code payments}, always writing the payment and its event together. */
@Repository
public class PaymentRepository {

    private final DynamoDbEnhancedClient enhancedClient;
    private final DynamoDbTable<PaymentEntity> paymentTable;
    private final DynamoDbIndex<PaymentEntity> paymentsByOrder;
    private final OutboxRepository outboxRepository;

    public PaymentRepository(DynamoDbEnhancedClient enhancedClient,
                             OutboxRepository outboxRepository,
                             @Value("${app.dynamodb.tables.payments:payments}") String paymentsTableName) {
        this.enhancedClient = enhancedClient;
        this.paymentTable = enhancedClient.table(paymentsTableName,
                TableSchema.fromBean(PaymentEntity.class));
        this.paymentsByOrder = paymentTable.index(PaymentEntity.ORDER_INDEX);
        this.outboxRepository = outboxRepository;
    }

    /**
     * Persists the attempt and the resulting event atomically.
     *
     * <p>This is the guarantee that matters for money: it is impossible to end up with a captured
     * payment whose {@code PaymentCaptured} event was never queued (the order would hang in
     * {@code INVENTORY_RESERVED} forever while the customer is charged), or with an event for a
     * payment row that does not exist.
     */
    public void saveWithEvent(PaymentEntity payment, OutboxRecord outboxRecord) {
        enhancedClient.transactWriteItems(TransactWriteItemsEnhancedRequest.builder()
                .addPutItem(paymentTable, payment)
                .addPutItem(outboxRepository.table(), outboxRecord)
                .build());
    }

    public Optional<PaymentEntity> findByOrderId(String orderId) {
        return paymentsByOrder
                .query(QueryConditional.keyEqualTo(Key.builder().partitionValue(orderId).build()))
                .stream()
                .map(Page::items)
                .flatMap(List::stream)
                // A failed attempt may be followed by a successful one; the capture wins.
                .reduce((first, second) -> second.isCaptured() ? second : first);
    }

    public Optional<PaymentEntity> findById(String paymentId) {
        return Optional.ofNullable(paymentTable.getItem(
                Key.builder().partitionValue(paymentId).build()));
    }
}
