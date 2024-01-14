package io.orderflow.order.repository;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbIndex;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.TransactWriteItemsEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

import io.orderflow.common.error.RetryableException;
import io.orderflow.common.observability.SagaMetrics;
import io.orderflow.common.outbox.OutboxRecord;
import io.orderflow.common.outbox.OutboxRepository;
import io.orderflow.order.model.OrderEntity;
import io.orderflow.order.model.OrderItemEntity;
import io.orderflow.order.support.PageCursor;

/**
 * Persistence for the order aggregate, and the place where the two hardest guarantees in this
 * service live: <em>atomic state change + event emission</em>, and <em>safe concurrent updates</em>.
 *
 * <p>Every mutation goes through {@link #mutate(String, Function)}, which
 *
 * <ol>
 *   <li>reads the current aggregate (including its {@code version});</li>
 *   <li>lets the caller mutate it and optionally produce an outbox record;</li>
 *   <li>writes the order and the outbox row in one {@code TransactWriteItems} call, guarded by the
 *       version condition the enhanced client generates;</li>
 *   <li>retries the whole read-modify-write on a version conflict with jittered back-off.</li>
 * </ol>
 *
 * <p>That is the outbox pattern in its useful form: there is no window in which an order is
 * confirmed but the confirmation event was never recorded, or vice versa.
 */
@Repository
public class OrderRepository {

    private static final Logger log = LoggerFactory.getLogger(OrderRepository.class);

    private final DynamoDbEnhancedClient enhancedClient;
    private final DynamoDbTable<OrderEntity> orderTable;
    private final DynamoDbTable<OrderItemEntity> orderItemTable;
    private final DynamoDbIndex<OrderEntity> customerIndex;
    private final DynamoDbIndex<OrderEntity> statusIndex;
    private final OutboxRepository outboxRepository;
    private final SagaMetrics metrics;
    private final int maxConflictRetries;

    public OrderRepository(DynamoDbEnhancedClient enhancedClient,
                           OutboxRepository outboxRepository,
                           SagaMetrics metrics,
                           @Value("${app.dynamodb.tables.orders:orders}") String ordersTable,
                           @Value("${app.dynamodb.tables.order-items:order_items}") String orderItemsTable,
                           @Value("${app.orders.max-conflict-retries:5}") int maxConflictRetries) {
        this.enhancedClient = enhancedClient;
        this.orderTable = enhancedClient.table(ordersTable, TableSchema.fromBean(OrderEntity.class));
        this.orderItemTable = enhancedClient.table(orderItemsTable, TableSchema.fromBean(OrderItemEntity.class));
        this.customerIndex = orderTable.index(OrderEntity.CUSTOMER_INDEX);
        this.statusIndex = orderTable.index(OrderEntity.STATUS_INDEX);
        this.outboxRepository = outboxRepository;
        this.metrics = metrics;
        this.maxConflictRetries = maxConflictRetries;
    }

    /** One page of query results plus the cursor for the next page ({@code null} when exhausted). */
    public record PageResult<T>(List<T> items, String nextCursor) {
    }

    /**
     * Creates the order, its lines and the {@code OrderCreated} outbox row in a single transaction.
     *
     * <p>DynamoDB caps a transaction at 100 items; the request DTO caps an order at 25 lines, so
     * the limit is enforced before we ever reach the wire.
     */
    public void create(OrderEntity order, List<OrderItemEntity> items, OutboxRecord outboxRecord) {
        TransactWriteItemsEnhancedRequest.Builder request = TransactWriteItemsEnhancedRequest.builder()
                .addPutItem(orderTable, order)
                .addPutItem(outboxRepository.table(), outboxRecord);
        items.forEach(item -> request.addPutItem(orderItemTable, item));
        try {
            enhancedClient.transactWriteItems(request.build());
        } catch (TransactionCanceledException e) {
            throw new RetryableException("Order creation transaction cancelled: " + e.getMessage(), e);
        }
    }

    public Optional<OrderEntity> findById(String orderId) {
        return Optional.ofNullable(orderTable.getItem(Key.builder().partitionValue(orderId).build()));
    }

    public List<OrderItemEntity> findItems(String orderId) {
        return orderItemTable
                .query(QueryConditional.keyEqualTo(Key.builder().partitionValue(orderId).build()))
                .stream()
                .map(Page::items)
                .flatMap(List::stream)
                .toList();
    }

    /**
     * Read-modify-write with optimistic concurrency.
     *
     * @param mutator mutates the entity in place and returns the event to emit, if any
     * @return the persisted entity, or {@link Optional#empty()} if the order does not exist
     */
    public Optional<OrderEntity> mutate(String orderId, Function<OrderEntity, Optional<OutboxRecord>> mutator) {
        for (int attempt = 1; attempt <= maxConflictRetries; attempt++) {
            Optional<OrderEntity> current = findById(orderId);
            if (current.isEmpty()) {
                return Optional.empty();
            }
            OrderEntity order = current.get();
            Optional<OutboxRecord> event = mutator.apply(order);
            try {
                TransactWriteItemsEnhancedRequest.Builder request = TransactWriteItemsEnhancedRequest.builder()
                        .addPutItem(orderTable, order);
                event.ifPresent(record -> request.addPutItem(outboxRepository.table(), record));
                enhancedClient.transactWriteItems(request.build());
                return Optional.of(order);
            } catch (TransactionCanceledException | ConditionalCheckFailedException e) {
                metrics.stepFailed("order-service", "ORDER_UPDATE", "version_conflict");
                log.warn("Optimistic lock conflict on order {} (attempt {}/{})",
                        orderId, attempt, maxConflictRetries);
                backOff(attempt);
            }
        }
        // Give up and let the Kafka error handler retry the whole event: the aggregate is hot.
        throw new RetryableException("Exhausted optimistic-lock retries for order " + orderId);
    }

    public PageResult<OrderEntity> findByCustomer(String customerId, String cursor, int limit) {
        return queryIndex(customerIndex, customerId, cursor, limit);
    }

    public PageResult<OrderEntity> findByStatus(String status, String cursor, int limit) {
        return queryIndex(statusIndex, status, cursor, limit);
    }

    private PageResult<OrderEntity> queryIndex(DynamoDbIndex<OrderEntity> index,
                                               String partitionValue,
                                               String cursor,
                                               int limit) {
        Map<String, AttributeValue> startKey = PageCursor.decode(cursor);
        QueryEnhancedRequest.Builder request = QueryEnhancedRequest.builder()
                .queryConditional(QueryConditional.keyEqualTo(
                        Key.builder().partitionValue(partitionValue).build()))
                .limit(limit)
                // Newest first: the only ordering a customer-facing list ever wants.
                .scanIndexForward(false);
        if (!startKey.isEmpty()) {
            request.exclusiveStartKey(startKey);
        }

        Optional<Page<OrderEntity>> page = index.query(request.build()).stream().findFirst();
        return page.map(p -> new PageResult<>(p.items(), PageCursor.encode(p.lastEvaluatedKey())))
                .orElseGet(() -> new PageResult<>(List.of(), null));
    }

    private void backOff(int attempt) {
        long baseMillis = Math.min(200L, 20L * attempt);
        long jitter = ThreadLocalRandom.current().nextLong(10, 40);
        try {
            Thread.sleep(Duration.ofMillis(baseMillis + jitter).toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RetryableException("Interrupted while backing off on optimistic lock conflict", e);
        }
    }
}
