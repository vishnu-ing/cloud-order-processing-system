package io.orderflow.common.idempotency;

import java.util.Optional;

import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.PutItemEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

/**
 * Conditional-write access to {@code processed_events}.
 *
 * <p>{@link #claim(ProcessedEvent)} is the primitive the whole exactly-once story rests on: a
 * single-round-trip {@code PutItem} with {@code attribute_not_exists(consumerGroup)}. DynamoDB
 * evaluates the condition inside the item's partition, so the check-and-set is atomic even with
 * dozens of consumer threads racing on the same event.
 */
public class ProcessedEventRepository {

    private static final Expression NOT_EXISTS = Expression.builder()
            .expression("attribute_not_exists(consumerGroup)")
            .build();

    private final DynamoDbTable<ProcessedEvent> table;

    public ProcessedEventRepository(DynamoDbEnhancedClient enhancedClient, String tableName) {
        this.table = enhancedClient.table(tableName, TableSchema.fromBean(ProcessedEvent.class));
    }

    /**
     * @return {@code true} when this caller inserted the marker (first delivery),
     *         {@code false} when the marker already existed (duplicate delivery)
     */
    public boolean claim(ProcessedEvent marker) {
        try {
            table.putItem(PutItemEnhancedRequest.builder(ProcessedEvent.class)
                    .item(marker)
                    .conditionExpression(NOT_EXISTS)
                    .build());
            return true;
        } catch (ConditionalCheckFailedException e) {
            return false;
        }
    }

    /**
     * Drops a marker so the event can be processed again.
     *
     * <p>Called when the handler threw after the claim: without this the Kafka retry would be
     * short-circuited as a "duplicate" and the saga step would be silently lost.
     */
    public void release(String consumerGroup, String eventId) {
        table.deleteItem(Key.builder()
                .partitionValue(consumerGroup)
                .sortValue(eventId)
                .build());
    }

    public boolean wasProcessed(String consumerGroup, String eventId) {
        return find(consumerGroup, eventId).isPresent();
    }

    /**
     * Reads a marker back. Used by {@code POST /api/v1/orders} to answer a retried request with
     * the order the first attempt created, instead of a 409 the client cannot act on.
     */
    public Optional<ProcessedEvent> find(String consumerGroup, String eventId) {
        return Optional.ofNullable(table.getItem(Key.builder()
                .partitionValue(consumerGroup)
                .sortValue(eventId)
                .build()));
    }
}
