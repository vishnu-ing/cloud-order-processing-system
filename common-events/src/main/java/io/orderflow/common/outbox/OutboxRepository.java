package io.orderflow.common.outbox;

import java.util.List;
import java.util.Map;

import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbIndex;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.UpdateItemEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

/**
 * Data access for the {@code outbox} table.
 *
 * <p>Exposes the underlying {@link DynamoDbTable} so producing services can enlist an outbox put
 * into the same {@code TransactWriteItems} request as their aggregate write.
 */
public class OutboxRepository {

    private final DynamoDbTable<OutboxRecord> table;
    private final DynamoDbIndex<OutboxRecord> statusIndex;
    private final String source;

    /**
     * @param source the owning service's name. The table is shared by every producing service, so
     *               the drain is filtered to rows this service wrote; otherwise each service would
     *               publish every other service's events as well.
     */
    public OutboxRepository(DynamoDbEnhancedClient enhancedClient, String tableName, String source) {
        this.table = enhancedClient.table(tableName, TableSchema.fromBean(OutboxRecord.class));
        this.statusIndex = table.index(OutboxRecord.STATUS_INDEX);
        this.source = source;
    }

    public DynamoDbTable<OutboxRecord> table() {
        return table;
    }

    public void save(OutboxRecord record) {
        table.putItem(record);
    }

    /**
     * Oldest-first page of this service's records still awaiting publication. Reading through the
     * {@code status-createdAt-index} keeps the drain query O(pending) instead of O(table).
     *
     * <p>The {@code source} filter is applied after the key condition, so a page can come back
     * short or empty when other services have a backlog. The page stream is consumed lazily until
     * {@code limit} own records are found or the pending range is exhausted, which means one
     * service's backlog can never starve another service's drain.
     */
    public List<OutboxRecord> findPending(int limit) {
        QueryEnhancedRequest.Builder request = QueryEnhancedRequest.builder()
                .queryConditional(QueryConditional.keyEqualTo(
                        Key.builder().partitionValue(OutboxRecord.STATUS_PENDING).build()))
                .limit(limit)
                .scanIndexForward(true);
        if (source != null) {
            request.filterExpression(Expression.builder()
                    .expression("#source = :source")
                    .expressionNames(Map.of("#source", "source"))
                    .expressionValues(Map.of(":source", AttributeValue.fromS(source)))
                    .build());
        }

        return statusIndex.query(request.build()).stream()
                .map(Page::items)
                .flatMap(List::stream)
                .limit(limit)
                .toList();
    }

    /**
     * Flips a record to {@code PUBLISHED}, but only if it is still {@code PENDING}.
     *
     * <p>The condition is what makes it safe to run the publisher on every replica: two schedulers
     * may pick up the same row, both may send to Kafka (hence at-least-once), but only one wins the
     * status flip and the duplicate is absorbed by consumer-side idempotency.
     *
     * @return {@code true} when this caller won the race
     */
    public boolean markPublished(OutboxRecord record) {
        record.markPublished();
        return conditionalUpdate(record, OutboxRecord.STATUS_PENDING);
    }

    /** Records a failed publish attempt, tripping the record to {@code FAILED} past max attempts. */
    public void markAttemptFailed(OutboxRecord record, String error, int maxAttempts) {
        record.markAttemptFailed(error, maxAttempts);
        conditionalUpdate(record, OutboxRecord.STATUS_PENDING);
    }

    private boolean conditionalUpdate(OutboxRecord record, String expectedStatus) {
        Expression condition = Expression.builder()
                .expression("#status = :expected")
                .expressionNames(Map.of("#status", "status"))
                .expressionValues(Map.of(":expected", AttributeValue.fromS(expectedStatus)))
                .build();
        try {
            table.updateItem(UpdateItemEnhancedRequest.builder(OutboxRecord.class)
                    .item(record)
                    .conditionExpression(condition)
                    .ignoreNulls(Boolean.FALSE)
                    .build());
            return true;
        } catch (ConditionalCheckFailedException e) {
            return false;
        }
    }
}
