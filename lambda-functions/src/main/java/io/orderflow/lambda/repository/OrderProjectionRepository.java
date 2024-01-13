package io.orderflow.lambda.repository;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

import io.orderflow.lambda.model.OrderReadModel;

/**
 * Read-model persistence for the Lambda slice.
 *
 * <p>The client is built once in a static initializer so it is created during the INIT phase of a
 * cold start, where Lambda gives the sandbox a full CPU burst, and then reused across warm
 * invocations. Doing it lazily inside {@code handleRequest} would pay the ~400 ms SDK bootstrap on
 * the first billed millisecond of a real request instead.
 */
public class OrderProjectionRepository {

    private static final DynamoDbClient DYNAMO_DB = buildClient();
    private static final DynamoDbEnhancedClient ENHANCED = DynamoDbEnhancedClient.builder()
            .dynamoDbClient(DYNAMO_DB)
            .build();

    private final DynamoDbTable<OrderReadModel> table;

    public OrderProjectionRepository() {
        this(System.getenv().getOrDefault("READ_MODEL_TABLE", "order_read_model"));
    }

    public OrderProjectionRepository(String tableName) {
        this.table = ENHANCED.table(tableName, TableSchema.fromBean(OrderReadModel.class));
    }

    private static DynamoDbClient buildClient() {
        String region = System.getenv().getOrDefault("AWS_REGION", "us-east-1");
        String endpoint = System.getenv("AWS_ENDPOINT_URL");

        DynamoDbClientBuilder builder = DynamoDbClient.builder()
                .region(Region.of(region))
                .httpClient(UrlConnectionHttpClient.create());
        if (endpoint != null && !endpoint.isBlank()) {
            builder = builder.endpointOverride(URI.create(endpoint));
        }
        return builder.build();
    }

    public Optional<OrderReadModel> findById(String orderId) {
        return Optional.ofNullable(table.getItem(Key.builder().partitionValue(orderId).build()));
    }

    public void upsert(OrderReadModel projection) {
        table.putItem(projection);
    }

    public void delete(String orderId) {
        table.deleteItem(Key.builder().partitionValue(orderId).build());
    }

    /** Most recently updated orders for a customer, newest first. */
    public List<OrderReadModel> findByCustomer(String customerId, int limit) {
        QueryEnhancedRequest request = QueryEnhancedRequest.builder()
                .queryConditional(QueryConditional.keyEqualTo(
                        Key.builder().partitionValue(customerId).build()))
                .limit(limit)
                .scanIndexForward(false)
                .build();

        return table.index(OrderReadModel.CUSTOMER_INDEX).query(request).stream()
                .map(Page::items)
                .flatMap(List::stream)
                .limit(limit)
                .toList();
    }
}
