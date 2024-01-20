package io.orderflow.order;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import io.orderflow.common.event.DomainEvent;
import io.orderflow.common.event.EventEnvelope;
import io.orderflow.common.event.EventJson;
import io.orderflow.common.event.Topics;
import io.orderflow.common.observability.CorrelationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end saga test for order-service against real infrastructure: a KRaft Kafka broker and
 * DynamoDB on LocalStack, both from Testcontainers.
 *
 * <p>order-service is the system under test; this class plays the parts of inventory-service and
 * payment-service by publishing their events onto the real topics. That is the right seam for an
 * integration test of a choreographed saga — it exercises the outbox drain, the Kafka serde, the
 * idempotency guard, the state machine and the DynamoDB writes, while keeping the assertions about
 * one service's behaviour.
 *
 * <p>What it proves:
 * <ol>
 *   <li><strong>happy path</strong>: POST → {@code orders.created} really reaches Kafka via the
 *       outbox → {@code InventoryReserved} then {@code PaymentCaptured} drive the order to
 *       {@code CONFIRMED} and emit {@code orders.confirmed};</li>
 *   <li><strong>compensation</strong>: {@code PaymentFailed} moves the order to {@code REJECTED}
 *       and emits {@code orders.cancelled} so inventory releases the hold;</li>
 *   <li><strong>idempotency</strong>: replaying the same {@code PaymentCaptured} envelope does not
 *       double-apply, and a repeated {@code Idempotency-Key} does not create a second order.</li>
 * </ol>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
class SagaOrchestrationIT {

    private static final DockerImageName KAFKA_IMAGE =
            DockerImageName.parse("confluentinc/cp-kafka:7.7.0");
    private static final DockerImageName LOCALSTACK_IMAGE =
            DockerImageName.parse("localstack/localstack:3.8");

    private static final KafkaContainer KAFKA = new KafkaContainer(KAFKA_IMAGE).withKraft();
    private static final LocalStackContainer LOCALSTACK = new LocalStackContainer(LOCALSTACK_IMAGE)
            .withServices(LocalStackContainer.Service.DYNAMODB);

    private static DynamoDbClient dynamoDb;

    @Autowired
    private TestRestTemplate rest;

    @LocalServerPort
    private int port;

    @BeforeAll
    static void startInfrastructure() {
        KAFKA.start();
        LOCALSTACK.start();

        // DefaultCredentialsProvider picks these up, so production code needs no test-only branch.
        System.setProperty("aws.accessKeyId", "test");
        System.setProperty("aws.secretAccessKey", "test");

        dynamoDb = DynamoDbClient.builder()
                .endpointOverride(LOCALSTACK.getEndpoint())
                .region(Region.of(LOCALSTACK.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")))
                .build();

        createOrdersTable();
        createOrderItemsTable();
        createOutboxTable();
        createProcessedEventsTable();
    }

    @AfterAll
    static void stopInfrastructure() {
        if (dynamoDb != null) {
            dynamoDb.close();
        }
        LOCALSTACK.stop();
        KAFKA.stop();
    }

    @DynamicPropertySource
    static void wireInfrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("aws.endpoint-url", () -> LOCALSTACK.getEndpoint().toString());
        registry.add("aws.region", LOCALSTACK::getRegion);
        // Drain the outbox aggressively so the test does not spend its budget waiting.
        registry.add("app.outbox.initial-delay-ms", () -> "200");
        registry.add("app.outbox.poll-interval-ms", () -> "200");
        registry.add("app.kafka.listener.concurrency", () -> "1");
    }

    @Test
    @DisplayName("happy path: created -> reserved -> captured -> confirmed")
    void happyPathConfirmsTheOrder() {
        try (KafkaConsumer<String, String> confirmed = consumerFor(Topics.ORDERS_CONFIRMED)) {
            String orderId = createOrder("customer-happy", new BigDecimal("42.50"));

            // The outbox must publish OrderCreated without any further prompting.
            EventEnvelope<DomainEvent> created =
                    awaitEvent(consumerFor(Topics.ORDERS_CREATED), orderId);
            assertThat(created.payload()).isInstanceOf(DomainEvent.OrderCreated.class);

            String reservationId = UUID.randomUUID().toString();
            publish(Topics.INVENTORY_RESERVED, orderId, new DomainEvent.InventoryReserved(
                    orderId, reservationId, "customer-happy",
                    List.of(new DomainEvent.OrderLine("SKU-BOOK-1", 1, new BigDecimal("42.50"))),
                    new BigDecimal("42.50"), "USD"));

            awaitStatus(orderId, "INVENTORY_RESERVED");

            String paymentId = UUID.randomUUID().toString();
            EventEnvelope<DomainEvent.PaymentCaptured> capture = envelope(
                    new DomainEvent.PaymentCaptured(orderId, paymentId, reservationId,
                            new BigDecimal("42.50"), "USD", "AUTH-OK"));
            publish(Topics.PAYMENTS_CAPTURED, orderId, capture);

            awaitStatus(orderId, "CONFIRMED");

            EventEnvelope<DomainEvent> confirmation = awaitEvent(confirmed, orderId);
            assertThat(confirmation.payload()).isInstanceOf(DomainEvent.OrderConfirmed.class);
            assertThat(confirmation.causationId()).isEqualTo(capture.eventId());

            // Re-delivering the exact same capture must not disturb a confirmed order.
            publish(Topics.PAYMENTS_CAPTURED, orderId, capture);
            awaitStatus(orderId, "CONFIRMED");

            String timeline = rest.getForObject(url("/api/v1/orders/" + orderId + "/timeline"), String.class);
            assertThat(timeline)
                    .contains("ORDER_CREATED")
                    .contains("INVENTORY_RESERVED")
                    .contains("PAYMENT_CAPTURED")
                    .contains("ORDER_CONFIRMED");
        }
    }

    @Test
    @DisplayName("compensation: payment failure rejects the order and emits orders.cancelled")
    void paymentFailureTriggersCompensation() {
        try (KafkaConsumer<String, String> cancelled = consumerFor(Topics.ORDERS_CANCELLED)) {
            String orderId = createOrder("customer-decline", new BigDecimal("99.00"));
            String reservationId = UUID.randomUUID().toString();

            publish(Topics.INVENTORY_RESERVED, orderId, new DomainEvent.InventoryReserved(
                    orderId, reservationId, "customer-decline",
                    List.of(new DomainEvent.OrderLine("SKU-TV-1", 1, new BigDecimal("99.00"))),
                    new BigDecimal("99.00"), "USD"));
            awaitStatus(orderId, "INVENTORY_RESERVED");

            publish(Topics.PAYMENTS_FAILED, orderId, new DomainEvent.PaymentFailed(
                    orderId, UUID.randomUUID().toString(), reservationId,
                    "CARD_DECLINED", "Issuer declined the transaction", false));

            awaitStatus(orderId, "REJECTED");

            EventEnvelope<DomainEvent> compensation = awaitEvent(cancelled, orderId);
            assertThat(compensation.payload()).isInstanceOf(DomainEvent.OrderCancelled.class);
            DomainEvent.OrderCancelled payload = (DomainEvent.OrderCancelled) compensation.payload();
            // The reservation id is what lets inventory-service release exactly what it held.
            assertThat(payload.reservationId()).isEqualTo(reservationId);
            assertThat(payload.reason()).contains("CARD_DECLINED");
        }
    }

    @Test
    @DisplayName("idempotency: the same Idempotency-Key returns the original order")
    void repeatedIdempotencyKeyDoesNotCreateASecondOrder() {
        String key = UUID.randomUUID().toString();

        ResponseEntity<String> first = post(key, "customer-idem", new BigDecimal("15.00"));
        ResponseEntity<String> second = post(key, "customer-idem", new BigDecimal("15.00"));

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(orderIdOf(second)).isEqualTo(orderIdOf(first));
    }

    // ------------------------------------------------------------------ helpers

    private String createOrder(String customerId, BigDecimal unitPrice) {
        ResponseEntity<String> response = post(UUID.randomUUID().toString(), customerId, unitPrice);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        return orderIdOf(response);
    }

    private ResponseEntity<String> post(String idempotencyKey, String customerId, BigDecimal unitPrice) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set(CorrelationContext.IDEMPOTENCY_KEY_HEADER, idempotencyKey);
        headers.set(CorrelationContext.CORRELATION_ID_HEADER, "it-" + idempotencyKey);

        String body = """
                {
                  "customerId": "%s",
                  "customerEmail": "%s@example.com",
                  "currency": "USD",
                  "lines": [
                    { "sku": "SKU-BOOK-1", "quantity": 1, "unitPrice": %s }
                  ]
                }""".formatted(customerId, customerId, unitPrice.toPlainString());

        return rest.exchange(url("/api/v1/orders"), HttpMethod.POST,
                new HttpEntity<>(body, headers), String.class);
    }

    private String orderIdOf(ResponseEntity<String> response) {
        try {
            return EventJson.mapper().readTree(response.getBody()).path("orderId").asText();
        } catch (Exception e) {
            throw new IllegalStateException("Unparseable order response: " + response.getBody(), e);
        }
    }

    private void awaitStatus(String orderId, String expectedStatus) {
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(250))
                .untilAsserted(() -> {
                    String body = rest.getForObject(url("/api/v1/orders/" + orderId), String.class);
                    assertThat(EventJson.mapper().readTree(body).path("status").asText())
                            .isEqualTo(expectedStatus);
                });
    }

    private <T extends DomainEvent> void publish(String topic, String key, T payload) {
        publish(topic, key, envelope(payload));
    }

    private void publish(String topic, String key, EventEnvelope<? extends DomainEvent> envelope) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<String, String> producer =
                     new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
            producer.send(new ProducerRecord<>(topic, key, EventJson.toJson(envelope)))
                    .get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to publish to " + topic, e);
        }
    }

    private <T extends DomainEvent> EventEnvelope<T> envelope(T payload) {
        return EventEnvelope.start(payload, "integration-test", "it-" + payload.orderId());
    }

    private KafkaConsumer<String, String> consumerFor(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-" + topic + "-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    private EventEnvelope<DomainEvent> awaitEvent(KafkaConsumer<String, String> consumer, String orderId) {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : records) {
                EventEnvelope<DomainEvent> envelope = EventJson.fromJson(record.value());
                if (orderId.equals(envelope.payload().orderId())) {
                    return envelope;
                }
            }
        }
        throw new AssertionError("No event for order " + orderId + " within the timeout");
    }

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    // ------------------------------------------------------------------ table fixtures

    private static void createOrdersTable() {
        dynamoDb.createTable(CreateTableRequest.builder()
                .tableName("orders")
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .attributeDefinitions(
                        attribute("orderId"), attribute("customerId"),
                        attribute("status"), attribute("createdAt"))
                .keySchema(hashKey("orderId"))
                .globalSecondaryIndexes(
                        index("customerId-createdAt-index", "customerId", "createdAt"),
                        index("status-createdAt-index", "status", "createdAt"))
                .build());
    }

    private static void createOrderItemsTable() {
        dynamoDb.createTable(CreateTableRequest.builder()
                .tableName("order_items")
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .attributeDefinitions(attribute("orderId"), attribute("sku"))
                .keySchema(hashKey("orderId"), rangeKey("sku"))
                .build());
    }

    private static void createOutboxTable() {
        dynamoDb.createTable(CreateTableRequest.builder()
                .tableName("outbox")
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .attributeDefinitions(attribute("recordId"), attribute("status"), attribute("createdAt"))
                .keySchema(hashKey("recordId"))
                .globalSecondaryIndexes(index("status-createdAt-index", "status", "createdAt"))
                .build());
    }

    private static void createProcessedEventsTable() {
        dynamoDb.createTable(CreateTableRequest.builder()
                .tableName("processed_events")
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .attributeDefinitions(attribute("consumerGroup"), attribute("eventId"))
                .keySchema(hashKey("consumerGroup"), rangeKey("eventId"))
                .build());
    }

    private static AttributeDefinition attribute(String name) {
        return AttributeDefinition.builder()
                .attributeName(name)
                .attributeType(ScalarAttributeType.S)
                .build();
    }

    private static KeySchemaElement hashKey(String name) {
        return KeySchemaElement.builder().attributeName(name).keyType(KeyType.HASH).build();
    }

    private static KeySchemaElement rangeKey(String name) {
        return KeySchemaElement.builder().attributeName(name).keyType(KeyType.RANGE).build();
    }

    private static GlobalSecondaryIndex index(String name, String partitionKey, String sortKey) {
        return GlobalSecondaryIndex.builder()
                .indexName(name)
                .keySchema(hashKey(partitionKey), rangeKey(sortKey))
                .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                .build();
    }
}
