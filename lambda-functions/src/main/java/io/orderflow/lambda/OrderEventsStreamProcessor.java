package io.orderflow.lambda;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent.DynamodbStreamRecord;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;

import io.orderflow.lambda.model.OrderReadModel;
import io.orderflow.lambda.repository.OrderProjectionRepository;

/**
 * DynamoDB Streams → read-model projection (and saga analytics).
 *
 * <p>Triggered by the {@code orders} table stream with {@code NEW_AND_OLD_IMAGES}, batch size 100,
 * and a bisect-on-error policy (see {@code infrastructure/terraform/lambda.tf}). For each changed
 * order it folds the new image into {@code order_read_model} and, when the order reaches a terminal
 * state, emits the end-to-end saga duration as a CloudWatch metric in embedded-metric format.
 *
 * <p>Two properties make this safe to re-run, which matters because stream records are delivered
 * at least once and a failed batch is retried:
 * <ul>
 *   <li>the projection is a full overwrite derived from the new image, not an increment, so
 *       replaying the same record converges to the same state;</li>
 *   <li>records are processed in stream order per partition key, so a stale image can never
 *       overwrite a newer one within a shard.</li>
 * </ul>
 *
 * <p>Note the deliberate model mix: the stream images arrive as {@code aws-lambda-java-events}'
 * own {@code AttributeValue} type (no AWS SDK dependency at all), while persistence uses the v2
 * enhanced client. Converting at this boundary keeps the handler free of a second SDK.
 */
public class OrderEventsStreamProcessor implements RequestHandler<DynamodbEvent, String> {

    private static final String EVENT_INSERT = "INSERT";
    private static final String EVENT_MODIFY = "MODIFY";
    private static final String EVENT_REMOVE = "REMOVE";

    private final OrderProjectionRepository projections;

    public OrderEventsStreamProcessor() {
        this(new OrderProjectionRepository());
    }

    /** Test seam. */
    public OrderEventsStreamProcessor(OrderProjectionRepository projections) {
        this.projections = projections;
    }

    @Override
    public String handleRequest(DynamodbEvent event, Context context) {
        int projected = 0;
        int removed = 0;
        int skipped = 0;

        for (DynamodbStreamRecord record : event.getRecords()) {
            String eventName = record.getEventName();
            if (EVENT_REMOVE.equals(eventName)) {
                String orderId = keyOf(record);
                if (orderId != null) {
                    projections.delete(orderId);
                    removed++;
                }
                continue;
            }
            if (!EVENT_INSERT.equals(eventName) && !EVENT_MODIFY.equals(eventName)) {
                skipped++;
                continue;
            }

            Map<String, AttributeValue> image = record.getDynamodb() == null
                    ? null
                    : record.getDynamodb().getNewImage();
            if (image == null || !image.containsKey("orderId")) {
                skipped++;
                continue;
            }

            OrderReadModel projection = project(image);
            projections.upsert(projection);
            projected++;

            if (projection.getSagaDurationMillis() != null) {
                emitSagaMetric(projection);
            }
        }

        String summary = "projected=" + projected + " removed=" + removed + " skipped=" + skipped;
        if (context != null && context.getLogger() != null) {
            context.getLogger().log("OrderEventsStreamProcessor " + summary + "\n");
        }
        return summary;
    }

    /** Folds a stream image into the projection shape. Visible for testing. */
    OrderReadModel project(Map<String, AttributeValue> image) {
        OrderReadModel projection = new OrderReadModel();
        projection.setOrderId(string(image, "orderId"));
        projection.setCustomerId(string(image, "customerId"));
        projection.setStatus(string(image, "status"));
        projection.setCurrency(string(image, "currency"));
        projection.setCorrelationId(string(image, "correlationId"));
        projection.setTotalAmount(number(image, "totalAmount"));
        projection.setLineCount(integer(image, "lineCount"));
        projection.setRevision(listSize(image, "timeline"));
        projection.setLastEventType(lastEventType(image));

        Instant createdAt = timestamp(image, "createdAt");
        Instant updatedAt = timestamp(image, "updatedAt");
        projection.setCreatedAt(createdAt);
        projection.setUpdatedAt(updatedAt == null ? createdAt : updatedAt);

        if (isTerminal(projection.getStatus()) && createdAt != null && updatedAt != null) {
            projection.setSagaDurationMillis(updatedAt.toEpochMilli() - createdAt.toEpochMilli());
        }
        return projection;
    }

    /**
     * CloudWatch embedded metric format: one structured log line becomes a metric, with no extra
     * API call and therefore no added latency or throttling risk in the hot path.
     */
    private void emitSagaMetric(OrderReadModel projection) {
        String emf = """
                {"_aws":{"Timestamp":%d,"CloudWatchMetrics":[{"Namespace":"OrderProcessing/Saga",\
                "Dimensions":[["Status"]],"Metrics":[{"Name":"SagaDurationMillis","Unit":"Milliseconds"}]}]},\
                "Status":"%s","SagaDurationMillis":%d,"orderId":"%s","correlationId":"%s"}"""
                .formatted(
                        System.currentTimeMillis(),
                        projection.getStatus(),
                        projection.getSagaDurationMillis(),
                        projection.getOrderId(),
                        projection.getCorrelationId() == null ? "" : projection.getCorrelationId());
        System.out.println(emf);
    }

    private static boolean isTerminal(String status) {
        return "CONFIRMED".equals(status) || "REJECTED".equals(status)
                || "CANCELLED".equals(status) || "FAILED".equals(status);
    }

    private static String keyOf(DynamodbStreamRecord record) {
        if (record.getDynamodb() == null || record.getDynamodb().getKeys() == null) {
            return null;
        }
        AttributeValue key = record.getDynamodb().getKeys().get("orderId");
        return key == null ? null : key.getS();
    }

    /** Newest timeline entry's event type, used as the projection's "what just happened". */
    private static String lastEventType(Map<String, AttributeValue> image) {
        AttributeValue timeline = image.get("timeline");
        if (timeline == null || timeline.getL() == null || timeline.getL().isEmpty()) {
            return null;
        }
        AttributeValue newest = timeline.getL().get(timeline.getL().size() - 1);
        if (newest.getM() == null) {
            return null;
        }
        return Optional.ofNullable(newest.getM().get("eventType"))
                .map(AttributeValue::getS)
                .orElse(null);
    }

    private static String string(Map<String, AttributeValue> image, String name) {
        AttributeValue value = image.get(name);
        return value == null ? null : value.getS();
    }

    private static BigDecimal number(Map<String, AttributeValue> image, String name) {
        AttributeValue value = image.get(name);
        return (value == null || value.getN() == null) ? null : new BigDecimal(value.getN());
    }

    private static int integer(Map<String, AttributeValue> image, String name) {
        AttributeValue value = image.get(name);
        return (value == null || value.getN() == null) ? 0 : Integer.parseInt(value.getN());
    }

    private static int listSize(Map<String, AttributeValue> image, String name) {
        AttributeValue value = image.get(name);
        return (value == null || value.getL() == null) ? 0 : value.getL().size();
    }

    private static Instant timestamp(Map<String, AttributeValue> image, String name) {
        String raw = string(image, name);
        return raw == null ? null : Instant.parse(raw);
    }
}
