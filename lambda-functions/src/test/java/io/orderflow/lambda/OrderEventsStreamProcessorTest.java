package io.orderflow.lambda;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent.DynamodbStreamRecord;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.orderflow.lambda.model.OrderReadModel;
import io.orderflow.lambda.repository.OrderProjectionRepository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Covers the projection contract: field mapping, saga-duration analytics on terminal states,
 * deletion on REMOVE, and the tolerance for malformed or irrelevant stream records that would
 * otherwise poison a whole batch.
 */
@ExtendWith(MockitoExtension.class)
class OrderEventsStreamProcessorTest {

    @Mock
    private OrderProjectionRepository projections;

    private static final String CREATED_AT = "2026-02-01T10:00:00Z";
    private static final String UPDATED_AT = "2026-02-01T10:00:04.250Z";

    @Test
    @DisplayName("INSERT is folded into a projection with the denormalised fields")
    void projectsInsert() {
        OrderEventsStreamProcessor processor = new OrderEventsStreamProcessor(projections);

        processor.handleRequest(eventOf(record("INSERT", imageOf("PENDING"))), null);

        ArgumentCaptor<OrderReadModel> captor = ArgumentCaptor.forClass(OrderReadModel.class);
        verify(projections).upsert(captor.capture());
        OrderReadModel projection = captor.getValue();

        assertThat(projection.getOrderId()).isEqualTo("order-1");
        assertThat(projection.getCustomerId()).isEqualTo("customer-7");
        assertThat(projection.getStatus()).isEqualTo("PENDING");
        assertThat(projection.getCurrency()).isEqualTo("USD");
        assertThat(projection.getTotalAmount()).isEqualByComparingTo("129.97");
        assertThat(projection.getLineCount()).isEqualTo(3);
        assertThat(projection.getRevision()).isEqualTo(1);
        assertThat(projection.getLastEventType()).isEqualTo("ORDER_CREATED");
        assertThat(projection.getCorrelationId()).isEqualTo("corr-42");
        // Still in flight: no saga duration yet.
        assertThat(projection.getSagaDurationMillis()).isNull();
    }

    @Test
    @DisplayName("a terminal status yields the end-to-end saga duration")
    void computesSagaDurationOnTerminalStatus() {
        OrderEventsStreamProcessor processor = new OrderEventsStreamProcessor(projections);

        OrderReadModel projection = processor.project(imageOf("CONFIRMED"));

        assertThat(projection.getStatus()).isEqualTo("CONFIRMED");
        assertThat(projection.getSagaDurationMillis()).isEqualTo(4_250L);
    }

    @Test
    @DisplayName("re-projecting the same image is idempotent")
    void projectionIsIdempotent() {
        OrderEventsStreamProcessor processor = new OrderEventsStreamProcessor(projections);
        Map<String, AttributeValue> image = imageOf("CONFIRMED");

        OrderReadModel first = processor.project(image);
        OrderReadModel second = processor.project(image);

        assertThat(first.getOrderId()).isEqualTo(second.getOrderId());
        assertThat(first.getStatus()).isEqualTo(second.getStatus());
        assertThat(first.getRevision()).isEqualTo(second.getRevision());
        assertThat(first.getSagaDurationMillis()).isEqualTo(second.getSagaDurationMillis());
    }

    @Test
    @DisplayName("REMOVE deletes the projection by key")
    void deletesProjectionOnRemove() {
        OrderEventsStreamProcessor processor = new OrderEventsStreamProcessor(projections);

        DynamodbStreamRecord record = new DynamodbStreamRecord();
        record.setEventName("REMOVE");
        StreamRecord streamRecord = new StreamRecord();
        streamRecord.setKeys(Map.of("orderId", new AttributeValue().withS("order-1")));
        record.setDynamodb(streamRecord);

        String summary = processor.handleRequest(eventOf(record), null);

        verify(projections).delete("order-1");
        verify(projections, never()).upsert(org.mockito.ArgumentMatchers.any());
        assertThat(summary).contains("removed=1");
    }

    @Test
    @DisplayName("records without a new image are skipped rather than failing the batch")
    void skipsRecordsWithoutUsableImage() {
        OrderEventsStreamProcessor processor = new OrderEventsStreamProcessor(projections);

        DynamodbStreamRecord noImage = new DynamodbStreamRecord();
        noImage.setEventName("MODIFY");
        noImage.setDynamodb(new StreamRecord());

        DynamodbStreamRecord unknownEvent = new DynamodbStreamRecord();
        unknownEvent.setEventName("SOMETHING_ELSE");

        String summary = processor.handleRequest(eventOf(noImage, unknownEvent), null);

        verifyNoInteractions(projections);
        assertThat(summary).isEqualTo("projected=0 removed=0 skipped=2");
    }

    // ------------------------------------------------------------------ fixtures

    private static DynamodbEvent eventOf(DynamodbStreamRecord... records) {
        DynamodbEvent event = new DynamodbEvent();
        event.setRecords(List.of(records));
        return event;
    }

    private static DynamodbStreamRecord record(String eventName, Map<String, AttributeValue> newImage) {
        DynamodbStreamRecord record = new DynamodbStreamRecord();
        record.setEventName(eventName);
        StreamRecord streamRecord = new StreamRecord();
        streamRecord.setKeys(Map.of("orderId", new AttributeValue().withS("order-1")));
        streamRecord.setNewImage(newImage);
        record.setDynamodb(streamRecord);
        return record;
    }

    private static Map<String, AttributeValue> imageOf(String status) {
        Map<String, AttributeValue> timelineEntry = new LinkedHashMap<>();
        timelineEntry.put("eventType", new AttributeValue().withS("ORDER_CREATED"));
        timelineEntry.put("status", new AttributeValue().withS("PENDING"));

        Map<String, AttributeValue> image = new LinkedHashMap<>();
        image.put("orderId", new AttributeValue().withS("order-1"));
        image.put("customerId", new AttributeValue().withS("customer-7"));
        image.put("status", new AttributeValue().withS(status));
        image.put("currency", new AttributeValue().withS("USD"));
        image.put("correlationId", new AttributeValue().withS("corr-42"));
        image.put("totalAmount", new AttributeValue().withN("129.97"));
        image.put("lineCount", new AttributeValue().withN("3"));
        image.put("createdAt", new AttributeValue().withS(CREATED_AT));
        image.put("updatedAt", new AttributeValue().withS(UPDATED_AT));
        image.put("timeline", new AttributeValue()
                .withL(new AttributeValue().withM(timelineEntry)));
        return image;
    }
}
