package io.orderflow.notification.repository;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;

import io.orderflow.notification.model.NotificationRecord;

/** Data access for the {@code notifications} send outbox. */
@Repository
public class NotificationRepository {

    private final DynamoDbTable<NotificationRecord> table;

    public NotificationRepository(
            DynamoDbEnhancedClient enhancedClient,
            @Value("${app.dynamodb.tables.notifications:notifications}") String tableName) {
        this.table = enhancedClient.table(tableName, TableSchema.fromBean(NotificationRecord.class));
    }

    public void save(NotificationRecord record) {
        table.putItem(record);
    }

    public NotificationRecord findById(String notificationId) {
        return table.getItem(Key.builder().partitionValue(notificationId).build());
    }
}
