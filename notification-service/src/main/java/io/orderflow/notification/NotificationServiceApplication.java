package io.orderflow.notification;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;

import io.orderflow.common.config.CommonInfraConfig;
import io.orderflow.common.config.DynamoDbConfig;
import io.orderflow.common.config.KafkaErrorHandlingConfig;

/**
 * notification-service: a pure sink.
 *
 * <p>It consumes terminal saga events and writes rendered messages to the {@code notifications}
 * table, which doubles as a send outbox for SES. It publishes no domain events, so
 * {@code OutboxConfig} and {@code @EnableScheduling} are intentionally absent — the least
 * privilege a service can have in this platform.
 */
@SpringBootApplication
@EnableKafka
@Import({DynamoDbConfig.class, CommonInfraConfig.class, KafkaErrorHandlingConfig.class})
public class NotificationServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(NotificationServiceApplication.class, args);
    }
}
