package io.orderflow.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

import io.orderflow.common.config.CommonInfraConfig;
import io.orderflow.common.config.DynamoDbConfig;
import io.orderflow.common.config.KafkaErrorHandlingConfig;
import io.orderflow.common.config.OutboxConfig;

/**
 * order-service: the write model and the saga driver.
 *
 * <p>It owns the {@code orders} aggregate, exposes the public REST API, and is the only service
 * allowed to change {@code OrderStatus}. Every other service reacts to events and reports back;
 * the decision about what an order <em>is</em> stays in one place.
 *
 * <p>{@code @EnableScheduling} activates the outbox drain; the shared configurations are imported
 * explicitly so the dependency on common-events is visible at the entry point.
 */
@SpringBootApplication
@EnableKafka
@EnableScheduling
@Import({DynamoDbConfig.class, CommonInfraConfig.class, OutboxConfig.class, KafkaErrorHandlingConfig.class})
public class OrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
