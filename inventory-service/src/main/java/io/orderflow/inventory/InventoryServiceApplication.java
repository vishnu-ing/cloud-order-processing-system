package io.orderflow.inventory;

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
 * inventory-service: the only writer of the {@code inventory} and {@code inventory_reservations}
 * tables.
 *
 * <p>It performs the first reversible step of the saga (holding stock) and owns the corresponding
 * compensating action (releasing it when {@code OrderCancelled} arrives).
 */
@SpringBootApplication
@EnableKafka
@EnableScheduling
@Import({DynamoDbConfig.class, CommonInfraConfig.class, OutboxConfig.class, KafkaErrorHandlingConfig.class})
public class InventoryServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(InventoryServiceApplication.class, args);
    }
}
