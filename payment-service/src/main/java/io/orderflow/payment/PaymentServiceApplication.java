package io.orderflow.payment;

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
 * payment-service: the irreversible step of the saga.
 *
 * <p>Because a capture cannot be silently undone, this is the service where idempotency matters
 * most: the dedupe marker, the per-order payment lookup and the gateway's own idempotency key all
 * exist to make sure one order is charged exactly once, whatever Kafka redelivers.
 */
@SpringBootApplication
@EnableKafka
@EnableScheduling
@Import({DynamoDbConfig.class, CommonInfraConfig.class, OutboxConfig.class, KafkaErrorHandlingConfig.class})
public class PaymentServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentServiceApplication.class, args);
    }
}
