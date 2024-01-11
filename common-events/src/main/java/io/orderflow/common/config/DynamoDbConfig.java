package io.orderflow.common.config;

import java.net.URI;
import java.time.Duration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * DynamoDB clients for every Spring service.
 *
 * <p>The endpoint is overridable through {@code AWS_ENDPOINT_URL} so the exact same image runs
 * against LocalStack in docker compose and against real DynamoDB on ECS with no profile-specific
 * code path — only an environment variable changes.
 *
 * <p>Retries use full jitter with a hard cap. DynamoDB throttling is the most common transient
 * failure in this system, and unjittered retries from a dozen consumer threads turn a brief
 * throttle into a synchronised stampede.
 */
@Configuration(proxyBeanMethods = false)
public class DynamoDbConfig {

    @Bean
    public DynamoDbClient dynamoDbClient(
            @Value("${aws.region:us-east-1}") String region,
            @Value("${aws.endpoint-url:}") String endpointUrl,
            @Value("${aws.dynamodb.max-retries:5}") int maxRetries,
            @Value("${aws.dynamodb.api-call-timeout-ms:3000}") long apiCallTimeoutMs) {

        RetryPolicy retryPolicy = RetryPolicy.builder()
                .numRetries(maxRetries)
                .backoffStrategy(FullJitterBackoffStrategy.builder()
                        .baseDelay(Duration.ofMillis(50))
                        .maxBackoffTime(Duration.ofSeconds(2))
                        .build())
                .build();

        var builder = DynamoDbClient.builder()
                .region(Region.of(region))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .apiCallTimeout(Duration.ofMillis(apiCallTimeoutMs))
                        .retryPolicy(retryPolicy)
                        .build());

        if (endpointUrl != null && !endpointUrl.isBlank()) {
            builder = builder.endpointOverride(URI.create(endpointUrl));
        }
        return builder.build();
    }

    @Bean
    public DynamoDbEnhancedClient dynamoDbEnhancedClient(DynamoDbClient dynamoDbClient) {
        return DynamoDbEnhancedClient.builder()
                .dynamoDbClient(dynamoDbClient)
                .build();
    }
}
