package io.orderflow.common.error;

/**
 * Transient failure: a throttled DynamoDB request, a gateway timeout, a lost leader election.
 *
 * <p>Kafka error handlers treat this as retryable and let the exponential back-off run before the
 * record is finally dead-lettered.
 */
public class RetryableException extends RuntimeException {

    public RetryableException(String message) {
        super(message);
    }

    public RetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
