package io.orderflow.common.error;

/**
 * Permanent failure: malformed JSON, an unknown schema version, an illegal state transition.
 *
 * <p>Retrying cannot change the outcome, so the Kafka error handler is configured to skip the
 * back-off entirely and publish straight to {@code <topic>.DLT}. Keeping this distinction explicit
 * is what stops one bad record from stalling a partition for minutes.
 */
public class NonRetryableException extends RuntimeException {

    public NonRetryableException(String message) {
        super(message);
    }

    public NonRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
