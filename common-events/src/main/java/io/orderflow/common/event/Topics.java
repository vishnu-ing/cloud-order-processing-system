package io.orderflow.common.event;

/**
 * Canonical Kafka topic names for the order saga.
 *
 * <p>Every topic is partitioned by {@code orderId}. That gives us total ordering of all events
 * belonging to one order aggregate (the only ordering guarantee the saga actually needs) while
 * still allowing horizontal scale across partitions.
 *
 * <p>Each topic has a matching {@code <topic>.DLT} dead-letter topic created by
 * {@code scripts/create-topics.sh}; see {@link #deadLetterFor(String)}.
 */
public final class Topics {

    public static final String ORDERS_CREATED = "orders.created";
    public static final String INVENTORY_RESERVED = "inventory.reserved";
    public static final String INVENTORY_REJECTED = "inventory.rejected";
    public static final String PAYMENTS_CAPTURED = "payments.captured";
    public static final String PAYMENTS_FAILED = "payments.failed";
    public static final String ORDERS_CONFIRMED = "orders.confirmed";
    public static final String ORDERS_CANCELLED = "orders.cancelled";

    /** Suffix appended by the {@code DeadLetterPublishingRecoverer}. */
    public static final String DLT_SUFFIX = ".DLT";

    private Topics() {
    }

    public static String deadLetterFor(String topic) {
        return topic.endsWith(DLT_SUFFIX) ? topic : topic + DLT_SUFFIX;
    }

    public static boolean isDeadLetter(String topic) {
        return topic != null && topic.endsWith(DLT_SUFFIX);
    }

    /** Strips the {@code .DLT} suffix so the DLQ reprocessor can replay onto the live topic. */
    public static String liveTopicFor(String deadLetterTopic) {
        return isDeadLetter(deadLetterTopic)
                ? deadLetterTopic.substring(0, deadLetterTopic.length() - DLT_SUFFIX.length())
                : deadLetterTopic;
    }
}
