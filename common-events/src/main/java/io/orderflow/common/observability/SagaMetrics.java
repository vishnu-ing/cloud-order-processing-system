package io.orderflow.common.observability;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 * One place for every saga meter, so dashboards and alarms can be built from a known metric
 * surface instead of whatever each service happened to instrument.
 *
 * <p>All meters are exported on {@code /actuator/prometheus}. The two that matter operationally:
 * {@code saga_events_consumed_total} minus {@code saga_events_published_total} per saga step shows
 * where a saga is stalling, and {@code saga_step_failures_total} is the alarm signal that precedes
 * DLQ depth growing.
 */
public class SagaMetrics {

    private static final String PUBLISHED = "saga.events.published";
    private static final String CONSUMED = "saga.events.consumed";
    private static final String DUPLICATES = "saga.events.duplicates";
    private static final String STEP_FAILURES = "saga.step.failures";
    private static final String STEP_LATENCY = "saga.step.latency";
    private static final String OUTBOX_FAILURES = "saga.outbox.publish.failures";
    private static final String OUTBOX_RACES = "saga.outbox.races.lost";
    private static final String TERMINAL = "saga.terminal";

    private final MeterRegistry registry;

    public SagaMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void eventPublished(String eventType) {
        counter(PUBLISHED, "eventType", eventType).increment();
    }

    public void eventConsumed(String consumerGroup, String eventType) {
        counter(CONSUMED, "consumerGroup", consumerGroup, "eventType", eventType).increment();
    }

    public void duplicateSuppressed(String consumerGroup, String eventType) {
        counter(DUPLICATES, "consumerGroup", consumerGroup, "eventType", eventType).increment();
    }

    public void stepFailed(String consumerGroup, String eventType, String reason) {
        counter(STEP_FAILURES, "consumerGroup", consumerGroup, "eventType", eventType, "reason", reason)
                .increment();
    }

    public void outboxPublishFailed(String eventType) {
        counter(OUTBOX_FAILURES, "eventType", eventType).increment();
    }

    public void outboxRaceLost(String eventType) {
        counter(OUTBOX_RACES, "eventType", eventType).increment();
    }

    /** Terminal saga outcome: {@code CONFIRMED}, {@code REJECTED}, {@code CANCELLED}, {@code FAILED}. */
    public void sagaTerminal(String status) {
        counter(TERMINAL, "status", status).increment();
    }

    /** Wall-clock duration of one saga step, tagged by step name. */
    public void recordStepLatency(String step, Duration duration) {
        timer(step).record(duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    public <T> T timeStep(String step, java.util.function.Supplier<T> body) {
        long startNanos = System.nanoTime();
        try {
            return body.get();
        } finally {
            timer(step).record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
        }
    }

    private Timer timer(String step) {
        return Timer.builder(STEP_LATENCY)
                .tag("step", step)
                .publishPercentileHistogram()
                .register(registry);
    }

    private Counter counter(String name, String... tags) {
        return Counter.builder(name).tags(tags).register(registry);
    }
}
