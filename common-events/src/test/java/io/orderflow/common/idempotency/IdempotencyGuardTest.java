package io.orderflow.common.idempotency;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.orderflow.common.event.DomainEvent;
import io.orderflow.common.event.EventEnvelope;
import io.orderflow.common.observability.SagaMetrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The guard is the difference between at-least-once delivery and effectively-once processing, so
 * the tests pin down all four behaviours that matter operationally:
 * first delivery runs, duplicate is suppressed, a failing handler releases its claim so Kafka can
 * retry, and the marker carries a TTL so the table does not grow without bound.
 */
@ExtendWith(MockitoExtension.class)
class IdempotencyGuardTest {

    private static final String GROUP = "inventory-service";

    @Mock
    private ProcessedEventRepository repository;

    private IdempotencyGuard guard;
    private EventEnvelope<DomainEvent.OrderCreated> envelope;

    @BeforeEach
    void setUp() {
        guard = new IdempotencyGuard(repository, new SagaMetrics(new SimpleMeterRegistry()),
                Duration.ofDays(7));
        envelope = EventEnvelope.start(
                new DomainEvent.OrderCreated(
                        "order-1",
                        "customer-1",
                        "customer@example.com",
                        List.of(new DomainEvent.OrderLine("SKU-1", 2, new BigDecimal("19.99"))),
                        new BigDecimal("39.98"),
                        "USD"),
                "order-service",
                "corr-1");
    }

    @Test
    @DisplayName("first delivery claims the marker and runs the handler")
    void runsHandlerOnFirstDelivery() {
        when(repository.claim(any(ProcessedEvent.class))).thenReturn(true);
        AtomicInteger invocations = new AtomicInteger();

        boolean processed = guard.runOnce(GROUP, envelope, invocations::incrementAndGet);

        assertThat(processed).isTrue();
        assertThat(invocations.get()).isEqualTo(1);
        verify(repository, never()).release(any(), any());
    }

    @Test
    @DisplayName("duplicate delivery short-circuits without touching the handler")
    void suppressesDuplicateDelivery() {
        when(repository.claim(any(ProcessedEvent.class))).thenReturn(false);
        AtomicInteger invocations = new AtomicInteger();

        boolean processed = guard.runOnce(GROUP, envelope, invocations::incrementAndGet);

        assertThat(processed).isFalse();
        assertThat(invocations.get()).isZero();
    }

    @Test
    @DisplayName("a failing handler releases the marker so the Kafka retry can reprocess")
    void releasesMarkerWhenHandlerFails() {
        when(repository.claim(any(ProcessedEvent.class))).thenReturn(true);

        assertThatThrownBy(() -> guard.runOnce(GROUP, envelope, () -> {
            throw new IllegalStateException("DynamoDB throttled");
        })).isInstanceOf(IllegalStateException.class);

        verify(repository).release(GROUP, envelope.eventId());
    }

    @Test
    @DisplayName("the marker is namespaced per consumer group and carries a TTL")
    void markerIsNamespacedAndExpires() {
        when(repository.claim(any(ProcessedEvent.class))).thenReturn(true);

        guard.runOnce(GROUP, envelope, () -> {
        });

        ArgumentCaptor<ProcessedEvent> captor = ArgumentCaptor.forClass(ProcessedEvent.class);
        verify(repository).claim(captor.capture());
        ProcessedEvent marker = captor.getValue();

        assertThat(marker.getConsumerGroup()).isEqualTo(GROUP);
        assertThat(marker.getEventId()).isEqualTo(envelope.eventId());
        assertThat(marker.getEventType()).isEqualTo("ORDER_CREATED");
        assertThat(marker.getOrderId()).isEqualTo("order-1");
        assertThat(marker.getExpiresAt())
                .isGreaterThan(marker.getProcessedAt().getEpochSecond());
    }

    @Test
    @DisplayName("wasProcessed delegates to the repository for the same key pair")
    void wasProcessedDelegates() {
        when(repository.wasProcessed(eq(GROUP), eq("event-9"))).thenReturn(true);

        assertThat(guard.wasProcessed(GROUP, "event-9")).isTrue();
    }
}
