package io.orderflow.inventory.service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

import io.orderflow.common.error.RetryableException;
import io.orderflow.common.event.DomainEvent;
import io.orderflow.common.event.EventEnvelope;
import io.orderflow.common.observability.SagaMetrics;
import io.orderflow.common.outbox.OutboxRecord;
import io.orderflow.inventory.model.InventoryItem;
import io.orderflow.inventory.model.Reservation;
import io.orderflow.inventory.repository.InventoryRepository;

/**
 * The reserve/release half of the saga.
 *
 * <p>Reserving is the first <em>reversible</em> step: it changes shared state that a later failure
 * must be able to undo. Everything here is therefore written twice over — once for the happy path,
 * once for the undo — and both paths are safe to run more than once:
 *
 * <ul>
 *   <li>a redelivered {@code OrderCreated} finds an existing reservation for the order and
 *       re-publishes nothing;</li>
 *   <li>a redelivered {@code OrderCancelled} finds the reservation already {@code RELEASED} and
 *       returns without touching the counters.</li>
 * </ul>
 */
@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);
    private static final String SOURCE = "inventory-service";

    private final InventoryRepository inventoryRepository;
    private final SagaMetrics metrics;
    private final int maxConflictRetries;

    public InventoryService(InventoryRepository inventoryRepository,
                            SagaMetrics metrics,
                            @Value("${app.inventory.max-conflict-retries:5}") int maxConflictRetries) {
        this.inventoryRepository = inventoryRepository;
        this.metrics = metrics;
        this.maxConflictRetries = maxConflictRetries;
    }

    /**
     * Attempts to hold stock for a new order and publishes either {@code InventoryReserved} or
     * {@code InventoryRejected}.
     */
    public void reserve(EventEnvelope<DomainEvent.OrderCreated> envelope) {
        DomainEvent.OrderCreated order = envelope.payload();

        Optional<Reservation> existing = inventoryRepository.findReservationByOrderId(order.orderId());
        if (existing.isPresent()) {
            log.info("Order {} already has reservation {} — skipping duplicate reserve",
                    order.orderId(), existing.get().getReservationId());
            return;
        }

        Map<String, Integer> wanted = requestedQuantities(order);
        for (int attempt = 1; attempt <= maxConflictRetries; attempt++) {
            Map<String, InventoryItem> stock = inventoryRepository.findBySkus(wanted.keySet());

            List<String> unavailable = unavailableSkus(wanted, stock);
            if (!unavailable.isEmpty()) {
                reject(envelope, unavailable);
                return;
            }

            List<InventoryItem> updated = new ArrayList<>(wanted.size());
            wanted.forEach((sku, quantity) -> {
                InventoryItem item = stock.get(sku);
                item.reserve(quantity);
                updated.add(item);
            });

            Reservation reservation = Reservation.held(order.orderId(), wanted, envelope.correlationId());
            DomainEvent.InventoryReserved reserved = new DomainEvent.InventoryReserved(
                    order.orderId(),
                    reservation.getReservationId(),
                    order.customerId(),
                    order.lines(),
                    order.totalAmount(),
                    order.currency());
            EventEnvelope<DomainEvent.InventoryReserved> outbound =
                    EventEnvelope.follow(reserved, SOURCE, envelope);

            try {
                inventoryRepository.commitReservation(updated, reservation, OutboxRecord.pending(outbound));
                log.info("Reserved {} SKU(s) for order {} reservationId={}",
                        wanted.size(), order.orderId(), reservation.getReservationId());
                return;
            } catch (TransactionCanceledException e) {
                // Another order won the race for one of these SKUs: re-read and re-evaluate,
                // because the correct answer may now be a rejection rather than a smaller hold.
                metrics.stepFailed(SOURCE, envelope.eventType().name(), "version_conflict");
                log.warn("Reservation conflict for order {} (attempt {}/{})",
                        order.orderId(), attempt, maxConflictRetries);
                backOff(attempt);
            }
        }
        throw new RetryableException("Could not reserve stock for order " + order.orderId()
                + " after " + maxConflictRetries + " attempts");
    }

    /**
     * Compensating action for {@code InventoryReserved}.
     *
     * <p>Triggered by {@code OrderCancelled}, which order-service emits both for a customer
     * cancellation and for saga rollback after a payment failure. Quantities come from the
     * reservation record, never from the event, so a replayed event cannot inflate stock.
     */
    public void release(EventEnvelope<DomainEvent.OrderCancelled> envelope) {
        DomainEvent.OrderCancelled cancellation = envelope.payload();

        Optional<Reservation> maybeReservation = cancellation.reservationId() != null
                ? inventoryRepository.findReservation(cancellation.reservationId())
                : inventoryRepository.findReservationByOrderId(cancellation.orderId());

        if (maybeReservation.isEmpty()) {
            // Cancelled while still PENDING: nothing was ever held, so there is nothing to undo.
            log.info("No reservation for cancelled order {} — nothing to release", cancellation.orderId());
            return;
        }

        Reservation reservation = maybeReservation.get();
        if (!reservation.isHeld()) {
            log.info("Reservation {} already {} — release is a no-op",
                    reservation.getReservationId(), reservation.getStatus());
            return;
        }

        for (int attempt = 1; attempt <= maxConflictRetries; attempt++) {
            Map<String, InventoryItem> stock =
                    inventoryRepository.findBySkus(reservation.getQuantities().keySet());

            List<InventoryItem> updated = new ArrayList<>();
            reservation.getQuantities().forEach((sku, quantity) -> {
                InventoryItem item = stock.get(sku);
                if (item != null) {
                    item.release(quantity);
                    updated.add(item);
                }
            });
            reservation.markReleased();

            try {
                inventoryRepository.releaseReservation(updated, reservation);
                log.info("Released reservation {} for order {} reason={}",
                        reservation.getReservationId(), cancellation.orderId(), cancellation.reason());
                return;
            } catch (TransactionCanceledException e) {
                metrics.stepFailed(SOURCE, envelope.eventType().name(), "version_conflict");
                reservation.setStatus(Reservation.STATUS_HELD);
                log.warn("Release conflict for reservation {} (attempt {}/{})",
                        reservation.getReservationId(), attempt, maxConflictRetries);
                backOff(attempt);
            }
        }
        throw new RetryableException("Could not release reservation " + reservation.getReservationId());
    }

    private void reject(EventEnvelope<DomainEvent.OrderCreated> envelope, List<String> unavailable) {
        DomainEvent.OrderCreated order = envelope.payload();
        String reason = "Insufficient stock for " + String.join(", ", unavailable);
        DomainEvent.InventoryRejected rejected =
                new DomainEvent.InventoryRejected(order.orderId(), reason, unavailable);
        EventEnvelope<DomainEvent.InventoryRejected> outbound =
                EventEnvelope.follow(rejected, SOURCE, envelope);

        inventoryRepository.recordRejection(OutboxRecord.pending(outbound));
        metrics.stepFailed(SOURCE, envelope.eventType().name(), "insufficient_stock");
        log.info("Rejected order {}: {}", order.orderId(), reason);
    }

    private Map<String, Integer> requestedQuantities(DomainEvent.OrderCreated order) {
        Map<String, Integer> wanted = new LinkedHashMap<>();
        // Same SKU may appear on several lines; collapse so the counters move once per SKU.
        order.lines().forEach(line -> wanted.merge(line.sku(), line.quantity(), Integer::sum));
        return wanted;
    }

    private List<String> unavailableSkus(Map<String, Integer> wanted, Map<String, InventoryItem> stock) {
        List<String> unavailable = new ArrayList<>();
        wanted.forEach((sku, quantity) -> {
            InventoryItem item = stock.get(sku);
            if (item == null || !item.canReserve(quantity)) {
                unavailable.add(sku);
            }
        });
        return unavailable;
    }

    private void backOff(int attempt) {
        try {
            Thread.sleep(Math.min(200L, 25L * attempt) + ThreadLocalRandom.current().nextLong(10, 40));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RetryableException("Interrupted while retrying an inventory write", e);
        }
    }
}
