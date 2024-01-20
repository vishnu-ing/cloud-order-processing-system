package io.orderflow.payment.service;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import io.orderflow.common.event.DomainEvent;
import io.orderflow.common.event.EventEnvelope;
import io.orderflow.common.observability.SagaMetrics;
import io.orderflow.common.outbox.OutboxRecord;
import io.orderflow.payment.gateway.SimulatedPaymentGateway;
import io.orderflow.payment.gateway.SimulatedPaymentGateway.CaptureResult;
import io.orderflow.payment.model.PaymentEntity;
import io.orderflow.payment.repository.PaymentRepository;

/**
 * Captures payment for an order whose stock is already held.
 *
 * <p>Three layers of duplicate protection, because this step moves money:
 * <ol>
 *   <li>the consumer-group dedupe marker keyed on {@code eventId};</li>
 *   <li>a {@code payments} lookup by {@code orderId} before calling the gateway, which also covers
 *       the case where the marker expired or was released after a partial failure;</li>
 *   <li>a gateway idempotency key derived from the order id, so even a duplicate call that reaches
 *       the PSP returns the original authorization rather than a second charge.</li>
 * </ol>
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);
    private static final String SOURCE = "payment-service";

    private final PaymentRepository paymentRepository;
    private final SimulatedPaymentGateway gateway;
    private final SagaMetrics metrics;

    public PaymentService(PaymentRepository paymentRepository,
                          SimulatedPaymentGateway gateway,
                          SagaMetrics metrics) {
        this.paymentRepository = paymentRepository;
        this.gateway = gateway;
        this.metrics = metrics;
    }

    public void capture(EventEnvelope<DomainEvent.InventoryReserved> envelope) {
        DomainEvent.InventoryReserved reserved = envelope.payload();
        String orderId = reserved.orderId();

        Optional<PaymentEntity> existing = paymentRepository.findByOrderId(orderId);
        if (existing.filter(PaymentEntity::isCaptured).isPresent()) {
            log.info("Order {} already captured as payment {} — skipping",
                    orderId, existing.get().getPaymentId());
            return;
        }

        // Stable across redeliveries: the PSP deduplicates on this value, not on our event id.
        String gatewayIdempotencyKey = "order-" + orderId;

        CaptureResult result = gateway.capture(
                orderId,
                reserved.customerId(),
                reserved.totalAmount(),
                reserved.currency(),
                gatewayIdempotencyKey);

        if (result.captured()) {
            PaymentEntity payment = PaymentEntity.captured(
                    orderId,
                    reserved.reservationId(),
                    reserved.totalAmount(),
                    reserved.currency(),
                    result.authorizationCode(),
                    gatewayIdempotencyKey,
                    envelope.correlationId());

            DomainEvent.PaymentCaptured captured = new DomainEvent.PaymentCaptured(
                    orderId,
                    payment.getPaymentId(),
                    reserved.reservationId(),
                    reserved.totalAmount(),
                    reserved.currency(),
                    result.authorizationCode());

            paymentRepository.saveWithEvent(payment,
                    OutboxRecord.pending(EventEnvelope.follow(captured, SOURCE, envelope)));
            log.info("Captured {} {} for order {} paymentId={} authorization={}",
                    reserved.totalAmount(), reserved.currency(), orderId,
                    payment.getPaymentId(), result.authorizationCode());
            return;
        }

        PaymentEntity payment = PaymentEntity.failed(
                orderId,
                reserved.reservationId(),
                reserved.totalAmount(),
                reserved.currency(),
                result.failureCode(),
                result.failureReason(),
                gatewayIdempotencyKey,
                envelope.correlationId());

        DomainEvent.PaymentFailed failed = new DomainEvent.PaymentFailed(
                orderId,
                payment.getPaymentId(),
                reserved.reservationId(),
                result.failureCode(),
                result.failureReason(),
                false);

        paymentRepository.saveWithEvent(payment,
                OutboxRecord.pending(EventEnvelope.follow(failed, SOURCE, envelope)));
        metrics.stepFailed(SOURCE, envelope.eventType().name(), result.failureCode());
        log.info("Payment failed for order {} code={} reason={} — compensation will release stock",
                orderId, result.failureCode(), result.failureReason());
    }
}
