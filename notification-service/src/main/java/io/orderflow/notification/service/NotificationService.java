package io.orderflow.notification.service;

import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import io.orderflow.common.event.DomainEvent;
import io.orderflow.common.event.EventEnvelope;
import io.orderflow.common.observability.SagaMetrics;
import io.orderflow.notification.model.NotificationRecord;
import io.orderflow.notification.repository.NotificationRepository;
import io.orderflow.notification.template.NotificationTemplateRenderer;
import io.orderflow.notification.template.NotificationTemplateRenderer.RenderedMessage;

/**
 * Turns a terminal saga event into a durable, rendered customer message.
 *
 * <p>The event-to-model mapping is a Java 21 pattern-matching {@code switch} over the sealed
 * {@link DomainEvent} hierarchy, so each event contributes exactly the variables its template
 * needs and the compiler checks the payload types. Non customer-facing events fall through
 * harmlessly; the set of messages that exist at all is declared in one place, the template
 * registry.
 *
 * <p>Sending is simulated: the record is written first, then "delivered" to SES and flipped to
 * {@code SENT}. Swapping the log line for an {@code SesV2Client.sendEmail} call is a one-method
 * change, and the retry story is already in place.
 */
@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);
    private static final String CHANNEL_EMAIL = "EMAIL";

    private final NotificationRepository notificationRepository;
    private final NotificationTemplateRenderer renderer;
    private final SagaMetrics metrics;
    private final String fallbackRecipient;

    public NotificationService(NotificationRepository notificationRepository,
                               NotificationTemplateRenderer renderer,
                               SagaMetrics metrics,
                               @Value("${app.notifications.fallback-recipient:orders@example.com}")
                               String fallbackRecipient) {
        this.notificationRepository = notificationRepository;
        this.renderer = renderer;
        this.metrics = metrics;
        this.fallbackRecipient = fallbackRecipient;
    }

    public void handle(EventEnvelope<DomainEvent> envelope) {
        if (!renderer.supports(envelope.eventType())) {
            log.debug("No template for {} — nothing to notify", envelope.eventType());
            return;
        }

        DomainEvent event = envelope.payload();
        Map<String, String> model = new LinkedHashMap<>();
        model.put("orderId", event.orderId());

        String recipient = fallbackRecipient;
        String customerId = "customer";

        switch (event) {
            case DomainEvent.OrderConfirmed confirmed -> {
                recipient = orDefault(confirmed.customerEmail());
                customerId = orDefault(confirmed.customerId(), customerId);
                model.put("customerId", customerId);
                model.put("amount", String.valueOf(confirmed.totalAmount()));
                model.put("currency", confirmed.currency());
                model.put("paymentId", confirmed.paymentId());
            }
            case DomainEvent.OrderCancelled cancelled -> {
                recipient = orDefault(cancelled.customerEmail());
                customerId = orDefault(cancelled.customerId(), customerId);
                model.put("customerId", customerId);
                model.put("reason", orDefault(cancelled.reason(), "cancelled"));
            }
            case DomainEvent.InventoryRejected rejected -> {
                model.put("reason", orDefault(rejected.reason(), "items unavailable"));
                model.put("skus", String.join(", ", rejected.unavailableSkus()));
            }
            case DomainEvent.PaymentFailed failed -> {
                model.put("failureCode", orDefault(failed.failureCode(), "UNKNOWN"));
                model.put("failureReason", orDefault(failed.failureReason(), "payment declined"));
            }
            default -> {
                log.debug("Event {} is not customer-facing", envelope.eventType());
                return;
            }
        }

        RenderedMessage message = renderer.render(envelope.eventType(), model);
        NotificationRecord record = NotificationRecord.pending(
                event.orderId(),
                customerId,
                recipient,
                CHANNEL_EMAIL,
                message.templateKey(),
                message.subject(),
                message.body(),
                envelope.eventId(),
                envelope.correlationId());

        notificationRepository.save(record);
        deliver(record);
    }

    /**
     * Simulated SES delivery. Writing the outcome back means a failed send is visible in the table
     * (and therefore alertable) instead of only in a log line that has already rotated away.
     */
    private void deliver(NotificationRecord record) {
        try {
            log.info("[SES] to={} subject=\"{}\" template={} orderId={} correlationId={}",
                    record.getRecipient(), record.getSubject(), record.getTemplateKey(),
                    record.getOrderId(), record.getCorrelationId());
            record.markSent();
            notificationRepository.save(record);
            metrics.eventPublished("NOTIFICATION_" + record.getTemplateKey());
        } catch (RuntimeException e) {
            record.markFailed(e.getMessage());
            notificationRepository.save(record);
            throw e;
        }
    }

    private String orDefault(String value) {
        return orDefault(value, fallbackRecipient);
    }

    private String orDefault(String value, String fallback) {
        return (value == null || value.isBlank()) ? fallback : value;
    }
}
