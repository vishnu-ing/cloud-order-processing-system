package io.orderflow.notification.template;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.stereotype.Component;

import io.orderflow.common.error.NonRetryableException;
import io.orderflow.common.event.EventType;

/**
 * Minimal, dependency-free template engine for transactional messages.
 *
 * <p>A full templating library is the wrong trade here: the four transactional messages change
 * rarely, must never fail to render at 3 a.m., and benefit from an unresolved placeholder being a
 * loud error rather than an empty string in a customer's inbox. Hence
 * {@link NonRetryableException} on a missing variable — it dead-letters the event with the exact
 * template key and variable name in the DLQ headers.
 */
@Component
public class NotificationTemplateRenderer {

    private static final Pattern PLACEHOLDER = Pattern.compile("\\{\\{(\\w+)}}");

    /** A resolved message, ready to hand to SES. */
    public record RenderedMessage(String templateKey, String subject, String body) {
    }

    private record Template(String key, String subject, String body) {
    }

    private static final Map<EventType, Template> TEMPLATES = Map.of(
            EventType.ORDER_CONFIRMED, new Template(
                    "order-confirmed",
                    "Your order {{orderId}} is confirmed",
                    """
                    Hi {{customerId}},

                    Good news - your order {{orderId}} is confirmed.
                    We captured {{amount}} {{currency}} (payment {{paymentId}}) and the items are
                    being prepared for dispatch.

                    You can follow progress at /api/v1/orders/{{orderId}}/timeline.

                    Thanks for shopping with us."""),

            EventType.ORDER_CANCELLED, new Template(
                    "order-cancelled",
                    "Your order {{orderId}} was cancelled",
                    """
                    Hi {{customerId}},

                    Your order {{orderId}} has been cancelled ({{reason}}).
                    Any stock we were holding has been released and no payment has been taken.

                    If this was not you, reply to this message and quote {{orderId}}."""),

            EventType.INVENTORY_REJECTED, new Template(
                    "order-out-of-stock",
                    "We could not fulfil order {{orderId}}",
                    """
                    Hi there,

                    Unfortunately we could not fulfil order {{orderId}}: {{reason}}.
                    Nothing has been charged.

                    The affected items were: {{skus}}."""),

            EventType.PAYMENT_FAILED, new Template(
                    "order-payment-failed",
                    "Payment problem with order {{orderId}}",
                    """
                    Hi there,

                    We could not take payment for order {{orderId}} ({{failureReason}}).
                    The order has been released, so please place it again with a different payment
                    method if you still want the items.

                    Reference: {{failureCode}}."""));

    public RenderedMessage render(EventType eventType, Map<String, String> model) {
        Template template = TEMPLATES.get(eventType);
        if (template == null) {
            throw new NonRetryableException("No notification template for event type " + eventType);
        }
        return new RenderedMessage(
                template.key(),
                substitute(template.key(), template.subject(), model),
                substitute(template.key(), template.body(), model));
    }

    public boolean supports(EventType eventType) {
        return TEMPLATES.containsKey(eventType);
    }

    private String substitute(String templateKey, String text, Map<String, String> model) {
        Matcher matcher = PLACEHOLDER.matcher(text);
        StringBuilder out = new StringBuilder();
        while (matcher.find()) {
            String variable = matcher.group(1);
            String value = model.get(variable);
            if (value == null) {
                throw new NonRetryableException(
                        "Template " + templateKey + " is missing variable " + variable);
            }
            matcher.appendReplacement(out, Matcher.quoteReplacement(value));
        }
        matcher.appendTail(out);
        return out.toString();
    }
}
