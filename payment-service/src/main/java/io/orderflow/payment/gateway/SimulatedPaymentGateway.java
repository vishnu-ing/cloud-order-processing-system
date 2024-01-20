package io.orderflow.payment.gateway;

import java.math.BigDecimal;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import io.orderflow.common.error.RetryableException;

/**
 * Stand-in for a real PSP (Stripe/Adyen style), with <strong>deterministic</strong> outcomes.
 *
 * <p>Determinism is the point. A random failure injector makes an integration test flaky; a rule
 * keyed on the amount, the customer id or the order id makes the compensation path reproducible
 * on demand: an order over the decline threshold always exercises the release of held stock.
 *
 * <p>Rules, in order of evaluation:
 * <ol>
 *   <li>amount above {@code app.payments.decline-above} → {@code INSUFFICIENT_FUNDS} (terminal);</li>
 *   <li>customer id ending in {@code -fraud} → {@code FRAUD_SUSPECTED} (terminal);</li>
 *   <li>order id hashing into the first {@code app.payments.timeout-bucket-percent} buckets →
 *       gateway timeout, raised as a {@link RetryableException} so Kafka retries with back-off;</li>
 *   <li>next {@code app.payments.decline-bucket-percent} buckets → {@code CARD_DECLINED}
 *       (terminal);</li>
 *   <li>otherwise capture, returning a stable authorization code.</li>
 * </ol>
 */
@Component
public class SimulatedPaymentGateway {

    private static final Logger log = LoggerFactory.getLogger(SimulatedPaymentGateway.class);

    private final BigDecimal declineAbove;
    private final int timeoutBucketPercent;
    private final int declineBucketPercent;

    public SimulatedPaymentGateway(
            @Value("${app.payments.decline-above:5000.00}") BigDecimal declineAbove,
            @Value("${app.payments.timeout-bucket-percent:0}") int timeoutBucketPercent,
            @Value("${app.payments.decline-bucket-percent:10}") int declineBucketPercent) {
        this.declineAbove = declineAbove;
        this.timeoutBucketPercent = timeoutBucketPercent;
        this.declineBucketPercent = declineBucketPercent;
    }

    /** Outcome of a capture attempt. A terminal failure carries {@code retryable = false}. */
    public record CaptureResult(
            boolean captured,
            String authorizationCode,
            String failureCode,
            String failureReason) {

        static CaptureResult ok(String authorizationCode) {
            return new CaptureResult(true, authorizationCode, null, null);
        }

        static CaptureResult declined(String code, String reason) {
            return new CaptureResult(false, null, code, reason);
        }
    }

    /**
     * @param idempotencyKey forwarded to the PSP so a retried capture returns the original
     *                       authorization instead of charging twice
     * @throws RetryableException on a simulated gateway timeout
     */
    public CaptureResult capture(String orderId,
                                 String customerId,
                                 BigDecimal amount,
                                 String currency,
                                 String idempotencyKey) {

        if (amount.compareTo(declineAbove) > 0) {
            return CaptureResult.declined("INSUFFICIENT_FUNDS",
                    "Amount " + amount + " " + currency + " exceeds the authorised limit");
        }

        if (customerId != null && customerId.toLowerCase(Locale.ROOT).endsWith("-fraud")) {
            return CaptureResult.declined("FRAUD_SUSPECTED",
                    "Customer flagged by the fraud rules engine");
        }

        int bucket = Math.floorMod(orderId.hashCode(), 100);

        if (bucket < timeoutBucketPercent) {
            log.warn("Simulated gateway timeout for order {} (bucket {})", orderId, bucket);
            throw new RetryableException("Payment gateway timed out for order " + orderId);
        }

        if (bucket < timeoutBucketPercent + declineBucketPercent) {
            return CaptureResult.declined("CARD_DECLINED",
                    "Issuer declined the transaction (do not honour)");
        }

        String authorizationCode = "AUTH-" + Integer.toHexString(orderId.hashCode()).toUpperCase(Locale.ROOT);
        log.debug("Captured {} {} for order {} idempotencyKey={}", amount, currency, orderId, idempotencyKey);
        return CaptureResult.ok(authorizationCode);
    }
}
