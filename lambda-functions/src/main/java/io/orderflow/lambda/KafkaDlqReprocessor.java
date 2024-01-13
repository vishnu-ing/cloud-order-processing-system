package io.orderflow.lambda;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import io.orderflow.common.error.NonRetryableException;
import io.orderflow.common.event.EventEnvelope;
import io.orderflow.common.event.EventJson;
import io.orderflow.common.event.Topics;
import io.orderflow.common.observability.CorrelationContext;

/**
 * Operator tool: replays dead-lettered events back onto their live topic.
 *
 * <p>Wiring: a small consumer mirrors {@code <topic>.DLT} into an SQS queue (or an operator drops
 * messages there by hand from the runbook), and this function is the SQS event-source target. SQS
 * gives us three things a hand-rolled replay script does not: at-least-once delivery with a
 * visibility timeout, a receive counter to cap replay attempts, and a redrive policy to a final
 * parking queue.
 *
 * <p>Expected message body:
 * <pre>{@code
 * { "topic": "inventory.reserved.DLT", "key": "0f6c...", "envelope": { ...EventEnvelope... } }
 * }</pre>
 *
 * <p>Safety rules, in order:
 * <ol>
 *   <li>the envelope must parse and be a supported schema version — otherwise the message is
 *       <em>dropped</em>, not retried, because replaying a poison pill loops forever;</li>
 *   <li>messages past {@code MAX_REPLAY_ATTEMPTS} receives are dropped for the same reason;</li>
 *   <li>the destination is always the live topic derived from the DLT name, never a topic supplied
 *       by the message, so a malformed or hostile body cannot inject events elsewhere;</li>
 *   <li>a Kafka send failure returns the message id in {@code batchItemFailures} so only the failed
 *       messages are redelivered — partial batch failure, not the whole batch.</li>
 * </ol>
 *
 * <p>Replay is safe precisely because every consumer is idempotent: an event that was already
 * partially processed before dead-lettering is suppressed by the {@code processed_events} marker.
 */
public class KafkaDlqReprocessor implements RequestHandler<SQSEvent, SQSBatchResponse> {

    private static final ObjectMapper MAPPER = EventJson.createMapper();
    private static final String RECEIVE_COUNT = "ApproximateReceiveCount";

    private final Producer<String, String> producer;
    private final int maxReplayAttempts;
    private final long sendTimeoutSeconds;

    public KafkaDlqReprocessor() {
        this(defaultProducer(),
                Integer.parseInt(System.getenv().getOrDefault("MAX_REPLAY_ATTEMPTS", "3")),
                Long.parseLong(System.getenv().getOrDefault("SEND_TIMEOUT_SECONDS", "10")));
    }

    /** Test seam. */
    public KafkaDlqReprocessor(Producer<String, String> producer,
                               int maxReplayAttempts,
                               long sendTimeoutSeconds) {
        this.producer = producer;
        this.maxReplayAttempts = maxReplayAttempts;
        this.sendTimeoutSeconds = sendTimeoutSeconds;
    }

    private static Producer<String, String> defaultProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 15_000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
        // MSK Serverless: IAM auth is configured through these two properties in deployment.
        String securityProtocol = System.getenv("KAFKA_SECURITY_PROTOCOL");
        if (securityProtocol != null && !securityProtocol.isBlank()) {
            props.put("security.protocol", securityProtocol);
            props.put("sasl.mechanism", System.getenv().getOrDefault("KAFKA_SASL_MECHANISM", "AWS_MSK_IAM"));
            props.put("sasl.jaas.config",
                    "software.amazon.msk.auth.iam.IAMLoginModule required;");
            props.put("sasl.client.callback.handler.class",
                    "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        }
        return new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
    }

    @Override
    public SQSBatchResponse handleRequest(SQSEvent event, Context context) {
        List<SQSBatchResponse.BatchItemFailure> failures = new ArrayList<>();

        for (SQSMessage message : event.getRecords()) {
            try {
                replay(message, context);
            } catch (NonRetryableException e) {
                // Poison: log loudly and let the message be deleted. It is already preserved in the
                // DLT topic, so nothing is lost — only the replay attempt is abandoned.
                log(context, "DROP messageId=" + message.getMessageId() + " cause=" + e.getMessage());
            } catch (Exception e) {
                log(context, "RETRY messageId=" + message.getMessageId() + " cause=" + e);
                failures.add(SQSBatchResponse.BatchItemFailure.builder()
                        .withItemIdentifier(message.getMessageId())
                        .build());
            }
        }
        return SQSBatchResponse.builder().withBatchItemFailures(failures).build();
    }

    private void replay(SQSMessage message, Context context) throws Exception {
        if (receiveCount(message) > maxReplayAttempts) {
            throw new NonRetryableException("Exceeded " + maxReplayAttempts + " replay attempts");
        }

        JsonNode body = MAPPER.readTree(message.getBody());
        JsonNode envelopeNode = body.path("envelope");
        if (envelopeNode.isMissingNode() || envelopeNode.isNull()) {
            throw new NonRetryableException("Message has no 'envelope' field");
        }

        // Parsing proves the payload is a valid, supported event before it goes back on the topic.
        EventEnvelope<?> envelope = EventJson.fromJson(envelopeNode.toString());

        String sourceTopic = body.path("topic").asText(null);
        if (sourceTopic == null || sourceTopic.isBlank()) {
            throw new NonRetryableException("Message has no 'topic' field");
        }
        String targetTopic = Topics.liveTopicFor(sourceTopic);
        if (targetTopic.equals(sourceTopic) && !Topics.isDeadLetter(sourceTopic)) {
            // Guard against replaying onto an arbitrary topic name supplied in the body.
            throw new NonRetryableException("Refusing to replay onto non-DLT topic " + sourceTopic);
        }

        String key = body.path("key").asText(envelope.payload().orderId());
        ProducerRecord<String, String> record =
                new ProducerRecord<>(targetTopic, key, envelopeNode.toString());
        record.headers()
                .add(CorrelationContext.CORRELATION_ID_HEADER, bytes(envelope.correlationId()))
                .add(CorrelationContext.EVENT_ID_HEADER, bytes(envelope.eventId()))
                .add(CorrelationContext.EVENT_TYPE_HEADER, bytes(envelope.eventType().name()))
                .add("X-Replayed-From", bytes(sourceTopic));

        producer.send(record).get(sendTimeoutSeconds, TimeUnit.SECONDS);
        log(context, "REPLAYED eventId=" + envelope.eventId() + " eventType=" + envelope.eventType()
                + " to=" + targetTopic + " correlationId=" + envelope.correlationId());
    }

    private static int receiveCount(SQSMessage message) {
        Map<String, String> attributes = message.getAttributes();
        if (attributes == null) {
            return 1;
        }
        return Integer.parseInt(attributes.getOrDefault(RECEIVE_COUNT, "1"));
    }

    private static byte[] bytes(String value) {
        return (value == null ? "" : value).getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static void log(Context context, String message) {
        if (context != null && context.getLogger() != null) {
            context.getLogger().log("KafkaDlqReprocessor " + message + "\n");
        } else {
            System.out.println("KafkaDlqReprocessor " + message);
        }
    }
}
