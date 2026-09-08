package com.tds.inbox.kafka;

import com.tds.inbox.config.InboxProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Publishes unprocessable messages to a dead-letter topic (DLT).
 *
 * <p>The DLT topic name is configured via {@code inbox.kafka.dead-letter-topic}
 * (default {@code cloud-events.DLT}).  Operations teams can replay or inspect
 * DLT records independently of the main consumer.</p>
 *
 * <h2>Blocking send</h2>
 * <p>{@link #send(ConsumerRecord)} blocks on the Kafka send future with a 5-second
 * timeout. This ensures the DLT write is confirmed before the caller commits the
 * source offset. If the send fails or times out, {@code false} is returned and the
 * caller must NOT acknowledge the source offset — preventing silent message loss.</p>
 */
@Component
public class DeadLetterPublisher {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterPublisher.class);
    private static final long   SEND_TIMEOUT_SECONDS = 5L;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String                         deadLetterTopic;

    public DeadLetterPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            InboxProperties properties) {
        this.kafkaTemplate  = kafkaTemplate;
        this.deadLetterTopic = properties.kafka().deadLetterTopic();
    }

    /**
     * Sends the original Kafka record to the DLT, preserving the original key.
     * Blocks until the broker acknowledges the send or the 5-second timeout elapses.
     *
     * @param record the unprocessable consumer record
     * @return {@code true} if the DLT send was acknowledged; {@code false} on failure
     */
    public boolean send(ConsumerRecord<String, String> record) {
        try {
            kafkaTemplate.send(deadLetterTopic, record.key(), record.value())
                    .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info("Published to DLT topic='{}' original_offset={}",
                    deadLetterTopic, record.offset());
            return true;
        } catch (Exception e) {
            log.error("Failed to publish to DLT topic='{}' offset={}: {}",
                    deadLetterTopic, record.offset(), e.getMessage(), e);
            return false;
        }
    }
}
