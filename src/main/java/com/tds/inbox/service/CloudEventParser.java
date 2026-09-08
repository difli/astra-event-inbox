package com.tds.inbox.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.tds.inbox.domain.CloudEvent;
import com.tds.inbox.domain.SlupEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Parses a raw CloudEvent JSON string into a {@link SlupEvent} ready for
 * persistence.
 *
 * <p>The mapper is configured at construction time; it is thread-safe and reused
 * across all Kafka consumer threads.</p>
 */
@Component
public class CloudEventParser implements EventParser {

    private static final Logger log = LoggerFactory.getLogger(CloudEventParser.class);

    private final ObjectMapper         mapper;
    private final WindowBucketCalculator bucketCalculator;

    public CloudEventParser(WindowBucketCalculator bucketCalculator) {
        this.bucketCalculator = bucketCalculator;
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /**
     * Parses {@code json} and returns a {@link SlupEvent}.
     *
     * @param json raw CloudEvent JSON from Kafka
     * @return parsed event
     * @throws CloudEventParseException if the JSON is malformed or mandatory fields are missing
     */
    public SlupEvent parse(String json) {
        CloudEvent envelope;
        try {
            envelope = mapper.readValue(json, CloudEvent.class);
        } catch (Exception e) {
            throw new CloudEventParseException("Failed to deserialise CloudEvent JSON", e);
        }

        validate(envelope);

        Instant eventTs = resolveEventTs(envelope);
        Instant now     = Instant.now();
        long    bucket  = bucketCalculator.bucket(eventTs);

        CloudEvent.SlupData data = envelope.data();

        return new SlupEvent(
                bucket,
                eventTs,
                envelope.id(),
                nullToEmpty(data.guid()),
                nullToEmpty(data.operation()),
                nullToEmpty(data.requestName()),
                nullToEmpty(data.applicationId()),
                nullToEmpty(data.clientAddress()),
                now,
                json
        );
    }

    // -------------------------------------------------------------------------
    // Internals
    // -------------------------------------------------------------------------

    /**
     * Resolves the event timestamp.
     * Prefers {@code data.timestamp}; falls back to the envelope {@code time} field.
     */
    private Instant resolveEventTs(CloudEvent envelope) {
        if (envelope.data() != null && envelope.data().timestamp() != null) {
            return envelope.data().timestamp();
        }
        if (envelope.time() != null) {
            log.warn("event_id={} – data.timestamp missing, falling back to envelope.time",
                    envelope.id());
            return envelope.time();
        }
        throw new CloudEventParseException(
                "event_id=" + envelope.id() + " – no usable timestamp found");
    }

    private void validate(CloudEvent envelope) {
        if (envelope.id() == null || envelope.id().isBlank()) {
            throw new CloudEventParseException("CloudEvent is missing required field 'id'");
        }
        if (envelope.data() == null) {
            throw new CloudEventParseException(
                    "CloudEvent id=" + envelope.id() + " has no 'data' payload");
        }
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }
}
