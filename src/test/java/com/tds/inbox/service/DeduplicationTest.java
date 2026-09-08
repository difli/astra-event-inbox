package com.tds.inbox.service;

import com.tds.inbox.config.InboxProperties;
import com.tds.inbox.domain.SlupEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the deduplication contract of {@link CloudEventParser} and
 * {@link WindowBucketCalculator}.
 *
 * <p>Storage is idempotent when {@code event_id}, {@code event_ts}, and the
 * calculated {@code window_bucket} remain identical across redeliveries. This is
 * <em>not</em> end-to-end exactly-once processing. The guarantee holds as long as
 * the upstream source assigns stable {@code event_id} and {@code event_ts} values
 * for the same logical event.</p>
 *
 * <p>No Spring context is loaded — the parser and its dependencies are
 * instantiated directly so tests run fast.</p>
 */
class DeduplicationTest {

    private static final String EVENT_JSON = """
            {
              "specversion": "1.0",
              "id": "dedup-event-id-001",
              "source": "urn:tds:prod",
              "type": "tds.slup.v1",
              "time": "2026-07-16T10:00:00.000Z",
              "datacontenttype": "application/json",
              "data": {
                "guid": "aabbcc001",
                "operation": "SET",
                "requestName": "UTS",
                "applicationId": "webmail",
                "timestamp": "2026-07-16T10:00:00.000Z",
                "clientAddress": "10.0.0.1",
                "fields": []
              }
            }
            """;

    /** Same event_id, different data.timestamp — produces a different primary key. */
    private static final String SAME_ID_DIFFERENT_TS_JSON = """
            {
              "specversion": "1.0",
              "id": "dedup-event-id-001",
              "source": "urn:tds:prod",
              "type": "tds.slup.v1",
              "time": "2026-07-16T10:00:00.000Z",
              "datacontenttype": "application/json",
              "data": {
                "guid": "aabbcc001",
                "operation": "SET",
                "requestName": "UTS",
                "applicationId": "webmail",
                "timestamp": "2026-07-16T10:00:05.000Z",
                "clientAddress": "10.0.0.1",
                "fields": []
              }
            }
            """;

    private CloudEventParser parser;

    @BeforeEach
    void setUp() {
        InboxProperties props = new InboxProperties(
                new InboxProperties.AstraProperties(
                        "bundle.zip", "id", "secret", "tds_inbox", "LOCAL_QUORUM", 5000,
                        86400L, false, 4),
                new InboxProperties.KafkaProperties("cloud-events", "cloud-events.DLT", 1000L,
                        new InboxProperties.KafkaProperties.HealthProperties(10_000L)),
                new InboxProperties.WindowProperties(5L, 60L),
                new InboxProperties.DrainerProperties(true, "primary", 5000L, "/tmp/inbox-drain",
                        "LOCAL_QUORUM", com.tds.inbox.drainer.DrainerBootstrapMode.LATEST, 0L)
        );
        parser = new CloudEventParser(new WindowBucketCalculator(props));
    }

    // -------------------------------------------------------------------------
    // 1. Same event_id, same event_ts, same bucket → idempotent upsert
    // -------------------------------------------------------------------------

    /**
     * Parsing the same CloudEvent JSON twice must produce {@link SlupEvent} records
     * with identical {@code windowBucket}, {@code eventTs}, and {@code eventId}.
     * When both records are written to Cassandra they produce the same primary key,
     * making the second write a no-op upsert.
     */
    @Test
    void sameEventIdSameEventTsSameBucket_idempotentUpsert() {
        SlupEvent first  = parser.parse(EVENT_JSON);
        SlupEvent second = parser.parse(EVENT_JSON);

        assertThat(first.eventId())     .isEqualTo(second.eventId());
        assertThat(first.eventTs())     .isEqualTo(second.eventTs());
        assertThat(first.windowBucket()).isEqualTo(second.windowBucket());

        // Both records would map to the same Cassandra row
        assertThat(first.eventId())     .isEqualTo("dedup-event-id-001");
        // epoch for 2026-07-16T10:00:00.000Z = 1784196000
        assertThat(first.eventTs().getEpochSecond()).isEqualTo(1784196000L);
        // floor(1784196000 / 5) * 5 = 1784196000 (already a multiple of 5)
        assertThat(first.windowBucket()).isEqualTo(1784196000L);
    }

    // -------------------------------------------------------------------------
    // 2. Same event_id, different event_ts → different primary key → two rows
    // -------------------------------------------------------------------------

    /**
     * When the upstream source sends the same logical event with a different
     * {@code data.timestamp}, the primary key differs ({@code event_ts} is different)
     * and Cassandra would produce two distinct rows — breaking idempotency.
     * This test documents and confirms the expected behaviour so it is not
     * accidentally "fixed" in the future.
     */
    @Test
    void sameEventIdDifferentEventTs_differentPrimaryKey() {
        SlupEvent original  = parser.parse(EVENT_JSON);
        SlupEvent different = parser.parse(SAME_ID_DIFFERENT_TS_JSON);

        assertThat(original.eventId()).isEqualTo(different.eventId());

        // Different data.timestamp → different eventTs
        assertThat(original.eventTs()).isNotEqualTo(different.eventTs());

        // Different eventTs → different windowBucket or different CK position
        // In this case both fall in different 5-second buckets
        // original:  epoch 1784196000 → bucket 1784196000
        // different: epoch 1784196005 → bucket 1784196005
        assertThat(original.windowBucket()).isNotEqualTo(different.windowBucket());
    }

    // -------------------------------------------------------------------------
    // 3. Duplicate Kafka delivery after restart → same SlupEvent
    // -------------------------------------------------------------------------

    /**
     * Simulates a Kafka redelivery after a restart by calling the parser with
     * the same raw JSON at two different wall-clock times. Because the parser is
     * a pure function of its input (both {@code event_id} and {@code event_ts} are
     * extracted from the JSON, not from wall-clock time), the resulting
     * {@link SlupEvent} records must have identical primary-key fields regardless
     * of when the parser is invoked.
     *
     * <p>Note: {@code ingestTime} is wall-clock time and will differ between
     * calls — this is expected and acceptable because {@code ingestTime} is not
     * part of the primary key.</p>
     */
    @Test
    void duplicateKafkaDeliveryAfterRestart_sameOffsetSameEvent() throws InterruptedException {
        SlupEvent beforeRestart = parser.parse(EVENT_JSON);

        // Simulate time passing between the two deliveries
        Thread.sleep(10);

        SlupEvent afterRestart = parser.parse(EVENT_JSON);

        // Primary key fields must be identical
        assertThat(beforeRestart.eventId())     .isEqualTo(afterRestart.eventId());
        assertThat(beforeRestart.eventTs())     .isEqualTo(afterRestart.eventTs());
        assertThat(beforeRestart.windowBucket()).isEqualTo(afterRestart.windowBucket());

        // ingestTime may differ (wall-clock field, not part of PK — that is fine)
        // We do not assert equality on ingestTime here
    }
}
