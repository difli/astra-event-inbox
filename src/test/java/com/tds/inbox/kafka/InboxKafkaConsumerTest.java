package com.tds.inbox.kafka;

import com.tds.inbox.domain.SlupEvent;
import com.tds.inbox.drainer.DrainerCursor;
import com.tds.inbox.repository.EventRepository;
import com.tds.inbox.service.CloudEventParseException;
import com.tds.inbox.service.EventParser;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link InboxKafkaConsumer}.
 *
 * Dependencies are mocked so no Kafka broker or Astra DB is needed.
 */
class InboxKafkaConsumerTest {

    private EventParser         parser;
    private EventRepository     repository;
    private DeadLetterPublisher deadLetter;
    private Acknowledgment      ack;
    private DrainerCursor       drainerCursor;
    private MeterRegistry       meterRegistry;
    private InboxKafkaConsumer  consumer;

    private static final SlupEvent DUMMY_EVENT = new SlupEvent(
            1784194885L,
            Instant.ofEpochSecond(1784194887L),
            "test-event-id",
            "guid1",
            "SET",
            "UTS",
            "webmail",
            "10.0.0.1",
            Instant.now(),
            "{}"
    );

    @BeforeEach
    void setUp() {
        parser        = mock(EventParser.class);
        repository    = mock(EventRepository.class);
        deadLetter    = mock(DeadLetterPublisher.class);
        ack           = mock(Acknowledgment.class);
        drainerCursor = new DrainerCursor();
        meterRegistry = new SimpleMeterRegistry();
        consumer      = new InboxKafkaConsumer(parser, repository, deadLetter,
                drainerCursor, meterRegistry);
    }

    private ConsumerRecord<String, String> record(String value) {
        return new ConsumerRecord<>("cloud-events", 0, 42L, null, value);
    }

    // -------------------------------------------------------------------------
    // Happy path
    // -------------------------------------------------------------------------

    @Test
    void consume_validEvent_savesAndAcknowledges() {
        when(parser.parse(any())).thenReturn(DUMMY_EVENT);

        consumer.consume(record("{}"), ack);

        verify(repository).save(DUMMY_EVENT);
        verify(ack).acknowledge();
        verifyNoInteractions(deadLetter);
    }

    // -------------------------------------------------------------------------
    // Parse error → DLT + acknowledge (do not re-stall)
    // -------------------------------------------------------------------------

    @Test
    void consume_parseError_routesToDLTAndAcknowledges() {
        when(parser.parse(any()))
                .thenThrow(new CloudEventParseException("bad json"));
        when(deadLetter.send(any())).thenReturn(true);

        consumer.consume(record("not-json"), ack);

        verify(deadLetter).send(any());
        verify(ack).acknowledge();        // offset must be committed
        verifyNoInteractions(repository);
    }

    @Test
    void consume_parseError_dltFails_throwsAndDoesNotAcknowledge() {
        // When DLT publish fails, consume() must throw so the DefaultErrorHandler
        // seeks the partition back to this offset.  Returning normally would allow
        // a later successful record from the same partition to advance the committed
        // offset past the failed record, silently losing it.
        when(parser.parse(any()))
                .thenThrow(new CloudEventParseException("bad json"));
        when(deadLetter.send(any())).thenReturn(false);

        assertThatThrownBy(() -> consumer.consume(record("not-json"), ack))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("DLT publication failed");

        verify(deadLetter).send(any());
        verify(ack, never()).acknowledge();  // DLT failed — offset must NOT be committed
        verifyNoInteractions(repository);
    }

    // -------------------------------------------------------------------------
    // Astra error → rethrow so error handler seeks back; do NOT acknowledge
    // -------------------------------------------------------------------------

    @Test
    void consume_astraError_doesNotAcknowledge() {
        when(parser.parse(any())).thenReturn(DUMMY_EVENT);
        doThrow(new RuntimeException("Astra unavailable"))
                .when(repository).save(any());

        // consume() must rethrow so the DefaultErrorHandler can seek the partition
        // back to the failed offset (offset-safety contract).
        assertThatThrownBy(() -> consumer.consume(record("{}"), ack))
                .isInstanceOf(RuntimeException.class);

        verify(ack, never()).acknowledge();
        verifyNoInteractions(deadLetter);
    }

    /**
     * Contract: when repository.save throws, consume() rethrows — allowing the
     * DefaultErrorHandler to seek the partition back to the failed offset.
     */
    @Test
    void consume_astraError_rethrowsForErrorHandler() {
        when(parser.parse(any())).thenReturn(DUMMY_EVENT);
        RuntimeException cause = new RuntimeException("Astra timeout");
        doThrow(cause).when(repository).save(any());

        assertThatThrownBy(() -> consumer.consume(record("{}"), ack))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Astra timeout");
    }

    /**
     * Contract: when parse fails, consume() does NOT throw — the error is handled
     * internally (DLT + ack). The error handler must NOT fire for parse errors.
     */
    @Test
    void consume_parseError_doesNotThrow() {
        when(parser.parse(any()))
                .thenThrow(new CloudEventParseException("bad json"));
        when(deadLetter.send(any())).thenReturn(true);

        // Must complete without throwing
        consumer.consume(record("not-json"), ack);

        verify(deadLetter).send(any());
        verify(ack).acknowledge();
    }

    // -------------------------------------------------------------------------
    // Idempotency: second call with same event still saves (no dedup at consumer)
    // -------------------------------------------------------------------------

    @Test
    void consume_sameEventTwice_saveCalledTwice() {
        when(parser.parse(any())).thenReturn(DUMMY_EVENT);

        consumer.consume(record("{}"), ack);
        consumer.consume(record("{}"), ack);

        // Dedup is handled by Cassandra's upsert; the consumer always attempts to save
        verify(repository, times(2)).save(any());
        verify(ack, times(2)).acknowledge();
    }

    // -------------------------------------------------------------------------
    // Late-arrival detection
    // -------------------------------------------------------------------------

    @Test
    void consume_bucketBehindCursor_incrementsLateArrivalCounter() {
        // DUMMY_EVENT has windowBucket = 1784194885
        // Set cursor to 1784194890 (one window ahead) → bucket 1784194885 is behind
        drainerCursor.advance(1784194890L);
        when(parser.parse(any())).thenReturn(DUMMY_EVENT);

        consumer.consume(record("{}"), ack);

        verify(ack).acknowledge();  // write still succeeds
        Counter counter = meterRegistry.find("inbox.events.late_arrival").counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }

    @Test
    void consume_bucketAheadOfCursor_doesNotIncrementLateArrivalCounter() {
        // cursor = 0 (drainer never ran) → no late arrival possible
        when(parser.parse(any())).thenReturn(DUMMY_EVENT);

        consumer.consume(record("{}"), ack);

        verify(ack).acknowledge();
        Counter counter = meterRegistry.find("inbox.events.late_arrival").counter();
        // counter is 0 or not yet registered (no increment was ever called)
        assertThat(counter == null || counter.count() == 0.0).isTrue();
    }

    // -------------------------------------------------------------------------
    // Offset-safety: DLT failure on one partition must not let a later record
    // from the same partition silently advance the committed offset past the
    // failed one.
    // -------------------------------------------------------------------------

    /**
     * Scenario (same partition, two consecutive records in a single poll batch):
     *
     * <pre>
     *   offset 10 — parse error, DLT publish fails  → must throw, no ack
     *   offset 11 — valid event                      → must NOT be processed until 10 is resolved
     * </pre>
     *
     * The test verifies that:
     * 1. {@code consume(offset=10)} throws {@link IllegalStateException}.
     * 2. The committed offset never advances past offset 10 (ack is never called for
     *    the offset-10 record).
     * 3. After the DLT recovers, {@code consume(offset=10)} succeeds, commits, and
     *    the subsequent {@code consume(offset=11)} is processed normally.
     */
    @Test
    void consume_dltFailure_offsetSafety_commitDoesNotAdvancePastFailedRecord() {
        // --- offset 10: parse error, DLT publish fails ---
        ConsumerRecord<String, String> rec10 =
                new ConsumerRecord<>("cloud-events", 2, 10L, null, "bad");
        Acknowledgment ack10 = mock(Acknowledgment.class);

        when(parser.parse("bad")).thenThrow(new CloudEventParseException("bad json"));
        when(deadLetter.send(rec10)).thenReturn(false); // DLT down

        // consume() must throw — DefaultErrorHandler will seek partition 2 back to offset 10
        assertThatThrownBy(() -> consumer.consume(rec10, ack10))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("DLT publication failed");

        // Committed offset must NOT have advanced past offset 10
        verify(ack10, never()).acknowledge();

        // --- offset 11: valid event — would have been silently processed before the fix ---
        // (In production, the DefaultErrorHandler prevents offset 11 from being delivered
        //  until offset 10 succeeds.  In this unit test we simulate the broker redelivering
        //  offset 10 once the DLT recovers, and then processing offset 11.)

        // DLT recovers
        when(deadLetter.send(rec10)).thenReturn(true);

        // Redeliver offset 10 (DLT now works)
        consumer.consume(rec10, ack10);
        verify(ack10, times(1)).acknowledge(); // offset 10 committed after successful DLT

        // Now offset 11 can proceed
        ConsumerRecord<String, String> rec11 =
                new ConsumerRecord<>("cloud-events", 2, 11L, null, "{}");
        Acknowledgment ack11 = mock(Acknowledgment.class);
        when(parser.parse("{}")).thenReturn(DUMMY_EVENT);

        consumer.consume(rec11, ack11);
        verify(ack11, times(1)).acknowledge(); // offset 11 committed after successful write
        verify(repository, times(1)).save(DUMMY_EVENT);
    }
}
