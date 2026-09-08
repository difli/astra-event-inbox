package com.tds.inbox.kafka;

import com.tds.inbox.domain.SlupEvent;
import com.tds.inbox.drainer.DrainerCursor;
import com.tds.inbox.repository.EventRepository;
import com.tds.inbox.service.CloudEventParseException;
import com.tds.inbox.service.EventParser;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * Kafka consumer for the CloudEvent inbox topic.
 *
 * <h2>Acknowledgement strategy</h2>
 * <p>Manual offset commit ({@code ack-mode: MANUAL_IMMEDIATE}): the offset is
 * committed only after the event has been durably written to Astra DB.  A crash
 * between the Astra write and the commit causes Kafka to redeliver the message;
 * the idempotent upsert absorbs the duplicate.</p>
 *
 * <h2>Synchronous path (default)</h2>
 * <p>{@code inbox.astra.async-writes=false}. {@link EventRepository#save} is called
 * directly. On success, the offset is committed. On failure the exception is rethrown
 * so the {@link org.springframework.kafka.listener.DefaultErrorHandler} seeks the
 * partition back to the failed offset — Kafka redelivers after the retry back-off.</p>
 *
 * <h2>Asynchronous path (opt-in)</h2>
 * <p>{@code inbox.astra.async-writes=true}. {@link EventRepository#saveAsync} is
 * called and the resulting {@link CompletableFuture} is collected in a per-partition
 * list. Before committing the offset, the consumer waits for all in-flight futures
 * for the current partition to complete successfully.</p>
 *
 * <p>Per-partition ordering semantics: no new write is started for a partition
 * if a previous write for that partition has failed — an {@link IllegalStateException}
 * is thrown so the error handler seeks back, and the offset is NOT committed.</p>
 *
 * <h2>Error handling</h2>
 * <ul>
 *   <li>Parse errors ({@link CloudEventParseException}) → forward to dead-letter topic,
 *       then commit the offset so the poison message does not block the partition.
 *       If the DLT send fails, an {@link IllegalStateException} is thrown so the
 *       error handler seeks back — the offset is NOT committed until the DLT succeeds.</li>
 *   <li>Astra errors (sync or async) → exception is thrown; the error handler seeks
 *       back; Kafka redelivers after {@code inbox.kafka.retry-interval-ms} (default
 *       1 000 ms), giving Astra time to recover.</li>
 * </ul>
 */
@Component
public class InboxKafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(InboxKafkaConsumer.class);

    private final EventParser         parser;
    private final EventRepository     repository;
    private final DeadLetterPublisher deadLetter;
    private final DrainerCursor       drainerCursor;

    private final Counter successCounter;
    private final Counter parseErrorCounter;
    private final Counter astraErrorCounter;
    private final Counter lateArrivalCounter;

    /**
     * Per-partition list of in-flight async write futures.
     * Used only when {@code inbox.astra.async-writes=true}.
     * ConcurrentHashMap because listener threads may belong to different partitions.
     */
    private final Map<TopicPartition, List<CompletableFuture<Void>>> inFlightByPartition =
            new ConcurrentHashMap<>();

    public InboxKafkaConsumer(
            EventParser         parser,
            EventRepository     repository,
            DeadLetterPublisher deadLetter,
            DrainerCursor       drainerCursor,
            MeterRegistry       meterRegistry) {
        this.parser         = parser;
        this.repository     = repository;
        this.deadLetter     = deadLetter;
        this.drainerCursor  = drainerCursor;

        this.successCounter     = meterRegistry.counter("inbox.events.success");
        this.parseErrorCounter  = meterRegistry.counter("inbox.events.parse_error");
        this.astraErrorCounter  = meterRegistry.counter("inbox.events.astra_error");
        this.lateArrivalCounter = meterRegistry.counter("inbox.events.late_arrival");
    }

    @KafkaListener(
            topics         = "${inbox.kafka.topic}",
            groupId        = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String raw = record.value();

        // 1. Parse
        SlupEvent event;
        try {
            event = parser.parse(raw);
        } catch (CloudEventParseException e) {
            log.warn("Parse error on topic={} partition={} offset={} – routing to DLT: {}",
                    record.topic(), record.partition(), record.offset(), e.getMessage());
            parseErrorCounter.increment();
            boolean dltSent = deadLetter.send(record);
            if (dltSent) {
                ack.acknowledge();   // commit: poison message must not stall the partition
            } else {
                // DLT send failed — throw so the DefaultErrorHandler seeks the partition
                // back to this offset.  Returning normally would allow a later successful
                // record in the same batch to advance the committed offset past this one,
                // silently losing it.
                log.error("DLT send failed for partition={} offset={} — seeking back for retry",
                        record.partition(), record.offset());
                throw new IllegalStateException(
                        "DLT publication failed for partition=" + record.partition()
                        + ", offset=" + record.offset());
            }
            return;
        }

        // 2. Persist to Astra DB
        if (repository.isAsyncWrites()) {
            consumeAsync(record, event, ack);
        } else {
            consumeSync(record, event, ack);
        }
    }

    // -------------------------------------------------------------------------
    // Synchronous write path
    // -------------------------------------------------------------------------

    private void consumeSync(ConsumerRecord<String, String> record,
                              SlupEvent event, Acknowledgment ack) {
        try {
            repository.save(event);
        } catch (Exception e) {
            astraErrorCounter.increment();
            log.error("Astra write failed for event_id={} – offset NOT committed (will redeliver): {}",
                    event.eventId(), e.getMessage(), e);
            // Rethrow so the DefaultErrorHandler seeks back to this offset,
            // preventing a later successful record in the same poll batch from
            // advancing the committed position past this failed record.
            throw e instanceof RuntimeException re ? re : new RuntimeException(e);
        }

        // Commit offset only after successful persistence
        ack.acknowledge();
        successCounter.increment();
        checkLateArrival(event);

        if (log.isDebugEnabled()) {
            log.debug("Processed event_id={} bucket={} offset={}",
                    event.eventId(), event.windowBucket(), record.offset());
        }
    }

    // -------------------------------------------------------------------------
    // Asynchronous write path
    // -------------------------------------------------------------------------

    private void consumeAsync(ConsumerRecord<String, String> record,
                               SlupEvent event, Acknowledgment ack) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());

        // If any prior write for this partition has already failed, do not start new writes.
        List<CompletableFuture<Void>> partitionFutures =
                inFlightByPartition.computeIfAbsent(tp, k -> new ArrayList<>());

        boolean partitionHasFailed = partitionFutures.stream()
                .anyMatch(CompletableFuture::isCompletedExceptionally);
        if (partitionHasFailed) {
            astraErrorCounter.increment();
            clearPartitionFutures(tp);
            // Throw so the DefaultErrorHandler seeks the partition back to this offset,
            // preventing a later successful record from advancing past the failed one.
            throw new IllegalStateException(
                    "Async write previously failed for partition=" + tp
                    + ", event_id=" + event.eventId() + " — seeking back for retry");
        }

        // Dispatch async write
        CompletableFuture<Void> future;
        try {
            future = repository.saveAsync(event);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            astraErrorCounter.increment();
            log.error("Interrupted waiting for semaphore for event_id={} — re-interrupting and throwing",
                    event.eventId());
            // Rethrow so the DefaultErrorHandler seeks back; swallowing InterruptedException
            // and returning normally would let a later offset advance past this one.
            throw new IllegalStateException(
                    "Interrupted acquiring semaphore for event_id=" + event.eventId(), e);
        }
        partitionFutures.add(future);

        // Wait for all in-flight futures for this partition before committing.
        // Block here to preserve the MANUAL_IMMEDIATE ack semantics — we must not
        // return from consume() without either ack-ing or explicitly not ack-ing.
        try {
            CompletableFuture.allOf(partitionFutures.toArray(new CompletableFuture[0])).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            astraErrorCounter.increment();
            log.error("Interrupted waiting for async writes for partition={} — re-interrupting and throwing", tp);
            // Rethrow so the DefaultErrorHandler seeks back; returning normally risks
            // advancing the committed offset past the failed record.
            throw new IllegalStateException(
                    "Interrupted waiting for async writes for partition=" + tp, e);
        } catch (ExecutionException e) {
            astraErrorCounter.increment();
            log.error("Async Astra write failed for partition={} event_id={} – seeking back: {}",
                    tp, event.eventId(), e.getCause().getMessage());
            clearPartitionFutures(tp);
            // Rethrow so the DefaultErrorHandler seeks the partition back to this offset.
            throw new IllegalStateException(
                    "Async Astra write failed for partition=" + tp
                    + ", event_id=" + event.eventId(), e.getCause());
        }

        // All writes for this partition completed successfully — commit and clear futures.
        clearPartitionFutures(tp);
        ack.acknowledge();
        successCounter.increment();
        checkLateArrival(event);

        if (log.isDebugEnabled()) {
            log.debug("Async processed event_id={} bucket={} offset={}",
                    event.eventId(), event.windowBucket(), record.offset());
        }
    }

    private void clearPartitionFutures(TopicPartition tp) {
        inFlightByPartition.remove(tp);
    }

    // -------------------------------------------------------------------------
    // Late-arrival detection (post-write, ingestion path unchanged)
    // -------------------------------------------------------------------------

    /**
     * Called after every successful Astra write.  If the event's
     * {@code window_bucket} is at or below the drainer cursor the bucket has
     * already been drained and this event will not be reprocessed unless an
     * operator initiates a manual replay.
     *
     * <p>The ingestion path is not changed: the event is durably written with
     * its natural event-time bucket regardless.  This method only emits
     * observability signals.</p>
     *
     * <p>When the drainer is disabled or has not yet drained any bucket the
     * cursor is {@code 0}, and no event can have a bucket ≤ 0 in practice,
     * so this check is a no-op.</p>
     */
    private void checkLateArrival(SlupEvent event) {
        long cursor = drainerCursor.get();
        if (cursor > 0 && event.windowBucket() <= cursor) {
            lateArrivalCounter.increment();
            log.warn("Late arrival: event_id={} bucket={} is at or before drainer cursor={} " +
                     "— event persisted but will NOT be reprocessed unless replayed manually",
                    event.eventId(), event.windowBucket(), cursor);
        }
    }
}
