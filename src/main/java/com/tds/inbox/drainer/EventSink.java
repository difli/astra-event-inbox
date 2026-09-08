package com.tds.inbox.drainer;

import com.tds.inbox.domain.SlupEvent;

import java.util.List;

/**
 * Downstream consumer of a fully-drained window.
 *
 * <p>The drainer calls {@link #write(long, List)} once per sealed window, in
 * ascending {@code window_bucket} order. Implementations must be idempotent:
 * the drainer may call {@code write} again for the same bucket on restart
 * (before {@link DrainProgressRepository} confirms the bucket was recorded).</p>
 *
 * <h2>Contract</h2>
 * <ul>
 *   <li>The list is ordered by {@code (event_ts ASC, event_id ASC)} — the
 *       natural Cassandra clustering order of {@code slup_inbox}.</li>
 *   <li>The list may be empty if the window contained no events (bucket was
 *       sealed but no producers wrote to it).</li>
 *   <li>{@code write} must complete (or throw) before the drainer records
 *       progress. A thrown exception prevents progress from being written and
 *       causes the drainer to retry the window on the next poll cycle.</li>
 * </ul>
 *
 * <h2>Extensibility</h2>
 * <p>Phase 2 ships with {@link FileWriterEventSink}. Future implementations
 * may write to S3, Pub/Sub, another Kafka topic, or a streaming pipeline
 * without changing the drainer logic.</p>
 */
public interface EventSink {

    /**
     * Receive all events from a sealed window.
     *
     * @param windowBucket the epoch-second bucket identifier (e.g. {@code 1_717_000_000})
     * @param events       ordered list of events in this window; may be empty
     * @throws Exception   any failure — causes the drainer to skip recording progress
     *                     and retry on the next poll cycle
     */
    void write(long windowBucket, List<SlupEvent> events) throws Exception;
}
