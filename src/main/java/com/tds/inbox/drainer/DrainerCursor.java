package com.tds.inbox.drainer;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Shared, thread-safe record of the highest {@code window_bucket} that the
 * drainer has successfully processed and recorded in {@code drain_progress}.
 *
 * <h2>Purpose</h2>
 * <p>The ingestion path ({@link com.tds.inbox.kafka.InboxKafkaConsumer}) reads
 * this value after every successful write to detect post-drain late arrivals —
 * events whose {@code window_bucket} is at or below the cursor, meaning the
 * drainer has already moved past that window.  The event is still written
 * correctly (deterministic bucketing is preserved), but the consumer emits a
 * WARN log and increments {@code inbox.events.late_arrival} so operators can
 * observe the situation.</p>
 *
 * <h2>Default value</h2>
 * <p>Starts at {@code 0}.  When the drainer is disabled
 * ({@code inbox.drainer.enabled=false}) or has not yet drained any bucket, the
 * cursor remains {@code 0} and no late-arrival detection fires.</p>
 *
 * <h2>Monotonicity</h2>
 * <p>{@link #advance(long)} only moves the cursor forward.  A concurrent call
 * with a smaller value is a no-op.</p>
 */
@Component
public class DrainerCursor {

    private final AtomicLong cursor = new AtomicLong(0L);

    /**
     * Advances the cursor to {@code windowBucket} if it is greater than the
     * current value.  Called by {@link InboxDrainer} after each successful drain.
     *
     * @param windowBucket the bucket that was just drained
     */
    public void advance(long windowBucket) {
        cursor.accumulateAndGet(windowBucket, Math::max);
    }

    /**
     * Returns the current cursor value — the highest bucket successfully drained,
     * or {@code 0} if no bucket has been drained in this JVM lifetime.
     */
    public long get() {
        return cursor.get();
    }
}
