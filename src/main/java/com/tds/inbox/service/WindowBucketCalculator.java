package com.tds.inbox.service;

import com.tds.inbox.config.InboxProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Computes the {@code window_bucket} for an incoming event.
 *
 * <h2>Bucket formula</h2>
 * <pre>
 *   window_bucket = floor(epoch_seconds(event_ts) / W) * W
 * </pre>
 * where {@code W} is the configured window size in seconds (default 5 s).
 *
 * <h2>Deterministic bucketing</h2>
 * <p>An event <em>always</em> belongs to its natural event-time bucket.
 * Bucket assignment is deterministic and never changes.  Late events
 * (events whose {@code event_ts} falls inside an already-drained window)
 * are handled through the drainer's allowed-lateness window, not through
 * re-bucketing.  This class never inspects wall-clock time.</p>
 *
 * <h2>Thread safety</h2>
 * <p>This class holds no mutable state and is safe to call from multiple
 * Kafka consumer threads concurrently.</p>
 */
@Component
public class WindowBucketCalculator {

    private static final Logger log = LoggerFactory.getLogger(WindowBucketCalculator.class);

    private final long windowSizeSeconds;

    // Astra DB warns when a partition exceeds 100 MB.
    // At peak 2,000 msg/sec × 4 KB/msg × W seconds = W × 8 MB per partition.
    // W=5s → 40 MB; W=10s → 80 MB; W=15s → 120 MB (over the warning threshold).
    // The hard limit is not documented, but staying under 100 MB is strongly advised.
    private static final long MAX_SAFE_WINDOW_SECONDS = 10L;
    private static final long PEAK_MSG_PER_SECOND     = 2_000L;
    private static final long ASSUMED_MSG_SIZE_BYTES  = 4_096L; // 4 KB

    public WindowBucketCalculator(InboxProperties properties) {
        this.windowSizeSeconds = properties.window().sizeSeconds();
        validateWindowSize();
    }

    /**
     * Returns the {@code window_bucket} for an event — always
     * {@code floor(epoch_seconds(eventTs) / W) * W}.
     *
     * <p>Bucket assignment is deterministic: the same {@code eventTs} always
     * produces the same {@code window_bucket}, regardless of when the event
     * is processed or the current wall-clock time.</p>
     *
     * @param eventTs the event timestamp from the CloudEvent envelope
     * @return the epoch-second bucket value used as the Cassandra partition key
     */
    public long bucket(Instant eventTs) {
        return floorToWindow(eventTs.getEpochSecond());
    }

    /**
     * Returns the configured window size in seconds.
     */
    public long getWindowSizeSeconds() {
        return windowSizeSeconds;
    }

    // -------------------------------------------------------------------------
    // Internals
    // -------------------------------------------------------------------------

    private void validateWindowSize() {
        long estimatedPartitionBytes = windowSizeSeconds * PEAK_MSG_PER_SECOND * ASSUMED_MSG_SIZE_BYTES;
        long estimatedPartitionMb    = estimatedPartitionBytes / (1024 * 1024);
        if (windowSizeSeconds > MAX_SAFE_WINDOW_SECONDS) {
            log.warn(
                    "window.size-seconds={} may produce partitions of ~{} MB at peak load ({} msg/s × {} KB). " +
                    "Astra DB issues a warning above 100 MB. Consider reducing to ≤{}s.",
                    windowSizeSeconds, estimatedPartitionMb,
                    PEAK_MSG_PER_SECOND, ASSUMED_MSG_SIZE_BYTES / 1024,
                    MAX_SAFE_WINDOW_SECONDS);
        } else {
            log.info("Window size {}s → estimated peak partition size ~{} MB (Astra 100 MB warning threshold)",
                    windowSizeSeconds, estimatedPartitionMb);
        }
    }

    private long floorToWindow(long epochSeconds) {
        return (epochSeconds / windowSizeSeconds) * windowSizeSeconds;
    }
}
