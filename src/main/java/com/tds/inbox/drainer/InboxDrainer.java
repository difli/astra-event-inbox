package com.tds.inbox.drainer;

import com.tds.inbox.config.InboxProperties;
import com.tds.inbox.domain.SlupEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Scheduled service that drains sealed time-windows from {@code slup_inbox}
 * and forwards them to the configured {@link EventSink}.
 *
 * <h2>Lifecycle</h2>
 * <ol>
 *   <li>On the first poll cycle, query {@code drain_progress} to find the last
 *       successfully drained bucket ({@code cursor}).  If none, use
 *       {@link DrainerBootstrapMode} to determine the starting cursor.</li>
 *   <li>On each cycle, walk forward from {@code cursor + W} until no more sealed
 *       buckets are available.</li>
 *   <li>A bucket is <em>sealed</em> when:
 *       {@code now >= window_bucket + W + allowedLatenessSeconds}.</li>
 *   <li>For each sealed bucket:
 *       <ol>
 *         <li>Read all events from {@code slup_inbox} via
 *             {@link DrainProgressRepository#readEvents(long)}.</li>
 *         <li>Forward to {@link EventSink#write(long, List)}.</li>
 *         <li>Record progress via
 *             {@link DrainProgressRepository#recordProgress(DrainedWindow)}.</li>
 *       </ol>
 *   </li>
 * </ol>
 *
 * <h2>Restart / replay safety</h2>
 * <p>On restart, the drainer reads {@code drain_progress} to restore its cursor AND
 * immediately calls {@link DrainerCursor#advance(long)} to update the shared cursor.
 * This ensures late-arrival detection in the ingestion path reflects the restored
 * cursor from the very first event processed after restart.</p>
 * <p>If progress was recorded the cursor starts at the last drained bucket; if the
 * sink write succeeded but progress recording failed, the same bucket is
 * re-processed (sink must be idempotent — {@link FileWriterEventSink} overwrites).
 * Empty buckets (no events in that window) are still recorded so gaps do not
 * cause the cursor to stall.</p>
 *
 * <h2>Enabling / disabling</h2>
 * <p>Conditional on {@code inbox.drainer.enabled=true}.  Default is {@code false}
 * (writer-only mode). Set to {@code true} to activate the drainer.</p>
 *
 * <p><strong>PoC constraint:</strong> only ONE drainer instance may run at a time
 * across the cluster. Distributed locking is not yet implemented. Running multiple
 * instances simultaneously will result in duplicate drain processing.</p>
 *
 * <h2>drain_progress TTL</h2>
 * <p>The {@code drain_progress} table has a {@code default_time_to_live} of 7 days.
 * Progress records older than 7 days expire. Operators must ensure the drainer runs
 * within the TTL window — if it is idle for longer than 7 days, the cursor will be
 * lost and the drainer will restart according to the configured
 * {@link DrainerBootstrapMode}.</p>
 */
@Service
@EnableScheduling
@ConditionalOnProperty(name = "inbox.drainer.enabled", havingValue = "true", matchIfMissing = false)
public class InboxDrainer {

    private static final Logger log = LoggerFactory.getLogger(InboxDrainer.class);

    private final DrainProgressRepository progressRepo;
    private final EventSink               sink;
    private final DrainerCursor           drainerCursor;
    private final String                  drainId;
    private final long                    windowSizeSeconds;
    private final long                    allowedLatenessSeconds;

    /**
     * In-memory cursor — the highest bucket that has been successfully recorded
     * in {@code drain_progress} during this JVM lifetime.
     * Initialised to -1 to indicate "not yet loaded"; first poll resolves it
     * from Astra or defaults to 0.
     */
    private volatile long cursor = -1L;

    private final DrainerBootstrapMode bootstrapMode;
    private final long                  startBucket;

    public InboxDrainer(DrainProgressRepository progressRepo,
                        EventSink sink,
                        DrainerCursor drainerCursor,
                        InboxProperties properties) {
        this.progressRepo           = progressRepo;
        this.sink                   = sink;
        this.drainerCursor          = drainerCursor;
        this.drainId                = properties.drainer().drainId();
        this.windowSizeSeconds      = properties.window().sizeSeconds();
        this.allowedLatenessSeconds = properties.window().allowedLatenessSeconds();
        this.bootstrapMode          = properties.drainer().bootstrapMode();
        this.startBucket            = properties.drainer().startBucket();
    }

    /**
     * Main drain loop.  Runs every {@code inbox.drainer.poll-interval-ms} milliseconds
     * (configured via {@code INBOX_DRAINER_POLL_INTERVAL_MS}, default 5000).
     */
    @Scheduled(fixedDelayString = "${inbox.drainer.poll-interval-ms:5000}")
    public void drainSealedBuckets() {
        try {
            initialiseCursorIfNeeded();
            drainAllAvailable();
        } catch (Exception e) {
            log.error("Drainer cycle failed (will retry on next poll): {}", e.getMessage(), e);
        }
    }

    // -------------------------------------------------------------------------
    // Internals
    // -------------------------------------------------------------------------

    /**
     * Loads the cursor from Astra on the first invocation.
     * Subsequent calls are no-ops because {@code cursor} is then ≥ 0.
     *
     * <p>When prior progress exists: restores {@code cursor} from Astra and
     * immediately updates the shared {@link DrainerCursor} so that
     * late-arrival detection in the ingestion path is accurate from the first
     * event processed after restart — not just after the first drain cycle.</p>
     *
     * <p>When no prior progress exists: uses {@link DrainerBootstrapMode} to
     * determine the starting cursor:
     * <ul>
     *   <li>{@code LATEST} — start from current time minus the lateness window
     *       (skips history).</li>
     *   <li>{@code CONFIGURED} — start from the configured {@code startBucket}
     *       epoch-second value.</li>
     * </ul>
     * </p>
     */
    void initialiseCursorIfNeeded() {
        if (cursor >= 0) return;

        Optional<Long> last = progressRepo.findLastDrainedBucket(drainId);
        if (last.isPresent()) {
            cursor = last.get();
            // Restore the shared DrainerCursor immediately so the ingestion path
            // can detect late arrivals from the very first event after restart.
            drainerCursor.advance(cursor);
            log.info("Drainer [{}] resuming from cursor={}", drainId, cursor);
        } else {
            cursor = resolveInitialCursor();
            log.info("Drainer [{}] no prior progress — bootstrapMode={} starting from cursor={}",
                    drainId, bootstrapMode, cursor);
        }
    }

    /**
     * Resolves the initial cursor value when no prior drain progress exists.
     *
     * <p>The returned value is the <em>cursor</em> (i.e. the last-drained bucket),
     * not the first bucket to drain.  {@link #drainAllAvailable()} always starts
     * from {@code cursor + W}, so the cursor must be {@code startBucket - W} for
     * {@code CONFIGURED} mode so that the first bucket actually drained is
     * {@code startBucket}.</p>
     */
    private long resolveInitialCursor() {
        long now = Instant.now().getEpochSecond();
        return switch (bootstrapMode) {
            case LATEST     -> firstCandidateBucket(now);
            case CONFIGURED -> {
                if (startBucket <= 0) {
                    log.warn("Drainer [{}] bootstrapMode=CONFIGURED but startBucket={} is not > 0; "
                            + "falling back to LATEST", drainId, startBucket);
                    yield firstCandidateBucket(now);
                }
                // Set cursor = startBucket - W so drainAllAvailable() computes
                // first candidate = cursor + W = startBucket.
                long configuredCursor = startBucket - windowSizeSeconds;
                log.info("Drainer [{}] bootstrapMode=CONFIGURED startBucket={} → cursor={}",
                        drainId, startBucket, configuredCursor);
                yield configuredCursor;
            }
        };
    }

    /**
     * Walks forward from {@code cursor + W}, draining every sealed bucket in order.
     * Stops when the next candidate bucket is not yet sealed.
     *
     * <p>The first candidate is always {@code cursor + W}.  {@code initialiseCursorIfNeeded()}
     * guarantees that {@code cursor} is a real value (not the sentinel {@code -1}) before
     * this method is called:
     * <ul>
     *   <li>{@code LATEST} no-history: cursor = {@code firstCandidateBucket(now)} which is
     *       {@code firstCandidate - W}, so {@code cursor + W == firstCandidate}.</li>
     *   <li>{@code CONFIGURED}: cursor = {@code startBucket - W}, so
     *       {@code cursor + W == startBucket}.</li>
     *   <li>Prior history: cursor = last drained bucket, so
     *       {@code cursor + W} is the next bucket to drain.</li>
     * </ul>
     * </p>
     */
    void drainAllAvailable() throws Exception {
        long now = Instant.now().getEpochSecond();

        // cursor is always a real value at this point (initialised by initialiseCursorIfNeeded).
        // The first candidate to drain is always the bucket immediately after the cursor.
        long candidate = cursor + windowSizeSeconds;

        if (candidate <= 0) {
            log.debug("Drainer [{}]: no sealed buckets available yet", drainId);
            return;
        }

        int drained = 0;
        while (isSealed(candidate, now)) {
            drainBucket(candidate);
            drained++;
            candidate += windowSizeSeconds;
        }

        if (drained == 0) {
            log.debug("Drainer [{}]: no new sealed buckets (next candidate={} sealed after {})",
                    drainId, candidate, sealedAfter(candidate));
        } else {
            log.info("Drainer [{}]: drained {} bucket(s) this cycle, cursor now={}",
                    drainId, drained, cursor);
        }
    }

    /**
     * Drains a single bucket: read → sink → record progress.
     */
    void drainBucket(long windowBucket) throws Exception {
        log.info("Drainer [{}]: draining bucket={}", drainId, windowBucket);

        List<SlupEvent> events = progressRepo.readEvents(windowBucket);
        log.info("Drainer [{}]: bucket={} read {} events", drainId, windowBucket, events.size());

        sink.write(windowBucket, events);

        DrainedWindow result = new DrainedWindow(
                drainId, windowBucket, events.size(), Instant.now());
        progressRepo.recordProgress(result);

        cursor = windowBucket;
        drainerCursor.advance(windowBucket);

        log.info("Drainer [{}]: bucket={} drained and recorded (events={})",
                drainId, windowBucket, events.size());
    }

    /**
     * Returns true when {@code candidate} bucket is sealed:
     * {@code now >= candidate + W + allowedLatenessSeconds}.
     */
    boolean isSealed(long candidate, long nowEpochSeconds) {
        return nowEpochSeconds >= sealedAfter(candidate);
    }

    /**
     * Epoch-second threshold at which {@code candidate} becomes sealed.
     */
    private long sealedAfter(long candidate) {
        return candidate + windowSizeSeconds + allowedLatenessSeconds;
    }

    /**
     * Returns the synthetic "already-drained" cursor value to use when no prior
     * progress exists, such that {@code cursor + W} is the first bucket the
     * drainer will actually attempt to drain.
     *
     * <p>The first candidate to drain in {@code LATEST} mode is the earliest
     * sealed bucket relative to now:
     * <pre>
     *   firstCandidate = floor((now - allowedLatenessSeconds - W) / W) * W
     * </pre>
     * Because {@code drainAllAvailable()} always starts from {@code cursor + W},
     * the cursor must be set to {@code firstCandidate - W} so that
     * {@code cursor + W == firstCandidate}.
     * </p>
     *
     * <p>Example (W=5, allowedLateness=60, now=1_000_000_080):
     * <pre>
     *   firstCandidate = floor((1_000_000_080 - 60 - 5) / 5) * 5 = 1_000_000_015
     *   cursor         = 1_000_000_015 - 5                        = 1_000_000_010
     *   drainAllAvailable candidate = cursor + W                  = 1_000_000_015 ✓
     * </pre>
     * </p>
     */
    private long firstCandidateBucket(long now) {
        long earliestRelevantEpoch = now - allowedLatenessSeconds - windowSizeSeconds;
        if (earliestRelevantEpoch < 0) return 0L;
        long firstCandidate = (earliestRelevantEpoch / windowSizeSeconds) * windowSizeSeconds;
        // Subtract one window so that cursor + W == firstCandidate
        return Math.max(0L, firstCandidate - windowSizeSeconds);
    }
}
