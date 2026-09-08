package com.tds.inbox.repository;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.OverloadedException;
import com.datastax.oss.driver.api.core.servererrors.WriteFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Retry policy for Astra DB write operations.
 *
 * <h2>Why this is needed</h2>
 * <p>Astra DB Serverless enforces a rate limit of approximately 12,000 ops/sec
 * (default) and a cold-start burst limit of 4,096 ops/sec.  When the database
 * is idle and traffic suddenly increases, Astra returns an
 * {@link OverloadedException} (CQL error code 0x1001).  Without retry logic the
 * Kafka consumer would stop committing offsets, causing ever-growing redelivery
 * pressure that makes the overload situation worse.</p>
 *
 * <h2>Strategy: exponential backoff with jitter</h2>
 * <pre>
 *   delay(attempt) = min(base * 2^attempt + jitter, max)
 *   jitter         = random [0, base)
 * </pre>
 * <p>The jitter spreads retries from concurrent consumer threads so they do not
 * all hammer Astra at the same instant.</p>
 *
 * <h2>Retryable exceptions</h2>
 * <ul>
 *   <li>{@link OverloadedException} – Astra rate limit / back-pressure</li>
 *   <li>{@link DriverTimeoutException} – transient network/timeout</li>
 *   <li>{@link AllNodesFailedException} – all tried nodes failed (transient)</li>
 * </ul>
 *
 * <h2>Non-retryable exceptions</h2>
 * <ul>
 *   <li>{@link WriteFailureException} – mutation rejected by Astra (e.g. too large)</li>
 *   <li>All other {@link RuntimeException} – unknown; propagated immediately</li>
 * </ul>
 */
@Component
public class AstraWriteRetryPolicy {

    private static final Logger log = LoggerFactory.getLogger(AstraWriteRetryPolicy.class);

    private final int      maxAttempts;
    private final Duration baseDelay;
    private final Duration maxDelay;

    /** Total number of retry attempts made (excludes first attempt). */
    private final AtomicLong retryCount      = new AtomicLong(0L);
    /** Total number of writes that exhausted all retry attempts. */
    private final AtomicLong failedWriteCount = new AtomicLong(0L);
    /** Total number of {@link OverloadedException} occurrences (rate-limited requests). */
    private final AtomicLong rateLimitCount  = new AtomicLong(0L);

    public AstraWriteRetryPolicy(
            @Value("${inbox.astra.retry.max-attempts:5}") int maxAttempts,
            @Value("${inbox.astra.retry.base-delay-ms:100}") long baseDelayMs,
            @Value("${inbox.astra.retry.max-delay-ms:5000}") long maxDelayMs) {
        this.maxAttempts = maxAttempts;
        this.baseDelay   = Duration.ofMillis(baseDelayMs);
        this.maxDelay    = Duration.ofMillis(maxDelayMs);
    }

    /**
     * Executes {@code operation} with retries.
     *
     * @param operation the write operation to execute
     * @param eventId   event identifier used in log messages
     * @throws RuntimeException if all attempts are exhausted or the exception is non-retryable
     */
    public void executeWithRetry(Runnable operation, String eventId) {
        int attempt = 0;
        while (true) {
            try {
                operation.run();
                if (attempt > 0) {
                    log.info("Astra write succeeded on attempt {} for event_id={}", attempt + 1, eventId);
                }
                return;
            } catch (Exception ex) {
                if (ex instanceof OverloadedException) {
                    rateLimitCount.incrementAndGet();
                }
                if (!isRetryable(ex)) {
                    log.error("Non-retryable Astra write error for event_id={}: {}", eventId, ex.getMessage());
                    throw ex;
                }
                attempt++;
                retryCount.incrementAndGet();
                if (attempt >= maxAttempts) {
                    failedWriteCount.incrementAndGet();
                    log.error("Astra write failed after {} attempts for event_id={}: {}",
                            maxAttempts, eventId, ex.getMessage());
                    throw ex;
                }
                Duration delay = computeDelay(attempt);
                log.warn("Astra write attempt {}/{} failed for event_id={} ({}); retrying in {}ms",
                        attempt, maxAttempts, eventId, ex.getClass().getSimpleName(), delay.toMillis());
                sleep(delay);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Counters (for load-test monitoring)
    // -------------------------------------------------------------------------

    /** Returns the total number of retry attempts made (first attempt not counted). */
    public long getRetryCount() {
        return retryCount.get();
    }

    /** Returns the total number of writes that exhausted all retry attempts. */
    public long getFailedWriteCount() {
        return failedWriteCount.get();
    }

    /** Returns the total number of {@link OverloadedException} occurrences. */
    public long getRateLimitCount() {
        return rateLimitCount.get();
    }

    /** Resets all counters to zero. Intended for use between load-test scenarios. */
    public void resetCounters() {
        retryCount.set(0L);
        failedWriteCount.set(0L);
        rateLimitCount.set(0L);
    }

    // -------------------------------------------------------------------------
    // Internals
    // -------------------------------------------------------------------------

    private boolean isRetryable(Exception ex) {
        return ex instanceof OverloadedException      // Astra rate limit
            || ex instanceof DriverTimeoutException   // transient timeout
            || ex instanceof AllNodesFailedException; // all nodes unreachable
    }

    private Duration computeDelay(int attempt) {
        long base    = baseDelay.toMillis();
        long jitter  = (long) (Math.random() * base);
        long backoff = base * (1L << (attempt - 1)) + jitter;   // base * 2^(attempt-1) + jitter
        return Duration.ofMillis(Math.min(backoff, maxDelay.toMillis()));
    }

    private void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during Astra retry backoff", ie);
        }
    }
}
