package com.tds.inbox.repository;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.tds.inbox.config.InboxProperties;
import com.tds.inbox.domain.SlupEvent;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

/**
 * Persists {@link SlupEvent} records into {@code tds_inbox.slup_inbox} using CQL
 * prepared statements.
 *
 * <h2>Design notes</h2>
 * <ul>
 *   <li>One {@link PreparedStatement} is created at startup and reused – the driver
 *       handles the server-side cache transparently.</li>
 *   <li>INSERT is idempotent by nature of the primary key
 *       ({@code window_bucket, event_ts, event_id}): a redelivery of the same
 *       {@code event_id} at the same {@code event_ts} is a no-op upsert.</li>
 *   <li>Consistency level is read from configuration ({@code LOCAL_QUORUM} default),
 *       making it trivial to tune without code changes.</li>
 *   <li>The {@code USING TTL} clause uses the configured {@code inbox.astra.ttl-seconds}
 *       (default 86400 s / 24 h), overriding the table-level default.</li>
 * </ul>
 *
 * <h2>Synchronous path (default)</h2>
 * <p>{@code inbox.astra.async-writes=false} (default). Each call to {@link #save}
 * blocks the calling thread until Astra acknowledges the write. This is the safe
 * default because it provides simple, deterministic back-pressure: if Astra is
 * slow the consumer thread stalls, consumer lag grows, and load naturally
 * throttles. No extra synchronisation is needed for per-partition ordering.</p>
 *
 * <h2>Asynchronous path (opt-in)</h2>
 * <p>{@code inbox.astra.async-writes=true}. Each call to {@link #save} dispatches
 * an async CQL write ({@code session.executeAsync}) and returns a
 * {@link CompletableFuture} immediately. The calling thread is not blocked.
 * The caller ({@code InboxKafkaConsumer}) must collect all futures for a partition
 * and wait for them to complete before committing the Kafka offset — this preserves
 * the "offset committed only after successful Astra write" invariant.</p>
 *
 * <p>Concurrency is bounded by a {@link Semaphore} with {@code maxInFlight} permits.
 * A permit is acquired before each write and released on future completion. This
 * prevents unbounded future accumulation and provides back-pressure equivalent to
 * the synchronous path under high latency.</p>
 *
 * <h2>Max-in-flight derivation</h2>
 * <pre>
 *   maxInFlight = ceil(target_tps / (threads × (1_000 / p50_ms)))
 *               = ceil(2_000 / (6 × (1_000 / 2)))
 *               = ceil(2_000 / 3_000)
 *               = 1  (minimum)
 * </pre>
 * <p>The default of 4 provides a 4× buffer against p99 spikes. At 6 threads ×
 * 4 in-flight × 500 ops/s per slot = 12,000 ops/s, which is exactly the Astra
 * default rate limit. Reduce {@code maxInFlight} if rate-limit errors appear.</p>
 */
@Repository
public class EventRepository {

    private static final Logger log = LoggerFactory.getLogger(EventRepository.class);

    private static final String INSERT_CQL = """
            INSERT INTO slup_inbox (
                window_bucket,
                event_ts,
                event_id,
                guid,
                operation,
                request_name,
                application_id,
                client_address,
                ingest_time,
                payload
            ) VALUES (
                :window_bucket,
                :event_ts,
                :event_id,
                :guid,
                :operation,
                :request_name,
                :application_id,
                :client_address,
                :ingest_time,
                :payload
            ) USING TTL :ttl
            """;

    private final CqlSession            session;
    private final ConsistencyLevel      writeConsistency;
    private final AstraWriteRetryPolicy retryPolicy;
    private final int                   ttlSeconds;
    private final boolean               asyncWrites;
    private final Semaphore             inFlightSemaphore;

    private PreparedStatement insertStmt;

    public EventRepository(CqlSession session, InboxProperties properties,
                           AstraWriteRetryPolicy retryPolicy) {
        this.session          = session;
        this.writeConsistency = DefaultConsistencyLevel.valueOf(
                properties.astra().writeConsistency());
        this.retryPolicy      = retryPolicy;
        this.ttlSeconds       = (int) properties.astra().ttlSeconds();
        this.asyncWrites      = properties.astra().asyncWrites();
        this.inFlightSemaphore = new Semaphore(properties.astra().maxInFlightWrites());
    }

    @PostConstruct
    void prepare() {
        insertStmt = session.prepare(INSERT_CQL);
        log.info("Prepared INSERT statement for slup_inbox at CL={} ttl={}s async={}",
                writeConsistency, ttlSeconds, asyncWrites);
    }

    /**
     * Returns {@code true} if async writes are enabled.
     * The {@link com.tds.inbox.kafka.InboxKafkaConsumer} uses this to decide
     * whether to collect a {@link CompletableFuture} per write.
     */
    public boolean isAsyncWrites() {
        return asyncWrites;
    }

    /**
     * Writes a single {@link SlupEvent} to Astra DB synchronously.
     *
     * <p>Calling this method with the same {@code event_id} and {@code event_ts} more
     * than once (redelivery scenario) is safe — the idempotent upsert is a no-op on
     * the second and subsequent calls.</p>
     *
     * <p>When {@code inbox.astra.async-writes=true} is configured, prefer
     * {@link #saveAsync(SlupEvent)} instead.</p>
     *
     * @param event the event to persist
     */
    public void save(SlupEvent event) {
        BoundStatement bound = buildStatement(event);
        retryPolicy.executeWithRetry(
                () -> session.execute(bound),
                event.eventId()
        );

        if (log.isDebugEnabled()) {
            log.debug("Persisted event id={} bucket={} ts={}",
                    event.eventId(), event.windowBucket(), event.eventTs());
        }
    }

    /**
     * Writes a single {@link SlupEvent} to Astra DB asynchronously.
     *
     * <p>Acquires one permit from the bounded {@link Semaphore} before dispatching
     * the write. The permit is released when the returned future completes
     * (successfully or exceptionally). This bounds the number of concurrent
     * in-flight writes and provides back-pressure equivalent to the synchronous
     * path under high latency.</p>
     *
     * <p>On failure, the future completes exceptionally with the cause. The caller
     * must NOT commit the Kafka offset if the future fails.</p>
     *
     * <p>Call this method only when {@code inbox.astra.async-writes=true}.
     * Use {@link #save(SlupEvent)} for the synchronous path.</p>
     *
     * @param event the event to persist
     * @return a {@link CompletableFuture} that completes when the write is acknowledged
     * @throws InterruptedException if the calling thread is interrupted while
     *                              waiting for a semaphore permit
     */
    public CompletableFuture<Void> saveAsync(SlupEvent event) throws InterruptedException {
        inFlightSemaphore.acquire();
        BoundStatement bound = buildStatement(event);

        CompletableFuture<Void> future = new CompletableFuture<>();
        session.executeAsync(bound)
                .whenComplete((rs, ex) -> {
                    inFlightSemaphore.release();
                    if (ex != null) {
                        log.error("Async Astra write failed for event_id={}: {}",
                                event.eventId(), ex.getMessage());
                        future.completeExceptionally(ex);
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("Async persisted event id={} bucket={} ts={}",
                                    event.eventId(), event.windowBucket(), event.eventTs());
                        }
                        future.complete(null);
                    }
                });
        return future;
    }

    // -------------------------------------------------------------------------
    // Internals
    // -------------------------------------------------------------------------

    private BoundStatement buildStatement(SlupEvent event) {
        return insertStmt.bind()
                .setLong    ("window_bucket",   event.windowBucket())
                .setInstant ("event_ts",        event.eventTs())
                .setString  ("event_id",        event.eventId())
                .setString  ("guid",            event.guid())
                .setString  ("operation",       event.operation())
                .setString  ("request_name",    event.requestName())
                .setString  ("application_id",  event.applicationId())
                .setString  ("client_address",  event.clientAddress())
                .setInstant ("ingest_time",     event.ingestTime())
                .setString  ("payload",         event.payload())
                .setInt     ("ttl",             ttlSeconds)
                .setConsistencyLevel(writeConsistency)
                // Mark the statement idempotent so the driver can safely retry it.
                // This INSERT is an idempotent upsert: same primary key (window_bucket,
                // event_ts, event_id) produces the same row with no counters or appends.
                .setIdempotent(Boolean.TRUE);
    }
}
