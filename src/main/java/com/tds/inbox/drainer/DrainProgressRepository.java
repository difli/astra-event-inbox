package com.tds.inbox.drainer;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.tds.inbox.config.InboxProperties;
import com.tds.inbox.domain.SlupEvent;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * CQL access layer for the {@code drain_progress} table (Phase 2 schema).
 *
 * <h2>Design notes</h2>
 * <ul>
 *   <li>All statements are prepared at startup via {@link #prepare()} — driver
 *       caches and reuses the server-side plan.</li>
 *   <li>Reads use the configured read-consistency level (default {@code LOCAL_QUORUM}).
 *       Writes (recording progress) also use {@code LOCAL_QUORUM}.</li>
 *   <li>{@link #readEvents(long)} reads all events for a given {@code window_bucket}
 *       from {@code slup_inbox}. Rows are returned in natural clustering order
 *       ({@code event_ts ASC, event_id ASC}) — no client-side sort required.</li>
 *   <li>{@link #recordProgress(DrainedWindow)} writes one row to {@code drain_progress}
 *       with a TTL equal to the table's {@code default_time_to_live} (7 days).</li>
 *   <li>{@link #findLastDrainedBucket(String)} returns the highest {@code window_bucket}
 *       already recorded, enabling the drainer to resume after a restart.</li>
 * </ul>
 */
@Repository
public class DrainProgressRepository {

    private static final Logger log = LoggerFactory.getLogger(DrainProgressRepository.class);

    // SELECT all events for one bucket — drives the drainer read loop
    private static final String SELECT_EVENTS_CQL = """
            SELECT window_bucket, event_ts, event_id,
                   guid, operation, request_name,
                   application_id, client_address,
                   ingest_time, payload
            FROM slup_inbox
            WHERE window_bucket = :window_bucket
            """;

    // Record successful drain of one window
    private static final String INSERT_PROGRESS_CQL = """
            INSERT INTO drain_progress (drain_id, window_bucket, drained_at, event_count)
            VALUES (:drain_id, :window_bucket, :drained_at, :event_count)
            """;

    // Find the last drained bucket for a given drain_id — used on restart
    private static final String SELECT_LAST_BUCKET_CQL = """
            SELECT window_bucket
            FROM drain_progress
            WHERE drain_id = :drain_id
            ORDER BY window_bucket DESC
            LIMIT 1
            """;

    private final CqlSession       session;
    private final ConsistencyLevel readConsistency;
    private final String           keyspace;

    private PreparedStatement selectEventsStmt;
    private PreparedStatement insertProgressStmt;
    private PreparedStatement selectLastBucketStmt;

    public DrainProgressRepository(CqlSession session, InboxProperties properties) {
        this.session         = session;
        this.readConsistency = DefaultConsistencyLevel.valueOf(
                properties.drainer().readConsistency());
        this.keyspace        = properties.astra().keyspace();
    }

    @PostConstruct
    void prepare() {
        selectEventsStmt     = session.prepare(SELECT_EVENTS_CQL);
        insertProgressStmt   = session.prepare(INSERT_PROGRESS_CQL);
        selectLastBucketStmt = session.prepare(SELECT_LAST_BUCKET_CQL);
        log.info("DrainProgressRepository prepared statements at CL={}", readConsistency);
    }

    /**
     * Reads all events for the given {@code windowBucket} from {@code slup_inbox},
     * ordered by {@code (event_ts ASC, event_id ASC)}.
     *
     * <p>The result may be empty if the bucket was sealed but contained no events.</p>
     *
     * <p>A page size of 500 is set on the bound statement so the DataStax driver
     * fetches rows in 500-row pages rather than loading the entire partition in one
     * network round-trip. The driver transparently fetches subsequent pages as the
     * iterator advances, so the ArrayList never holds more than one fetched page
     * worth of rows in network buffers at a time — though all rows are ultimately
     * collected into the returned list.</p>
     *
     * <p><strong>PoC note:</strong> for a 5-second window at 2,000 msg/s this list
     * is bounded at ~10,000 rows, which is acceptable. A production implementation
     * should stream rows directly to the sink rather than collecting into a list,
     * to avoid holding all rows in heap simultaneously.</p>
     *
     * @param windowBucket epoch-second bucket to read
     * @return ordered, immutable list of events in this bucket
     */
    public List<SlupEvent> readEvents(long windowBucket) {
        BoundStatement bound = selectEventsStmt.bind()
                .setLong("window_bucket", windowBucket)
                .setConsistencyLevel(readConsistency)
                .setPageSize(500);  // driver fetches in 500-row pages

        List<SlupEvent> events = new ArrayList<>();
        for (Row row : session.execute(bound)) {
            events.add(rowToEvent(row));
        }
        log.debug("readEvents bucket={} → {} rows", windowBucket, events.size());
        return List.copyOf(events);
    }

    /**
     * Persists a {@link DrainedWindow} record to {@code drain_progress}.
     *
     * <p>Idempotent: writing the same {@code (drain_id, window_bucket)} twice is
     * a no-op upsert. The TTL is governed by the table's
     * {@code default_time_to_live} (7 days).</p>
     *
     * @param drained the drain result to record
     */
    public void recordProgress(DrainedWindow drained) {
        BoundStatement bound = insertProgressStmt.bind()
                .setString  ("drain_id",      drained.drainId())
                .setLong    ("window_bucket",  drained.windowBucket())
                .setInstant ("drained_at",     drained.drainedAt())
                .setLong    ("event_count",    drained.eventCount())
                // LOCAL_QUORUM for progress writes — same guarantee as inbox writes
                .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM);
        session.execute(bound);
        log.debug("Progress recorded drain_id={} bucket={} events={}",
                drained.drainId(), drained.windowBucket(), drained.eventCount());
    }

    /**
     * Returns the highest {@code window_bucket} already successfully drained by
     * {@code drainId}, or {@link Optional#empty()} if no progress has been recorded yet.
     *
     * <p>Used on startup to resume from where the drainer left off.</p>
     *
     * @param drainId the drain identity to look up
     * @return last drained bucket, or empty if none
     */
    public Optional<Long> findLastDrainedBucket(String drainId) {
        BoundStatement bound = selectLastBucketStmt.bind()
                .setString("drain_id", drainId)
                .setConsistencyLevel(readConsistency);

        Row row = session.execute(bound).one();
        if (row == null) {
            return Optional.empty();
        }
        return Optional.of(row.getLong("window_bucket"));
    }

    // -------------------------------------------------------------------------
    // Internals
    // -------------------------------------------------------------------------

    private static SlupEvent rowToEvent(Row row) {
        return new SlupEvent(
                row.getLong    ("window_bucket"),
                row.getInstant ("event_ts"),
                row.getString  ("event_id"),
                row.getString  ("guid"),
                row.getString  ("operation"),
                row.getString  ("request_name"),
                row.getString  ("application_id"),
                row.getString  ("client_address"),
                row.getInstant ("ingest_time"),
                row.getString  ("payload")
        );
    }
}
