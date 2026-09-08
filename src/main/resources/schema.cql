-- =============================================================================
-- Astra Event Inbox – CQL Schema
-- Keyspace: tds_inbox
-- =============================================================================
--
-- ASTRA DB SERVERLESS COMPATIBILITY NOTES
-- ========================================
-- The following DDL properties are SILENTLY IGNORED by Astra DB Serverless
-- and must NOT be used (they are accepted without error but have no effect):
--
--   compaction              → Only UnifiedCompactionStrategy (UCS) is allowed.
--                             TWCS / STCS / LCS clauses are accepted but ignored.
--                             Astra automatically applies UCS for all tables.
--                             UCS subsumes TWCS behaviour for time-series data.
--
--   gc_grace_seconds        → Ignored. Astra manages repairs internally.
--
--   bloom_filter_fp_chance  → Ignored.
--   caching                 → Ignored.
--   compression             → Ignored.
--   min/max_index_interval  → Ignored.
--   memtable_flush_period   → Ignored.
--   read_repair             → Ignored.
--   speculative_retry       → Ignored.
--
-- The following CQL commands are NOT supported:
--   CREATE KEYSPACE  /  ALTER KEYSPACE  /  DROP KEYSPACE
--   → Create keyspaces via the Astra Portal or CLI instead.
--
-- Consistency levels disallowed for writes:  ANY, ONE, LOCAL_ONE
--   → Use LOCAL_QUORUM (our default), QUORUM, ALL, EACH_QUORUM, or SERIAL.
--
-- Table property default_time_to_live IS supported and effective.
--
-- Rate limits:
--   Default: ~12,000 ops/sec
--   Cold-start burst: 4,096 ops/sec (warm up the database before peak load)
--   Each CQL INSERT counts as 1 operation.
--   At 2,000 msg/sec peak we are comfortably within the default limit.
--
-- Partition size limit:
--   Astra warns at >100 MB per partition.
--   With a 5-second window, ~2-4 KB per event, and 2,000 msg/sec peak:
--     max events per window ≈ 10,000
--     max partition size    ≈ 10,000 × 4 KB = 40 MB  → well within the limit
--   If peak rate increases significantly, reduce INBOX_WINDOW_SIZE_SECONDS.
--
-- =============================================================================
-- HOW TO APPLY THIS SCHEMA
-- ========================
-- 1. Create the keyspace in the Astra Portal (UI) or via the Astra CLI:
--      astra db create-keyspace <db-name> -k tds_inbox
--
-- 2. Connect with cqlsh using the Secure Connect Bundle:
--      cqlsh -u <client_id> -p <client_secret> \
--            --secure-connect-bundle /path/to/secure-connect.zip
--
-- 3. Switch to the keyspace and run this file:
--      USE tds_inbox;
--      SOURCE 'schema.cql';
--
--    Or run directly:
--      cqlsh ... -k tds_inbox -f src/main/resources/schema.cql
-- =============================================================================

-- =============================================================================
-- Main inbox table
--
-- Partition key  : window_bucket  – epoch seconds floored to window size W.
--                  One partition per time window.  The drainer reads and
--                  deletes exactly one partition at a time.
--
-- Clustering key : (event_ts ASC, event_id ASC)
--                  event_ts  – ordering key; rows come back in event-time order.
--                  event_id  – dedup key; idempotent upsert absorbs redeliveries.
--
-- window_bucket computation (writer):
--   b_event = floor(epoch_seconds(event_ts) / W) * W
--   if b_event > last_sealed_bucket  →  window_bucket = b_event  (on-time)
--   else                             →  window_bucket = b_open   (late, re-bucket)
--
-- Deduplication:
--   The primary key (window_bucket, event_ts, event_id) makes every INSERT
--   an idempotent upsert at the Cassandra storage layer. Kafka redeliveries of
--   the same event_id at the same event_ts are no-ops. No read-before-write needed.
--
-- TTL (effective on Astra):
--   default_time_to_live = 86400 s (24 h) is a safety backstop. The drainer
--   deletes partitions explicitly after draining; the TTL prevents unbounded
--   accumulation if the drainer falls behind or is not yet deployed (Phase 1).
--
-- Compaction (Astra-managed):
--   Astra applies UnifiedCompactionStrategy (UCS) automatically. UCS supports
--   time-series workloads and replaces TWCS / STCS / LCS. No compaction clause
--   is needed in the CREATE TABLE statement.
--
-- gc_grace_seconds (Astra-managed):
--   Astra manages node repairs internally; gc_grace_seconds is ignored and
--   must not be specified.
-- =============================================================================

CREATE TABLE IF NOT EXISTS tds_inbox.slup_inbox (
    window_bucket   bigint,
    event_ts        timestamp,
    event_id        text,
    guid            text,
    operation       text,
    request_name    text,
    application_id  text,
    client_address  text,
    ingest_time     timestamp,
    payload         text,
    PRIMARY KEY ((window_bucket), event_ts, event_id)
)
WITH CLUSTERING ORDER BY (event_ts ASC, event_id ASC)
  AND default_time_to_live = 86400
  AND comment = 'Durable event inbox partitioned by processing-time window. Compaction and gc_grace managed by Astra DB.';
