-- =============================================================================
-- Astra Event Inbox – Phase 2 CQL Schema
-- Keyspace: tds_inbox
-- =============================================================================
--
-- THIS FILE IS NOT APPLIED BY SchemaInitializer.
-- It is a Phase 2 artefact; apply manually via cqlsh after the drainer is
-- deployed to production.
--
-- HOW TO APPLY
-- ============
-- 1. Connect with cqlsh using the Secure Connect Bundle:
--      cqlsh -u <client_id> -p <client_secret> \
--            --secure-connect-bundle /path/to/secure-connect.zip
--
-- 2. Switch to the keyspace and run this file:
--      USE tds_inbox;
--      SOURCE 'schema-phase2.cql';
--
-- =============================================================================
-- ASTRA DB SERVERLESS COMPATIBILITY NOTES
-- ========================================
-- See schema.cql for the full list of silently-ignored properties.
-- All compaction, gc_grace, and repair settings are Astra-managed.
-- CREATE KEYSPACE / DROP KEYSPACE are not supported.
-- =============================================================================

-- =============================================================================
-- drain_progress
--
-- Durable record of which windows the drainer has successfully processed.
--
-- Design rationale
-- ----------------
-- The drainer does NOT DELETE rows from slup_inbox after draining a window.
-- Issuing wide-partition deletes would accumulate tombstones and degrade
-- subsequent read performance.  Instead, TTL on slup_inbox expires rows
-- naturally.  The drain_progress table provides a durable, queryable record
-- that:
--   1. Prevents the drainer from re-processing a window on restart.
--   2. Allows cross-checking event_count against the Kafka topic lag.
--   3. Retains enough history (7 days) for post-incident analysis.
--
-- Partition key: drain_id
--   Logical drainer identity (e.g. "primary").  Multiple drainer instances
--   can coexist without key collision — each uses a distinct drain_id.
--
-- Clustering key: window_bucket ASC
--   Allows range scans (e.g. "find all windows not yet drained") and
--   monotonic progress tracking.  Ascending order matches the natural
--   drain direction (oldest windows first).
--
-- TTL
-- ---
-- 7 days (604800 s).  Progress records are small (one row per 5-second
-- window per drainer = ~17,280 rows/day) and are not queried after 24 h
-- in normal operation.  7 days retains enough history for post-incident
-- analysis.  This is independent of the slup_inbox TTL.
--
-- Sealed-bucket contract (Phase 2)
-- ----------------------------------
-- A bucket is considered sealed when the stream's event-time watermark has
-- advanced beyond:
--
--   window_bucket + W + allowed_lateness_seconds
--
-- where W = inbox.window.size-seconds (default 5 s) and
-- allowed_lateness_seconds = inbox.window.allowed-lateness-seconds
-- (default 60 s).
--
-- The drainer calls WindowBucketCalculator.advanceSealedBucket(window_bucket)
-- after each successful drain, enabling late-event re-bucketing in the writer.
--
-- slup_inbox TTL requirement
-- --------------------------
-- The slup_inbox TTL must satisfy:
--
--   TTL >= watermark_advance_time + allowed_lateness + drain_processing_time
--          + safety_margin
--
-- With default values:
--   watermark_advance_time ≈ W         =   5 s
--   allowed_lateness                   =  60 s
--   drain_processing_time (estimated)  =  30 s
--   safety_margin                      = 86_305 s  (nearly 24 h)
--   ----------------------------------------------------------
--   minimum TTL                        = 86_400 s  (24 h default)
--
-- The default INBOX_TTL_SECONDS=86400 meets this requirement.  Adjust
-- upward if the drainer SLA or allowed-lateness window increases.
-- =============================================================================

CREATE TABLE IF NOT EXISTS tds_inbox.drain_progress (
    drain_id        text,        -- logical drainer identity (e.g. "primary")
    window_bucket   bigint,      -- the window that was processed
    drained_at      timestamp,   -- wall clock when drain completed
    event_count     bigint,      -- rows read from slup_inbox for this bucket
    PRIMARY KEY ((drain_id), window_bucket)
) WITH CLUSTERING ORDER BY (window_bucket ASC)
  AND default_time_to_live = 604800
  AND comment = 'Durable drain progress. Written after successful downstream processing. Never deleted manually.';
