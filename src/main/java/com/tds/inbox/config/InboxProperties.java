package com.tds.inbox.config;

import com.tds.inbox.drainer.DrainerBootstrapMode;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Typed configuration for the inbox — bound from {@code inbox.*} in application.yml.
 * All values can be overridden via environment variables (Spring Boot convention:
 * {@code INBOX_ASTRA_SECURE_BUNDLE_PATH}, etc.).
 */
@ConfigurationProperties(prefix = "inbox")
public record InboxProperties(
        AstraProperties  astra,
        KafkaProperties  kafka,
        WindowProperties window,
        DrainerProperties drainer
) {

    @ConfigurationProperties(prefix = "inbox.astra")
    public record AstraProperties(
            String  secureBundlePath,
            String  clientId,
            String  clientSecret,
            String  keyspace,
            String  writeConsistency,
            long    requestTimeoutMs,
            /**
             * TTL in seconds applied to every INSERT via the {@code USING TTL} clause.
             * Overrides the table-level {@code default_time_to_live} when set.
             * Configurable via {@code INBOX_TTL_SECONDS} env var.
             * Default: 86400 s (24 h).
             * <p>
             * Note: 24 h is a Phase 1 safety backstop and is NOT the final production value.
             * The TTL must satisfy:
             * {@code TTL >= watermark_advance_time + allowed_lateness + drain_processing_time + safety_margin}
             * Revisit when the drainer SLA is defined.
             */
            long    ttlSeconds,
            /**
             * Enable asynchronous CQL writes behind a bounded semaphore.
             * Default: {@code false} (synchronous, safe default).
             * <p>
             * Switch to {@code true} only when load testing confirms that the synchronous
             * path cannot sustain the required throughput and the bottleneck is write latency.
             * See {@code EventRepository} Javadoc for the max-in-flight derivation.
             */
            boolean asyncWrites,
            /**
             * Maximum number of concurrent in-flight Astra writes per consumer thread
             * when {@code async-writes=true}.
             * Derivation: {@code ceil(target_tps / (threads × (1_000 / p50_ms)))}
             * Default: 4 (adequate for 2,000 msg/s at p50=2 ms with 6 threads).
             */
            int     maxInFlightWrites
    ) {}

    @ConfigurationProperties(prefix = "inbox.kafka")
    public record KafkaProperties(
            String topic,
            String deadLetterTopic,
            /**
             * Interval in milliseconds between successive retry attempts for a record
             * that failed to write to Astra DB.  Passed to the {@link FixedBackOff}
             * inside {@code KafkaConfig}.
             * <p>
             * A zero delay causes a tight retry loop that spins the consumer thread and
             * produces excessive log noise during an Astra outage.  1 000 ms is a safe
             * default that allows the upstream Kafka consumer poll timeout to also act
             * as natural back-pressure.
             * </p>
             * Configurable via {@code INBOX_KAFKA_RETRY_INTERVAL_MS} env var. Default: 1000.
             */
            long retryIntervalMs,
            HealthProperties health
    ) {
        /**
         * Kafka consumer health check configuration.
         */
        @ConfigurationProperties(prefix = "inbox.kafka.health")
        public record HealthProperties(
                /**
                 * Maximum tolerated consumer lag (messages behind head) per partition
                 * before the health indicator reports DOWN.
                 * Configurable via {@code INBOX_KAFKA_HEALTH_MAX_LAG} env var.
                 * Default: 10000.
                 */
                long maxLag
        ) {}
    }

    @ConfigurationProperties(prefix = "inbox.window")
    public record WindowProperties(
            long sizeSeconds,
            /**
             * Allowed-lateness in seconds.  A bucket is considered sealed when the
             * stream's event-time watermark has advanced beyond:
             * {@code window_bucket + W + allowedLatenessSeconds}.
             * <p>
             * This is a documented Phase 2 contract.  No code enforces it in Phase 1 —
             * the drainer will enforce it when implemented.
             * Configurable via {@code INBOX_ALLOWED_LATENESS_SECONDS} env var.
             * Default: 60 s.
             */
            long allowedLatenessSeconds
    ) {}

    /**
     * Configuration for the Phase 2 drainer service.
     * Configurable via {@code INBOX_DRAINER_*} environment variables.
     */
    @ConfigurationProperties(prefix = "inbox.drainer")
    public record DrainerProperties(
            /**
             * Whether the drainer is enabled.
             * Set {@code false} to run the application in writer-only mode.
             * Configurable via {@code INBOX_DRAINER_ENABLED} env var. Default: false.
             *
             * <p><strong>PoC constraint:</strong> only one drainer instance may run at a
             * time. Distributed locking is not yet implemented. Running multiple instances
             * simultaneously will cause duplicate drain processing.</p>
             */
            boolean enabled,
            /**
             * Logical identity of this drainer instance.
             * Recorded in {@code drain_progress.drain_id}.
             * Multiple drainer instances must use distinct drain-id values.
             * Configurable via {@code INBOX_DRAINER_DRAIN_ID} env var. Default: "primary".
             */
            String drainId,
            /**
             * How often to poll for sealed buckets (milliseconds).
             * Configurable via {@code INBOX_DRAINER_POLL_INTERVAL_MS} env var. Default: 5000.
             */
            long pollIntervalMs,
            /**
             * Directory where the {@link com.tds.inbox.drainer.FileWriterEventSink} writes output files.
             * One file is created per drained window: {@code <drain-id>_<window_bucket>.jsonl}.
             * Configurable via {@code INBOX_DRAINER_OUTPUT_PATH} env var. Default: /tmp/inbox-drain.
             */
            String outputPath,
            /**
             * CQL read consistency level for drainer SELECT queries.
             * Default: LOCAL_QUORUM.
             * Configurable via {@code INBOX_DRAINER_READ_CONSISTENCY} env var.
             */
            String readConsistency,
            /**
             * Controls how the drainer positions its cursor on first run when no prior
             * drain progress exists in {@code drain_progress}.
             * <ul>
             *   <li>{@code LATEST} (default) — start from current time minus the
             *       allowed-lateness window; skips historical data.</li>
             *   <li>{@code CONFIGURED} — start from the epoch-second value in
             *       {@code startBucket}; use for historical backfills.</li>
             * </ul>
             * Configurable via {@code INBOX_DRAINER_BOOTSTRAP_MODE} env var.
             */
            DrainerBootstrapMode bootstrapMode,
            /**
             * Epoch-second bucket to start from when {@code bootstrapMode=CONFIGURED}.
             * Must be > 0 when {@code CONFIGURED} mode is active; ignored otherwise.
             * Configurable via {@code INBOX_DRAINER_START_BUCKET} env var. Default: 0.
             */
            long startBucket
    ) {}
}
