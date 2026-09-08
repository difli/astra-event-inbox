package com.tds.inbox.drainer;

/**
 * Determines how the drainer positions its cursor on the very first run
 * (i.e., when no prior drain progress exists in {@code drain_progress}).
 *
 * <ul>
 *   <li>{@link #LATEST} — start from current wall-clock time minus the allowed-lateness
 *       window. Skips all historical data. Suitable for demos and fresh deployments
 *       that do not need to backfill past events. This is the default.</li>
 *   <li>{@link #CONFIGURED} — start from the explicit epoch-second value in
 *       {@code inbox.drainer.start-bucket}. Use when you need the drainer to begin
 *       from a known point in history (e.g., after a data migration or replay).</li>
 * </ul>
 *
 * <p>This setting only applies when no progress has been recorded in
 * {@code drain_progress}. Once the drainer has run at least one cycle the cursor
 * is restored from Astra and this setting has no effect.</p>
 *
 * <p>Configure via {@code INBOX_DRAINER_BOOTSTRAP_MODE} environment variable
 * (case-insensitive). Default: {@code LATEST}.</p>
 */
public enum DrainerBootstrapMode {

    /**
     * Skip history — start draining from the current wall-clock time minus the
     * allowed-lateness window. Safe default for new deployments.
     */
    LATEST,

    /**
     * Start from the explicit {@code inbox.drainer.start-bucket} epoch-second value.
     * Must be > 0. Use for controlled historical backfills or after a data migration.
     */
    CONFIGURED
}
