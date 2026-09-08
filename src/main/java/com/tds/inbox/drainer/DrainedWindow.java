package com.tds.inbox.drainer;

import java.time.Instant;

/**
 * Immutable value object representing the outcome of draining one window.
 *
 * @param drainId      the logical drainer identity that processed this window
 * @param windowBucket epoch-second bucket that was drained
 * @param eventCount   number of events read from {@code slup_inbox} for this bucket
 * @param drainedAt    wall-clock instant when the drain completed
 */
public record DrainedWindow(
        String  drainId,
        long    windowBucket,
        long    eventCount,
        Instant drainedAt
) {}
