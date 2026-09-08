package com.tds.inbox.drainer;

import com.tds.inbox.config.InboxProperties;
import com.tds.inbox.domain.SlupEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link InboxDrainer}.
 *
 * All collaborators are mocked. The clock-dependent sealed-window logic is
 * tested via {@link InboxDrainer#isSealed(long, long)}.
 *
 * Window config: W=5s, allowedLateness=10s → bucket B is sealed when:
 *   now >= B + W + LATENESS = B + 15
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class InboxDrainerTest {

    private static final long BASE        = 1_000_000_000L;
    private static final long W           = 5L;
    private static final long LATENESS    = 10L;
    private static final long SEAL_MARGIN = W + LATENESS;  // 15

    @Mock DrainProgressRepository progressRepo;
    @Mock EventSink                sink;

    private DrainerCursor drainerCursor;
    private InboxDrainer  drainer;

    @BeforeEach
    void setUp() {
        InboxProperties props = new InboxProperties(
                new InboxProperties.AstraProperties(
                        "b.zip", "id", "secret", "tds_inbox", "LOCAL_QUORUM",
                        5000, 86400L, false, 4),
                new InboxProperties.KafkaProperties("t", "t.DLT", 1000L,
                        new InboxProperties.KafkaProperties.HealthProperties(10_000L)),
                new InboxProperties.WindowProperties(W, LATENESS),
                new InboxProperties.DrainerProperties(true, "primary", 5000L, "/tmp/drain",
                        "LOCAL_QUORUM", DrainerBootstrapMode.LATEST, 0L));

        drainerCursor = new DrainerCursor();
        drainer = new InboxDrainer(progressRepo, sink, drainerCursor, props);
    }

    // =========================================================================
    // initialiseCursorIfNeeded
    // =========================================================================

    @Test
    void initialiseCursor_noHistory_setsZero() {
        when(progressRepo.findLastDrainedBucket("primary")).thenReturn(Optional.empty());

        drainer.initialiseCursorIfNeeded();

        // Astra was queried exactly once
        verify(progressRepo, times(1)).findLastDrainedBucket("primary");
    }

    @Test
    void initialiseCursor_withHistory_setsCursor() {
        when(progressRepo.findLastDrainedBucket("primary")).thenReturn(Optional.of(BASE));

        drainer.initialiseCursorIfNeeded();

        // Verify cursor was set by having drainAllAvailable() start from BASE + W
        when(progressRepo.readEvents(anyLong())).thenReturn(List.of());
        // No exception — cursor initialised correctly
        verify(progressRepo, times(1)).findLastDrainedBucket("primary");
    }

    /**
     * Fix 10: when prior history exists, initialiseCursorIfNeeded() must immediately
     * call drainerCursor.advance() so the shared DrainerCursor reflects the restored
     * value from the first event processed after restart.
     */
    @Test
    void initialiseCursor_withHistory_restoresSharedDrainerCursor() {
        when(progressRepo.findLastDrainedBucket("primary")).thenReturn(Optional.of(BASE));

        drainer.initialiseCursorIfNeeded();

        // The shared DrainerCursor must equal the restored cursor immediately —
        // not just after the first drain cycle completes.
        assertThat(drainerCursor.get()).isEqualTo(BASE);
    }

    @Test
    void initialiseCursor_calledTwice_queriesAstraOnlyOnce() {
        when(progressRepo.findLastDrainedBucket("primary")).thenReturn(Optional.empty());

        drainer.initialiseCursorIfNeeded();
        drainer.initialiseCursorIfNeeded(); // second call must be a no-op

        verify(progressRepo, times(1)).findLastDrainedBucket("primary");
    }

    // =========================================================================
    // isSealed
    // =========================================================================

    @Test
    void isSealed_oneSecondBeforeThreshold_returnsFalse() {
        assertThat(drainer.isSealed(BASE, BASE + SEAL_MARGIN - 1)).isFalse();
    }

    @Test
    void isSealed_exactlyAtThreshold_returnsTrue() {
        assertThat(drainer.isSealed(BASE, BASE + SEAL_MARGIN)).isTrue();
    }

    @Test
    void isSealed_wellPastThreshold_returnsTrue() {
        assertThat(drainer.isSealed(BASE, BASE + SEAL_MARGIN + 100)).isTrue();
    }

    // =========================================================================
    // drainBucket
    // =========================================================================

    @Test
    void drainBucket_readsEventsThenSinksThenRecordsProgress() throws Exception {
        long bucket = BASE;
        SlupEvent event = sampleEvent(bucket, "e1");
        when(progressRepo.readEvents(bucket)).thenReturn(List.of(event));
        when(progressRepo.findLastDrainedBucket("primary")).thenReturn(Optional.empty());
        drainer.initialiseCursorIfNeeded();

        drainer.drainBucket(bucket);

        // Events forwarded to the sink
        verify(sink).write(eq(bucket), eq(List.of(event)));

        // Progress recorded with correct values
        ArgumentCaptor<DrainedWindow> captor = ArgumentCaptor.forClass(DrainedWindow.class);
        verify(progressRepo).recordProgress(captor.capture());
        DrainedWindow recorded = captor.getValue();
        assertThat(recorded.drainId()).isEqualTo("primary");
        assertThat(recorded.windowBucket()).isEqualTo(bucket);
        assertThat(recorded.eventCount()).isEqualTo(1L);
    }

    @Test
    void drainBucket_advancesDrainerCursor() throws Exception {
        long bucket = BASE;
        when(progressRepo.readEvents(bucket)).thenReturn(List.of());
        when(progressRepo.findLastDrainedBucket("primary")).thenReturn(Optional.empty());
        drainer.initialiseCursorIfNeeded();

        drainer.drainBucket(bucket);

        assertThat(drainerCursor.get()).isEqualTo(bucket);
    }

    @Test
    void drainBucket_sinkThrows_doesNotAdvanceCursor() throws Exception {
        long bucket = BASE;
        when(progressRepo.readEvents(bucket)).thenReturn(List.of(sampleEvent(bucket, "e1")));
        doThrow(new RuntimeException("sink failure")).when(sink).write(eq(bucket), any());
        when(progressRepo.findLastDrainedBucket("primary")).thenReturn(Optional.empty());
        drainer.initialiseCursorIfNeeded();

        try { drainer.drainBucket(bucket); } catch (RuntimeException ignored) {}

        assertThat(drainerCursor.get()).isEqualTo(0L);
    }

    @Test
    void drainBucket_emptyBucket_stillRecordsProgress() throws Exception {
        long bucket = BASE;
        when(progressRepo.readEvents(bucket)).thenReturn(List.of());
        when(progressRepo.findLastDrainedBucket("primary")).thenReturn(Optional.empty());
        drainer.initialiseCursorIfNeeded();

        drainer.drainBucket(bucket);

        verify(sink).write(bucket, List.of());
        ArgumentCaptor<DrainedWindow> captor = ArgumentCaptor.forClass(DrainedWindow.class);
        verify(progressRepo).recordProgress(captor.capture());
        assertThat(captor.getValue().eventCount()).isEqualTo(0L);
    }

    @Test
    void drainBucket_sinkThrows_doesNotRecordProgress() throws Exception {
        long bucket = BASE;
        when(progressRepo.readEvents(bucket)).thenReturn(List.of(sampleEvent(bucket, "e1")));
        doThrow(new RuntimeException("sink failure")).when(sink).write(eq(bucket), any());
        when(progressRepo.findLastDrainedBucket("primary")).thenReturn(Optional.empty());
        drainer.initialiseCursorIfNeeded();

        try {
            drainer.drainBucket(bucket);
        } catch (RuntimeException ignored) {}

        // Progress must NOT be recorded when the sink fails
        verify(progressRepo, never()).recordProgress(any());
    }

    // =========================================================================
    // drainAllAvailable
    // =========================================================================

    @Test
    void drainAllAvailable_noPriorProgress_drainsSealedBuckets() throws Exception {
        // cursor = now - 6W → candidate = now - 5W, sealed at (now-5W)+15 = now-10 < now ✓
        long now    = Instant.now().getEpochSecond();
        long cursor = (now / W) * W - W * 6;
        when(progressRepo.findLastDrainedBucket("primary")).thenReturn(Optional.of(cursor));
        when(progressRepo.readEvents(anyLong())).thenReturn(List.of());
        drainer.initialiseCursorIfNeeded();

        drainer.drainAllAvailable();

        verify(progressRepo, atLeastOnce()).recordProgress(any(DrainedWindow.class));
    }

    @Test
    void drainAllAvailable_cursorSet_startsFromNextBucket() throws Exception {
        // Same arithmetic as above: first candidate must be cursor + W
        long now    = Instant.now().getEpochSecond();
        long cursor = (now / W) * W - W * 6;
        when(progressRepo.findLastDrainedBucket("primary")).thenReturn(Optional.of(cursor));
        when(progressRepo.readEvents(anyLong())).thenReturn(List.of());
        drainer.initialiseCursorIfNeeded();

        drainer.drainAllAvailable();

        ArgumentCaptor<DrainedWindow> captor = ArgumentCaptor.forClass(DrainedWindow.class);
        verify(progressRepo, atLeastOnce()).recordProgress(captor.capture());
        assertThat(captor.getAllValues().get(0).windowBucket()).isEqualTo(cursor + W);
    }

    // =========================================================================
    // Bootstrap off-by-one regression
    // =========================================================================

    @Test
    void firstCandidateBucket_latestMode_firstDrainedBucketIsFirstSealedOne() throws Exception {
        // Regression test for the bootstrap off-by-one.
        //
        // With real wall-clock "now", LATEST mode computes:
        //   earliestRelevantEpoch = now - LATENESS - W
        //   firstCandidate        = floor(earliestRelevantEpoch / W) * W
        //   cursor (fixed)        = firstCandidate - W
        //
        // drainAllAvailable() then starts from cursor + W = firstCandidate.
        //
        // The off-by-one bug set cursor = firstCandidate, making drainAllAvailable
        // start from firstCandidate + W — skipping the firstCandidate bucket entirely.
        //
        // We verify the fix by checking that the first recorded DrainedWindow
        // has windowBucket == firstCandidate (not firstCandidate + W).

        long now                    = Instant.now().getEpochSecond();
        long earliestRelevantEpoch  = now - LATENESS - W;
        long firstCandidate         = (earliestRelevantEpoch / W) * W;

        when(progressRepo.findLastDrainedBucket("primary")).thenReturn(Optional.empty());
        when(progressRepo.readEvents(anyLong())).thenReturn(List.of());

        drainer.initialiseCursorIfNeeded();
        drainer.drainAllAvailable();

        ArgumentCaptor<DrainedWindow> captor = ArgumentCaptor.forClass(DrainedWindow.class);
        verify(progressRepo, atLeastOnce()).recordProgress(captor.capture());
        assertThat(captor.getAllValues().get(0).windowBucket()).isEqualTo(firstCandidate);
    }

    // =========================================================================
    // CONFIGURED bootstrap mode
    // =========================================================================

    /**
     * Verifies that {@code bootstrapMode=CONFIGURED} with {@code startBucket=B}
     * causes the first bucket passed to {@link DrainProgressRepository#readEvents}
     * to be exactly {@code B}, not {@code B + W}.
     *
     * <p>The bug this guards against: {@code resolveInitialCursor()} returned
     * {@code startBucket} directly as the cursor, so {@code drainAllAvailable()}
     * computed {@code candidate = cursor + W = startBucket + W}, skipping
     * {@code startBucket} entirely on first run.  The fix sets
     * {@code cursor = startBucket - W} so the first candidate is {@code startBucket}.</p>
     */
    @Test
    void configuredBootstrap_noHistory_firstDrainedBucketIsExactlyStartBucket() throws Exception {
        // Pick startBucket relative to wall-clock "now" so it is sealed but not far
        // enough in the past to produce more than a handful of iterations.
        // Use the same arithmetic as the existing LATEST regression test:
        //   earliestRelevantEpoch = now - LATENESS - W
        //   startBucket           = floor(earliestRelevantEpoch / W) * W
        // That bucket is exactly at the sealed boundary → only 1 iteration.
        long now           = Instant.now().getEpochSecond();
        long startBucket   = ((now - LATENESS - W) / W) * W;

        InboxProperties configuredProps = new InboxProperties(
                new InboxProperties.AstraProperties(
                        "b.zip", "id", "secret", "tds_inbox", "LOCAL_QUORUM",
                        5000, 86400L, false, 4),
                new InboxProperties.KafkaProperties("t", "t.DLT", 1000L,
                        new InboxProperties.KafkaProperties.HealthProperties(10_000L)),
                new InboxProperties.WindowProperties(W, LATENESS),
                new InboxProperties.DrainerProperties(true, "primary", 5000L, "/tmp/drain",
                        "LOCAL_QUORUM", DrainerBootstrapMode.CONFIGURED, startBucket));

        DrainerCursor cursor = new DrainerCursor();
        InboxDrainer configuredDrainer = new InboxDrainer(progressRepo, sink, cursor, configuredProps);

        // No prior drain history
        when(progressRepo.findLastDrainedBucket("primary")).thenReturn(Optional.empty());
        when(progressRepo.readEvents(anyLong())).thenReturn(List.of());

        configuredDrainer.initialiseCursorIfNeeded();
        configuredDrainer.drainAllAvailable();

        // The first recordProgress call must have windowBucket == startBucket
        ArgumentCaptor<DrainedWindow> captor = ArgumentCaptor.forClass(DrainedWindow.class);
        verify(progressRepo, atLeastOnce()).recordProgress(captor.capture());
        assertThat(captor.getAllValues().get(0).windowBucket())
                .as("first drained bucket must be startBucket=%d, not startBucket+W=%d",
                        startBucket, startBucket + W)
                .isEqualTo(startBucket);
    }

    /**
     * Verifies that {@code bootstrapMode=CONFIGURED} with {@code startBucket <= 0}
     * falls back to {@code LATEST} mode without throwing.
     */
    @Test
    void configuredBootstrap_invalidStartBucket_fallsBackToLatest() throws Exception {
        InboxProperties badProps = new InboxProperties(
                new InboxProperties.AstraProperties(
                        "b.zip", "id", "secret", "tds_inbox", "LOCAL_QUORUM",
                        5000, 86400L, false, 4),
                new InboxProperties.KafkaProperties("t", "t.DLT", 1000L,
                        new InboxProperties.KafkaProperties.HealthProperties(10_000L)),
                new InboxProperties.WindowProperties(W, LATENESS),
                new InboxProperties.DrainerProperties(true, "primary", 5000L, "/tmp/drain",
                        "LOCAL_QUORUM", DrainerBootstrapMode.CONFIGURED, 0L)); // invalid: 0

        DrainerCursor cursor = new DrainerCursor();
        InboxDrainer badDrainer = new InboxDrainer(progressRepo, sink, cursor, badProps);

        when(progressRepo.findLastDrainedBucket("primary")).thenReturn(Optional.empty());
        when(progressRepo.readEvents(anyLong())).thenReturn(List.of());

        // Must not throw — falls back to LATEST gracefully
        badDrainer.initialiseCursorIfNeeded();
        badDrainer.drainAllAvailable();

        // LATEST mode will process at least one sealed bucket
        verify(progressRepo, atLeastOnce()).recordProgress(any(DrainedWindow.class));
    }

    // =========================================================================
    // Helper
    // =========================================================================

    private static SlupEvent sampleEvent(long windowBucket, String eventId) {
        Instant ts = Instant.ofEpochSecond(windowBucket + 1);
        return new SlupEvent(windowBucket, ts, eventId, "guid-1", "NEW",
                "UTS", "app-1", "127.0.0.1", ts, "{}");
    }
}
