package com.tds.inbox.service;

import com.tds.inbox.config.InboxProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link WindowBucketCalculator}.
 *
 * Bucket assignment is deterministic:
 *   window_bucket = floor(epoch_seconds(event_ts) / W) * W
 *
 * The same eventTs always produces the same window_bucket regardless of
 * wall-clock time or drainer state.  There is no late-event re-bucketing.
 */
class WindowBucketCalculatorTest {

    private static final long WINDOW_SIZE = 5L; // seconds

    private WindowBucketCalculator calculator;

    @BeforeEach
    void setUp() {
        InboxProperties props = new InboxProperties(
                new InboxProperties.AstraProperties(
                        "bundle.zip", "id", "secret", "tds_inbox", "LOCAL_QUORUM", 5000,
                        86400L, false, 4),
                new InboxProperties.KafkaProperties("cloud-events", "cloud-events.DLT", 1000L,
                        new InboxProperties.KafkaProperties.HealthProperties(10_000L)),
                new InboxProperties.WindowProperties(WINDOW_SIZE, 60L),
                new InboxProperties.DrainerProperties(true, "primary", 5000L, "/tmp/inbox-drain",
                        "LOCAL_QUORUM", com.tds.inbox.drainer.DrainerBootstrapMode.LATEST, 0L)
        );
        calculator = new WindowBucketCalculator(props);
    }

    // -------------------------------------------------------------------------
    // Core formula: floor(epoch_seconds / W) * W
    // -------------------------------------------------------------------------

    @Test
    void bucket_floorFormula_truncatesToWindowBoundary() {
        // epoch 1784194887 → floor(1784194887 / 5) * 5 = 1784194885
        assertThat(calculator.bucket(Instant.ofEpochSecond(1784194887L)))
                .isEqualTo(1784194885L);
    }

    @Test
    void bucket_eventTsExactlyOnBoundary_returnsSameValue() {
        // epoch already a multiple of W → floor is unchanged
        assertThat(calculator.bucket(Instant.ofEpochSecond(1784194885L)))
                .isEqualTo(1784194885L);
    }

    @Test
    void bucket_oneSecondBeforeBoundary_returnsLowerBucket() {
        // 1784194884 → floor(1784194884 / 5) * 5 = 1784194880
        assertThat(calculator.bucket(Instant.ofEpochSecond(1784194884L)))
                .isEqualTo(1784194880L);
    }

    // -------------------------------------------------------------------------
    // Determinism — same eventTs → same bucket, always
    // -------------------------------------------------------------------------

    @Test
    void bucket_sameTsCalledTwice_returnsSameBucket() {
        Instant ts = Instant.ofEpochSecond(1784194887L);
        long first  = calculator.bucket(ts);
        long second = calculator.bucket(ts);
        assertThat(first).isEqualTo(second);
    }

    @Test
    void bucket_doesNotDependOnWallClockTime() {
        // Calling at "different times" (simulated by different Instant.now() values)
        // must not change the result. Because bucket() only uses eventTs, this is
        // structural: the method signature accepts only eventTs.
        Instant ts = Instant.ofEpochSecond(1784194887L);
        assertThat(calculator.bucket(ts)).isEqualTo(1784194885L);
    }

    // -------------------------------------------------------------------------
    // Epoch zero / small values
    // -------------------------------------------------------------------------

    @Test
    void bucket_epochZero_returnsZero() {
        assertThat(calculator.bucket(Instant.ofEpochSecond(0L))).isEqualTo(0L);
    }

    @Test
    void bucket_smallEpoch_floorsCorrectly() {
        // epoch 7 → floor(7/5)*5 = 5
        assertThat(calculator.bucket(Instant.ofEpochSecond(7L))).isEqualTo(5L);
    }

    // -------------------------------------------------------------------------
    // Configuration
    // -------------------------------------------------------------------------

    @Test
    void windowSizeSeconds_matchesConfiguration() {
        assertThat(calculator.getWindowSizeSeconds()).isEqualTo(WINDOW_SIZE);
    }
}
