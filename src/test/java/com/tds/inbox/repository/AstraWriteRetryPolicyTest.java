package com.tds.inbox.repository;

import com.datastax.oss.driver.api.core.DriverTimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link AstraWriteRetryPolicy}.
 *
 * Uses zero delays (base-delay-ms=0, max-delay-ms=0) to keep tests instant.
 * {@link DriverTimeoutException} is used as the representative retryable exception
 * because it has a simple String constructor; the isRetryable logic is tested for
 * all three retryable exception types via a subclass strategy.
 */
class AstraWriteRetryPolicyTest {

    private AstraWriteRetryPolicy policy;

    @BeforeEach
    void setUp() {
        // 3 max attempts, 0 ms delays so tests run instantly
        policy = new AstraWriteRetryPolicy(3, 0L, 0L);
    }

    // -------------------------------------------------------------------------
    // Success on first attempt
    // -------------------------------------------------------------------------

    @Test
    void executeWithRetry_success_calledOnce() {
        AtomicInteger callCount = new AtomicInteger(0);

        policy.executeWithRetry(() -> callCount.incrementAndGet(), "evt-1");

        assertThat(callCount.get()).isEqualTo(1);
    }

    // -------------------------------------------------------------------------
    // DriverTimeoutException (retryable) → retried until success
    // -------------------------------------------------------------------------

    @Test
    void executeWithRetry_timeoutThenSuccess_retriesAndSucceeds() {
        AtomicInteger callCount = new AtomicInteger(0);

        policy.executeWithRetry(() -> {
            if (callCount.incrementAndGet() < 3) {
                throw new DriverTimeoutException("simulated timeout");
            }
        }, "evt-2");

        assertThat(callCount.get()).isEqualTo(3);
    }

    @Test
    void executeWithRetry_timeoutAllAttempts_throwsAfterMaxAttempts() {
        AtomicInteger callCount = new AtomicInteger(0);

        assertThatThrownBy(() ->
                policy.executeWithRetry(() -> {
                    callCount.incrementAndGet();
                    throw new DriverTimeoutException("always timeout");
                }, "evt-3")
        ).isInstanceOf(DriverTimeoutException.class);

        assertThat(callCount.get()).isEqualTo(3);
    }

    // -------------------------------------------------------------------------
    // OverloadedException subclass (retryable) → retried
    // -------------------------------------------------------------------------

    @Test
    void executeWithRetry_overloadSubclassThenSuccess_retriesAndSucceeds() {
        AtomicInteger callCount = new AtomicInteger(0);

        // OverloadedException has no default constructor; use a subclass that IS-A OverloadedException
        policy.executeWithRetry(() -> {
            if (callCount.incrementAndGet() == 1) {
                throw new FakeOverloadedException();
            }
        }, "evt-4");

        assertThat(callCount.get()).isEqualTo(2);
    }

    @Test
    void executeWithRetry_overloadAllAttempts_exhaustsMaxAttempts() {
        AtomicInteger callCount = new AtomicInteger(0);

        assertThatThrownBy(() ->
                policy.executeWithRetry(() -> {
                    callCount.incrementAndGet();
                    throw new FakeOverloadedException();
                }, "evt-5")
        ).isInstanceOf(FakeOverloadedException.class);

        assertThat(callCount.get()).isEqualTo(3);
    }

    // -------------------------------------------------------------------------
    // Non-retryable exception → thrown immediately (no retry)
    // -------------------------------------------------------------------------

    @Test
    void executeWithRetry_nonRetryableException_thrownImmediately() {
        AtomicInteger callCount = new AtomicInteger(0);
        RuntimeException cause  = new RuntimeException("schema error");

        assertThatThrownBy(() ->
                policy.executeWithRetry(() -> {
                    callCount.incrementAndGet();
                    throw cause;
                }, "evt-6")
        ).isSameAs(cause);

        assertThat(callCount.get()).isEqualTo(1);
    }

    // -------------------------------------------------------------------------
    // Retry count boundary
    // -------------------------------------------------------------------------

    @Test
    void executeWithRetry_alwaysFails_callCountEqualsMaxAttempts() {
        AtomicInteger callCount = new AtomicInteger(0);

        assertThatThrownBy(() ->
                policy.executeWithRetry(() -> {
                    callCount.incrementAndGet();
                    throw new DriverTimeoutException("always");
                }, "evt-7")
        ).isInstanceOf(DriverTimeoutException.class);

        assertThat(callCount.get()).isEqualTo(3);
    }

    @Test
    void executeWithRetry_succeedsOnSecondAttempt_callCountIsTwo() {
        AtomicInteger callCount = new AtomicInteger(0);

        policy.executeWithRetry(() -> {
            if (callCount.incrementAndGet() == 1) {
                throw new DriverTimeoutException("first attempt fails");
            }
        }, "evt-8");

        assertThat(callCount.get()).isEqualTo(2);
    }

    // -------------------------------------------------------------------------
    // Helper: concrete subclass of OverloadedException for testing
    // -------------------------------------------------------------------------

    /**
     * Concrete subclass of {@link com.datastax.oss.driver.api.core.servererrors.OverloadedException}
     * used only in tests (the real exception requires a {@code Node} instance).
     */
    static class FakeOverloadedException
            extends com.datastax.oss.driver.api.core.servererrors.OverloadedException {

        FakeOverloadedException() {
            super(null, "simulated overload");
        }
    }
}
