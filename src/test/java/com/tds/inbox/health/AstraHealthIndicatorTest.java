package com.tds.inbox.health;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.tds.inbox.config.InboxProperties;
import com.tds.inbox.drainer.DrainerBootstrapMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link AstraHealthIndicator}.
 *
 * Verifies the fix for the double result.one() bug: the row is read once into
 * a local variable and the second access uses that variable, not a second call.
 */
class AstraHealthIndicatorTest {

    private CqlSession            session;
    private AstraHealthIndicator  indicator;

    @BeforeEach
    void setUp() {
        session = mock(CqlSession.class);

        InboxProperties props = new InboxProperties(
                new InboxProperties.AstraProperties(
                        "b.zip", "id", "secret", "tds_inbox", "LOCAL_QUORUM",
                        5000, 86400L, false, 4),
                new InboxProperties.KafkaProperties("t", "t.DLT", 1000L,
                        new InboxProperties.KafkaProperties.HealthProperties(10_000L)),
                new InboxProperties.WindowProperties(5L, 60L),
                new InboxProperties.DrainerProperties(false, "primary", 5000L, "/tmp/drain",
                        "LOCAL_QUORUM", DrainerBootstrapMode.LATEST, 0L));

        indicator = new AstraHealthIndicator(session, props);
    }

    @Test
    void health_sessionResponds_returnsUpWithReleaseVersion() {
        Row row = mock(Row.class);
        when(row.getString("release_version")).thenReturn("4.0.0");

        ResultSet resultSet = mock(ResultSet.class);
        // one() should only ever be called once due to the fix
        when(resultSet.one()).thenReturn(row);

        when(session.execute(anyString())).thenReturn(resultSet);

        Metadata metadata = mock(Metadata.class);
        when(metadata.getClusterName()).thenReturn(Optional.of("test-cluster"));
        when(session.getMetadata()).thenReturn(metadata);

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails().get("release_version")).isEqualTo("4.0.0");
        assertThat(health.getDetails().get("keyspace")).isEqualTo("tds_inbox");
        assertThat(health.getDetails().get("cluster")).isEqualTo("test-cluster");

        // Verify result.one() is only called once — not twice (the bug was calling it twice)
        verify(resultSet, times(1)).one();
    }

    @Test
    void health_rowIsNull_returnsUpWithUnknownVersion() {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.one()).thenReturn(null);

        when(session.execute(anyString())).thenReturn(resultSet);

        Metadata metadata = mock(Metadata.class);
        when(metadata.getClusterName()).thenReturn(Optional.empty());
        when(session.getMetadata()).thenReturn(metadata);

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails().get("release_version")).isEqualTo("unknown");
    }

    @Test
    void health_sessionThrows_returnsDown() {
        when(session.execute(anyString())).thenThrow(new RuntimeException("connection refused"));

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails().get("error")).asString().contains("connection refused");
    }
}
