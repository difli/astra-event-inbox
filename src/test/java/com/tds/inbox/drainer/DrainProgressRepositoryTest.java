package com.tds.inbox.drainer;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.tds.inbox.config.InboxProperties;
import com.tds.inbox.domain.SlupEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link DrainProgressRepository}.
 *
 * Uses Mockito to stub CqlSession, PreparedStatement, and ResultSet — no
 * real Astra connection required.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DrainProgressRepositoryTest {

    @Mock CqlSession       session;
    @Mock PreparedStatement selectEventsStmt;
    @Mock PreparedStatement insertProgressStmt;
    @Mock PreparedStatement selectLastBucketStmt;
    @Mock BoundStatement    boundStmt;
    @Mock ResultSet         resultSet;

    private DrainProgressRepository repo;

    @BeforeEach
    void setUp() {
        InboxProperties props = new InboxProperties(
                new InboxProperties.AstraProperties(
                        "b.zip", "id", "secret", "tds_inbox", "LOCAL_QUORUM",
                        5000, 86400L, false, 4),
                new InboxProperties.KafkaProperties("t", "t.DLT", 1000L,
                        new InboxProperties.KafkaProperties.HealthProperties(10_000L)),
                new InboxProperties.WindowProperties(5L, 60L),
                new InboxProperties.DrainerProperties(true, "primary", 5000L, "/tmp/drain",
                        "LOCAL_QUORUM", DrainerBootstrapMode.LATEST, 0L));

        repo = new DrainProgressRepository(session, props);

        // Stub session.prepare() for each statement
        when(session.prepare(contains("FROM slup_inbox"))).thenReturn(selectEventsStmt);
        when(session.prepare(contains("INTO drain_progress"))).thenReturn(insertProgressStmt);
        when(session.prepare(contains("FROM drain_progress"))).thenReturn(selectLastBucketStmt);

        // Stub bind() on each prepared statement
        when(selectEventsStmt.bind()).thenReturn(boundStmt);
        when(insertProgressStmt.bind()).thenReturn(boundStmt);
        when(selectLastBucketStmt.bind()).thenReturn(boundStmt);
        when(boundStmt.setLong(anyString(), anyLong())).thenReturn(boundStmt);
        when(boundStmt.setString(anyString(), anyString())).thenReturn(boundStmt);
        when(boundStmt.setInstant(anyString(), any(Instant.class))).thenReturn(boundStmt);
        when(boundStmt.setConsistencyLevel(any())).thenReturn(boundStmt);
        when(boundStmt.setPageSize(anyInt())).thenReturn(boundStmt);

        repo.prepare();
    }

    // -------------------------------------------------------------------------
    // readEvents
    // -------------------------------------------------------------------------

    @Test
    void readEvents_returnsEventsFromResultSet() {
        Row row = mockEventRow("e-1", 1_717_000_010L);
        when(session.execute(boundStmt)).thenReturn(resultSet);
        when(resultSet.iterator()).thenReturn(List.of(row).iterator());

        List<SlupEvent> events = repo.readEvents(1_717_000_010L);

        assertThat(events).hasSize(1);
        assertThat(events.get(0).eventId()).isEqualTo("e-1");
        assertThat(events.get(0).windowBucket()).isEqualTo(1_717_000_010L);
    }

    @Test
    void readEvents_emptyResultSet_returnsEmptyList() {
        when(session.execute(boundStmt)).thenReturn(resultSet);
        when(resultSet.iterator()).thenReturn(List.<Row>of().iterator());

        List<SlupEvent> events = repo.readEvents(1_717_000_000L);

        assertThat(events).isEmpty();
    }

    @Test
    void readEvents_returnsImmutableList() {
        when(session.execute(boundStmt)).thenReturn(resultSet);
        when(resultSet.iterator()).thenReturn(List.<Row>of().iterator());

        List<SlupEvent> events = repo.readEvents(1_717_000_000L);

        assertThat(events).isUnmodifiable();
    }

    // -------------------------------------------------------------------------
    // recordProgress
    // -------------------------------------------------------------------------

    @Test
    void recordProgress_executesInsert() {
        when(session.execute(boundStmt)).thenReturn(resultSet);

        DrainedWindow drained = new DrainedWindow("primary", 1_717_000_000L, 42L, Instant.now());
        repo.recordProgress(drained);

        verify(session).execute(boundStmt);
    }

    // -------------------------------------------------------------------------
    // findLastDrainedBucket
    // -------------------------------------------------------------------------

    @Test
    void findLastDrainedBucket_returnsValueWhenPresent() {
        Row row = mock(Row.class);
        when(row.getLong("window_bucket")).thenReturn(1_717_000_000L);
        when(session.execute(boundStmt)).thenReturn(resultSet);
        when(resultSet.one()).thenReturn(row);

        Optional<Long> result = repo.findLastDrainedBucket("primary");

        assertThat(result).isPresent().hasValue(1_717_000_000L);
    }

    @Test
    void findLastDrainedBucket_returnsEmptyWhenNoRows() {
        when(session.execute(boundStmt)).thenReturn(resultSet);
        when(resultSet.one()).thenReturn(null);

        Optional<Long> result = repo.findLastDrainedBucket("primary");

        assertThat(result).isEmpty();
    }

    // -------------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------------

    private static Row mockEventRow(String eventId, long windowBucket) {
        Row row = mock(Row.class);
        Instant ts = Instant.ofEpochSecond(windowBucket);
        when(row.getLong("window_bucket")).thenReturn(windowBucket);
        when(row.getInstant("event_ts")).thenReturn(ts);
        when(row.getString("event_id")).thenReturn(eventId);
        when(row.getString("guid")).thenReturn("guid-1");
        when(row.getString("operation")).thenReturn("NEW");
        when(row.getString("request_name")).thenReturn("UTS");
        when(row.getString("application_id")).thenReturn("app-1");
        when(row.getString("client_address")).thenReturn("127.0.0.1");
        when(row.getInstant("ingest_time")).thenReturn(ts);
        when(row.getString("payload")).thenReturn("{}");
        return row;
    }
}
