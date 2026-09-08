package com.tds.inbox.drainer;

import com.tds.inbox.domain.SlupEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link FileWriterEventSink}.
 *
 * Tests use a temp directory so no real filesystem paths are assumed.
 * No Spring context is loaded — FileWriterEventSink is constructed directly.
 */
class FileWriterEventSinkTest {

    @TempDir
    Path tempDir;

    // -------------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------------

    private FileWriterEventSink sink(String drainId) {
        // Minimal InboxProperties — only drainer properties are used by the sink
        var drainerProps = new com.tds.inbox.config.InboxProperties.DrainerProperties(
                true, drainId, 5000L, tempDir.toString(),
                "LOCAL_QUORUM", com.tds.inbox.drainer.DrainerBootstrapMode.LATEST, 0L);
        var props = new com.tds.inbox.config.InboxProperties(
                new com.tds.inbox.config.InboxProperties.AstraProperties(
                        "b.zip", "id", "secret", "tds_inbox", "LOCAL_QUORUM",
                        5000, 86400L, false, 4),
                new com.tds.inbox.config.InboxProperties.KafkaProperties("t", "t.DLT", 1000L,
                        new com.tds.inbox.config.InboxProperties.KafkaProperties.HealthProperties(10_000L)),
                new com.tds.inbox.config.InboxProperties.WindowProperties(5L, 60L),
                drainerProps);
        return new FileWriterEventSink(props);
    }

    private static SlupEvent event(String id) {
        Instant ts = Instant.ofEpochSecond(1_717_000_010L);
        return new SlupEvent(1_717_000_010L, ts, id, "guid-1", "NEW",
                "UTS", "app-1", "127.0.0.1", ts, "{\"key\":\"val\"}");
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    @Test
    void write_createsJsonlFile() throws Exception {
        FileWriterEventSink s = sink("primary");
        s.write(1_717_000_000L, List.of(event("e1"), event("e2")));

        Path output = tempDir.resolve("primary_1717000000.jsonl");
        assertThat(output).exists();
        List<String> lines = Files.readAllLines(output);
        assertThat(lines).hasSize(2);
        assertThat(lines.get(0)).contains("\"eventId\":\"e1\"");
        assertThat(lines.get(1)).contains("\"eventId\":\"e2\"");
    }

    @Test
    void write_emptyEventList_createsEmptyFile() throws Exception {
        FileWriterEventSink s = sink("primary");
        s.write(1_717_000_005L, List.of());

        Path output = tempDir.resolve("primary_1717000005.jsonl");
        assertThat(output).exists();
        assertThat(Files.readAllLines(output)).isEmpty();
    }

    @Test
    void write_idempotent_overwritesPreviousFile() throws Exception {
        FileWriterEventSink s = sink("primary");
        s.write(1_717_000_000L, List.of(event("e1")));
        s.write(1_717_000_000L, List.of(event("e2")));  // second call — must overwrite

        Path output = tempDir.resolve("primary_1717000000.jsonl");
        List<String> lines = Files.readAllLines(output);
        assertThat(lines).hasSize(1);
        assertThat(lines.get(0)).contains("\"eventId\":\"e2\"");
    }

    @Test
    void write_filenameIncludesDrainIdAndBucket() throws Exception {
        FileWriterEventSink s = sink("replica");
        s.write(9_999_999_000L, List.of());

        assertThat(tempDir.resolve("replica_9999999000.jsonl")).exists();
    }

    @Test
    void write_createsOutputDirectoryIfMissing() throws Exception {
        // Use a sub-directory that does not yet exist
        Path sub = tempDir.resolve("new-subdir");
        var drainerProps = new com.tds.inbox.config.InboxProperties.DrainerProperties(
                true, "primary", 5000L, sub.toString(),
                "LOCAL_QUORUM", com.tds.inbox.drainer.DrainerBootstrapMode.LATEST, 0L);
        var props = new com.tds.inbox.config.InboxProperties(
                new com.tds.inbox.config.InboxProperties.AstraProperties(
                        "b.zip", "id", "secret", "tds_inbox", "LOCAL_QUORUM",
                        5000, 86400L, false, 4),
                new com.tds.inbox.config.InboxProperties.KafkaProperties("t", "t.DLT", 1000L,
                        new com.tds.inbox.config.InboxProperties.KafkaProperties.HealthProperties(10_000L)),
                new com.tds.inbox.config.InboxProperties.WindowProperties(5L, 60L),
                drainerProps);
        FileWriterEventSink s = new FileWriterEventSink(props);

        s.write(1_717_000_000L, List.of(event("e1")));

        assertThat(sub.resolve("primary_1717000000.jsonl")).exists();
    }
}
