package com.tds.inbox.drainer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.tds.inbox.config.InboxProperties;
import com.tds.inbox.domain.SlupEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * {@link EventSink} implementation that writes drained events to a JSONL file
 * on the local filesystem.
 *
 * <h2>Output format</h2>
 * <p>One file per window, named {@code <drain-id>_<window_bucket>.jsonl}
 * (e.g. {@code primary_1717000000.jsonl}), written to the configured
 * {@code inbox.drainer.output-path} directory.  Each line is a single
 * JSON-serialised {@link SlupEvent}.</p>
 *
 * <h2>Idempotency</h2>
 * <p>If the file already exists it is <em>overwritten</em>.  This is safe because
 * the drainer only calls {@link #write} for a bucket that has not yet been recorded
 * in {@code drain_progress}; a pre-existing file means the previous run wrote the
 * events but crashed before recording progress — rewriting is correct.</p>
 *
 * <h2>Atomicity</h2>
 * <p>The file is written to a {@code .tmp} sibling and renamed to the final name
 * on completion.  A crash mid-write leaves a {@code .tmp} file that is harmless
 * and will be overwritten on the next retry.</p>
 */
@Component
public class FileWriterEventSink implements EventSink {

    private static final Logger log = LoggerFactory.getLogger(FileWriterEventSink.class);

    private final Path         outputDir;
    private final String       drainId;
    private final ObjectMapper mapper;

    public FileWriterEventSink(InboxProperties properties) {
        this.drainId   = properties.drainer().drainId();
        this.outputDir = Path.of(properties.drainer().outputPath());
        this.mapper    = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /**
     * Writes all events for {@code windowBucket} to a JSONL file.
     *
     * @param windowBucket the epoch-second bucket that was drained
     * @param events       ordered list of events (may be empty)
     * @throws IOException if the file cannot be written
     */
    @Override
    public void write(long windowBucket, List<SlupEvent> events) throws IOException {
        Files.createDirectories(outputDir);

        String filename  = drainId + "_" + windowBucket + ".jsonl";
        Path   tmpPath   = outputDir.resolve(filename + ".tmp");
        Path   finalPath = outputDir.resolve(filename);

        try (BufferedWriter writer = Files.newBufferedWriter(
                tmpPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

            for (SlupEvent event : events) {
                writer.write(mapper.writeValueAsString(event));
                writer.newLine();
            }
        }

        // Atomic rename — replaces any pre-existing file from a previous failed attempt
        Files.move(tmpPath, finalPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        log.info("FileWriterEventSink: wrote {} event(s) → {}", events.size(), finalPath);
    }
}
