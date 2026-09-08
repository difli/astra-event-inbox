package com.tds.inbox.config;

import com.datastax.oss.driver.api.core.CqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Applies {@code schema.cql} to Astra DB synchronously during bean
 * construction — before {@code @PostConstruct} methods on other beans
 * (e.g. {@link com.tds.inbox.repository.EventRepository#prepare()}) run.
 *
 * <p>Called directly from {@link CassandraConfig#cqlSession()} immediately
 * after the session is established, so the table is guaranteed to exist
 * before any prepared statement is compiled.</p>
 *
 * <p>{@code CREATE TABLE IF NOT EXISTS} is idempotent — re-runs are safe.</p>
 */
@Component
public class SchemaInitializer {

    private static final Logger log = LoggerFactory.getLogger(SchemaInitializer.class);

    /**
     * Applies all CQL statements in {@code schema.cql} using the given session.
     * Called by {@link CassandraConfig} before the session bean is published.
     */
    public void apply(CqlSession session) {
        applyFile(session, "schema.cql");
        applyFile(session, "schema-phase2.cql");
    }

    // -------------------------------------------------------------------------
    // Internals
    // -------------------------------------------------------------------------

    private void applyFile(CqlSession session, String classpathResource) {
        log.info("Applying {} to Astra DB…", classpathResource);
        List<String> statements;
        try {
            statements = loadStatements(classpathResource);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load " + classpathResource + " from classpath", e);
        }

        int applied = 0;
        for (String stmt : statements) {
            // All statements use CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS,
            // which are idempotent — Astra suppresses AlreadyExistsException for them.
            // Any other exception is unexpected (syntax error, permission error, etc.)
            // and must fail the application rather than being silently swallowed.
            session.execute(stmt);
            applied++;
            log.info("Schema [{}]: OK — {}", classpathResource, abbrev(stmt));
        }
        log.info("Schema [{}] initialisation complete — {} statement(s) applied",
                classpathResource, applied);
    }

    private List<String> loadStatements(String classpathResource) throws Exception {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                new ClassPathResource(classpathResource).getInputStream(),
                StandardCharsets.UTF_8))) {

            String line;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                // Skip full-line comments and blank lines
                if (trimmed.startsWith("--") || trimmed.isEmpty()) continue;

                // Strip inline trailing comments
                int commentIdx = trimmed.indexOf("--");
                if (commentIdx > 0) trimmed = trimmed.substring(0, commentIdx).trim();

                current.append(trimmed).append(" ");

                if (trimmed.endsWith(";")) {
                    String stmt = current.toString().trim();
                    if (!stmt.isEmpty()) statements.add(stmt);
                    current.setLength(0);
                }
            }
        }
        return statements;
    }

    private static String abbrev(String s) {
        return s.length() > 80 ? s.substring(0, 80) + "…" : s;
    }
}
