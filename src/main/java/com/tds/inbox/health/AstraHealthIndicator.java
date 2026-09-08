package com.tds.inbox.health;

import com.datastax.oss.driver.api.core.CqlSession;
import com.tds.inbox.config.InboxProperties;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Spring Boot {@link HealthIndicator} for the Astra DB connection.
 *
 * <p>On each health check, executes {@code SELECT release_version FROM system.local}
 * against the connected cluster.  This is a lightweight, non-destructive query that
 * every CQL node answers locally without coordinator involvement.</p>
 *
 * <h2>Security</h2>
 * <p>The health output deliberately omits credentials, the Secure Connect Bundle
 * path, and the client secret.  Only the cluster name and keyspace are reported.</p>
 *
 * <h2>Lifecycle</h2>
 * <p>Returns {@code DOWN} if the CQL session is closed or the query fails for any
 * reason.  The actuator aggregate health rolls up to {@code DOWN} if this indicator
 * is down, preventing readiness probes from passing until Astra recovers.</p>
 */
@Component
public class AstraHealthIndicator implements HealthIndicator {

    private static final String PROBE_QUERY = "SELECT release_version FROM system.local";

    private final CqlSession      session;
    private final String          keyspace;

    public AstraHealthIndicator(CqlSession session, InboxProperties properties) {
        this.session  = session;
        this.keyspace = properties.astra().keyspace();
    }

    @Override
    public Health health() {
        try {
            var result = session.execute(PROBE_QUERY);
            // Store the row reference — result.one() consumes the row on first call;
            // calling it a second time returns null.
            var row = result.one();
            String releaseVersion = row != null ? row.getString("release_version") : "unknown";

            String clusterName = session.getMetadata()
                    .getClusterName()
                    .orElse("unknown");

            return Health.up()
                    .withDetail("cluster",         clusterName)
                    .withDetail("keyspace",         keyspace)
                    .withDetail("release_version",  releaseVersion)
                    .build();

        } catch (Exception ex) {
            return Health.down()
                    .withDetail("error", ex.getMessage())
                    .build();
        }
    }
}
