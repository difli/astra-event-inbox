package com.tds.inbox.config;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.time.Duration;

/**
 * Configures and exposes a {@link CqlSession} connected to Astra DB via the
 * Secure Connect Bundle (SCB).
 *
 * <p>Only CQL is used; the Astra Data API, GraphQL, and Stargate endpoints are
 * not touched.</p>
 *
 * <p>Credentials are read exclusively from environment variables; they are
 * never hard-coded.</p>
 */
@Configuration
public class CassandraConfig {

    private static final Logger log = LoggerFactory.getLogger(CassandraConfig.class);

    private final InboxProperties    properties;
    private final SchemaInitializer  schemaInitializer;

    public CassandraConfig(InboxProperties properties, SchemaInitializer schemaInitializer) {
        this.properties       = properties;
        this.schemaInitializer = schemaInitializer;
    }

    @Bean
    public CqlSession cqlSession() {
        InboxProperties.AstraProperties astra = properties.astra();

        log.info("Connecting to Astra DB – keyspace='{}', bundle='{}'",
                astra.keyspace(), astra.secureBundlePath());

        DriverConfigLoader configLoader = DriverConfigLoader.programmaticBuilder()
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT,
                        Duration.ofMillis(astra.requestTimeoutMs()))
                // Keep-alive: send a heartbeat every 30 s on idle connections
                .withDuration(DefaultDriverOption.HEARTBEAT_INTERVAL, Duration.ofSeconds(30))
                .build();

        CqlSession session = CqlSession.builder()
                .withCloudSecureConnectBundle(Path.of(astra.secureBundlePath()))
                .withAuthCredentials(astra.clientId(), astra.clientSecret())
                .withKeyspace(astra.keyspace())
                .withConfigLoader(configLoader)
                .build();

        log.info("Astra DB session established – cluster='{}', keyspace='{}'",
                session.getMetadata().getClusterName().orElse("unknown"),
                astra.keyspace());

        schemaInitializer.apply(session);

        return session;
    }
}
