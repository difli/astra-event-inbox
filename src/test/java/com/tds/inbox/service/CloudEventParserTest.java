package com.tds.inbox.service;

import com.tds.inbox.config.InboxProperties;
import com.tds.inbox.domain.SlupEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link CloudEventParser}.
 *
 * No Spring context is loaded — the parser and its dependencies are
 * instantiated directly so tests run fast.
 */
class CloudEventParserTest {

    private static final String VALID_EVENT = """
            {
              "specversion": "1.0",
              "id": "5f2b8c1a-9d3e-4a7b-8c6d-1e2f3a4b5c6d",
              "source": "urn:tds:prod",
              "type": "tds.slup.v1",
              "time": "2026-07-16T09:41:27.123456Z",
              "datacontenttype": "application/json",
              "data": {
                "guid": "000000000000000123456789",
                "operation": "SET",
                "requestName": "UTS",
                "applicationId": "webmail",
                "timestamp": "2026-07-16T09:41:27.123456Z",
                "slupTimestamp": 1784194887,
                "clientAddress": "10.20.30.40",
                "fields": []
              }
            }
            """;

    private CloudEventParser parser;

    @BeforeEach
    void setUp() {
        InboxProperties props = new InboxProperties(
                new InboxProperties.AstraProperties(
                        "bundle.zip", "id", "secret", "tds_inbox", "LOCAL_QUORUM", 5000,
                        86400L, false, 4),
                new InboxProperties.KafkaProperties("cloud-events", "cloud-events.DLT", 1000L,
                        new InboxProperties.KafkaProperties.HealthProperties(10_000L)),
                new InboxProperties.WindowProperties(5L, 60L),
                new InboxProperties.DrainerProperties(true, "primary", 5000L, "/tmp/inbox-drain",
                        "LOCAL_QUORUM", com.tds.inbox.drainer.DrainerBootstrapMode.LATEST, 0L)
        );
        parser = new CloudEventParser(new WindowBucketCalculator(props));
    }

    // -------------------------------------------------------------------------
    // Happy path
    // -------------------------------------------------------------------------

    @Test
    void parse_validCloudEvent_returnsSlupEvent() {
        SlupEvent event = parser.parse(VALID_EVENT);

        assertThat(event.eventId()).isEqualTo("5f2b8c1a-9d3e-4a7b-8c6d-1e2f3a4b5c6d");
        assertThat(event.guid()).isEqualTo("000000000000000123456789");
        assertThat(event.operation()).isEqualTo("SET");
        assertThat(event.requestName()).isEqualTo("UTS");
        assertThat(event.applicationId()).isEqualTo("webmail");
        assertThat(event.clientAddress()).isEqualTo("10.20.30.40");
        assertThat(event.payload().strip()).isEqualTo(VALID_EVENT.strip());
    }

    @Test
    void parse_validCloudEvent_eventTsIsDataTimestamp() {
        SlupEvent event = parser.parse(VALID_EVENT);

        // data.timestamp = 2026-07-16T09:41:27.123456Z → epoch 1784194887.123456
        assertThat(event.eventTs().getEpochSecond()).isEqualTo(1784194887L);
    }

    @Test
    void parse_validCloudEvent_windowBucketCorrect() {
        SlupEvent event = parser.parse(VALID_EVENT);

        // floor(1784194887 / 5) * 5 = 1784194885
        assertThat(event.windowBucket()).isEqualTo(1784194885L);
    }

    @Test
    void parse_validCloudEvent_ingestTimeIsApproximatelyNow() {
        Instant before = Instant.now();
        SlupEvent event = parser.parse(VALID_EVENT);
        Instant after  = Instant.now();

        assertThat(event.ingestTime()).isBetween(before, after);
    }

    // -------------------------------------------------------------------------
    // Fallback: envelope.time used when data.timestamp is absent
    // -------------------------------------------------------------------------

    @Test
    void parse_missingDataTimestamp_fallsBackToEnvelopeTime() {
        String json = """
                {
                  "specversion": "1.0",
                  "id": "aabbccdd-0000-0000-0000-000000000001",
                  "source": "urn:tds:prod",
                  "type": "tds.slup.v1",
                  "time": "2026-07-16T09:41:27.000Z",
                  "data": {
                    "guid": "g1",
                    "operation": "NEW"
                  }
                }
                """;

        SlupEvent event = parser.parse(json);

        assertThat(event.eventTs().getEpochSecond()).isEqualTo(1784194887L);
    }

    // -------------------------------------------------------------------------
    // Null / empty field coercion
    // -------------------------------------------------------------------------

    @Test
    void parse_nullFields_convertedToEmptyString() {
        String json = """
                {
                  "specversion": "1.0",
                  "id": "aabbccdd-0000-0000-0000-000000000002",
                  "source": "urn:tds:prod",
                  "type": "tds.slup.v1",
                  "data": {
                    "timestamp": "2026-07-16T09:41:27.000Z"
                  }
                }
                """;

        SlupEvent event = parser.parse(json);

        assertThat(event.guid()).isEmpty();
        assertThat(event.operation()).isEmpty();
        assertThat(event.requestName()).isEmpty();
        assertThat(event.applicationId()).isEmpty();
        assertThat(event.clientAddress()).isEmpty();
    }

    // -------------------------------------------------------------------------
    // Error cases
    // -------------------------------------------------------------------------

    @Test
    void parse_malformedJson_throwsParseException() {
        assertThatThrownBy(() -> parser.parse("{not-valid-json"))
                .isInstanceOf(CloudEventParseException.class)
                .hasMessageContaining("Failed to deserialise");
    }

    @Test
    void parse_missingId_throwsParseException() {
        String json = """
                {
                  "specversion": "1.0",
                  "source": "urn:tds:prod",
                  "data": { "timestamp": "2026-07-16T09:41:27.000Z" }
                }
                """;

        assertThatThrownBy(() -> parser.parse(json))
                .isInstanceOf(CloudEventParseException.class)
                .hasMessageContaining("missing required field 'id'");
    }

    @Test
    void parse_missingData_throwsParseException() {
        String json = """
                {
                  "specversion": "1.0",
                  "id": "aabbccdd-0000-0000-0000-000000000003",
                  "source": "urn:tds:prod"
                }
                """;

        assertThatThrownBy(() -> parser.parse(json))
                .isInstanceOf(CloudEventParseException.class)
                .hasMessageContaining("no 'data' payload");
    }

    @Test
    void parse_noTimestampAnywhere_throwsParseException() {
        String json = """
                {
                  "specversion": "1.0",
                  "id": "aabbccdd-0000-0000-0000-000000000004",
                  "source": "urn:tds:prod",
                  "data": {
                    "guid": "g1"
                  }
                }
                """;

        assertThatThrownBy(() -> parser.parse(json))
                .isInstanceOf(CloudEventParseException.class)
                .hasMessageContaining("no usable timestamp");
    }
}
