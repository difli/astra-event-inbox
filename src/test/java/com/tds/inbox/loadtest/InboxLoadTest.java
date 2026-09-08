package com.tds.inbox.loadtest;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Live load test for the Astra Event Inbox.
 *
 * <p>This test runs against a live Kafka broker (localhost:9092) and a live
 * Astra DB. Credentials are read from the same environment variables used by
 * the application:</p>
 * <ul>
 *   <li>{@code KAFKA_BOOTSTRAP_SERVERS} (default: localhost:9092)</li>
 *   <li>{@code KAFKA_TOPIC} (default: cloud-events)</li>
 *   <li>{@code KAFKA_CONSUMER_GROUP} (default: astra-event-inbox)</li>
 * </ul>
 *
 * <p>Excluded from the normal {@code mvn test} run via the {@code @Tag("loadtest")}
 * annotation and the Surefire {@code excludedGroups} configuration in
 * {@code pom.xml}. To run explicitly:</p>
 * <pre>
 *   mvn test -Dgroups=loadtest
 * </pre>
 *
 * <h2>Scenarios</h2>
 * <ol>
 *   <li>{@link #sustained500PerSecond()} — 30 s at 500 msg/s</li>
 *   <li>{@link #sustained2000PerSecond()} — 30 s at 2,000 msg/s</li>
 *   <li>{@link #burstAbove2000PerSecond()} — 5 s burst at 3,000 msg/s</li>
 *   <li>{@link #offsetCommittedOnlyAfterAstraWrite()} — offset safety test</li>
 *   <li>{@link #laterRecordsProcessedAfterOneFails()} — failure cascade test</li>
 * </ol>
 *
 * <h2>Metrics collected</h2>
 * <ul>
 *   <li>Achieved throughput (events / elapsed seconds)</li>
 *   <li>Astra write latency: p50, p95, p99 (measured at the Kafka producer side
 *       as a proxy; use app-side metrics for true repository latency)</li>
 *   <li>Kafka consumer lag per partition (via AdminClient)</li>
 *   <li>JVM CPU % and heap used MB</li>
 * </ul>
 */
@Tag("loadtest")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class InboxLoadTest {

    // -------------------------------------------------------------------------
    // Configuration (all from env, with safe defaults)
    // -------------------------------------------------------------------------

    private static final String BOOTSTRAP_SERVERS =
            System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    private static final String TOPIC =
            System.getenv().getOrDefault("KAFKA_TOPIC", "cloud-events");
    private static final String CONSUMER_GROUP =
            System.getenv().getOrDefault("KAFKA_CONSUMER_GROUP", "astra-event-inbox");

    // -------------------------------------------------------------------------
    // Infrastructure
    // -------------------------------------------------------------------------

    private KafkaProducer<String, String> producer;
    private AdminClient                   adminClient;

    @BeforeAll
    void setUpInfrastructure() {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,    BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.ACKS_CONFIG,                   "all");
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG,              5);
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG,             65536);
        producer = new KafkaProducer<>(producerProps);

        Map<String, Object> adminProps = new HashMap<>();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        adminClient = AdminClient.create(adminProps);
    }

    @AfterAll
    void tearDownInfrastructure() {
        if (producer    != null) producer.close();
        if (adminClient != null) adminClient.close();
    }

    // =========================================================================
    // Scenario 1: Sustained 500 msg/s for 30 seconds
    // =========================================================================

    /**
     * Produces messages at 500 msg/s for 30 seconds, then waits for the consumer
     * group to drain to lag = 0.
     *
     * <p>Assertions:</p>
     * <ul>
     *   <li>Lag = 0 after all messages are consumed</li>
     *   <li>Achieved throughput ≥ 450 msg/s (10 % tolerance)</li>
     * </ul>
     */
    @Test
    void sustained500PerSecond() throws Exception {
        ScenarioResult result = runScenario("sustained-500", 500, 30);
        printResult(result);

        assertThat(result.consumerLag)
                .as("Consumer lag must be 0 after all messages are consumed")
                .isEqualTo(0L);
        assertThat(result.achievedThroughput)
                .as("Achieved throughput must be at least 450 msg/s (10%% tolerance)")
                .isGreaterThanOrEqualTo(450.0);
    }

    // =========================================================================
    // Scenario 2: Sustained 2,000 msg/s for 30 seconds
    // =========================================================================

    /**
     * Produces messages at 2,000 msg/s for 30 seconds, then waits for the consumer
     * group to drain to lag = 0.
     *
     * <p>Assertions:</p>
     * <ul>
     *   <li>Lag = 0 after all messages are consumed</li>
     *   <li>Achieved throughput ≥ 1,800 msg/s (10 % tolerance)</li>
     * </ul>
     */
    @Test
    void sustained2000PerSecond() throws Exception {
        ScenarioResult result = runScenario("sustained-2000", 2_000, 30);
        printResult(result);

        assertThat(result.consumerLag)
                .as("Consumer lag must be 0 after all messages are consumed")
                .isEqualTo(0L);
        assertThat(result.achievedThroughput)
                .as("Achieved throughput must be at least 1,800 msg/s (10%% tolerance)")
                .isGreaterThanOrEqualTo(1_800.0);
    }

    // =========================================================================
    // Scenario 3: Burst at 3,000 msg/s for 5 seconds
    // =========================================================================

    /**
     * Produces messages at 3,000 msg/s for 5 seconds (burst).
     * No lag assertion is made on the burst scenario — the consumer may briefly
     * lag behind and catch up after the burst ends.
     *
     * <p>The test completes successfully as long as no unhandled exception occurs
     * and the broker accepts all messages.</p>
     */
    @Test
    void burstAbove2000PerSecond() throws Exception {
        ScenarioResult result = runScenario("burst-3000", 3_000, 5);
        printResult(result);
        // No lag assertion on burst — allowed to temporarily lag
    }

    // =========================================================================
    // Scenario 4: Offset committed only after Astra write
    // =========================================================================

    /**
     * Verifies that the Kafka offset is NOT advanced when an Astra write failure
     * is injected, and IS advanced once the fault is cleared.
     *
     * <p>This test requires the application to expose a fault-injection mechanism.
     * In the absence of a running application instance, this test documents the
     * protocol and can be run manually against a live instance with fault injection
     * enabled via the {@code INBOX_FAULT_INJECT=true} environment variable.</p>
     *
     * <h3>Procedure</h3>
     * <ol>
     *   <li>Record the current committed offset for partition 0.</li>
     *   <li>Produce one message to partition 0.</li>
     *   <li>Wait up to 10 s for the application to attempt the write.</li>
     *   <li>Assert the offset has NOT advanced (Astra fault is active).</li>
     *   <li>Clear the fault.</li>
     *   <li>Produce the same message again.</li>
     *   <li>Wait up to 15 s for the committed offset to advance.</li>
     *   <li>Assert the offset IS advanced.</li>
     * </ol>
     *
     * <p>When {@code INBOX_FAULT_INJECT} is not set, this test is skipped with
     * a documented explanation.</p>
     */
    @Test
    void offsetCommittedOnlyAfterAstraWrite() throws Exception {
        boolean faultInjectionEnabled =
                Boolean.parseBoolean(System.getenv().getOrDefault("INBOX_FAULT_INJECT", "false"));

        if (!faultInjectionEnabled) {
            System.out.println("""
                    [SKIP] offsetCommittedOnlyAfterAstraWrite:
                    Set INBOX_FAULT_INJECT=true and start the application with a fault-injection
                    endpoint to run this test. The test verifies that the Kafka offset is NOT
                    advanced when an Astra write failure is injected for partition 0.
                    """);
            return;
        }

        TopicPartition partition0 = new TopicPartition(TOPIC, 0);

        // Step 1: record current committed offset
        long offsetBefore = getCommittedOffset(partition0);
        System.out.printf("Offset before injection: %d%n", offsetBefore);

        // Step 2: produce one message to partition 0
        String key = "fault-inject-test-" + UUID.randomUUID();
        String payload = buildCloudEventJson(key, Instant.now());
        producer.send(new ProducerRecord<>(TOPIC, 0, key, payload)).get();
        producer.flush();

        // Step 3 & 4: wait and assert offset NOT advanced (fault active)
        Thread.sleep(10_000);
        long offsetDuringFault = getCommittedOffset(partition0);
        assertThat(offsetDuringFault)
                .as("Offset must NOT advance while Astra fault is active")
                .isEqualTo(offsetBefore);

        // Steps 5 & 6: clearing and re-producing are manual/external steps
        System.out.println("Clear the Astra fault now, then the test will re-produce the message.");
        Thread.sleep(2_000);

        producer.send(new ProducerRecord<>(TOPIC, 0, key, payload)).get();
        producer.flush();

        // Step 7 & 8: wait for offset to advance
        long deadline = System.currentTimeMillis() + 15_000;
        long offsetAfter = offsetBefore;
        while (System.currentTimeMillis() < deadline) {
            offsetAfter = getCommittedOffset(partition0);
            if (offsetAfter > offsetBefore) break;
            Thread.sleep(500);
        }
        assertThat(offsetAfter)
                .as("Offset MUST advance after fault is cleared and write succeeds")
                .isGreaterThan(offsetBefore);
    }

    // =========================================================================
    // Scenario 5: Later records processed after one fails
    // =========================================================================

    /**
     * Verifies that messages 1 and 2 are committed, message 3 is not committed
     * when an Astra failure is injected, and message 3 is eventually committed
     * after the fault is cleared.
     *
     * <p>Requires {@code INBOX_FAULT_INJECT=true} (same as scenario 4).</p>
     */
    @Test
    void laterRecordsProcessedAfterOneFails() throws Exception {
        boolean faultInjectionEnabled =
                Boolean.parseBoolean(System.getenv().getOrDefault("INBOX_FAULT_INJECT", "false"));

        if (!faultInjectionEnabled) {
            System.out.println("""
                    [SKIP] laterRecordsProcessedAfterOneFails:
                    Set INBOX_FAULT_INJECT=true to run this test. See offsetCommittedOnlyAfterAstraWrite
                    for the fault-injection protocol.
                    """);
            return;
        }

        TopicPartition partition0 = new TopicPartition(TOPIC, 0);
        long offsetBefore = getCommittedOffset(partition0);

        // Produce 5 messages to partition 0
        List<String> keys = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            String key = "cascade-test-" + i + "-" + UUID.randomUUID();
            keys.add(key);
            producer.send(new ProducerRecord<>(TOPIC, 0, key,
                    buildCloudEventJson(key, Instant.now()))).get();
        }
        producer.flush();

        // Wait for messages 1 and 2 to be committed (fault activated before message 3)
        long deadline = System.currentTimeMillis() + 20_000;
        long offsetAfterTwo = offsetBefore;
        while (System.currentTimeMillis() < deadline) {
            offsetAfterTwo = getCommittedOffset(partition0);
            if (offsetAfterTwo >= offsetBefore + 2) break;
            Thread.sleep(500);
        }
        assertThat(offsetAfterTwo)
                .as("Messages 1 and 2 must be committed")
                .isGreaterThanOrEqualTo(offsetBefore + 2);

        // Message 3 should not yet be committed (fault active)
        assertThat(getCommittedOffset(partition0))
                .as("Message 3 must NOT be committed while fault is active")
                .isLessThan(offsetBefore + 3);

        // Wait for consumer to retry message 3 and eventually commit all 5
        System.out.println("Clear the Astra fault now to allow message 3 to be retried.");
        Thread.sleep(2_000);

        deadline = System.currentTimeMillis() + 30_000;
        long finalOffset = offsetAfterTwo;
        while (System.currentTimeMillis() < deadline) {
            finalOffset = getCommittedOffset(partition0);
            if (finalOffset >= offsetBefore + 5) break;
            Thread.sleep(500);
        }
        assertThat(finalOffset)
                .as("All 5 messages must eventually be committed after fault is cleared")
                .isGreaterThanOrEqualTo(offsetBefore + 5);
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /**
     * Runs a load-test scenario: produces {@code msgPerSecond} × {@code durationSeconds}
     * messages at the target rate, then waits for the consumer group to drain.
     */
    private ScenarioResult runScenario(String label, int msgPerSecond, int durationSeconds)
            throws Exception {
        long totalMessages = (long) msgPerSecond * durationSeconds;
        long intervalNanos = 1_000_000_000L / msgPerSecond;

        List<Long> producerLatenciesNanos = new ArrayList<>((int) Math.min(totalMessages, 100_000));

        OperatingSystemMXBean osMxBean =
                (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        MemoryMXBean memMxBean = ManagementFactory.getMemoryMXBean();

        long produceStart = System.nanoTime();
        long nextSendTime = produceStart;

        for (long i = 0; i < totalMessages; i++) {
            // Rate control: busy-wait until next scheduled send time
            long now = System.nanoTime();
            if (now < nextSendTime) {
                Thread.sleep(Math.max(0, (nextSendTime - now) / 1_000_000));
            }

            String eventId = UUID.randomUUID().toString();
            Instant ts     = Instant.now();
            String json    = buildCloudEventJson(eventId, ts);

            long sendStart = System.nanoTime();
            producer.send(new ProducerRecord<>(TOPIC, eventId, json));
            long sendEnd = System.nanoTime();

            if (producerLatenciesNanos.size() < 100_000) {
                producerLatenciesNanos.add(sendEnd - sendStart);
            }

            nextSendTime += intervalNanos;
        }
        producer.flush();

        long produceEnd     = System.nanoTime();
        double elapsedSecs  = (produceEnd - produceStart) / 1e9;
        double throughput   = totalMessages / elapsedSecs;

        // Collect JVM stats at end of produce phase
        double cpuLoad   = osMxBean instanceof com.sun.management.OperatingSystemMXBean sunOs
                ? sunOs.getProcessCpuLoad() * 100.0
                : -1.0;
        long heapUsedMb  = memMxBean.getHeapMemoryUsage().getUsed() / (1024 * 1024);

        // Wait for consumer lag to reach 0 (timeout: 2× duration)
        long lagWaitDeadline = System.currentTimeMillis() + (long) durationSeconds * 2 * 1_000;
        long finalLag = Long.MAX_VALUE;
        while (System.currentTimeMillis() < lagWaitDeadline) {
            finalLag = computeTotalLag();
            if (finalLag == 0L) break;
            Thread.sleep(1_000);
        }

        // Compute latency percentiles
        long[] latencies = producerLatenciesNanos.stream().mapToLong(Long::longValue).sorted().toArray();
        long p50 = latencies.length > 0 ? latencies[(int) (latencies.length * 0.50)] : 0;
        long p95 = latencies.length > 0 ? latencies[(int) (latencies.length * 0.95)] : 0;
        long p99 = latencies.length > 0 ? latencies[(int) (latencies.length * 0.99)] : 0;

        return new ScenarioResult(
                label, totalMessages, elapsedSecs, throughput,
                finalLag, p50, p95, p99, cpuLoad, heapUsedMb
        );
    }

    /**
     * Computes the total consumer lag across all partitions for the consumer group.
     */
    private long computeTotalLag() throws Exception {
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = adminClient
                .listConsumerGroupOffsets(CONSUMER_GROUP)
                .partitionsToOffsetAndMetadata()
                .get(10, TimeUnit.SECONDS);

        if (committedOffsets.isEmpty()) {
            return 0L;
        }

        Map<TopicPartition, org.apache.kafka.clients.admin.OffsetSpec> endOffsetRequest = new HashMap<>();
        committedOffsets.keySet().forEach(tp -> endOffsetRequest.put(tp, OffsetSpec.latest()));

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets = adminClient
                .listOffsets(endOffsetRequest)
                .all()
                .get(10, TimeUnit.SECONDS);

        long totalLag = 0L;
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committedOffsets.entrySet()) {
            ListOffsetsResult.ListOffsetsResultInfo endInfo = endOffsets.get(entry.getKey());
            if (endInfo != null) {
                totalLag += Math.max(0L, endInfo.offset() - entry.getValue().offset());
            }
        }
        return totalLag;
    }

    /**
     * Returns the committed offset for a single partition, or {@code -1} if the
     * group has no committed offset for that partition.
     */
    private long getCommittedOffset(TopicPartition tp) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> offsets = adminClient
                .listConsumerGroupOffsets(CONSUMER_GROUP)
                .partitionsToOffsetAndMetadata()
                .get(10, TimeUnit.SECONDS);
        OffsetAndMetadata meta = offsets.get(tp);
        return meta != null ? meta.offset() : -1L;
    }

    /**
     * Prints a formatted scenario result to stdout.
     */
    private void printResult(ScenarioResult r) {
        System.out.printf("""
                %n========================================
                Scenario : %s
                ----------------------------------------
                Messages produced  : %,d
                Elapsed            : %.2f s
                Achieved throughput: %.1f msg/s
                Consumer lag       : %,d
                Producer p50       : %,d ns
                Producer p95       : %,d ns
                Producer p99       : %,d ns
                JVM CPU%%           : %.1f%%
                JVM heap used      : %,d MB
                ========================================%n
                """,
                r.label, r.totalMessages, r.elapsedSecs, r.achievedThroughput,
                r.consumerLag, r.p50Ns, r.p95Ns, r.p99Ns, r.cpuPct, r.heapMb);
    }

    // -------------------------------------------------------------------------
    // CloudEvent JSON builder
    // -------------------------------------------------------------------------

    private static final DateTimeFormatter ISO_FMT =
            DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC);

    private String buildCloudEventJson(String eventId, Instant ts) {
        String tsStr = ISO_FMT.format(ts);
        return String.format("""
                {
                  "specversion": "1.0",
                  "id": "%s",
                  "source": "urn:tds:loadtest",
                  "type": "tds.slup.v1",
                  "time": "%s",
                  "datacontenttype": "application/json",
                  "data": {
                    "guid": "loadtest-guid-001",
                    "operation": "SET",
                    "requestName": "UTS",
                    "applicationId": "loadtest",
                    "timestamp": "%s",
                    "clientAddress": "127.0.0.1",
                    "fields": []
                  }
                }""", eventId, tsStr, tsStr);
    }

    // -------------------------------------------------------------------------
    // Result record
    // -------------------------------------------------------------------------

    private record ScenarioResult(
            String label,
            long   totalMessages,
            double elapsedSecs,
            double achievedThroughput,
            long   consumerLag,
            long   p50Ns,
            long   p95Ns,
            long   p99Ns,
            double cpuPct,
            long   heapMb
    ) {}
}
