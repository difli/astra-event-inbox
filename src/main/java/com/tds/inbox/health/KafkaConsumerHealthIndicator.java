package com.tds.inbox.health;

import com.tds.inbox.config.InboxProperties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Spring Boot {@link HealthIndicator} for the Kafka consumer group.
 *
 * <p>On each health check:</p>
 * <ol>
 *   <li>Describes the consumer group — verifies it has at least one active member.</li>
 *   <li>Reports {@code DOWN} only for terminal/error conditions:
 *       AdminClient exception, no active members, or {@code DEAD} state.</li>
 *   <li>Transient states ({@code PREPARING_REBALANCE}, {@code COMPLETING_REBALANCE})
 *       return {@code UP} with a {@code "state"} detail — these are normal during
 *       startup and consumer group rebalance and must not cause pod restarts.</li>
 *   <li>Consumer lag is exposed as a detail ({@code maxLagObserved}) for observability
 *       but does NOT affect readiness. Lag is a throughput metric, not a connectivity
 *       indicator — high lag during a burst should not trigger unnecessary restarts.</li>
 * </ol>
 *
 * <p>{@code maxLagThreshold} is retained for configuration compatibility but is used
 * only as a detail label, not as a DOWN trigger.</p>
 */
@Component
public class KafkaConsumerHealthIndicator implements HealthIndicator {

    private static final long ADMIN_TIMEOUT_SECONDS = 5L;

    private final KafkaAdmin  kafkaAdmin;
    private final String      groupId;
    private final long        maxLagThreshold;

    public KafkaConsumerHealthIndicator(
            KafkaAdmin kafkaAdmin,
            InboxProperties properties,
            @Value("${spring.kafka.consumer.group-id}") String groupId) {
        this.kafkaAdmin      = kafkaAdmin;
        this.groupId         = groupId;
        this.maxLagThreshold = properties.kafka().health().maxLag();
    }

    @Override
    public Health health() {
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            return checkHealth(admin);
        } catch (Exception ex) {
            return Health.down()
                    .withDetail("error", ex.getMessage())
                    .build();
        }
    }

    private Health checkHealth(AdminClient admin) throws Exception {
        // 1. Describe consumer group
        var describeResult = admin.describeConsumerGroups(List.of(groupId))
                .all()
                .get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        ConsumerGroupDescription desc = describeResult.get(groupId);

        if (desc == null || desc.members().isEmpty()) {
            return Health.down()
                    .withDetail("reason", "No active members in consumer group: " + groupId)
                    .withDetail("groupId", groupId)
                    .build();
        }

        ConsumerGroupState state = desc.state();

        // DEAD is a terminal error — report DOWN.
        if (state == ConsumerGroupState.DEAD) {
            return Health.down()
                    .withDetail("reason",  "Consumer group is DEAD: " + groupId)
                    .withDetail("groupId", groupId)
                    .withDetail("state",   state.toString())
                    .build();
        }

        // Transient rebalancing states are normal during startup and rolling restarts.
        // Report UP with state detail so Kubernetes readiness probes do not kill the pod.
        if (state == ConsumerGroupState.PREPARING_REBALANCE
                || state == ConsumerGroupState.COMPLETING_REBALANCE) {
            return Health.up()
                    .withDetail("groupId",      groupId)
                    .withDetail("state",        state.toString())
                    .withDetail("memberCount",  desc.members().size())
                    .build();
        }

        // 2. Get committed offsets for the group
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = admin
                .listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        if (committedOffsets.isEmpty()) {
            return Health.up()
                    .withDetail("partitionCount",   0)
                    .withDetail("maxLagObserved",   0)
                    .withDetail("groupId",          groupId)
                    .withDetail("memberCount",      desc.members().size())
                    .build();
        }

        // 3. Get end offsets for the same partitions
        Map<TopicPartition, OffsetSpec> endOffsetRequest = new HashMap<>();
        committedOffsets.keySet().forEach(tp -> endOffsetRequest.put(tp, OffsetSpec.latest()));

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets = admin
                .listOffsets(endOffsetRequest)
                .all()
                .get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // 4. Compute lag per partition — for observability only; does NOT affect readiness.
        // High lag is a throughput concern, not a connectivity failure.
        long maxLag = 0L;
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committedOffsets.entrySet()) {
            TopicPartition tp = entry.getKey();
            long committed     = entry.getValue().offset();
            ListOffsetsResult.ListOffsetsResultInfo endInfo = endOffsets.get(tp);
            if (endInfo != null) {
                long lag = Math.max(0L, endInfo.offset() - committed);
                maxLag = Math.max(maxLag, lag);
            }
        }

        // Always UP when STABLE — include lag details for observability.
        // maxLagThreshold is included as a reference value only.
        return Health.up()
                .withDetail("groupId",        groupId)
                .withDetail("state",          state.toString())
                .withDetail("memberCount",    desc.members().size())
                .withDetail("partitionCount", committedOffsets.size())
                .withDetail("maxLagObserved", maxLag)
                .withDetail("maxLagAllowed",  maxLagThreshold)
                .build();
    }
}
