package com.tds.inbox.config;

import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Kafka listener container factory configuration.
 *
 * <h2>Offset safety</h2>
 * <p>Configures a {@link DefaultErrorHandler} with unlimited retries and a
 * configurable back-off delay ({@code inbox.kafka.retry-interval-ms}, default 1 000 ms).
 * When the listener method throws an exception, the error handler seeks the partition
 * back to the failed offset so the record is redelivered on the next poll cycle.
 * This guarantees that a failed Astra write at offset N can never be skipped by a
 * later successful write at offset N+1 in the same poll batch.</p>
 *
 * <h2>Why unlimited retries</h2>
 * <p>The no-arg {@code new DefaultErrorHandler()} uses {@code FixedBackOff(0, 9)} —
 * 9 retries then skip.  For a durable inbox this is wrong: a persistent Astra outage
 * lasting more than 10 consecutive poll cycles would silently discard records.
 * {@link FixedBackOff#UNLIMITED_ATTEMPTS} prevents any record from being skipped
 * regardless of how long the outage lasts.</p>
 *
 * <h2>Why non-zero back-off</h2>
 * <p>A zero-delay {@code FixedBackOff} causes a tight retry loop that spins the consumer
 * thread and floods logs during an Astra outage.  The configurable {@code retryIntervalMs}
 * (default 1 000 ms) introduces a pause between attempts, giving Astra time to recover
 * and reducing log noise without sacrificing correctness.</p>
 *
 * <h2>Why {@link ConcurrentKafkaListenerContainerFactoryConfigurer}</h2>
 * <p>Constructing {@link ConcurrentKafkaListenerContainerFactory} directly bypasses
 * Spring Boot's auto-configuration.  Properties such as
 * {@code spring.kafka.listener.concurrency}, {@code ack-mode}, and deserialiser settings
 * would not be applied.  By injecting the configurer and calling
 * {@link ConcurrentKafkaListenerContainerFactoryConfigurer#configure configure()} first,
 * all Boot-managed settings are inherited; we then override only the error handler.</p>
 *
 * <h2>Parse errors</h2>
 * <p>Parse errors are handled inside the listener method (routed to DLT + ack) and
 * do NOT propagate as exceptions, so the error handler never fires for them.</p>
 */
@Configuration
public class KafkaConfig {

    /**
     * Creates the {@link ConcurrentKafkaListenerContainerFactory} bean used by
     * {@code @KafkaListener(containerFactory = "kafkaListenerContainerFactory")}.
     *
     * <p>The configurer applies all Spring Boot Kafka listener settings (concurrency,
     * ack-mode, deserializers, etc.) before we attach the custom error handler.</p>
     *
     * @param configurer      Spring Boot's auto-configurer for the container factory
     * @param consumerFactory the auto-configured Spring Kafka consumer factory
     * @param props           typed inbox configuration
     * @return the configured container factory
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<String, String> consumerFactory,
            InboxProperties props) {

        long retryIntervalMs = props.kafka().retryIntervalMs();

        // FixedBackOff(retryIntervalMs, UNLIMITED_ATTEMPTS):
        //   - UNLIMITED_ATTEMPTS: a persistent Astra outage never causes a record to be dropped.
        //   - retryIntervalMs > 0: avoids a zero-delay tight loop that would spin the thread
        //     and flood logs; default 1000 ms provides natural back-pressure.
        DefaultErrorHandler errorHandler =
                new DefaultErrorHandler(new FixedBackOff(retryIntervalMs, FixedBackOff.UNLIMITED_ATTEMPTS));

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        // Apply all Spring Boot Kafka auto-configuration first (concurrency=6, ack-mode, etc.)
        // so that application.yml settings are not bypassed by manual construction.
        // The raw-type cast is required because the Boot configurer API declares
        // ConcurrentKafkaListenerContainerFactory<Object,Object>; it is safe here because
        // the configurer only writes Spring-internal settings and does not read back the
        // parameterised consumer type.
        @SuppressWarnings("unchecked")
        ConcurrentKafkaListenerContainerFactory<Object, Object> rawFactory =
                (ConcurrentKafkaListenerContainerFactory<Object, Object>) (Object) factory;
        configurer.configure(rawFactory, (ConsumerFactory<Object, Object>) (Object) consumerFactory);

        // Override only the error handler; all other settings come from the configurer.
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}
