package com.taskmesh.producer.config;

import com.taskmesh.common.constants.KafkaTopics;
import com.taskmesh.common.events.TaskEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka producer wiring for the producer service.
 *
 * <p>Configured for durability (acks=all + idempotent producer) so that an accepted task is
 * never silently lost between the API and the broker. Also declares the {@code task-events}
 * topic — the one topic this service owns — since broker auto-creation is disabled.
 * The worker service declares the topics it produces to (results, DLQ) in its own phases.
 */
@Configuration
public class KafkaProducerConfig {

    private final String bootstrapServers;
    private final int taskEventsPartitions;
    private final short taskEventsReplication;

    public KafkaProducerConfig(
            @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers,
            @Value("${kafka.topics.task-events.partitions:4}") int taskEventsPartitions,
            @Value("${kafka.topics.task-events.replication-factor:1}") short taskEventsReplication) {
        this.bootstrapServers = bootstrapServers;
        this.taskEventsPartitions = taskEventsPartitions;
        this.taskEventsReplication = taskEventsReplication;
    }

    @Bean
    public ProducerFactory<String, TaskEvent> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        // Durability: wait for all in-sync replicas and dedupe on the broker.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, TaskEvent> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public org.apache.kafka.clients.admin.NewTopic taskEventsTopic() {
        return TopicBuilder.name(KafkaTopics.TASK_EVENTS)
                .partitions(taskEventsPartitions)
                .replicas(taskEventsReplication)
                .build();
    }
}
