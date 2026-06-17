package com.taskmesh.worker.config;

import com.taskmesh.common.constants.KafkaTopics;
import com.taskmesh.common.events.TaskResultEvent;
import org.apache.kafka.clients.admin.NewTopic;
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
 * Kafka producer wiring for the worker's outbound topics.
 *
 * <p>The worker owns the topics it produces to: {@code dead-letter-queue} (failed tasks) and
 * {@code task-results} (outcomes — declared now for Part 2 readiness). Durable settings
 * (acks=all + idempotent producer) mirror the producer service. Broker auto-creation is disabled,
 * so the topics are declared here as {@link NewTopic} beans.
 */
@Configuration
public class KafkaProducerConfig {

    private final String bootstrapServers;
    private final int dlqPartitions;
    private final short dlqReplication;
    private final int resultsPartitions;
    private final short resultsReplication;

    public KafkaProducerConfig(
            @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers,
            @Value("${kafka.topics.dead-letter-queue.partitions:2}") int dlqPartitions,
            @Value("${kafka.topics.dead-letter-queue.replication-factor:1}") short dlqReplication,
            @Value("${kafka.topics.task-results.partitions:4}") int resultsPartitions,
            @Value("${kafka.topics.task-results.replication-factor:1}") short resultsReplication) {
        this.bootstrapServers = bootstrapServers;
        this.dlqPartitions = dlqPartitions;
        this.dlqReplication = dlqReplication;
        this.resultsPartitions = resultsPartitions;
        this.resultsReplication = resultsReplication;
    }

    @Bean
    public ProducerFactory<String, TaskResultEvent> taskResultProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, TaskResultEvent> taskResultKafkaTemplate() {
        return new KafkaTemplate<>(taskResultProducerFactory());
    }

    @Bean
    public NewTopic deadLetterQueueTopic() {
        return TopicBuilder.name(KafkaTopics.DEAD_LETTER_QUEUE)
                .partitions(dlqPartitions)
                .replicas(dlqReplication)
                .build();
    }

    @Bean
    public NewTopic taskResultsTopic() {
        return TopicBuilder.name(KafkaTopics.TASK_RESULTS)
                .partitions(resultsPartitions)
                .replicas(resultsReplication)
                .build();
    }
}
