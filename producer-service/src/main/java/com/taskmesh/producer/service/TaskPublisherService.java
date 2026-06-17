package com.taskmesh.producer.service;

import com.taskmesh.common.constants.KafkaTopics;
import com.taskmesh.common.enums.TaskStatus;
import com.taskmesh.common.events.TaskEvent;
import com.taskmesh.producer.dto.TaskRequest;
import com.taskmesh.producer.dto.TaskResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Builds {@link TaskEvent}s from API requests and publishes them to {@code task-events}.
 *
 * <p>Each task gets a server-assigned id (returned to the caller immediately) which the
 * worker reuses as the PostgreSQL primary key, so no DB round-trip is needed at intake.
 * Sends are asynchronous; the durability guarantee comes from acks=all on the producer.
 */
@Service
@Slf4j
public class TaskPublisherService {

    private static final int DEFAULT_PRIORITY = 5;
    private static final int DEFAULT_MAX_RETRIES = 3;

    private final KafkaTemplate<String, TaskEvent> kafkaTemplate;

    public TaskPublisherService(KafkaTemplate<String, TaskEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public TaskResponse publish(TaskRequest request) {
        UUID taskId = UUID.randomUUID();
        String idempotencyKey = resolveIdempotencyKey(request.getIdempotencyKey());
        Instant now = Instant.now();

        TaskEvent event = TaskEvent.builder()
                .taskId(taskId)
                .type(request.getType())
                .payload(request.getPayload())
                .idempotencyKey(idempotencyKey)
                .priority(request.getPriority() != null ? request.getPriority() : DEFAULT_PRIORITY)
                .maxRetries(request.getMaxRetries() != null ? request.getMaxRetries() : DEFAULT_MAX_RETRIES)
                .createdAt(now)
                .build();

        // Key by task id so all events for a task land on the same partition (ordering).
        kafkaTemplate.send(KafkaTopics.TASK_EVENTS, taskId.toString(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish task to Kafka. taskId={}, type={}, error={}",
                                taskId, request.getType(), ex.getMessage(), ex);
                    } else {
                        log.info("Published task. taskId={}, type={}, idempotencyKey={}, partition={}, offset={}",
                                taskId, request.getType(), idempotencyKey,
                                result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                    }
                });

        return TaskResponse.builder()
                .id(taskId)
                .type(request.getType())
                .status(TaskStatus.PENDING)
                .idempotencyKey(idempotencyKey)
                .createdAt(now)
                .message("Task accepted and queued for processing")
                .build();
    }

    public List<TaskResponse> publishBatch(List<TaskRequest> requests) {
        List<TaskResponse> responses = new ArrayList<>(requests.size());
        for (TaskRequest request : requests) {
            responses.add(publish(request));
        }
        log.info("Published batch of {} tasks", responses.size());
        return responses;
    }

    private String resolveIdempotencyKey(String supplied) {
        if (supplied != null && !supplied.isBlank()) {
            return supplied;
        }
        return "auto-" + UUID.randomUUID();
    }
}
