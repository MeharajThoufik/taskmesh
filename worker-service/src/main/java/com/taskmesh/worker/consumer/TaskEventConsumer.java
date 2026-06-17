package com.taskmesh.worker.consumer;

import com.taskmesh.common.constants.KafkaTopics;
import com.taskmesh.common.enums.TaskStatus;
import com.taskmesh.common.events.TaskEvent;
import com.taskmesh.worker.entity.Task;
import com.taskmesh.worker.repository.TaskRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Durable intake: turns each {@link TaskEvent} into a PENDING row in PostgreSQL.
 *
 * <p>This is deliberately separate from execution — the consumer only persists. The scheduled
 * {@code TaskCoordinator} later claims and runs the row. That hand-off through the database is
 * what lets a crashed worker's in-flight tasks be reclaimed without replaying Kafka.
 *
 * <p>Dedup is by {@code idempotency_key}: a pre-check covers ordinary redelivery; the UNIQUE
 * constraint backstops the rare concurrent race (the listener retries, then the pre-check skips).
 */
@Component
@Slf4j
public class TaskEventConsumer {

    private final TaskRepository taskRepository;

    public TaskEventConsumer(TaskRepository taskRepository) {
        this.taskRepository = taskRepository;
    }

    @KafkaListener(topics = KafkaTopics.TASK_EVENTS, containerFactory = "kafkaListenerContainerFactory")
    public void onTaskEvent(TaskEvent event) {
        if (event == null || event.getIdempotencyKey() == null) {
            log.warn("Discarding malformed task event: {}", event);
            return;
        }
        if (taskRepository.existsByIdempotencyKey(event.getIdempotencyKey())) {
            log.debug("Task already persisted; skipping. idempotencyKey={}", event.getIdempotencyKey());
            return;
        }

        Task task = new Task();
        task.setId(event.getTaskId());
        task.setType(event.getType());
        task.setPayload(event.getPayload());
        task.setStatus(TaskStatus.PENDING);
        task.setPriority(event.getPriority());
        task.setMaxRetries(event.getMaxRetries());
        task.setRetryCount(0);
        task.setWorkflowId(event.getWorkflowId());
        task.setWorkflowStep(event.getWorkflowStep());
        task.setIdempotencyKey(event.getIdempotencyKey());
        task.setCreatedAt(event.getCreatedAt());

        try {
            taskRepository.save(task);
            log.info("Persisted task as PENDING. taskId={}, type={}", task.getId(), task.getType());
        } catch (DataIntegrityViolationException e) {
            // Lost the race to a concurrent delivery — the row already exists. Safe to ignore.
            log.warn("Duplicate task insert ignored. taskId={}, idempotencyKey={}",
                    event.getTaskId(), event.getIdempotencyKey());
        }
    }
}
