package com.taskmesh.worker.retry;

import com.taskmesh.common.constants.KafkaTopics;
import com.taskmesh.common.enums.TaskStatus;
import com.taskmesh.common.events.TaskResultEvent;
import com.taskmesh.worker.entity.Task;
import io.github.resilience4j.retry.Retry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Wraps task execution with Resilience4j exponential-backoff retry and routes permanently failed
 * tasks to the dead-letter queue (CLAUDE.md §7.5).
 */
@Component
@Slf4j
public class RetryHandler {

    private final Retry taskRetry;
    private final KafkaTemplate<String, TaskResultEvent> taskResultKafkaTemplate;

    public RetryHandler(Retry taskRetry, KafkaTemplate<String, TaskResultEvent> taskResultKafkaTemplate) {
        this.taskRetry = taskRetry;
        this.taskResultKafkaTemplate = taskResultKafkaTemplate;
    }

    /**
     * Run {@code work} under the retry policy. Backoff waits happen on the calling worker thread.
     * If every attempt fails, the last exception propagates to the caller.
     */
    public void runWithRetry(Runnable work) {
        Retry.decorateRunnable(taskRetry, work).run();
    }

    /** Publish a DEAD result for a task that exhausted its retries to {@code dead-letter-queue}. */
    public void routeToDeadLetterQueue(Task task, String error) {
        TaskResultEvent event = TaskResultEvent.builder()
                .taskId(task.getId())
                .idempotencyKey(task.getIdempotencyKey())
                .status(TaskStatus.DEAD)
                .workerId(task.getWorkerId())
                .errorMessage(error)
                .retryCount(task.getRetryCount())
                .workflowId(task.getWorkflowId())
                .workflowStep(task.getWorkflowStep())
                .completedAt(Instant.now())
                .build();

        taskResultKafkaTemplate.send(KafkaTopics.DEAD_LETTER_QUEUE, task.getId().toString(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish task to DLQ. taskId={}, error={}",
                                task.getId(), ex.getMessage(), ex);
                    } else {
                        log.warn("Task routed to dead-letter-queue. taskId={}, idempotencyKey={}",
                                task.getId(), task.getIdempotencyKey());
                    }
                });
    }
}
