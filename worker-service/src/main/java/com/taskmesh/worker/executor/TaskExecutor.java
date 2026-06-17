package com.taskmesh.worker.executor;

import com.taskmesh.common.constants.KafkaTopics;
import com.taskmesh.common.enums.TaskStatus;
import com.taskmesh.common.enums.TaskType;
import com.taskmesh.worker.entity.Task;
import com.taskmesh.worker.entity.TaskAuditLog;
import com.taskmesh.worker.idempotency.IdempotencyService;
import com.taskmesh.worker.repository.TaskAuditLogRepository;
import com.taskmesh.worker.repository.TaskRepository;
import com.taskmesh.worker.retry.RetryHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Core execution logic for a single claimed task.
 *
 * <p>Execution is simulated (CLAUDE.md §11): each type maps to a latency band and a random failure
 * rate. Every run is guarded by the Redis idempotency lock (at-most-once) and wrapped by
 * {@link RetryHandler} for exponential-backoff retry. When retries are exhausted the task is marked
 * DEAD and routed to the dead-letter queue. Every transition is written to the audit log.
 *
 * <p>A {@code "forceFail": true} flag in the payload makes execution fail deterministically — a
 * test hook for exercising the retry → DLQ path.
 */
@Component
@Slf4j
public class TaskExecutor {

    private final TaskRepository taskRepository;
    private final IdempotencyService idempotencyService;
    private final RetryHandler retryHandler;
    private final TaskAuditLogRepository auditLogRepository;

    public TaskExecutor(
            TaskRepository taskRepository,
            IdempotencyService idempotencyService,
            RetryHandler retryHandler,
            TaskAuditLogRepository auditLogRepository) {
        this.taskRepository = taskRepository;
        this.idempotencyService = idempotencyService;
        this.retryHandler = retryHandler;
        this.auditLogRepository = auditLogRepository;
    }

    public void execute(UUID taskId) {
        Task task = taskRepository.findById(taskId).orElse(null);
        if (task == null) {
            log.warn("Claimed task vanished before execution. taskId={}", taskId);
            return;
        }

        String key = task.getIdempotencyKey();
        if (!idempotencyService.acquireIdempotencyLock(key)) {
            log.info("Idempotency lock not acquired; task already processed/in-flight. taskId={}", taskId);
            return;
        }

        audit(task, "CLAIMED", "worker began processing");
        AtomicInteger attempts = new AtomicInteger(0);

        try {
            retryHandler.runWithRetry(() -> {
                int attempt = attempts.incrementAndGet();
                if (attempt > 1) {
                    audit(task, "RETRY", "attempt " + attempt);
                    log.warn("Retrying task. taskId={}, attempt={}", taskId, attempt);
                }
                simulate(task);
            });

            idempotencyService.markCompleted(key);
            task.setStatus(TaskStatus.DONE);
            task.setRetryCount(attempts.get() - 1);
            task.setErrorMessage(null);
            taskRepository.save(task);
            audit(task, "DONE", "completed after " + attempts.get() + " attempt(s)");
            log.info("Task DONE. taskId={}, type={}, attempts={}, worker={}",
                    taskId, task.getType(), attempts.get(), task.getWorkerId());
        } catch (Exception e) {
            // Retries exhausted (or a non-retryable error) — dead-letter the task.
            idempotencyService.releaseLock(key);
            task.setStatus(TaskStatus.DEAD);
            task.setRetryCount(attempts.get());
            task.setErrorMessage(e.getMessage());
            taskRepository.save(task);
            audit(task, "DEAD", "exhausted " + attempts.get() + " attempt(s): " + e.getMessage());
            retryHandler.routeToDeadLetterQueue(task, e.getMessage());
            audit(task, "DLQ_ROUTED", "published to " + KafkaTopics.DEAD_LETTER_QUEUE);
            log.error("Task moved to DLQ after {} attempt(s). taskId={}, type={}, error={}",
                    attempts.get(), taskId, task.getType(), e.getMessage());
        }
    }

    /** Simulate work: a {@code forceFail} payload always fails; otherwise per-type latency + random failure (§11). */
    private void simulate(Task task) {
        Map<String, Object> payload = task.getPayload();
        if (payload != null && Boolean.TRUE.equals(payload.get("forceFail"))) {
            sleepMillis(50);
            throw new TaskExecutionException("Forced failure (forceFail=true) for " + task.getType());
        }

        int minMs;
        int maxMs;
        double failureRate;
        TaskType type = task.getType();
        switch (type) {
            case PAYMENT_CAPTURE -> { minMs = 200; maxMs = 500; failureRate = 0.05; }
            case FRAUD_CHECK -> { minMs = 500; maxMs = 2000; failureRate = 0.10; }
            case NOTIFICATION -> { minMs = 50; maxMs = 100; failureRate = 0.02; }
            case INVENTORY_RESERVE -> { minMs = 100; maxMs = 300; failureRate = 0.03; }
            case INVOICE_GENERATE -> { minMs = 100; maxMs = 200; failureRate = 0.01; }
            default -> { minMs = 100; maxMs = 200; failureRate = 0.0; }
        }

        sleepMillis(ThreadLocalRandom.current().nextInt(minMs, maxMs + 1));

        if (ThreadLocalRandom.current().nextDouble() < failureRate) {
            throw new TaskExecutionException("Simulated failure for task type " + type);
        }
    }

    private void sleepMillis(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new TaskExecutionException("Interrupted during execution", ie);
        }
    }

    private void audit(Task task, String action, String detail) {
        TaskAuditLog entry = new TaskAuditLog();
        entry.setTaskId(task.getId());
        entry.setWorkerId(task.getWorkerId());
        entry.setAction(action);
        entry.setDetail(detail);
        auditLogRepository.save(entry);
    }
}
