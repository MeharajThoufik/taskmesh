package com.taskmesh.worker.executor;

import com.taskmesh.common.enums.TaskStatus;
import com.taskmesh.common.enums.TaskType;
import com.taskmesh.worker.entity.Task;
import com.taskmesh.worker.idempotency.IdempotencyService;
import com.taskmesh.worker.repository.TaskRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Core execution logic for a single claimed task.
 *
 * <p>Execution is simulated (CLAUDE.md §11): each task type maps to a latency band and a random
 * failure rate, which makes the retry and dead-letter paths meaningful under load. Every run is
 * guarded by the Redis idempotency lock so a task executes at most once.
 *
 * <p>Phase 4 handles the success path (→ DONE) and records failures (→ FAILED). Exponential-backoff
 * retry and dead-lettering on top of FAILED are added in Phase 6.
 */
@Component
@Slf4j
public class TaskExecutor {

    private final TaskRepository taskRepository;
    private final IdempotencyService idempotencyService;

    public TaskExecutor(TaskRepository taskRepository, IdempotencyService idempotencyService) {
        this.taskRepository = taskRepository;
        this.idempotencyService = idempotencyService;
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

        try {
            simulate(task.getType());

            idempotencyService.markCompleted(key);
            task.setStatus(TaskStatus.DONE);
            task.setErrorMessage(null);
            taskRepository.save(task);
            log.info("Task DONE. taskId={}, type={}, worker={}", taskId, task.getType(), task.getWorkerId());
        } catch (TaskExecutionException e) {
            // Release the lock so a retry (Phase 6) can re-acquire it.
            idempotencyService.releaseLock(key);
            task.setStatus(TaskStatus.FAILED);
            task.setErrorMessage(e.getMessage());
            taskRepository.save(task);
            log.error("Task execution failed. taskId={}, type={}, attempt={}, error={}",
                    taskId, task.getType(), task.getRetryCount(), e.getMessage());
        }
    }

    /** Simulate work for the given type: realistic latency band + random failure rate (§11). */
    private void simulate(TaskType type) {
        int minMs;
        int maxMs;
        double failureRate;
        switch (type) {
            case PAYMENT_CAPTURE -> { minMs = 200; maxMs = 500; failureRate = 0.05; }
            case FRAUD_CHECK -> { minMs = 500; maxMs = 2000; failureRate = 0.10; }
            case NOTIFICATION -> { minMs = 50; maxMs = 100; failureRate = 0.02; }
            case INVENTORY_RESERVE -> { minMs = 100; maxMs = 300; failureRate = 0.03; }
            case INVOICE_GENERATE -> { minMs = 100; maxMs = 200; failureRate = 0.01; }
            default -> { minMs = 100; maxMs = 200; failureRate = 0.0; }
        }

        int sleepMs = ThreadLocalRandom.current().nextInt(minMs, maxMs + 1);
        try {
            Thread.sleep(sleepMs);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new TaskExecutionException("Interrupted while executing " + type, ie);
        }

        if (ThreadLocalRandom.current().nextDouble() < failureRate) {
            throw new TaskExecutionException("Simulated failure for task type " + type);
        }
    }
}
