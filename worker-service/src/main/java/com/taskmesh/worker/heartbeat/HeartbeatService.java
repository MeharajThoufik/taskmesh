package com.taskmesh.worker.heartbeat;

import com.taskmesh.common.enums.TaskStatus;
import com.taskmesh.worker.entity.WorkerHealth;
import com.taskmesh.worker.entity.WorkerStatus;
import com.taskmesh.worker.idempotency.IdempotencyService;
import com.taskmesh.worker.repository.TaskRepository;
import com.taskmesh.worker.repository.WorkerHealthRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * Worker liveness and dead-node recovery (CLAUDE.md §7.7).
 *
 * <ul>
 *   <li>{@link #publishHeartbeat()} — every 10s, upserts this worker's row as ACTIVE with a fresh
 *       timestamp.</li>
 *   <li>{@link #detectDeadWorkers()} — every 30s, finds workers whose last heartbeat is older than
 *       the staleness threshold (60s), marks them INACTIVE, resets their RUNNING tasks to PENDING,
 *       and releases the tasks' Redis idempotency locks so a healthy worker can re-execute them.</li>
 * </ul>
 *
 * <p>Releasing the lock on reclaim is essential: a worker that died mid-execution may still hold
 * the {@code processing} lock; without releasing it the reclaimed task would be skipped forever
 * (until the 300s TTL) by the idempotency guard.
 */
@Service
@Slf4j
public class HeartbeatService {

    private final WorkerHealthRepository workerHealthRepository;
    private final TaskRepository taskRepository;
    private final IdempotencyService idempotencyService;
    private final String workerId;
    private final long deadThresholdSeconds;

    public HeartbeatService(
            WorkerHealthRepository workerHealthRepository,
            TaskRepository taskRepository,
            IdempotencyService idempotencyService,
            @Value("${worker.id:worker-local}") String workerId,
            @Value("${worker.dead-threshold-seconds:60}") long deadThresholdSeconds) {
        this.workerHealthRepository = workerHealthRepository;
        this.taskRepository = taskRepository;
        this.idempotencyService = idempotencyService;
        this.workerId = workerId;
        this.deadThresholdSeconds = deadThresholdSeconds;
    }

    @Scheduled(fixedDelayString = "${worker.heartbeat-interval-ms:10000}")
    @Transactional
    public void publishHeartbeat() {
        Instant now = Instant.now();
        WorkerHealth health = workerHealthRepository.findById(workerId).orElseGet(() -> {
            WorkerHealth created = new WorkerHealth();
            created.setWorkerId(workerId);
            created.setStartedAt(now);
            created.setTasksProcessed(0);
            return created;
        });
        health.setLastHeartbeat(now);
        health.setStatus(WorkerStatus.ACTIVE);
        health.setTasksProcessed((int) taskRepository.countByWorkerIdAndStatus(workerId, TaskStatus.DONE));
        workerHealthRepository.save(health);
        log.debug("Heartbeat published. worker={}, at={}", workerId, now);
    }

    @Scheduled(fixedDelayString = "${worker.detect-interval-ms:30000}")
    @Transactional
    public void detectDeadWorkers() {
        Instant threshold = Instant.now().minusSeconds(deadThresholdSeconds);
        List<WorkerHealth> deadWorkers =
                workerHealthRepository.findByStatusAndLastHeartbeatBefore(WorkerStatus.ACTIVE, threshold);

        for (WorkerHealth dead : deadWorkers) {
            // A live worker heartbeats itself, so it won't appear stale; guard anyway.
            if (dead.getWorkerId().equals(workerId)) {
                continue;
            }

            // Capture the keys before the bulk reset so we can free their idempotency locks.
            List<String> lockedKeys =
                    taskRepository.findIdempotencyKeys(dead.getWorkerId(), TaskStatus.RUNNING);

            int reclaimed = taskRepository.reclaimTasksFromWorker(
                    dead.getWorkerId(), TaskStatus.PENDING, TaskStatus.RUNNING, Instant.now());

            dead.setStatus(WorkerStatus.INACTIVE);
            workerHealthRepository.save(dead);

            lockedKeys.forEach(idempotencyService::releaseLock);

            log.warn("Dead worker detected: {} (last heartbeat {}). Marked INACTIVE; reclaimed {} "
                            + "RUNNING task(s) -> PENDING and released {} idempotency lock(s).",
                    dead.getWorkerId(), dead.getLastHeartbeat(), reclaimed, lockedKeys.size());
        }
    }
}
