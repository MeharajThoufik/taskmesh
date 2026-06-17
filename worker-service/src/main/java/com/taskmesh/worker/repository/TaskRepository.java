package com.taskmesh.worker.repository;

import com.taskmesh.common.enums.TaskStatus;
import com.taskmesh.worker.entity.Task;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface TaskRepository extends JpaRepository<Task, UUID> {

    /**
     * Atomically claim a batch of PENDING tasks. {@code FOR UPDATE SKIP LOCKED} lets multiple
     * workers poll concurrently without ever locking on — or claiming — each other's rows.
     * Must run inside a transaction; the caller flips the rows to RUNNING before commit.
     */
    @Query(value = """
            SELECT * FROM tasks
            WHERE status = 'PENDING'
            ORDER BY priority DESC, created_at ASC
            LIMIT :batchSize
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<Task> claimPendingTasks(@Param("batchSize") int batchSize);

    boolean existsByIdempotencyKey(String idempotencyKey);

    /** Cumulative count of tasks this worker has driven to a given status (e.g. DONE). */
    long countByWorkerIdAndStatus(String workerId, TaskStatus status);

    /** Idempotency keys of a worker's tasks in a given status — used to release Redis locks on reclaim. */
    @Query("SELECT t.idempotencyKey FROM Task t WHERE t.workerId = :workerId AND t.status = :status")
    List<String> findIdempotencyKeys(@Param("workerId") String workerId, @Param("status") TaskStatus status);

    /**
     * Reclaim a dead worker's in-flight tasks: flip {@code fromStatus} (RUNNING) rows owned by
     * {@code workerId} back to {@code toStatus} (PENDING), clear the owner, and bump the version
     * so any stale managed copy can't silently overwrite them. Returns the number reclaimed.
     */
    @Modifying
    @Query("""
            UPDATE Task t
               SET t.status = :toStatus,
                   t.workerId = null,
                   t.version = t.version + 1,
                   t.updatedAt = :now
             WHERE t.workerId = :workerId
               AND t.status = :fromStatus
            """)
    int reclaimTasksFromWorker(@Param("workerId") String workerId,
                               @Param("toStatus") TaskStatus toStatus,
                               @Param("fromStatus") TaskStatus fromStatus,
                               @Param("now") Instant now);
}
