package com.taskmesh.worker.repository;

import com.taskmesh.worker.entity.Task;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

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
}
