package com.taskmesh.worker.repository;

import com.taskmesh.worker.entity.WorkerHealth;
import com.taskmesh.worker.entity.WorkerStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.List;

public interface WorkerHealthRepository extends JpaRepository<WorkerHealth, String> {

    /** Workers still marked ACTIVE whose last heartbeat predates the staleness threshold = dead. */
    List<WorkerHealth> findByStatusAndLastHeartbeatBefore(WorkerStatus status, Instant threshold);
}
