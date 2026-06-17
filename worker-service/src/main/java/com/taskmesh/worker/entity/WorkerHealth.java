package com.taskmesh.worker.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

/**
 * JPA mapping for the {@code worker_health} table — one row per worker, used for liveness
 * tracking and dead-node detection. Schema is owned by init.sql (no DDL generation).
 */
@Entity
@Table(name = "worker_health")
@Getter
@Setter
@NoArgsConstructor
public class WorkerHealth {

    @Id
    @Column(name = "worker_id", length = 100, nullable = false)
    private String workerId;

    @Column(name = "last_heartbeat", nullable = false)
    private Instant lastHeartbeat;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", length = 50)
    private WorkerStatus status;

    @Column(name = "tasks_processed")
    private Integer tasksProcessed;

    @Column(name = "current_task_id")
    private UUID currentTaskId;

    @Column(name = "started_at")
    private Instant startedAt;
}
