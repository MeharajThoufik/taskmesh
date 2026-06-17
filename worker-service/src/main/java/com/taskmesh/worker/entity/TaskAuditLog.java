package com.taskmesh.worker.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

/**
 * Append-only audit trail of task state transitions ({@code task_audit_log}).
 *
 * <p>One row per action (CLAIMED, RETRY, DONE, DEAD, DLQ_ROUTED, …) so the full lifecycle of a
 * task is reconstructable. Schema owned by init.sql (no DDL generation).
 */
@Entity
@Table(name = "task_audit_log")
@Getter
@Setter
@NoArgsConstructor
public class TaskAuditLog {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id", updatable = false, nullable = false)
    private UUID id;

    @Column(name = "task_id", nullable = false)
    private UUID taskId;

    @Column(name = "worker_id", length = 100)
    private String workerId;

    @Column(name = "action", nullable = false, length = 100)
    private String action;

    @Column(name = "detail", columnDefinition = "text")
    private String detail;

    @Column(name = "created_at")
    private Instant createdAt;

    @PrePersist
    void onPersist() {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
    }
}
