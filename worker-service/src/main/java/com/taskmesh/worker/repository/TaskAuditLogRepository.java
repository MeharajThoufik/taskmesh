package com.taskmesh.worker.repository;

import com.taskmesh.worker.entity.TaskAuditLog;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface TaskAuditLogRepository extends JpaRepository<TaskAuditLog, UUID> {
}
