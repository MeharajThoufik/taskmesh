package com.taskmesh.producer.dto;

import com.taskmesh.common.enums.TaskStatus;
import com.taskmesh.common.enums.TaskType;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.time.Instant;
import java.util.UUID;

/**
 * Response returned immediately after a task is accepted and published to Kafka.
 *
 * <p>Status is always {@link TaskStatus#PENDING} here — the producer never blocks on
 * execution. Downstream state lives in PostgreSQL once the worker picks the task up.
 */
@Getter
@Builder
@ToString
public class TaskResponse {

    private final UUID id;
    private final TaskType type;
    private final TaskStatus status;
    private final String idempotencyKey;
    private final Instant createdAt;
    private final String message;
}
