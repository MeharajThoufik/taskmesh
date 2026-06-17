package com.taskmesh.common.events;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.taskmesh.common.enums.TaskType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Kafka message published by the producer to {@code task-events} and consumed by workers.
 *
 * <p>Carries everything a worker needs to persist and execute a task. The Part 2 fields
 * ({@code workflowId}, {@code workflowStep}) are present now but left null in Part 1, so
 * the Orchestrator can populate them later without changing this contract.
 *
 * <p>Deserialized via the no-args constructor + setters; {@code @Builder} is provided for
 * convenient construction on the producer side.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TaskEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Server-assigned task id (matches tasks.id once persisted). */
    private UUID taskId;

    /** Kind of work to perform. */
    private TaskType type;

    /** Free-form business payload (persisted as JSONB). */
    private Map<String, Object> payload;

    /** Deduplication key; unique per logical task. */
    private String idempotencyKey;

    /** Scheduling priority (higher first). Defaults to 5 server-side when null. */
    private Integer priority;

    /** Maximum execution attempts before dead-lettering. */
    private Integer maxRetries;

    /** Part 2: owning workflow instance. Null in Part 1. */
    private UUID workflowId;

    /** Part 2: step index within the workflow. Null in Part 1. */
    private Integer workflowStep;

    /** When the producer created the event. */
    private Instant createdAt;
}
