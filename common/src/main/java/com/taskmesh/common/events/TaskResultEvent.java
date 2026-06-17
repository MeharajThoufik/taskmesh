package com.taskmesh.common.events;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.taskmesh.common.enums.TaskStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

/**
 * Kafka message published by a worker to {@code task-results} after a task reaches a
 * terminal-for-this-attempt state (DONE, FAILED, or DEAD).
 *
 * <p>The Part 2 fields ({@code workflowId}, {@code workflowStep}) are echoed back from the
 * originating {@link TaskEvent} so the Orchestrator can correlate step completions without
 * a schema change.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TaskResultEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Task this result refers to. */
    private UUID taskId;

    /** Echoed deduplication key. */
    private String idempotencyKey;

    /** Outcome of the attempt. */
    private TaskStatus status;

    /** Worker that produced this result. */
    private String workerId;

    /** Failure detail when status is FAILED or DEAD; null on success. */
    private String errorMessage;

    /** Attempt number reflected by this result. */
    private Integer retryCount;

    /** Part 2: owning workflow instance. Null in Part 1. */
    private UUID workflowId;

    /** Part 2: step index within the workflow. Null in Part 1. */
    private Integer workflowStep;

    /** When the worker finished this attempt. */
    private Instant completedAt;
}
