package com.taskmesh.common.enums;

/**
 * Lifecycle states of a task.
 *
 * <pre>
 * PENDING  -> claimable by a worker
 * RUNNING  -> claimed and executing
 * DONE     -> completed successfully
 * FAILED   -> execution failed (will be retried while retry_count < max_retries)
 * DEAD     -> exhausted retries; routed to the dead-letter queue
 * </pre>
 */
public enum TaskStatus {
    PENDING,
    RUNNING,
    DONE,
    FAILED,
    DEAD
}
