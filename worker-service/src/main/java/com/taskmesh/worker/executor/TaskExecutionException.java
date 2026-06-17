package com.taskmesh.worker.executor;

/**
 * Thrown when a task's (simulated) execution fails. Drives retry/dead-letter handling
 * (Resilience4j) introduced in Phase 6.
 */
public class TaskExecutionException extends RuntimeException {

    public TaskExecutionException(String message) {
        super(message);
    }

    public TaskExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
