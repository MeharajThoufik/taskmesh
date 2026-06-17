package com.taskmesh.worker.entity;

/**
 * Liveness state of a worker in the {@code worker_health} table.
 *
 * <p>ACTIVE while heartbeating; flipped to INACTIVE by the dead-worker monitor once a worker's
 * last heartbeat goes stale. A restarted worker heartbeats itself back to ACTIVE.
 */
public enum WorkerStatus {
    ACTIVE,
    INACTIVE
}
