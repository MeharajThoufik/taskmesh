package com.taskmesh.common.constants;

/**
 * Central registry of Kafka topic names.
 *
 * <p>Topic name strings must never be hardcoded elsewhere — always reference these
 * constants so that Part 2 (FlowMesh) can add topics without hunting for literals.
 */
public final class KafkaTopics {

    private KafkaTopics() {
        // Constants holder; not instantiable.
    }

    // ---- Part 1 topics (created and used now) ----

    /** Producer publishes newly submitted tasks here. */
    public static final String TASK_EVENTS = "task-events";

    /** Workers publish task completion outcomes here. */
    public static final String TASK_RESULTS = "task-results";

    /** Permanently failed tasks (exceeded max retries) land here. */
    public static final String DEAD_LETTER_QUEUE = "dead-letter-queue";

    // ---- Part 2 topics (names reserved now; topics NOT created in Part 1) ----
    // Declared here so Part 2 services reference constants instead of literals.

    /** Part 2: AI Classifier output. */
    public static final String CLASSIFIED_EVENTS = "classified-events";

    /** Part 2: Orchestrator dispatches workflow steps here. */
    public static final String WORKFLOW_STEPS = "workflow-steps";

    /** Part 2: Compliance audit stream. */
    public static final String AUDIT_EVENTS = "audit-events";
}
