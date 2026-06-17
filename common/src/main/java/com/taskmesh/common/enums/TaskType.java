package com.taskmesh.common.enums;

/**
 * The kinds of work a task can represent.
 *
 * <p>Execution is simulated (see CLAUDE.md §11): each type maps to a mock latency
 * and a random failure rate, exercising the retry and dead-letter machinery under load.
 */
public enum TaskType {
    PAYMENT_CAPTURE,
    FRAUD_CHECK,
    NOTIFICATION,
    INVENTORY_RESERVE,
    INVOICE_GENERATE
}
