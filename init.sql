-- TaskMesh database schema
-- Auto-loaded via Docker entrypoint (docker-entrypoint-initdb.d) on first container start.
--
-- NOTE: workflow_id, workflow_step, priority columns are used in Part 2 (FlowMesh).
-- They exist now so Part 2 requires zero schema migration.

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Main task queue table
CREATE TABLE tasks (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type              VARCHAR(100) NOT NULL,
    payload           JSONB NOT NULL,
    status            VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    priority          INTEGER DEFAULT 5,
    workflow_id       UUID,                        -- Part 2: set by Orchestrator
    workflow_step     INTEGER,                     -- Part 2: step number in workflow
    retry_count       INTEGER DEFAULT 0,
    max_retries       INTEGER DEFAULT 3,
    version           INTEGER DEFAULT 0,           -- Optimistic locking
    idempotency_key   VARCHAR(255) UNIQUE NOT NULL,
    worker_id         VARCHAR(100),                -- Which worker claimed this task
    error_message     TEXT,
    created_at        TIMESTAMP DEFAULT NOW(),
    updated_at        TIMESTAMP DEFAULT NOW()
);

-- Index for efficient PENDING task claiming
CREATE INDEX idx_tasks_status_priority ON tasks(status, priority DESC, created_at ASC);
CREATE INDEX idx_tasks_idempotency_key ON tasks(idempotency_key);
CREATE INDEX idx_tasks_workflow_id ON tasks(workflow_id);    -- Part 2 ready

-- Worker health tracking for dead node detection
CREATE TABLE worker_health (
    worker_id         VARCHAR(100) PRIMARY KEY,
    last_heartbeat    TIMESTAMP NOT NULL,
    status            VARCHAR(50) DEFAULT 'ACTIVE',
    tasks_processed   INTEGER DEFAULT 0,
    current_task_id   UUID,
    started_at        TIMESTAMP DEFAULT NOW()
);

-- Full audit trail of every task state change
CREATE TABLE task_audit_log (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id           UUID NOT NULL REFERENCES tasks(id),
    worker_id         VARCHAR(100),
    action            VARCHAR(100) NOT NULL,
    detail            TEXT,
    created_at        TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_audit_task_id ON task_audit_log(task_id);
