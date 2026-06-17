# TaskMesh — CLAUDE.md
# Project Intelligence File — Read This First, Every Session

---

## 1. What This Project Is

TaskMesh is a **fault-tolerant distributed task execution engine** built with Java and Spring Boot.

It is **Part 1 of a two-part project.**

- **Part 1 (this):** TaskMesh — distributed task execution with Kafka, PostgreSQL, Redis
- **Part 2 (future):** FlowMesh — extends TaskMesh with AI classification, workflow orchestration, and a dashboard service

> CRITICAL: Every design decision in Part 1 must keep Part 2 extension in mind.
> Nothing should need to be rewritten when Part 2 is added.
> Only new services and new Kafka topics get added in Part 2.
> The Worker Service built here must remain unchanged in Part 2.

---

## 2. Business Problem Being Solved

Simulate a payment/order task processing system where:

- Tasks must execute **exactly once** — no duplicate payment processing (idempotency)
- Worker node crashes must **never lose tasks** (fault tolerance)
- 1 million concurrent task submissions must not overwhelm downstream systems (Kafka buffering)
- Multiple worker instances must **never process the same task simultaneously** (SKIP LOCKED)
- Dead tasks must be isolated and not block healthy task flow (dead letter queue)

Real-world equivalents:
- Flipkart order processing pipeline
- PayPal payment retry jobs
- Freshworks CRM workflow automation
- Chargebee subscription billing engine

---

## 3. Tech Stack — Do Not Deviate Without Asking

| Layer | Technology | Version |
|---|---|---|
| Language | Java | 17 |
| Framework | Spring Boot | 3.2.x |
| Messaging | Apache Kafka | 3.6.x (via Docker) |
| Database | PostgreSQL | 15 (via Docker) |
| Cache / Idempotency | Redis | 7 (via Docker) |
| Build Tool | Maven | 3.9.x |
| Containerization | Docker + Docker Compose | Latest |
| Resilience | Resilience4j | 2.x |
| Boilerplate Reduction | Lombok | Latest stable |
| ORM | Spring Data JPA + Hibernate | Via Spring Boot |
| Testing | JUnit 5 + Testcontainers | Via Spring Boot |

---

## 4. Project Structure — Follow Exactly

```
taskmesh/
├── CLAUDE.md                        ← This file. Read every session.
├── docker-compose.yml               ← PostgreSQL + Redis + Kafka + Zookeeper
├── init.sql                         ← Database schema (auto-loaded by Docker)
├── README.md                        ← Architecture + load test results
│
├── common/                          ← Shared module (events, DTOs, constants)
│   ├── pom.xml
│   └── src/main/java/com/taskmesh/common/
│       ├── events/
│       │   ├── TaskEvent.java       ← Kafka message model (producer → worker)
│       │   └── TaskResultEvent.java ← Kafka message model (worker → results)
│       ├── enums/
│       │   ├── TaskStatus.java      ← PENDING, RUNNING, DONE, FAILED, DEAD
│       │   └── TaskType.java        ← PAYMENT_CAPTURE, FRAUD_CHECK, NOTIFICATION, etc.
│       └── constants/
│           └── KafkaTopics.java     ← All topic name constants
│
├── producer-service/                ← Service 1: Task intake REST API
│   ├── pom.xml
│   └── src/main/java/com/taskmesh/producer/
│       ├── ProducerApplication.java
│       ├── controller/
│       │   └── TaskController.java  ← POST /api/tasks, GET /api/tasks/{id}
│       ├── service/
│       │   └── TaskPublisherService.java
│       ├── dto/
│       │   ├── TaskRequest.java
│       │   └── TaskResponse.java
│       └── config/
│           └── KafkaProducerConfig.java
│
└── worker-service/                  ← Service 2: Task execution engine
    ├── pom.xml
    └── src/main/java/com/taskmesh/worker/
        ├── WorkerApplication.java
        ├── consumer/
        │   └── TaskEventConsumer.java    ← Kafka listener
        ├── executor/
        │   └── TaskExecutor.java         ← Core execution logic
        ├── coordinator/
        │   └── TaskCoordinator.java      ← SKIP LOCKED claiming
        ├── idempotency/
        │   └── IdempotencyService.java   ← Redis NX checks
        ├── heartbeat/
        │   └── HeartbeatService.java     ← Worker health + dead node detection
        ├── retry/
        │   └── RetryHandler.java         ← Exponential backoff + DLQ routing
        ├── entity/
        │   ├── Task.java                 ← JPA entity
        │   ├── WorkerHealth.java         ← JPA entity
        │   └── TaskAuditLog.java         ← JPA entity
        ├── repository/
        │   ├── TaskRepository.java
        │   ├── WorkerHealthRepository.java
        │   └── TaskAuditLogRepository.java
        └── config/
            ├── KafkaConsumerConfig.java
            ├── RedisConfig.java
            ├── ThreadPoolConfig.java
            └── Resilience4jConfig.java
```

---

## 5. Database Schema — Follow Exactly, Do Not Modify Column Names

```sql
-- Auto-loaded via init.sql in Docker

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Main task queue table
-- NOTE: workflow_id, workflow_step, priority columns are used in Part 2.
-- They exist now so Part 2 requires zero schema migration.
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
```

---

## 6. Kafka Topics — All Topics

### Part 1 Topics (build now)
```
task-events          ← Producer publishes new tasks here
task-results         ← Worker publishes completion outcomes here
dead-letter-queue    ← Permanently failed tasks (exceeded max retries)
```

### Part 2 Topics (do NOT create now, but do NOT hardcode topic names)
```
classified-events    ← Part 2: AI Classifier output
workflow-steps       ← Part 2: Orchestrator dispatches steps here
audit-events         ← Part 2: Compliance audit stream
```

### Topic Configuration
```yaml
# application.yml format
kafka:
  topics:
    task-events:
      name: task-events
      partitions: 4
      replication-factor: 1
    task-results:
      name: task-results
      partitions: 4
      replication-factor: 1
    dead-letter-queue:
      name: dead-letter-queue
      partitions: 2
      replication-factor: 1
```

All topic names must come from `KafkaTopics.java` constants.
Never hardcode topic name strings anywhere else.

---

## 7. Core Conventions — Enforce Always

### 7.1 Dependency Injection
```java
// ✅ CORRECT — always constructor injection
@Service
public class TaskExecutor {
    private final TaskRepository taskRepository;
    private final IdempotencyService idempotencyService;
    private final RedisTemplate<String, String> redisTemplate;

    public TaskExecutor(
        TaskRepository taskRepository,
        IdempotencyService idempotencyService,
        RedisTemplate<String, String> redisTemplate
    ) {
        this.taskRepository = taskRepository;
        this.idempotencyService = idempotencyService;
        this.redisTemplate = redisTemplate;
    }
}

// ❌ WRONG — never field injection
@Autowired
private TaskRepository taskRepository;
```

### 7.2 JPA Entities
```java
// ✅ CORRECT
@Entity
@Table(name = "tasks")
@Getter
@Setter
@NoArgsConstructor
public class Task {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Version
    private Integer version;    // Always include for optimistic locking

    // ... other fields
}

// ❌ WRONG — never use @Data on JPA entities (causes issues with equals/hashCode)
@Data
public class Task { }
```

### 7.3 Idempotency Pattern
```java
// Before executing ANY task, always follow this exact pattern:
public boolean acquireIdempotencyLock(String idempotencyKey) {
    String redisKey = "task:" + idempotencyKey;
    Boolean acquired = redisTemplate.opsForValue()
        .setIfAbsent(redisKey, "processing", Duration.ofSeconds(300));
    return Boolean.TRUE.equals(acquired);
}

// After task completes successfully:
public void markCompleted(String idempotencyKey) {
    String redisKey = "task:" + idempotencyKey;
    redisTemplate.opsForValue().set(redisKey, "completed", Duration.ofHours(24));
}

// Flow:
// 1. Try to acquire lock → if fails, task already processed → skip silently
// 2. Execute task
// 3. Mark completed in Redis
// 4. Update PostgreSQL status to DONE
```

### 7.4 Worker Task Claiming (SKIP LOCKED)
```java
// In TaskRepository — always use this pattern, never plain SELECT FOR UPDATE
@Query(value = """
    SELECT * FROM tasks
    WHERE status = 'PENDING'
    ORDER BY priority DESC, created_at ASC
    LIMIT :batchSize
    FOR UPDATE SKIP LOCKED
    """, nativeQuery = true)
List<Task> claimPendingTasks(@Param("batchSize") int batchSize);

// Batch size: always 10 per worker thread
// Never claim one at a time — inefficient
// Never claim without SKIP LOCKED — causes worker collisions
```

### 7.5 Retry with Exponential Backoff
```java
// Retry delays: 1s → 2s → 4s (max 3 retries, then dead letter queue)
// Use Resilience4j — not manual Thread.sleep()

@Bean
public RetryConfig taskRetryConfig() {
    return RetryConfig.custom()
        .maxAttempts(3)
        .waitDuration(Duration.ofSeconds(1))
        .intervalFunction(IntervalFunction.ofExponentialBackoff(1000, 2))
        .retryExceptions(TaskExecutionException.class)
        .build();
}
```

### 7.6 Thread Pool Configuration
```java
// Worker thread pool — always these bounds
@Bean
public ThreadPoolTaskExecutor workerThreadPool() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(10);
    executor.setMaxPoolSize(50);
    executor.setQueueCapacity(500);
    executor.setThreadNamePrefix("task-worker-");
    executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
    executor.initialize();
    return executor;
}
// CallerRunsPolicy = backpressure — slows intake instead of crashing
```

### 7.7 Heartbeat Monitor
```java
// HeartbeatService must:
// 1. Write heartbeat every 10 seconds (scheduled)
// 2. Monitor thread runs every 30 seconds
// 3. Any worker with last_heartbeat > 60 seconds ago = DEAD
// 4. Tasks claimed by dead workers → reset to PENDING status
// 5. Dead worker status updated to INACTIVE in worker_health table

@Scheduled(fixedDelay = 10000)
public void publishHeartbeat() { }

@Scheduled(fixedDelay = 30000)
public void detectDeadWorkers() { }
```

### 7.8 Error Handling
```java
// ✅ CORRECT — always log with task context
log.error("Task execution failed. taskId={}, type={}, attempt={}, error={}",
    task.getId(), task.getType(), task.getRetryCount(), e.getMessage(), e);

// ❌ WRONG — never swallow silently
try {
    executeTask(task);
} catch (Exception e) {
    // do nothing
}
```

### 7.9 Payload Design
```java
// Task payload is always JSONB — flexible for Part 2 extension
// Example payloads:

// PAYMENT_CAPTURE task:
{
    "taskType": "PAYMENT_CAPTURE",
    "orderId": "ORD-123",
    "amount": 47000,
    "currency": "INR",
    "customerId": "CUST-456"
    // Part 2 will add: "riskScore": 0.87, "workflowTemplate": "STRICT"
}

// FRAUD_CHECK task:
{
    "taskType": "FRAUD_CHECK",
    "orderId": "ORD-123",
    "customerId": "CUST-456",
    "amount": 47000,
    "locationMatch": false
}
```

---

## 8. Docker Compose Setup

```yaml
# docker-compose.yml structure
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: taskmesh
      POSTGRES_USER: taskmesh
      POSTGRES_PASSWORD: taskmesh123
    ports:
      - "5432:5432"
    volumes:
      - ./init.sql:/docker-entrypoint-initdb.d/init.sql

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: false
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  producer-service:
    build: ./producer-service
    ports:
      - "8081:8081"
    depends_on:
      - kafka
      - postgres

  # Run 3 replicas of worker service to simulate distributed workers
  worker-service-1:
    build: ./worker-service
    environment:
      WORKER_ID: worker-1
    depends_on:
      - kafka
      - postgres
      - redis

  worker-service-2:
    build: ./worker-service
    environment:
      WORKER_ID: worker-2
    depends_on:
      - kafka
      - postgres
      - redis

  worker-service-3:
    build: ./worker-service
    environment:
      WORKER_ID: worker-3
    depends_on:
      - kafka
      - postgres
      - redis
```

---

## 9. REST API Design

### Producer Service (port 8081)

```
POST   /api/tasks              ← Submit a new task
GET    /api/tasks/{id}         ← Get task status by ID
GET    /api/tasks?status=FAILED ← Filter tasks by status
POST   /api/tasks/batch        ← Submit multiple tasks (for load testing)
GET    /api/health             ← Health check
```

### Worker Service (port 8082, 8083, 8084 for replicas)
```
GET    /api/worker/health      ← Worker status + tasks processed count
GET    /api/worker/metrics     ← Queue depth, processing rate
```

---

## 10. Build Phases — Follow This Order Strictly

### Phase 1 — Infrastructure (Day 1)
- [ ] docker-compose.yml with PostgreSQL, Redis, Kafka, Zookeeper
- [ ] init.sql with exact schema from Section 5
- [ ] Verify: `docker-compose up` → all services healthy
- [ ] Verify: Connect to PostgreSQL, confirm tables created
- **STOP. Confirm with user before Phase 2.**

### Phase 2 — Common Module (Day 1)
- [ ] Maven multi-module parent pom.xml
- [ ] common/pom.xml
- [ ] TaskEvent.java (Kafka message model)
- [ ] TaskResultEvent.java
- [ ] TaskStatus.java enum
- [ ] TaskType.java enum
- [ ] KafkaTopics.java constants
- **STOP. Confirm with user before Phase 3.**

### Phase 3 — Producer Service (Day 2)
- [ ] Spring Boot app with Kafka producer
- [ ] TaskController with POST /api/tasks
- [ ] TaskPublisherService — publishes TaskEvent to Kafka
- [ ] TaskRequest / TaskResponse DTOs
- [ ] KafkaProducerConfig
- [ ] Verify: Submit task via curl → appears in Kafka topic
- **STOP. Confirm with user before Phase 4.**

### Phase 4 — Worker Service Core (Days 3-4)
- [ ] Kafka consumer (TaskEventConsumer)
- [ ] Task entity + repository
- [ ] TaskCoordinator — SKIP LOCKED claiming
- [ ] IdempotencyService — Redis NX pattern
- [ ] TaskExecutor — core execution logic (simulated task types)
- [ ] Verify: Worker consumes task → marks DONE in PostgreSQL
- **STOP. Confirm with user before Phase 5.**

### Phase 5 — Heartbeat Monitor (Day 5)
- [ ] HeartbeatService — publishes every 10s
- [ ] Dead worker detection — runs every 30s
- [ ] Task reclaim from dead workers
- [ ] WorkerHealth entity + repository
- [ ] Verify: Kill one worker → tasks reclaimed by healthy workers
- **STOP. Confirm with user before Phase 6.**

### Phase 6 — Retry + Dead Letter Queue (Day 6)
- [ ] RetryHandler with Resilience4j exponential backoff
- [ ] Dead letter queue publisher
- [ ] TaskAuditLog — record every state transition
- [ ] Verify: Force task failure → retries 3 times → moves to DLQ
- **STOP. Confirm with user before Phase 7.**

### Phase 7 — Integration Testing + Load Testing (Day 7-8)
- [ ] Run all 3 worker replicas via Docker Compose
- [ ] Submit 1000 tasks via batch endpoint
- [ ] Kill worker-2 mid-execution
- [ ] Verify: No tasks lost, no tasks duplicated
- [ ] Apache Bench load test: `ab -n 10000 -c 100 http://localhost:8081/api/tasks`
- [ ] Document results in README.md
- [ ] **Part 1 COMPLETE**

---

## 11. Simulated Task Types (Mock Execution)

Since this is a portfolio project, task execution is simulated.
Each task type has a realistic mock implementation:

```java
// TaskType.PAYMENT_CAPTURE → sleep 200-500ms, 5% random failure rate
// TaskType.FRAUD_CHECK     → sleep 500-2000ms, 10% random failure rate
// TaskType.NOTIFICATION    → sleep 50-100ms, 2% random failure rate
// TaskType.INVENTORY_RESERVE → sleep 100-300ms, 3% random failure rate
// TaskType.INVOICE_GENERATE  → sleep 100-200ms, 1% random failure rate
```

Failure rates trigger the retry mechanism.
This makes load tests realistic and meaningful.

---

## 12. What Part 2 Will Add (Do Not Build Now)

When Part 2 begins, these will be added as new services.
Zero changes to worker-service or producer-service.

```
NEW: ai-classifier-service/
     └── Reads from task-events Kafka topic
     └── Scores risk using order features
     └── Publishes to classified-events topic

NEW: orchestrator-service/
     └── Reads from classified-events
     └── Loads workflow template from DB
     └── Creates WorkflowInstance
     └── Dispatches steps to workflow-steps topic

NEW: dashboard-service/
     └── Metrics API
     └── Workflow completion rates
     └── AI accuracy tracking
     └── Worker health visibility

NEW DB TABLES (migration, not replacement):
     └── workflow_templates
     └── workflow_instances
     └── workflow_step_definitions
```

---

## 13. Definition of Done — Part 1

Part 1 is complete only when ALL of these are true:

```
✅ docker-compose up starts everything with one command
✅ POST /api/tasks submits a task and returns task ID immediately
✅ Worker consumes task from Kafka and executes it
✅ Same task submitted twice → executes only once (idempotency proven)
✅ 3 worker replicas run simultaneously without duplicate processing
✅ Kill worker-2 mid-execution → tasks reclaimed, no data lost
✅ Failed task retries 3 times with exponential backoff
✅ After 3 failures → task moves to dead-letter-queue
✅ Apache Bench results documented in README.md
✅ Architecture diagram in README.md
✅ Every class follows conventions from Section 7
```

---

## 14. Key Interview Questions This Project Answers

When asked in an interview, you must be able to answer these
from your own code — not from memory:

1. "Why SKIP LOCKED instead of a distributed lock like Redis SETNX?"
2. "What happens if Redis goes down — does your idempotency break?"
3. "How does your heartbeat monitor detect a dead worker?"
4. "Walk me through what happens when a task fails 3 times."
5. "How do you prevent the thundering herd when all workers restart?"
6. "What's the difference between at-least-once and exactly-once delivery?"
7. "Why Kafka instead of a simple DB polling approach?"
8. "How would you scale this to 10x the current load?"
9. "What's your consistency model — CP or AP?"
10. "Walk me through a complete task lifecycle from submission to completion."

---

## 15. Session Startup Checklist

At the start of every Claude Code session:

1. Read this CLAUDE.md fully
2. Check which Phase was last completed
3. Do not start a new phase until the previous one is confirmed working
4. Follow all conventions in Section 7 without exception
5. If a decision is not covered here — ask the user before deciding

---

*Last updated: 2026-06-18 — Phases 1–6 complete and verified*
*Current phase: Phase 6 (Retry + Dead Letter Queue) DONE*
*Next action: Phase 7 — Integration Testing + Load Testing*

---

## Progress Log

- ✅ **Phase 1 — Infrastructure:** `docker-compose.yml` + `init.sql`. All 4 services healthy; schema loaded.
- ✅ **Phase 2 — Common Module:** parent `pom.xml`, `common` module (events, enums, `KafkaTopics`). Maven wrapper generated. Builds to Java 17 bytecode.
- ✅ **Phase 3 — Producer Service:** `POST /api/tasks` + `/batch` + `/api/health`, publishes `TaskEvent` to `task-events` (acks=all, idempotent). Verified messages land in Kafka. Read-status endpoints (`GET /api/tasks/{id}`, `?status=`) deferred — need persistence layer; decide owner (producer read-repo vs Part 2 dashboard) later.
- ✅ **Phase 4 — Worker Service Core:** consumer → persist PENDING → coordinator claim (`FOR UPDATE SKIP LOCKED`, batch 10) → executor (Redis-NX idempotency + simulated §11 work) → DONE. Idempotency proven.
- ✅ **Phase 5 — Heartbeat Monitor:** `WorkerHealth` entity + repo, `HeartbeatService` (publish every 10s, detect dead >60s every 30s). Dead worker → INACTIVE; its RUNNING tasks reset to PENDING **and their Redis idempotency locks released** (else reclaimed tasks would be skipped by the guard). Verified via injected zombie worker: detected → reclaimed → re-executed to DONE by the live worker. `tasks_processed` maintained via a DONE-count query; `current_task_id` left null (model runs a concurrent batch, not one task). Full 3-replica process-kill is exercised in Phase 7. TaskAuditLog + Resilience4j retry/DLQ still to come (Phase 6).
  - 🐞 **Fix (crash-restart orphan bug):** dead-detection alone misses a worker that crashes and restarts within the 60s window — it keeps its heartbeat fresh, is never declared dead, and its abandoned RUNNING tasks would stay stuck forever. Added `HeartbeatService.recoverOwnOrphanedTasks()` (`@EventListener(ApplicationReadyEvent)`): on startup a worker resets its own RUNNING rows → PENDING and releases their locks (a just-started worker can't legitimately own RUNNING work). Verified: orphans with a fresh heartbeat were recovered to DONE within ~2s of restart. Assumes one live instance per `worker_id`.
- ✅ **Phase 6 — Retry + Dead Letter Queue:** `Resilience4jConfig` (Retry bean: 3 attempts, exponential backoff 1s→2s, retry on `TaskExecutionException`), `RetryHandler` (runs work under retry + routes exhausted tasks to `dead-letter-queue`), worker `KafkaProducerConfig` (TaskResultEvent producer; declares `dead-letter-queue` 2/1 + `task-results` 4/1 — minor extension to §4 since the worker must produce to the DLQ), `TaskAuditLog` entity + repo. `TaskExecutor` rewritten: idempotency lock → retry-wrapped simulate → DONE, or on exhaustion → DEAD + DLQ; writes audit rows (CLAIMED/RETRY/DONE/DEAD/DLQ_ROUTED). Retries are **in-process** (schema has no next-retry-at column, so no DB-driven delayed re-claim) — task stays RUNNING during backoff. Test hook: `"forceFail": true` in payload fails deterministically. Verified: forceFail task → 3 attempts (backoff 1s, 2s confirmed in logs) → DEAD (retry_count=3) → TaskResultEvent(DEAD) on `dead-letter-queue` + full audit trail; normal task → DONE (1 attempt). Note: `FAILED` status now unused in Part 1 (outcomes are DONE or DEAD).

## Resume Notes (local dev environment — non-obvious)

- **Maven:** no `mvn` on PATH; use the generated wrapper `./mvnw`. (It resolves Maven 3.9.11 from the user's `~/.m2/wrapper` cache.) JDK is 21, but builds target Java 17 via `--release 17`.
- **Postgres host port is 5433, not 5432.** A native PostgreSQL already occupies the host's 5432, so the container is published on **5433** (container still 5432 internally — in-Docker networking for Phase 7 is unchanged). Worker local default URL: `jdbc:postgresql://127.0.0.1:5433/taskmesh`.
- **Redis host pinned to `127.0.0.1`** (IPv4) to hit the Docker Redis, not a native IPv6 `::1` listener.
- **Worker forces JVM `TimeZone=UTC`** in `main()` — host default `Asia/Calcutta` is rejected by Postgres; all timestamps stored UTC (`hibernate.jdbc.time_zone=UTC`).
- **`jackson-datatype-jsr310`** added to worker so the Kafka deserializer can read `Instant`.
- **`spring-kafka-test` removed** from producer (pulled heavyweight RocksDB/embedded-Kafka; not needed).
- **Disk space is tight** on `C:` — watch for "not enough space" during builds.
- **Run locally:** `docker compose up -d`, then `java -jar producer-service/target/*.jar` (8081) and `java -jar worker-service/target/*.jar`. Stop workers with `taskkill //F //PID <pid>`.
