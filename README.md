# TaskMesh

A **fault-tolerant distributed task execution engine** built with Java 17 and Spring Boot.
Tasks are submitted over REST, buffered through Kafka, and executed by a pool of worker
replicas that guarantee **exactly-once execution**, **no task loss on crash**, and
**no duplicate processing** across workers.

This is **Part 1** of a two-part project. Part 2 (FlowMesh) layers AI classification,
workflow orchestration, and a dashboard on top — without changing the worker or producer.

---

## What problem it solves

A payment/order-style processing pipeline where:

- Tasks must execute **exactly once** — no duplicate payment capture (idempotency).
- A worker crash must **never lose tasks** (fault tolerance + reclaim).
- A burst of submissions must not overwhelm downstream systems (Kafka buffering + backpressure).
- Multiple workers must **never process the same task simultaneously** (`FOR UPDATE SKIP LOCKED`).
- Permanently failing tasks must be isolated (dead-letter queue) without blocking healthy flow.

---

## Architecture

```
                          POST /api/tasks
                          POST /api/tasks/batch
                                 │
                                 ▼
                        ┌──────────────────┐
                        │ Producer Service │   acks=all, idempotent producer
                        │   (REST, 8081)   │
                        └────────┬─────────┘
                                 │ publish
                                 ▼
                       ┌─────────────────────┐
                       │  Kafka: task-events │  (4 partitions)
                       └──────────┬──────────┘
                                  │  one consumer group, partitions
                                  │  spread across replicas
            ┌─────────────────────┼─────────────────────┐
            ▼                     ▼                     ▼
   ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
   │   worker-1      │  │   worker-2      │  │   worker-3      │
   │                 │  │                 │  │                 │
   │ Consumer  → persist PENDING (Postgres)                    │
   │ Coordinator → claim batch (FOR UPDATE SKIP LOCKED, 10)    │
   │ Executor   → Redis NX idempotency → simulate work         │
   │            → DONE, or retry (1s,2s) → DEAD → DLQ          │
   │ Heartbeat  → liveness (10s) + dead-worker reclaim (30s)   │
   └───────┬─────────────────┬───────────────────┬────────────┘
           ▼                 ▼                   ▼
   ┌──────────────┐   ┌─────────────┐   ┌──────────────────────┐
   │ PostgreSQL   │   │   Redis     │   │ Kafka                │
   │ tasks        │   │ idempotency │   │ task-results         │
   │ worker_health│   │ locks (NX)  │   │ dead-letter-queue    │
   │ task_audit_log   └─────────────┘   └──────────────────────┘
   └──────────────┘
```

**Why this shape?** Kafka is the durable intake buffer; **PostgreSQL is the source of truth**
for task state. The consumer only *persists* tasks as `PENDING`; a separate scheduled
coordinator *claims and executes* them via `SKIP LOCKED`. That hand-off through the database
is what lets a crashed worker's in-flight tasks be reclaimed and re-run **without replaying
Kafka**.

---

## Tech stack

| Layer | Technology |
|---|---|
| Language / framework | Java 17, Spring Boot 3.2 |
| Messaging | Apache Kafka 3.x (Confluent 7.5) |
| Database | PostgreSQL 15 |
| Cache / idempotency | Redis 7 |
| Resilience | Resilience4j 2.x (retry + backoff) |
| Build | Maven (multi-module) |
| Runtime | Docker + Docker Compose |

Modules: **`common`** (shared events/enums/topics), **`producer-service`** (REST intake),
**`worker-service`** (execution engine).

---

## Core guarantees & how they're implemented

| Guarantee | Mechanism |
|---|---|
| Exactly-once execution | Redis `SET key NX` processing lock + `tasks.idempotency_key` UNIQUE |
| No double-processing across workers | `SELECT … FOR UPDATE SKIP LOCKED` batch claim |
| No task loss on crash (stayed down) | Heartbeat dead-detection (>60s) → reclaim RUNNING → PENDING |
| No task loss on crash (quick restart) | Startup self-recovery resets the worker's own orphaned RUNNING tasks |
| Backpressure under load | Bounded thread pool (10/50/500) with `CallerRunsPolicy` |
| Retry transient failures | Resilience4j exponential backoff (1s → 2s), 3 attempts |
| Isolate poison tasks | Dead-letter queue after retries exhausted |
| Full traceability | `task_audit_log` row per transition (CLAIMED/RETRY/DONE/DEAD/DLQ_ROUTED) |

---

## Running it

```bash
# 1. Build the service jars (the Dockerfiles copy these in)
./mvnw clean package

# 2. Build images + start the whole stack (infra + producer + 3 workers)
docker compose up -d --build

# 3. Check everything is up
docker compose ps
```

> **Host port note:** PostgreSQL is published on **5433** (not 5432) to avoid colliding with a
> native PostgreSQL commonly already on the host. In-Docker, services still talk to
> `postgres:5432`, `redis:6379`, and `kafka:29092`.

### Submit a task

```bash
curl -X POST http://localhost:8081/api/tasks \
  -H 'Content-Type: application/json' \
  -d '{"type":"PAYMENT_CAPTURE","payload":{"orderId":"ORD-1","amount":47000},"idempotencyKey":"ord-1"}'
```

### REST API (producer, port 8081)

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/tasks` | Submit one task |
| `POST` | `/api/tasks/batch` | Submit many tasks (load testing) |
| `GET`  | `/api/health` | Liveness |

Task types (simulated latency + failure rate): `PAYMENT_CAPTURE`, `FRAUD_CHECK`,
`NOTIFICATION`, `INVENTORY_RESERVE`, `INVOICE_GENERATE`. A `"forceFail": true` payload flag
makes a task fail deterministically (used to exercise the retry → DLQ path).

---

## Test results

### Integration test — fault tolerance (1,000 tasks, kill a worker mid-flight)

`scripts/` drives the scenario: submit 1,000 tasks across 3 replicas, then `docker kill`
worker-2 while it holds in-flight tasks.

| Result | Outcome |
|---|---|
| Tasks submitted | 1,000 |
| Reached terminal state (DONE) | **1,000 / 1,000 — no loss** |
| Duplicate rows | **0** (1,000 rows = 1,000 distinct idempotency keys) |
| worker-2 after kill | `INACTIVE` (dead-detected by survivors) |
| Tasks reclaimed & re-run elsewhere | 9 (claimed by worker-2, re-claimed by worker-1/3) |

Two recovery mechanisms were observed working together: a **Kafka consumer-group rebalance**
re-assigned the dead worker's partition (its un-consumed events were picked up by survivors),
and **dead-detection** (~60s) reclaimed its already-claimed `RUNNING` rows. Example audit
trail of a reclaimed task:

```
CLAIMED  worker-2  20:06:47
CLAIMED  worker-1  20:07:56   ← reclaimed ~69s later (60s threshold + detect cycle)
DONE     worker-1  20:07:57
```

### Load test — producer intake (Apache Bench, 10,000 requests @ concurrency 100)

```
Requests per second:    2729.00 [#/sec] (mean)
Time taken for tests:   3.664 seconds
Failed requests:        0 (HTTP 202 for all; 8 benign body-length variations)
Latency:  p50=36ms  p95=44ms  p99=53ms  max=70ms
```

> Apache Bench was run from a throwaway container on the Docker network
> (`ab` / `hey` / `wrk` were not installed on the host). The producer sustains
> **~2,700 task submissions/sec** on a single instance.

---

## Project layout

```
taskmesh/
├── docker-compose.yml      # infra + producer + 3 worker replicas
├── init.sql                # schema (tasks, worker_health, task_audit_log)
├── common/                 # shared events, enums, Kafka topic constants
├── producer-service/       # REST intake → publishes to task-events
├── worker-service/         # consume → claim (SKIP LOCKED) → execute → retry/DLQ
└── scripts/                # load + integration test helpers
```

---

## Part 2 (future, not in this repo yet)

New services only — `worker-service` and `producer-service` stay unchanged:
`ai-classifier-service`, `orchestrator-service`, `dashboard-service`, plus workflow tables.
The `tasks` table already carries `workflow_id` / `workflow_step`, and `TaskEvent` /
`TaskResultEvent` already carry the workflow fields, so Part 2 needs **zero migration** of
existing code.
