package com.taskmesh.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.TimeZone;

/**
 * Worker service entry point: the task execution engine.
 *
 * <p>Consumes {@link com.taskmesh.common.events.TaskEvent}s from Kafka, persists them to
 * PostgreSQL, and a scheduled coordinator claims and executes them. {@code @EnableScheduling}
 * drives the SKIP LOCKED claim loop (and, from Phase 5, the heartbeat).
 */
@SpringBootApplication
@EnableScheduling
public class WorkerApplication {

    public static void main(String[] args) {
        // Run the JVM in UTC so the pgjdbc driver advertises a timezone PostgreSQL accepts
        // (the host JVM default may be a legacy alias like "Asia/Calcutta") and so all stored
        // timestamps are unambiguously UTC, matching hibernate.jdbc.time_zone=UTC.
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SpringApplication.run(WorkerApplication.class, args);
    }
}
