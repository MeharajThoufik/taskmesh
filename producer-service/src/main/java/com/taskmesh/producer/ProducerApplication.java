package com.taskmesh.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Producer service entry point: the task intake REST API.
 *
 * <p>Accepts task submissions over HTTP and publishes them to the {@code task-events}
 * Kafka topic. It is intentionally stateless and does not touch PostgreSQL — durable
 * persistence is owned by the worker service.
 */
@SpringBootApplication
public class ProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }
}
