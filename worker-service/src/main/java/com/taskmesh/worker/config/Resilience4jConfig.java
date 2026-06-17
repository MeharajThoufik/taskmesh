package com.taskmesh.worker.config;

import com.taskmesh.worker.executor.TaskExecutionException;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Resilience4j retry policy for task execution (CLAUDE.md §7.5).
 *
 * <p>Exponential backoff (default 1s, then 2s) over up to {@code maxAttempts} tries, retrying only
 * on {@link TaskExecutionException}. When all attempts are exhausted the last exception propagates,
 * and {@code TaskExecutor} routes the task to the dead-letter queue.
 */
@Configuration
public class Resilience4jConfig {

    @Bean
    public Retry taskRetry(
            @Value("${worker.retry.max-attempts:3}") int maxAttempts,
            @Value("${worker.retry.initial-backoff-ms:1000}") long initialBackoffMs,
            @Value("${worker.retry.backoff-multiplier:2.0}") double backoffMultiplier) {
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(maxAttempts)
                .intervalFunction(IntervalFunction.ofExponentialBackoff(initialBackoffMs, backoffMultiplier))
                .retryExceptions(TaskExecutionException.class)
                .build();
        return RetryRegistry.of(config).retry("taskExecution");
    }
}
