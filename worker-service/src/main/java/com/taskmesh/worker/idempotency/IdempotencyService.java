package com.taskmesh.worker.idempotency;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

/**
 * Redis-backed idempotency guard (see CLAUDE.md §7.3).
 *
 * <p>A {@code SET key value NX} acquires a short-lived processing lock so that — even if the
 * same task is delivered twice or two workers race — exactly one execution proceeds. On success
 * the key is flipped to a long-lived {@code completed} marker; on failure it is released so a
 * retry can re-acquire it.
 */
@Service
@Slf4j
public class IdempotencyService {

    private static final String KEY_PREFIX = "task:";
    private static final Duration PROCESSING_TTL = Duration.ofSeconds(300);
    private static final Duration COMPLETED_TTL = Duration.ofHours(24);

    private final RedisTemplate<String, String> redisTemplate;

    public IdempotencyService(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /** @return true if this caller won the lock and should execute; false if already taken. */
    public boolean acquireIdempotencyLock(String idempotencyKey) {
        String redisKey = KEY_PREFIX + idempotencyKey;
        Boolean acquired = redisTemplate.opsForValue()
                .setIfAbsent(redisKey, "processing", PROCESSING_TTL);
        return Boolean.TRUE.equals(acquired);
    }

    /** Mark the task as completed so future duplicates are skipped for a long window. */
    public void markCompleted(String idempotencyKey) {
        String redisKey = KEY_PREFIX + idempotencyKey;
        redisTemplate.opsForValue().set(redisKey, "completed", COMPLETED_TTL);
    }

    /** Release the processing lock after a failure so the task can be retried. */
    public void releaseLock(String idempotencyKey) {
        redisTemplate.delete(KEY_PREFIX + idempotencyKey);
    }
}
