package com.taskmesh.producer.dto;

import com.taskmesh.common.enums.TaskType;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Map;

/**
 * Incoming task submission body for {@code POST /api/tasks}.
 *
 * <p>{@code idempotencyKey} is optional: supply a stable key to make repeated submissions
 * collapse to a single execution; if omitted, the producer generates one. {@code priority}
 * and {@code maxRetries} fall back to server defaults when null.
 */
@Getter
@Setter
@ToString
public class TaskRequest {

    @NotNull(message = "type is required")
    private TaskType type;

    @NotEmpty(message = "payload must not be empty")
    private Map<String, Object> payload;

    /** Optional client-supplied idempotency key; generated when absent. */
    private String idempotencyKey;

    /** Optional scheduling priority (higher first). Defaults to 5. */
    private Integer priority;

    /** Optional max attempts before dead-lettering. Defaults to 3. */
    private Integer maxRetries;
}
