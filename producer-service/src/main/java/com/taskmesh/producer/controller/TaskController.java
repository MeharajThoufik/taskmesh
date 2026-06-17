package com.taskmesh.producer.controller;

import com.taskmesh.producer.dto.TaskRequest;
import com.taskmesh.producer.dto.TaskResponse;
import com.taskmesh.producer.service.TaskPublisherService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Task intake REST API.
 *
 * <p>Part 1 exposes the write side (submit single / batch). The read side
 * (GET /api/tasks/{id} and GET /api/tasks?status=) reads persisted task state, which is
 * owned by the worker service's database — those endpoints are added once the persistence
 * layer is online (Phase 4+).
 */
@RestController
@RequestMapping("/api/tasks")
@Validated
public class TaskController {

    private final TaskPublisherService taskPublisherService;

    public TaskController(TaskPublisherService taskPublisherService) {
        this.taskPublisherService = taskPublisherService;
    }

    @PostMapping
    public ResponseEntity<TaskResponse> submit(@Valid @RequestBody TaskRequest request) {
        TaskResponse response = taskPublisherService.publish(request);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }

    @PostMapping("/batch")
    public ResponseEntity<List<TaskResponse>> submitBatch(
            @RequestBody @NotEmpty(message = "batch must not be empty") List<@Valid TaskRequest> requests) {
        List<TaskResponse> responses = taskPublisherService.publishBatch(requests);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(responses);
    }
}
