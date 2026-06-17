package com.taskmesh.worker.coordinator;

import com.taskmesh.common.enums.TaskStatus;
import com.taskmesh.worker.entity.Task;
import com.taskmesh.worker.executor.TaskExecutor;
import com.taskmesh.worker.repository.TaskRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.UUID;

/**
 * Polls for PENDING tasks and dispatches them to the worker thread pool.
 *
 * <p>Claiming and executing are split across two transactions on purpose. The claim runs inside
 * a {@link TransactionTemplate}: it locks a batch with {@code FOR UPDATE SKIP LOCKED}, flips the
 * rows to RUNNING, and <em>commits</em> — releasing the row locks — before anything executes.
 * Only then are tasks handed to the pool, each executing in its own transaction. This keeps row
 * locks held for microseconds (not for the duration of execution) and avoids an optimistic-lock
 * clash between the claim's RUNNING write and the executor's DONE write.
 */
@Component
@Slf4j
public class TaskCoordinator {

    private final TaskRepository taskRepository;
    private final TaskExecutor taskExecutor;
    private final ThreadPoolTaskExecutor workerThreadPool;
    private final TransactionTemplate transactionTemplate;
    private final String workerId;
    private final int batchSize;

    public TaskCoordinator(
            TaskRepository taskRepository,
            TaskExecutor taskExecutor,
            ThreadPoolTaskExecutor workerThreadPool,
            PlatformTransactionManager transactionManager,
            @Value("${worker.id:worker-local}") String workerId,
            @Value("${worker.batch-size:10}") int batchSize) {
        this.taskRepository = taskRepository;
        this.taskExecutor = taskExecutor;
        this.workerThreadPool = workerThreadPool;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.workerId = workerId;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${worker.poll-interval-ms:1000}")
    public void pollAndDispatch() {
        List<UUID> claimedIds = claimBatch();
        if (claimedIds.isEmpty()) {
            return;
        }
        log.info("Claimed {} task(s) as {}: {}", claimedIds.size(), workerId, claimedIds);
        for (UUID id : claimedIds) {
            workerThreadPool.execute(() -> taskExecutor.execute(id));
        }
    }

    /**
     * Lock + claim a batch in one short transaction, returning the claimed ids. The RUNNING
     * status and worker_id are flushed on commit, so no other worker can re-claim these rows.
     */
    private List<UUID> claimBatch() {
        return transactionTemplate.execute(status -> {
            List<Task> tasks = taskRepository.claimPendingTasks(batchSize);
            for (Task task : tasks) {
                task.setStatus(TaskStatus.RUNNING);
                task.setWorkerId(workerId);
            }
            return tasks.stream().map(Task::getId).toList();
        });
    }
}
