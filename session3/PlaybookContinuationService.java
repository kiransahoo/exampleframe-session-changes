package com.#exampleframe#.orchestrator.service;

import com.#exampleframe#.orchestrator.config.PlaybookProperties.PlaybookDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The missing half of playbook HITL: continuations.
 * <p>
 * {@code PlaybookExecutor} pauses a run when a step's child agent raises an approval gate -
 * the steps after the gate never ran and synthesis is withheld (deliberately, so an operator
 * is not shown a confident RCA built from partial evidence). The resume paths, however,
 * historically resumed only the CHILD CALL: the approved tool ran, the child's answer came
 * back, and the investigation ended there - no remaining steps, no synthesis. An operator who
 * approved a gate mid-playbook got a dead conversation ("it doesn't even go talk to oracle").
 * <p>
 * This service parks the executor's position at pause time, keyed by the gate's taskId - the
 * same key {@code InvestigationStateService} tracks the pending approval under - so the resume
 * paths ({@code /resume-child} on the UI door, {@code /a2a/resume} on the delegated door) can
 * hand the child's answer back to {@link PlaybookExecutor#resumeContinuation} and run the rest
 * of the playbook. Chained gates re-key the parked state to the new taskId.
 * <p>
 * Replica-local by design, like every pending map in this orchestrator (pending playbooks,
 * meta forwards, HITL records): one replica per cell is the deployment shape, and a lost
 * continuation degrades to today's behavior (the child's answer alone), never to an error.
 */
@Service
public class PlaybookContinuationService {

    private static final Logger logger = LoggerFactory.getLogger(PlaybookContinuationService.class);

    /** Approvals are human-paced: allow a coffee, not a lunch. */
    private static final Duration TTL = Duration.ofMinutes(30);

    /**
     * A paused playbook run: everything the executor needs to finish it once the gated
     * step's answer arrives.
     *
     * @param resumeStepIndex index in {@code playbook.steps()} of the first step that has
     *                        NOT run yet (the loop's position after the gated step/batch)
     */
    public record PlaybookContinuation(
            PlaybookDefinition playbook,
            Map<String, String> context,
            List<PlaybookExecutor.StepResult> results,
            int resumeStepIndex,
            String gatedStepId,
            String gatedAgent,
            String gatedTask,
            String threadId,
            String originalQuery,
            Instant createdAt
    ) {}

    private final ConcurrentHashMap<String, PlaybookContinuation> continuations = new ConcurrentHashMap<>();

    public void park(String taskId, PlaybookContinuation continuation) {
        if (taskId == null || continuation == null) {
            return;
        }
        continuations.put(taskId, continuation);
        logger.info("[playbook] Parked continuation for task {} - playbook '{}' will resume at step {} "
                        + "of {} once the gate is answered",
                taskId, continuation.playbook().name(), continuation.resumeStepIndex(),
                continuation.playbook().steps() != null ? continuation.playbook().steps().size() : 0);
    }

    /** Removes and returns the continuation for this gate, if present and fresh. */
    public PlaybookContinuation claim(String taskId) {
        if (taskId == null) {
            return null;
        }
        PlaybookContinuation c = continuations.remove(taskId);
        if (c == null) {
            return null;
        }
        if (Duration.between(c.createdAt(), Instant.now()).compareTo(TTL) > 0) {
            logger.info("[playbook] Continuation for task {} expired - resume returns the child's "
                    + "answer alone", taskId);
            return null;
        }
        return c;
    }

    /**
     * A chained gate replaced the pending approval's taskId; the parked run moves with it,
     * so answering the LAST gate still finishes the playbook.
     */
    public void rekey(String oldTaskId, String newTaskId) {
        if (oldTaskId == null || newTaskId == null) {
            return;
        }
        PlaybookContinuation c = continuations.remove(oldTaskId);
        if (c != null) {
            continuations.put(newTaskId, c);
            logger.info("[playbook] Continuation re-keyed {} -> {} (chained gate)", oldTaskId, newTaskId);
        }
    }

    @Scheduled(fixedDelay = 300_000)
    void evictStale() {
        Instant cutoff = Instant.now().minus(TTL);
        int before = continuations.size();
        continuations.entrySet().removeIf(e -> e.getValue().createdAt().isBefore(cutoff));
        int evicted = before - continuations.size();
        if (evicted > 0) {
            logger.info("[playbook] Evicted {} stale continuation(s), {} remaining",
                    evicted, continuations.size());
        }
    }
}
