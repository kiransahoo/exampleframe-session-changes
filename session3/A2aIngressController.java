package com.#exampleframe#.orchestrator.api.controller;

import com.#exampleframe#.orchestrator.service.DelegationExecutionService;
import org.springframework.security.access.prepost.PreAuthorize;
import com.#exampleframe#.orchestrator.config.OrchestratorProperties;
import com.#exampleframe#.orchestrator.incident.InvestigationRunner;
import com.#exampleframe#.orchestrator.incident.OrchestratorInvestigationRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.#exampleframe#.orchestrator.service.HitlAwareA2aClient;
import com.#exampleframe#.orchestrator.service.InvestigationStateService;
import com.#exampleframe#.orchestrator.service.PlaybookExecutor;
import com.#exampleframe#.orchestrator.service.PlaybookContinuationService;
import com.#exampleframe#.orchestrator.service.PlaybookResolver;
import com.#exampleframe#.orchestrator.thread.history.model.Message;
import com.#exampleframe#.orchestrator.thread.history.model.ThreadKey;
import com.#exampleframe#.orchestrator.thread.history.store.ThreadStore;
import com.#exampleframe#.orchestrator.tools.AgentInvocationService;
import com.#exampleframe#.orchestrator.exception.ChildAgentHitlException;
import com.#exampleframe#.orchestrator.exception.ChildAgentHitlRuntimeException;
import io.a2a.spec.AgentCard;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * A2A ingress for this orchestrator - makes a domain cell "just another A2A agent"
 * so the meta-orchestrator can delegate to it with the exact client it uses for
 * every other agent ({@code POST {cellUrl}/a2a/message}).
 * <p>
 * Wire contract mirrors the agents' {@code A2aServerController}: request carries the
 * task in {@code params.message.parts[0].text} (or {@code input}/{@code message});
 * response is {@code {"jsonrpc":"2.0","id":..,"result":{"status":"completed",
 * "taskId":..,"artifacts":[{"parts":[{"type":"text","text":..}]}]}}} or a JSON-RPC
 * error object.
 * <p>
 * Optional params a #ExampleFrame# meta additionally sends (plain agents never read them):
 * {@code params.contextId} - the meta's conversation thread; the cell derives a STABLE
 * local threadId from it so checkpointed history and the evidence board survive across
 * delegated turns. {@code params.rawTask} / {@code params.taskParams} - the bare task
 * and its structured parameters, so the deterministic playbook pre-route (the same
 * engine the cell's own UI door tries first) matches on clean text instead of parsing
 * the labeled delegation envelope back apart. First turn of a conversation, strong
 * match, all required params known -> the playbook runs; anything less -> ReAct,
 * exactly as before.
 * <p>
 * Security: this path is service-to-service only. {@code SecurityConfig} requires
 * ROLE_SERVICE (X-Service-Token) - never a user JWT - and it is never reachable
 * without the token when the token is configured.
 * <p>
 * HITL across the federation boundary: when a child agent inside this cell needs human
 * approval while serving a meta request, the ingress answers exactly like an agent would -
 * {@code status: pending_approval} with the child's {@code taskId} and {@code pendingTools} -
 * so the meta shows the same approval card it shows for a direct agent. The meta then calls
 * {@code POST /a2a/resume} here with that {@code taskId} and the tool feedbacks; this cell
 * resumes its child (at the instance URL that raised the gate), records the outcome on its
 * evidence board, and returns {@code completed} (or another {@code pending_approval} for a
 * chained gate). Incident investigations are read-only and never hit this.
 *
 * @author kiransahoo
 */
@RestController
@RequestMapping("/a2a")
public class A2aIngressController {

    private static final Logger logger = LoggerFactory.getLogger(A2aIngressController.class);

    /** contextIds usable verbatim as a thread key; anything else is hashed. */
    private static final Pattern SAFE_CONTEXT_ID = Pattern.compile("[A-Za-z0-9._-]{1,64}");

    private final OrchestratorInvestigationRunner runner;
    private final OrchestratorProperties properties;
    private final @Nullable HitlAwareA2aClient hitlAwareA2aClient;
    private final @Nullable InvestigationStateService investigationStateService;
    private final @Nullable AgentInvocationService agentInvocationService;
    private final @Nullable PlaybookResolver playbookResolver;
    private final @Nullable PlaybookExecutor playbookExecutor;
    private final com.alibaba.cloud.ai.graph.checkpoint.@Nullable BaseCheckpointSaver checkpointSaver;
    private final @Nullable ThreadStore threadStore;
    private final com.#exampleframe#.orchestrator.observation.cost.@Nullable SessionCostAccumulator sessionCostAccumulator;

    /**
     * Derived threadIds with a request currently executing. A caller-supplied contextId
     * makes thread keys stable, so a meta retry (or a concurrent delegation on the same
     * conversation) could otherwise run two graph executions against the same checkpoint
     * thread and corrupt its history - reject the overlap instead.
     * <p>
     * REPLICA-LOCAL: this guards one JVM. With multiple cell replicas behind one URL,
     * two requests for the same conversation can land on different replicas and race the
     * shared checkpoint store anyway - horizontal scaling needs sticky per-thread routing
     * or a distributed lease. Current deployments run one replica per cell.
     */
    private final Set<String> inFlight = ConcurrentHashMap.newKeySet();

    public A2aIngressController(OrchestratorInvestigationRunner runner, OrchestratorProperties properties,
                                @Autowired(required = false) @Nullable HitlAwareA2aClient hitlAwareA2aClient,
                                @Autowired(required = false) @Nullable InvestigationStateService investigationStateService,
                                @Autowired(required = false) @Nullable AgentInvocationService agentInvocationService,
                                @Autowired(required = false) @Nullable PlaybookResolver playbookResolver,
                                @Autowired(required = false) @Nullable PlaybookExecutor playbookExecutor,
                                @Autowired(required = false)
                                com.alibaba.cloud.ai.graph.checkpoint.@Nullable BaseCheckpointSaver checkpointSaver,
                                @Autowired(required = false) @Nullable ThreadStore threadStore,
                                @Autowired(required = false)
                                com.#exampleframe#.orchestrator.observation.cost.@Nullable SessionCostAccumulator sessionCostAccumulator) {
        this.runner = runner;
        this.properties = properties;
        this.hitlAwareA2aClient = hitlAwareA2aClient;
        this.investigationStateService = investigationStateService;
        this.agentInvocationService = agentInvocationService;
        this.playbookResolver = playbookResolver;
        this.playbookExecutor = playbookExecutor;
        this.checkpointSaver = checkpointSaver;
        this.threadStore = threadStore;
        this.sessionCostAccumulator = sessionCostAccumulator;
    }

    /** Thread-history identity for delegated conversations - segregated from real users' thread lists. */
    private static final String A2A_APP_NAME = "orchestrator_agent";
    private static final String A2A_USER_ID = "a2a-ingress";

    @PreAuthorize("@authz.service()")
    @PostMapping(value = {"", "/message", "/message/isolated"},
            consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> handleMessage(@RequestBody Map<String, Object> request) {
        String requestId = extractRequestId(request);
        String input = extractInput(request);
        if (input == null || input.isBlank()) {
            return ResponseEntity.ok(error(requestId, -32602, "No input text found in A2A request"));
        }
        Map<String, Object> params = paramsOf(request);
        String contextId = stringParam(params, "contextId");
        String rawTask = stringParam(params, "rawTask");
        Map<String, String> taskParams = taskParamsOf(params);

        // Context retention: a caller-supplied contextId (the meta's conversation thread)
        // maps to a STABLE cell-side threadId, so the checkpoint saver restores this
        // conversation's history and the evidence board persists across delegated turns.
        // No contextId = fresh random thread per request (previous behavior).
        String threadId = deriveThreadId(contextId);
        // contextId is caller-supplied text: sanitize before logging (a CR/LF inside it
        // would forge log lines), and cap it - the derived threadId is the real identity.
        logger.info("=== A2A INGRESS === id: {} threadId: {}{} ({} chars{})", sanitizeForLog(requestId), threadId,
                contextId != null ? " (contextId " + sanitizeForLog(contextId) + ")" : "", input.length(),
                rawTask != null ? ", rawTask " + rawTask.length() + " chars" : "");

        if (!inFlight.add(threadId)) {
            return ResponseEntity.ok(error(requestId, -32003,
                    "An investigation for this conversation is already running on this cell (threadId "
                            + threadId + "). Retry after it completes."));
        }
        // NOTE deliberately NOT binding SessionCostAccumulator's ThreadLocal here: the ReAct
        // path's graph model calls run on this servlet thread and are recorded by
        // CostTrackingInterceptor - a request-wide binding would make the ObservationFilter
        // record them a SECOND time. The playbook path binds inside PlaybookGateway/
        // PlaybookExecutor, scoped to exactly the calls only the filter can see.
        try {
            // First-turn gate: prior state for a stable thread means this is a follow-up
            // turn - it continues in ReAct with restored context, never a fresh playbook.
            // Three layers: in-memory investigation state (fast path), the checkpoint
            // saver (durable for ReAct turns), and the thread-history store (durable for
            // playbook turns, which run outside the graph and write no checkpoint - the
            // playbook path records its turn there below). Together they survive cell
            // restarts and the 2h state TTL.
            boolean followUpTurn = contextId != null
                    && ((investigationStateService != null && investigationStateService.hasState(threadId))
                        || hasCheckpointedHistory(threadId)
                        || hasPersistedTurn(threadId));
            if (investigationStateService != null) {
                investigationStateService.initThread(threadId);
            }

            String playbookAnswer;
            try {
                playbookAnswer = tryPlaybook(rawTask != null ? rawTask : input,
                        taskParams, threadId, followUpTurn);
            } catch (com.#exampleframe#.orchestrator.exception.CellInputRequiredException e) {
                // Same relay shape as pending_approval: the forwarding meta prompts the user
                // for the named keys and re-forwards the original request with them merged.
                Map<String, Object> result = new HashMap<>();
                result.put("status", "input_required");
                result.put("taskId", threadId);
                result.put("playbookId", e.getPlaybookId());
                result.put("playbookName", e.getPlaybookName());
                result.put("missingKeys", e.getMissingKeys());
                List<Map<String, String>> missingParams = new ArrayList<>();
                e.getMissingPrompts().forEach((k, v) ->
                        missingParams.add(Map.of("key", k, "prompt", v)));
                result.put("missingParams", missingParams);
                // Defense in depth: a client that ignores the status and extracts output
                // renders a readable sentence, never "Completed but no output extracted".
                result.put("output", "This playbook needs values for: "
                        + String.join(", ", e.getMissingKeys())
                        + ". Re-send the request with them as taskParams.");
                Map<String, Object> response = new HashMap<>();
                response.put("jsonrpc", "2.0");
                response.put("id", requestId);
                response.put("result", result);
                return ResponseEntity.ok(response);
            }
            if (playbookAnswer != null) {
                return ResponseEntity.ok(success(requestId, threadId, playbookAnswer,
                        threadId, contextId == null));
            }

            String output = runner.investigate(new InvestigationRunner.InvestigationTask(input, threadId, null));
            return ResponseEntity.ok(success(requestId, threadId, output, threadId, contextId == null));
        } catch (Exception e) {
            // A child agent in this cell needs approval. On the ReAct path the exception
            // arrives wrapped (ChildAgentHitlRuntimeException, possibly nested inside the
            // graph runner's own wrappers), on the playbook path it arrives raw - walk the
            // cause chain for both. Remember which child (and at which instance URL) so
            // /a2a/resume can go back to it, and answer like an agent would.
            ChildAgentHitlException hitl = findHitl(e);
            if (hitl != null) {
                return ResponseEntity.ok(pendingApproval(requestId, threadId, hitl));
            }
            logger.error("A2A ingress failed (id {}): {}", sanitizeForLog(requestId),
                    sanitizeForLog(String.valueOf(e.getMessage())), e);
            return ResponseEntity.ok(error(requestId, -32000, e.getMessage() != null ? e.getMessage() : "Unknown error"));
        } finally {
            com.#exampleframe#.orchestrator.observation.cost.SessionCostAccumulator.clearCurrentThreadId();
            inFlight.remove(threadId);
        }
    }

    /**
     * Deterministic playbook pre-route for the delegated path - the same engine the UI
     * door tries first. Runs only on the FIRST turn of a conversation, only on a strong
     * match, and only when every required parameter is already known (task text, caller
     * {@code taskParams}, or app-context); anything less falls back to ReAct. Never
     * touches the gateway's interactive pending-prompt state.
     *
     * @return the synthesized report, or null when the request should go to ReAct
     */
    private @Nullable String tryPlaybook(String matchText, Map<String, String> taskParams,
                                         String threadId, boolean followUpTurn) {
        if (playbookResolver == null || playbookExecutor == null || investigationStateService == null) {
            return null;
        }
        String forwardedPlaybookId = taskParams.get("playbookId");
        if (forwardedPlaybookId != null && forwardedPlaybookId.isBlank()) {
            forwardedPlaybookId = null;
        }
        // A deliberate deterministic forward - the meta's playbook door pins an id for an
        // explicit user reference, or sends the bare marker for its scored matches (the cell
        // routes the text itself then). Either way the first-turn gate must not demote the
        // turn to ReAct just because this conversation touched the cell before: the CALLER
        // already judged this message to be a fresh playbook-shaped request, exactly the
        // per-message judgement the direct door's gateway applies to its own traffic.
        boolean deterministicForward = forwardedPlaybookId != null
                || "true".equalsIgnoreCase(taskParams.get(
                        com.#exampleframe#.orchestrator.federation.MetaPlaybookRouting.DETERMINISTIC_FORWARD_PARAM));
        if (followUpTurn) {
            if (!deterministicForward) {
                logger.info("A2A thread {} is a follow-up turn - continuing in ReAct with restored context", threadId);
                return null;
            }
            logger.info("A2A thread {} is a follow-up turn but the caller made a deterministic forward ({}) - honoring it",
                    threadId, forwardedPlaybookId != null
                            ? "playbookId '" + sanitizeForLog(forwardedPlaybookId) + "'" : "cell routes the text");
        }
        // A forwarded id SELECTS the playbook - text matching must not be able to pick a
        // different one (or miss on re-phrased text) after the caller already resolved it.
        // The resolve runs LLM calls (matcher, param extraction) that only the observation
        // filter can record - bound to the conversation thread so their spend lands in this
        // turn's costData instead of nowhere (or, worse, a stale binding another request
        // leaked on this pooled thread). Never seen by the graph interceptor: no double count.
        final String pinnedId = forwardedPlaybookId;
        PlaybookResolver.Resolution resolution =
                com.#exampleframe#.orchestrator.observation.cost.SessionCostAccumulator.withThreadId(threadId,
                        () -> pinnedId != null
                                ? playbookResolver.resolveById(pinnedId, matchText, taskParams).orElse(null)
                                : playbookResolver.resolve(matchText, taskParams).orElse(null));
        if (resolution == null) {
            if (forwardedPlaybookId != null) {
                logger.warn("Caller-forwarded playbookId '{}' is unknown or not runnable on this cell - deferring to ReAct",
                        sanitizeForLog(forwardedPlaybookId));
            }
            return null;
        }
        if (resolution.softMatch()) {
            logger.info("Playbook '{}' soft-matched on the delegated path - non-interactive, deferring to ReAct",
                    resolution.playbookId());
            return null;
        }
        if (!resolution.missingRequiredKeys().isEmpty()) {
            // Only a caller that DECLARED it understands input_required may receive it: an
            // older meta (and any pin-only third party - the pin alone sets the marker)
            // treats the unknown status as completed and renders "no output" garbage, which
            // is strictly worse than the ReAct fallback. The capability param is sent only
            // by metas carrying the relay loop.
            boolean acceptsInputRequired = "true".equalsIgnoreCase(taskParams.get(
                    com.#exampleframe#.orchestrator.federation.MetaPlaybookRouting.ACCEPTS_INPUT_REQUIRED_PARAM));
            if (deterministicForward && acceptsInputRequired) {
                // The forwarding meta relays the question to the user and re-forwards the
                // original request with the answer merged - the same loop pending_approval
                // already rides. Deferring to ReAct here silently degrades the investigation
                // instead (one-agent answer for a question the direct door runs as a full
                // playbook after prompting). The cell's own prompt wording travels too: the
                // meta's param definitions may not know a cell-only key.
                logger.info("Playbook '{}' missing required params {} on the delegated path - "
                                + "returning input_required to the forwarding meta",
                        resolution.playbookId(), resolution.missingRequiredKeys());
                java.util.Map<String, String> prompts = new LinkedHashMap<>();
                for (var missing : resolution.missingParams()) {
                    if (!missing.optional() && missing.prompt() != null && !missing.prompt().isBlank()) {
                        prompts.put(missing.key(), missing.prompt());
                    }
                }
                throw new com.#exampleframe#.orchestrator.exception.CellInputRequiredException(
                        properties.agent() != null ? cardName() : "cell", threadId,
                        resolution.playbookId(), resolution.definition().name(),
                        resolution.missingRequiredKeys(), prompts);
            }
            // A plain A2A caller (an agent, a third party, an older meta) has no user to
            // relay a prompt to: the ReAct fallback remains the best available answer.
            logger.info("Playbook '{}' missing required params {} on the delegated path - deferring to ReAct",
                    resolution.playbookId(), resolution.missingRequiredKeys());
            return null;
        }
        logger.info("Playbook matched: {} - deterministic execution on the delegated path (threadId={})",
                resolution.playbookId(), threadId);
        investigationStateService.recordUserMessage(threadId, matchText);
        InvestigationStateService.CURRENT_THREAD_ID.set(threadId);
        investigationStateService.bindCurrentThread(threadId);
        try {
            String report = playbookExecutor.executeBlocking(
                    resolution.definition(), resolution.params(), threadId, matchText);
            // Durable first-turn marker: playbooks run outside the agent graph and write
            // no checkpoint, so persist this turn to the thread-history store - the next
            // message on this conversation gates to ReAct even after a restart, and the
            // delegated conversation gets the same recorded history as the UI door's.
            persistPlaybookTurn(threadId, matchText, report);
            return report;
        } finally {
            InvestigationStateService.CURRENT_THREAD_ID.remove();
            investigationStateService.unbindCurrentThread(threadId);
        }
    }

    /** Records a completed playbook turn in the thread-history store. Failure never fails the answer. */
    private void persistPlaybookTurn(String threadId, String userText, String report) {
        if (threadStore == null) {
            return;
        }
        try {
            ThreadKey key = ThreadKey.of(A2A_APP_NAME, A2A_USER_ID, threadId);
            threadStore.getOrCreateThread(key);
            threadStore.appendMessage(key, new Message(UUID.randomUUID().toString(), A2A_APP_NAME,
                    threadId, Message.MessageRole.USER, userText, Instant.now(), Map.of()));
            threadStore.appendMessage(key, new Message(UUID.randomUUID().toString(), A2A_APP_NAME,
                    threadId, Message.MessageRole.ASSISTANT, report, Instant.now(), Map.of()));
        } catch (Exception e) {
            logger.warn("Failed to persist playbook turn for thread {}: {}", threadId,
                    sanitizeForLog(String.valueOf(e.getMessage())));
        }
    }

    /** Durable playbook-turn marker in the thread-history store (playbooks write no checkpoint). */
    private boolean hasPersistedTurn(String threadId) {
        if (threadStore == null) {
            return false;
        }
        try {
            return threadStore.threadExists(ThreadKey.of(A2A_APP_NAME, A2A_USER_ID, threadId));
        } catch (Exception e) {
            logger.warn("Thread-history lookup for thread {} failed ({}) - ignoring this gate layer",
                    threadId, sanitizeForLog(String.valueOf(e.getMessage())));
            return false;
        }
    }

    /**
     * Stable thread key for a caller conversation; random when the caller sent none.
     * <p>
     * TRUST BOUNDARY: the contextId is chosen by the caller, and this endpoint is gated
     * only by the federation-wide service token - so any token holder can attach to any
     * conversation's cell-side thread by replaying its contextId. That is not new
     * capability (the same token already allows arbitrary investigations and HITL resume
     * attempts against this cell); hashing here is key hygiene, NOT authorization. Real
     * caller-scoped isolation needs per-caller service credentials - tracked as a
     * federation roadmap item, deliberately not solved with the forgeable
     * X-On-Behalf-Of header (absent on worker threads, it would also break retention).
     */
    private String deriveThreadId(@Nullable String contextId) {
        if (contextId == null || contextId.isBlank()) {
            return "a2a-" + UUID.randomUUID();
        }
        // cellName in the key: the same meta conversation delegates to several cells with
        // the same contextId - if cells ever share checkpoint storage, identical keys
        // would cross-bleed history between domains.
        String cell = cellName == null || cellName.isBlank() ? "" : cellName.trim() + "-";
        String key = SAFE_CONTEXT_ID.matcher(contextId).matches() ? contextId : sha256Hex(contextId).substring(0, 16);
        return "a2a-" + cell + key;
    }

    /** Externally sourced text made safe for a single log line: CR/LF/tab stripped, length capped. */
    private static String sanitizeForLog(@Nullable String value) {
        if (value == null) {
            return "null";
        }
        String cleaned = value.replaceAll("[\\r\\n\\t]", " ");
        return cleaned.length() > 80 ? cleaned.substring(0, 77) + "..." : cleaned;
    }

    /**
     * Durable side of the first-turn gate: checkpointed graph history for this thread.
     * A saver failure only degrades the gate to the in-memory check (logged, never fatal).
     */
    private boolean hasCheckpointedHistory(String threadId) {
        if (checkpointSaver == null) {
            return false;
        }
        try {
            return checkpointSaver.get(com.alibaba.cloud.ai.graph.RunnableConfig.builder()
                    .threadId(threadId).build()).isPresent();
        } catch (Exception e) {
            logger.warn("Checkpoint lookup for thread {} failed ({}) - treating as first turn",
                    threadId, e.getMessage());
            return false;
        }
    }

    private static String sha256Hex(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            StringBuilder sb = new StringBuilder();
            for (byte b : digest.digest(value.getBytes(StandardCharsets.UTF_8))) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            // SHA-256 is mandatory on every JVM; fall back to something stable anyway
            return Integer.toHexString(value.hashCode()) + "00000000000000000000000000000000";
        }
    }

    /** The HITL exception anywhere in the cause chain (raw or runtime-wrapped), or null. */
    private static @Nullable ChildAgentHitlException findHitl(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof ChildAgentHitlException hitl) {
                return hitl;
            }
            if (c instanceof ChildAgentHitlRuntimeException wrapped) {
                return wrapped.getHitlException();
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> paramsOf(Map<String, Object> request) {
        return request.get("params") instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
    }

    private static @Nullable String stringParam(Map<String, Object> params, String key) {
        Object v = params.get(key);
        return v instanceof String s && !s.isBlank() ? s : null;
    }

    /** Structured task params from the wire - string values only, blanks dropped. */
    private static Map<String, String> taskParamsOf(Map<String, Object> params) {
        Object v = params.get("taskParams");
        if (!(v instanceof Map<?, ?> m)) {
            return Map.of();
        }
        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<?, ?> e : m.entrySet()) {
            if (e.getKey() != null && e.getValue() instanceof String s && !s.isBlank()) {
                out.put(e.getKey().toString(), s);
            }
        }
        return out;
    }

    /**
     * Meta -> cell HITL resume. Same wire contract the meta uses with agents:
     * {@code {"method":"task/resume","params":{"taskId":..,"toolFeedbacks":[..]}}}. The taskId
     * is the child agent's task from the earlier {@code pending_approval}; the cell resumes that
     * child at the instance URL that raised the gate.
     */
    @PreAuthorize("@authz.service()")
    @PostMapping(value = "/resume", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @SuppressWarnings("unchecked")
    public ResponseEntity<Map<String, Object>> resume(@RequestBody Map<String, Object> request) {
        String requestId = extractRequestId(request);
        Map<String, Object> params = request.get("params") instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
        String taskId = params.get("taskId") != null ? String.valueOf(params.get("taskId")) : null;
        List<Map<String, Object>> feedbacks = params.get("toolFeedbacks") instanceof List<?> l
                ? (List<Map<String, Object>>) l : List.of();
        if (taskId == null || taskId.isBlank()) {
            return ResponseEntity.ok(error(requestId, -32602, "Missing taskId"));
        }
        if (hitlAwareA2aClient == null || investigationStateService == null) {
            return ResponseEntity.ok(error(requestId, -32000, "HITL resume not available on this cell"));
        }
        String childAgent = investigationStateService.getChildAgentKeyForTask(taskId);
        if (childAgent == null) {
            return ResponseEntity.ok(error(requestId, -32001, "No pending approval for taskId " + taskId
                    + " in this cell (already resumed, or the cell restarted)"));
        }
        String ownerDenied = investigationStateService.checkHitlOwner(taskId);
        if (ownerDenied != null) {
            return ResponseEntity.ok(error(requestId, -32002, ownerDenied));
        }
        // The service token is shared by every agent in every domain, so possession of it is not
        // authority over this cell's children: re-check domain access before resuming.
        if (delegationExecutionService != null) {
            String denied = delegationExecutionService.checkDomainAccess(childAgent, taskId);
            if (denied != null) {
                return ResponseEntity.ok(error(requestId, -32001, denied));
            }
        }
        // Atomically CLAIM (consume) the pending approval: no concurrent resume and no replay
        // of this taskId can act on it after this point.
        InvestigationStateService.ClaimedHitl claimed = investigationStateService.claimPendingHitl(taskId);
        if (claimed == null) {
            return ResponseEntity.ok(error(requestId, -32001, "No pending approval for taskId " + taskId
                    + " in this cell (already resumed, or the cell restarted)"));
        }
        String childUrl = claimed.childBaseUrl();
        if (childUrl == null && agentInvocationService != null) {
            childUrl = agentInvocationService.getAgentBaseUrl(childAgent);
        }
        if (childUrl == null) {
            investigationStateService.restorePendingHitl(taskId, claimed);
            return ResponseEntity.ok(error(requestId, -32000, "Cannot resolve URL for child agent " + childAgent));
        }
        String cellThread = claimed.threadId();
        logger.info("=== A2A INGRESS RESUME === id: {} taskId: {} child: {} @ {} thread: {} feedbacks: {}",
                sanitizeForLog(requestId), sanitizeForLog(taskId), sanitizeForLog(childAgent),
                sanitizeForLog(childUrl), cellThread, feedbacks.size());
        try {
            hitlAwareA2aClient.setOrchestratorThreadId(cellThread);
            String output;
            try {
                output = hitlAwareA2aClient.resumeAgent(childAgent, childUrl, taskId, feedbacks,
                        agentInvocationService != null
                                ? agentInvocationService.configuredTimeoutSeconds(childAgent) : null);
            } finally {
                hitlAwareA2aClient.clearOrchestratorThreadId();
            }
            investigationStateService.resolveClaimedHitl(taskId, claimed, output);
            // The gate paused a PLAYBOOK step: run the remaining steps and synthesise, so the
            // forwarding meta receives the full investigation, not the child's answer alone.
            if (playbookContinuationService != null && playbookExecutor != null) {
                var continuation = playbookContinuationService.claim(taskId);
                if (continuation != null) {
                    try {
                        String report = playbookExecutor.resumeContinuation(continuation, output);
                        persistPlaybookTurn(continuation.threadId(), continuation.originalQuery(), report);
                        return ResponseEntity.ok(success(requestId,
                                cellThread != null ? cellThread : taskId, report, cellThread, false));
                    } catch (ChildAgentHitlException next) {
                        // A later step raised its own gate; the executor stored its pending
                        // record and re-parked the continuation under the new taskId.
                        return ResponseEntity.ok(pendingApproval(requestId, cellThread, next, false));
                    } catch (Exception ex) {
                        logger.warn("Playbook continuation after ingress resume failed - returning "
                                + "the child's answer alone: {}", ex.getMessage(), ex);
                    }
                }
            }
            // Cost travels only when the real conversation thread is known - a HITL task id
            // is not a session key and must not be looked up as one.
            return ResponseEntity.ok(success(requestId, cellThread != null ? cellThread : taskId, output,
                    cellThread, false));
        } catch (ChildAgentHitlException chained) {
            // The answered tool ran; the child raised the NEXT gate. Replace the consumed
            // record with the new pending task, preserving the original owner and thread.
            investigationStateService.chainClaimedHitl(
                    claimed, chained.getTaskId(), chained.getAgentName(), chained.getAgentBaseUrl());
            if (playbookContinuationService != null) {
                playbookContinuationService.rekey(taskId, chained.getTaskId());
            }
            return ResponseEntity.ok(pendingApproval(requestId, cellThread, chained, false));
        } catch (Exception e) {
            // Delivery failed: put the approval back so the meta can retry it.
            investigationStateService.restorePendingHitl(taskId, claimed);
            logger.error("A2A ingress resume failed (id {}): {}", sanitizeForLog(requestId),
                    sanitizeForLog(String.valueOf(e.getMessage())), e);
            return ResponseEntity.ok(error(requestId, -32000, e.getMessage() != null ? e.getMessage() : "Unknown error"));
        }
    }

    private Map<String, Object> pendingApproval(String requestId, @Nullable String cellThread,
                                                ChildAgentHitlException e) {
        return pendingApproval(requestId, cellThread, e, true);
    }

    /**
     * Build the agent-style pending_approval result; {@code store} records the pending child
     * (the chained-resume path passes false - chainClaimedHitl already registered it with the
     * original owner preserved).
     */
    private Map<String, Object> pendingApproval(String requestId, @Nullable String cellThread,
                                                ChildAgentHitlException e, boolean store) {
        if (store && investigationStateService != null) {
            investigationStateService.storePendingHitl(e.getTaskId(), cellThread, e.getAgentName(), e.getAgentBaseUrl());
        }
        logger.info("A2A ingress (id {}): child '{}' needs approval (taskId {}, {} tool(s)) - returning pending_approval",
                sanitizeForLog(requestId), sanitizeForLog(e.getAgentName()), sanitizeForLog(e.getTaskId()),
                e.getPendingTools() != null ? e.getPendingTools().size() : 0);
        Map<String, Object> result = new HashMap<>();
        result.put("status", "pending_approval");
        result.put("taskId", e.getTaskId());
        result.put("pendingTools", e.getPendingTools() != null ? e.getPendingTools() : List.of());
        result.put("childAgent", e.getAgentName());
        Map<String, Object> response = new HashMap<>();
        response.put("jsonrpc", "2.0");
        response.put("id", requestId);
        response.put("result", result);
        return response;
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        return ResponseEntity.ok(Map.of("status", "UP", "agent", properties.agent().name(), "role", "orchestrator"));
    }

    /** Which domain this orchestrator is the cell for (empty = single / meta). Its card is {@code <agent>__<cell>}. */
    private @Nullable DelegationExecutionService delegationExecutionService;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setDelegationExecutionService(@Nullable DelegationExecutionService service) {
        this.delegationExecutionService = service;
    }

    private @Nullable PlaybookContinuationService playbookContinuationService;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setPlaybookContinuationService(@Nullable PlaybookContinuationService service) {
        this.playbookContinuationService = service;
    }

    @org.springframework.beans.factory.annotation.Value("${#exampleframe#.orchestrator.federation.cell-name:}")
    private String cellName = "";

    /** The registered self-card, when the A2A server autoconfiguration is active - already
     *  renamed by {@link com.#exampleframe#.orchestrator.config.CellAgentCardPostProcessor}. */
    private @Nullable AgentCard selfCard;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setSelfCard(@Nullable AgentCard selfCard) {
        this.selfCard = selfCard;
    }

    /** The name this cell announces on its A2A card - what the meta's remote-agents entry must use.
     *  Prefers the actual {@link io.a2a.spec.AgentCard} bean (already renamed by
     *  {@code CellAgentCardPostProcessor}), so this endpoint reports the name that is REALLY
     *  registered in Nacos even if {@code spring.ai.alibaba.a2a.server.card.name} and
     *  {@code #exampleframe#.orchestrator.agent.name} are overridden apart from each other. The
     *  {@code CellAgentNames} fallback covers deployments without the a2a server beans; it
     *  unifies the suffix derivation, but its BASE name is only as aligned as those two
     *  properties are kept. */
    public String cardName() {
        if (selfCard != null) {
            return selfCard.name();
        }
        return com.#exampleframe#.orchestrator.config.CellAgentNames.cellCardName(properties.agent().name(), cellName);
    }

    @GetMapping("/.well-known/agent.json")
    public ResponseEntity<Map<String, Object>> agentCard() {
        return ResponseEntity.ok(Map.of(
                "name", cardName(),
                "description", "#ExampleFrame# orchestrator" + (cellName != null && !cellName.isBlank()
                        ? " - domain cell '" + cellName.trim() + "'" : " (domain cell)") + " - full investigation stack",
                "version", "1.0.0",
                "url", "/a2a/message",
                "capabilities", Map.of("streaming", false, "humanApproval", true)));
    }

    // ---- wire helpers (same shapes the agents use) ----

    @SuppressWarnings("unchecked")
    private static String extractInput(Map<String, Object> request) {
        Object params = request.get("params");
        if (params instanceof Map<?, ?> p) {
            Object message = p.get("message");
            if (message instanceof Map<?, ?> m) {
                Object parts = m.get("parts");
                if (parts instanceof List<?> list && !list.isEmpty() && list.get(0) instanceof Map<?, ?> part) {
                    Object text = part.get("text");
                    if (text != null) {
                        return text.toString();
                    }
                }
            }
        }
        for (String key : new String[]{"input", "message", "text", "query"}) {
            Object v = request.get(key);
            if (v instanceof String s && !s.isBlank()) {
                return s;
            }
        }
        return null;
    }

    private static String extractRequestId(Map<String, Object> request) {
        Object id = request.get("id");
        return id != null ? id.toString() : "a2a-" + System.currentTimeMillis();
    }

    /**
     * @param costThreadId the cost-tracked conversation thread whose accumulated snapshot this
     *        response should carry, or null to attach no cost (an unknown thread must never be
     *        looked up - a task id is not a session key)
     * @param isolatedSnapshot true when the thread lived only for this request (no caller
     *        contextId), so the snapshot is per-task and the caller adds it directly; false for
     *        a stable conversation thread, whose CUMULATIVE snapshot the caller de-dupes with
     *        its per-child watermark
     */
    private Map<String, Object> success(String requestId, String taskId, String output,
                                        @Nullable String costThreadId, boolean isolatedSnapshot) {
        Map<String, Object> textPart = new HashMap<>();
        textPart.put("type", "text");
        textPart.put("text", output != null ? output : "");
        List<Map<String, Object>> parts = new ArrayList<>();
        parts.add(textPart);
        Map<String, Object> artifact = new HashMap<>();
        artifact.put("parts", parts);
        List<Map<String, Object>> artifacts = new ArrayList<>();
        artifacts.add(artifact);

        Map<String, Object> result = new HashMap<>();
        result.put("artifacts", artifacts);
        result.put("taskId", taskId);
        result.put("status", "completed");
        result.put("output", output != null ? output : "");

        // What this delegated turn cost - the cell's own LLM calls plus its child agents',
        // accumulated under the cell-side conversation thread. Without this the caller (a
        // meta) shows only its OWN llm spend for the conversation: the investigation it paid
        // a cell for never reaches its cost panel. Plain agents send the same shape.
        if (sessionCostAccumulator != null && costThreadId != null) {
            var costSnapshot = sessionCostAccumulator.getSnapshot(costThreadId);
            if (costSnapshot.inputTokens() > 0 || costSnapshot.llmCalls() > 0) {
                Map<String, Object> costData = new HashMap<>();
                costData.put("inputTokens", costSnapshot.inputTokens());
                costData.put("outputTokens", costSnapshot.outputTokens());
                costData.put("llmCalls", costSnapshot.llmCalls());
                costData.put("totalCost", costSnapshot.totalCost());
                costData.put("isolated", isolatedSnapshot);
                result.put("costData", costData);
                logger.info("A2A response for thread {} carries costData: in={}, out={}, calls={}, cost={}, isolated={}",
                        costThreadId, costSnapshot.inputTokens(), costSnapshot.outputTokens(),
                        costSnapshot.llmCalls(), costSnapshot.totalCost(), isolatedSnapshot);
            }
        }

        Map<String, Object> response = new HashMap<>();
        response.put("jsonrpc", "2.0");
        response.put("id", requestId);
        response.put("result", result);
        return response;
    }

    private static Map<String, Object> error(String requestId, int code, String message) {
        Map<String, Object> err = new HashMap<>();
        err.put("code", code);
        err.put("message", message);
        Map<String, Object> response = new HashMap<>();
        response.put("jsonrpc", "2.0");
        response.put("id", requestId);
        response.put("error", err);
        return response;
    }
}
