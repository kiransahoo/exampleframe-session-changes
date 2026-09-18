package com.#exampleframe#.orchestrator.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.#exampleframe#.orchestrator.config.PlaybookProperties.PlaybookDefinition;
import com.#exampleframe#.orchestrator.config.PlaybookProperties.PlaybookStep;
import com.#exampleframe#.orchestrator.exception.ChildAgentHitlException;
import com.#exampleframe#.orchestrator.security.CallerContext;
import com.#exampleframe#.orchestrator.security.CallerContextRegistry;
import com.#exampleframe#.orchestrator.tools.AgentInvocationService;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.#exampleframe#.orchestrator.tools.ToolFormatUtils.isErrorResponse;

/**
 * Deterministic step execution engine for playbooks.
 *
 * <p>Executes playbook steps sequentially (or in parallel batches for consecutive
 * {@code parallel:true} steps), records findings to the evidence board via
 * {@link DelegationExecutionService}, and synthesizes a final report.
 */
@Service
public class PlaybookExecutor {

    private static final Logger logger = LoggerFactory.getLogger(PlaybookExecutor.class);
    private static final int DEFAULT_TIMEOUT = 900; // seconds

    /**
     * Buffer reserved for the LLM synthesis call that runs AFTER all steps complete
     * but BEFORE any SSE events are emitted. Without this buffer, the outer Reactor
     * timeout can fire during synthesis even though all steps finished successfully.
     */
    private static final int SYNTHESIS_BUFFER_SECONDS = 120;

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    // Condition parsing patterns
    private static final Pattern CONDITION_FIELD = Pattern.compile("\\{(\\w+)}");
    private static final Pattern CONDITION_GT = Pattern.compile(
            "\\{(\\w+)}\\s*>\\s*(\\d+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern CONDITION_CONTAINS = Pattern.compile(
            "\\{(\\w+)}\\s+contains\\s+(\\S+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern CONDITION_NOT_EMPTY = Pattern.compile(
            "\\{(\\w+)}\\s+is\\s+not\\s+empty", Pattern.CASE_INSENSITIVE);
    private static final Pattern CONDITION_EMPTY = Pattern.compile(
            "\\{(\\w+)}\\s+is\\s+empty", Pattern.CASE_INSENSITIVE);
    private static final Pattern CONDITION_EQUALS = Pattern.compile(
            "\\{(\\w+)}\\s*==\\s*(\\S+)", Pattern.CASE_INSENSITIVE);

    /**
     * Patterns indicating a Kubernetes infrastructure resource (pod, deployment, service)
     * does not exist. Only applied to the {@code kubernetes} agent to avoid false-positives
     * on agents like oracle ("no SQL found") or azure-monitor ("no traces found") where
     * a negative finding is a valid investigation result, not a missing resource.
     */
    private static final Pattern INFRA_NOT_FOUND_PATTERN = Pattern.compile(
            "(?i)no pods matching|no pods found|not found in .+ namespace|" +
                    "no matching pods found|no pods were found|" +
                    "no deployments? (matching |found)|no services? (matching |found)|" +
                    "could not find (pod|deployment|service|container|namespace)");

    /** Agents where "not found" means the target infrastructure resource doesn't exist. */
    private static final Set<String> INFRA_AGENTS = Set.of("kubernetes");

    private final DelegationExecutionService delegationExecutionService;
    private final AgentInvocationService agentInvocationService;
    private final InvestigationStateService investigationStateService;
    private final AgentResponseSinkRegistry agentResponseSinkRegistry;
    private final ChildHitlEventBuilder childHitlEventBuilder;
    private final ChatModel chatModel;
    private final ObjectMapper objectMapper;

    public PlaybookExecutor(
            DelegationExecutionService delegationExecutionService,
            AgentInvocationService agentInvocationService,
            InvestigationStateService investigationStateService,
            AgentResponseSinkRegistry agentResponseSinkRegistry,
            ChildHitlEventBuilder childHitlEventBuilder,
            ChatModel chatModel,
            ObjectMapper objectMapper) {
        this.delegationExecutionService = delegationExecutionService;
        this.agentInvocationService = agentInvocationService;
        this.investigationStateService = investigationStateService;
        this.agentResponseSinkRegistry = agentResponseSinkRegistry;
        this.childHitlEventBuilder = childHitlEventBuilder;
        this.chatModel = chatModel;
        this.objectMapper = objectMapper;
    }

    /**
     * Executes a playbook deterministically. Returns SSE flux.
     * Used by {@link PlaybookGateway} (streaming path).
     */
    public Flux<ServerSentEvent<String>> execute(
            PlaybookDefinition playbook,
            Map<String, String> params,
            String threadId,
            String originalQuery) {

        int effectiveTimeout = resolveTimeout(playbook);

        // Steps get (timeout - buffer), outer Reactor gets (timeout + buffer) to leave room for synthesis
        int stepTimeout = Math.max(effectiveTimeout - SYNTHESIS_BUFFER_SECONDS, 60);
        int outerTimeout = effectiveTimeout + SYNTHESIS_BUFFER_SECONDS;
        Instant deadline = Instant.now().plusSeconds(stepTimeout);

        logger.info("[playbook] Timeout budget: steps={}s, synthesis-buffer={}s, outer={}s",
                stepTimeout, SYNTHESIS_BUFFER_SECONDS, outerTimeout);

        return Mono.fromCallable(() -> withCostThread(threadId,
                        () -> executeSteps(playbook, params, threadId, originalQuery, true, deadline)))
                .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
                .flatMapMany(results ->
                        Flux.fromIterable(withCostThread(threadId,
                                () -> buildTerminalEvents(playbook, results, originalQuery))))
                .timeout(Duration.ofSeconds(outerTimeout))
                .onErrorResume(e -> {
                    logger.error("Playbook execution failed: {}", e.getMessage(), e);
                    String errorMsg = "Investigation failed: " + e.getMessage();
                    return Flux.just(ServerSentEvent.<String>builder()
                            .event("error")
                            .data(errorMsg)
                            .build());
                });
    }

    /**
     * Blocking execution for tool path ({@link com.#exampleframe#.orchestrator.tools.PlaybookTools}).
     * No sink registration, no step-progress SSE events.
     *
     * @throws ChildAgentHitlException if a step hits HITL
     */
    public String executeBlocking(
            PlaybookDefinition playbook,
            Map<String, String> params,
            String threadId,
            String originalQuery) throws ChildAgentHitlException {

        // Thread ID guarantee
        if (threadId == null) {
            threadId = investigationStateService.resolveCurrentThreadId();
        }
        boolean threadIdGenerated = false;
        if (threadId == null) {
            threadId = UUID.randomUUID().toString();
            threadIdGenerated = true;
            investigationStateService.initThread(threadId);
            logger.info("Generated fallback threadId for blocking playbook execution: {}", threadId);
        }

        int effectiveTimeout = resolveTimeout(playbook);
        int stepTimeout = Math.max(effectiveTimeout - SYNTHESIS_BUFFER_SECONDS, 60);
        Instant deadline = Instant.now().plusSeconds(stepTimeout);

        // Cost identity: a MINTED thread id is one no cost panel ever reads - if the calling
        // thread already carries a real session binding (the tool door: the controller bound
        // the session and reactor propagated it here while resolveCurrentThreadId drew a
        // blank), recording under the mint would LOSE that spend. Prefer the live binding;
        // the mint is only a last resort so the accumulator is never keyed by null.
        String liveBinding = com.#exampleframe#.orchestrator.observation.cost.SessionCostAccumulator.getCurrentThreadId();
        final String costThread = (threadIdGenerated && liveBinding != null) ? liveBinding : threadId;
        final String execThread = threadId;
        List<StepResult> results = withCostThread(costThread,
                () -> executeSteps(playbook, params, execThread, originalQuery, false, deadline));

        // Check for HITL - throw so caller can wrap in ToolExecutionException
        for (StepResult result : results) {
            if (result.hitlPending && result.hitlException != null) {
                throw result.hitlException;
            }
        }

        return withCostThread(costThread, () -> synthesizeReport(playbook, results, originalQuery));
    }

    /**
     * The executor owns cost attribution so EVERY door - UI gateway, delegated ingress,
     * tool path - gets its playbook llm calls (step extraction, synthesis) attributed to
     * the session; without it the ObservationFilter sees no threadId on the playbook path
     * and the spend silently misses the cost panel (child-agent costs travel separately
     * and were never affected).
     */
    private static <T> T withCostThread(String threadId, java.util.function.Supplier<T> work) {
        return com.#exampleframe#.orchestrator.observation.cost.SessionCostAccumulator.withThreadId(threadId, work);
    }


    private List<StepResult> executeSteps(
            PlaybookDefinition playbook,
            Map<String, String> params,
            String threadId,
            String originalQuery,
            boolean emitProgress,
            Instant deadline) {

        List<PlaybookStep> steps = playbook.steps();
        if (steps == null || steps.isEmpty()) {
            return List.of();
        }

        // Context map: params + extracted fields from step responses
        Map<String, String> context = new HashMap<>(params);

        // Normalize empty namespace to "all" so kubernetes steps search across all namespaces
        if (!context.containsKey("namespace") || context.get("namespace") == null
                || context.get("namespace").isBlank()) {
            context.put("namespace", "all");
        }

        List<StepResult> results = new ArrayList<>();
        int breakIndex = runStepLoop(steps, 0, context, results, threadId, playbook.name(), emitProgress, deadline);
        maybeParkContinuation(playbook, context, results, breakIndex, threadId, originalQuery);
        return results;
    }

    /**
     * Runs steps from {@code startIndex}, appending to {@code results} and mutating
     * {@code context}; returns the index of the first step NOT run (the resume position
     * when a step paused on an approval). Shared by first execution and continuation.
     */
    private int runStepLoop(List<PlaybookStep> steps, int startIndex, Map<String, String> context,
                            List<StepResult> results, String threadId, String playbookName,
                            boolean emitProgress, Instant deadline) {
        int i = startIndex;
        while (i < steps.size()) {
            // Check deadline before each step/batch
            if (Instant.now().isAfter(deadline)) {
                logger.warn("[playbook] Deadline exceeded after {} of {} steps - aborting remaining steps",
                        results.size(), steps.size());
                break;
            }

            // Collect consecutive parallel steps into a batch
            if (steps.get(i).parallel() && i + 1 < steps.size() && steps.get(i + 1).parallel()) {
                List<PlaybookStep> batch = new ArrayList<>();
                while (i < steps.size() && steps.get(i).parallel()) {
                    batch.add(steps.get(i));
                    i++;
                }
                List<StepResult> batchResults = executeParallelBatch(
                        batch, context, threadId, playbookName, emitProgress, deadline);
                results.addAll(batchResults);

                // Check if any parallel step hit HITL
                boolean hitl = batchResults.stream().anyMatch(r -> r.hitlPending);
                if (hitl) break;
            } else {
                PlaybookStep step = steps.get(i);
                i++;

                StepResult result = executeSingleStep(
                        step, context, threadId, playbookName, emitProgress, deadline);
                results.add(result);

                if (result.hitlPending) break;
            }
        }

        return i;
    }

    private StepResult executeSingleStep(
            PlaybookStep step,
            Map<String, String> context,
            String threadId,
            String playbookName,
            boolean emitProgress,
            Instant deadline) {

        // Skip if this agent already reported "not found" for the target resource
        String agentUnavailableKey = step.agent() + "_resource_not_found";
        if ("true".equals(context.get(agentUnavailableKey))) {
            logger.info("[playbook] Skipping step '{}' - {} previously reported resource not found",
                    step.id(), step.agent());
            return StepResult.skipped(step.id(), step.agent(),
                    resolveTemplate(step.task(), context),
                    step.agent() + " could not find the target resource in a previous step", Duration.ZERO);
        }

        // Evaluate condition
        if (step.condition() != null && !step.condition().isBlank()) {
            if (!evaluateCondition(step.condition(), context)) {
                logger.info("[playbook] Skipping step '{}' - condition not met: {}",
                        step.id(), step.condition());
                return StepResult.skipped(step.id(), step.agent(),
                        resolveTemplate(step.task(), context),
                        "Condition not met: " + step.condition(), Duration.ZERO);
            }
        }

        String resolvedTask = resolveTemplate(step.task(), context);

        Instant start = Instant.now();
        try {
            String response = delegationExecutionService.delegateToAgent(
                    step.agent(), resolvedTask, threadId, context, null, deadline);
            Duration duration = Duration.between(start, Instant.now());

            // Classify error responses
            if (isErrorResponse(response)) {
                logger.warn("[playbook] Step '{}' returned error: {}", step.id(),
                        response.substring(0, Math.min(200, response.length())));
                return StepResult.error(step.id(), step.agent(), resolvedTask, response, duration);
            }

            // Detect infra "not found" responses - flag so subsequent steps with same agent are skipped
            if (isInfraResourceNotFound(step.agent(), response)) {
                logger.warn("[playbook] Step '{}' ({}) - infrastructure resource not found, flagging agent to skip future steps",
                        step.id(), step.agent());
                context.put(agentUnavailableKey, "true");

                // Still emit so the user sees what happened
                if (emitProgress) {
                    emitAgentResponse(threadId, step.agent(), response);
                }
                return StepResult.error(step.id(), step.agent(), resolvedTask, response, duration);
            }

            // Emit the actual agent response through the sink (same as ReAct path's hook)
            if (emitProgress) {
                emitAgentResponse(threadId, step.agent(), response);
            }

            // Extract fields from response
            if (step.extract() != null && !step.extract().isEmpty()) {
                Map<String, String> extracted = extractFields(step.extract(), response);
                context.putAll(extracted);
            }

            return StepResult.success(step.id(), step.agent(), resolvedTask, response, duration);

        } catch (ChildAgentHitlException e) {
            Duration duration = Duration.between(start, Instant.now());
            ServerSentEvent<String> hitlEvent = childHitlEventBuilder.buildEvent(e);
            storePendingHitl(e, threadId);

            return StepResult.hitlPending(step.id(), step.agent(), resolvedTask,
                    "Step paused: child agent requires approval", duration, hitlEvent, e);
        }
    }

    /**
     * Records the pending approval so {@code /resume-child} (UI door) can route the user's
     * decision back to the child that raised it - the streaming ReAct path stores this in
     * the controller's error handler, but on the playbook path the exception never reaches
     * it (flux path converts to an SSE event; blocking path is caught by the A2A ingress
     * which stores its own entry - same key, harmless overwrite).
     * <p>
     * The owner must be the user bound to this thread at the HTTP boundary: this runs on a
     * worker thread whose security context is empty, and an owner of "anonymous" would make
     * the later approval by the real user look like a cross-user access and be refused.
     */
    private void storePendingHitl(ChildAgentHitlException e, String threadId) {
        try {
            CallerContext caller =
                    callerContextRegistry != null ? callerContextRegistry.resolve(threadId) : null;
            if (caller != null) {
                investigationStateService.storePendingHitlAs(
                        e.getTaskId(), threadId, e.getAgentName(), e.getAgentBaseUrl(), caller.name());
            } else {
                investigationStateService.storePendingHitl(
                        e.getTaskId(), threadId, e.getAgentName(), e.getAgentBaseUrl());
            }
        } catch (Exception ex) {
            logger.warn("Failed to store pending HITL for task {}: {}", e.getTaskId(), ex.getMessage());
        }
    }

    private @Nullable PlaybookContinuationService continuationService;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setContinuationService(@Nullable PlaybookContinuationService service) {
        this.continuationService = service;
    }

    /**
     * Parks the run's position when exactly one step paused on an approval, keyed by that
     * gate's taskId, so the resume paths can finish the playbook instead of ending the
     * conversation with the child's answer alone. A parallel batch that raised several
     * gates is not parked: answers arrive per-gate and no single resume owns the run.
     */
    private void maybeParkContinuation(PlaybookDefinition playbook, Map<String, String> context,
                                       List<StepResult> results, int resumeStepIndex,
                                       String threadId, String originalQuery) {
        if (continuationService == null) {
            return;
        }
        List<StepResult> gated = results.stream()
                .filter(r -> r.hitlPending() && r.hitlException() != null)
                .toList();
        if (gated.size() != 1) {
            if (gated.size() > 1) {
                logger.info("[playbook] {} steps paused on approvals in one batch - continuation "
                        + "not parked (answers arrive per-gate)", gated.size());
            }
            return;
        }
        StepResult g = gated.get(0);
        continuationService.park(g.hitlException().getTaskId(),
                new PlaybookContinuationService.PlaybookContinuation(
                        playbook, new HashMap<>(context), new ArrayList<>(results), resumeStepIndex,
                        g.stepId(), g.agent(), g.task(), threadId, originalQuery, Instant.now()));
    }

    /**
     * Finishes a playbook that paused on an approval: the gated step's answer replaces its
     * pending placeholder, its extractions run, the REMAINING steps execute, and the report
     * is synthesised - keeping {@code buildTerminalEvents}'s promise that "the report is
     * synthesised on resume, once the approved step has actually run". A gate raised by a
     * LATER step parks a fresh continuation under the new taskId and rethrows, so answering
     * the next approval continues from there.
     */
    public String resumeContinuation(PlaybookContinuationService.PlaybookContinuation c,
                                     String childResponse) throws ChildAgentHitlException {
        PlaybookDefinition playbook = c.playbook();
        Map<String, String> context = new HashMap<>(c.context());
        List<StepResult> results = new ArrayList<>(c.results());
        int priorSize = results.size();

        for (int idx = results.size() - 1; idx >= 0; idx--) {
            if (results.get(idx).hitlPending()) {
                StepResult g = results.get(idx);
                results.set(idx, StepResult.success(g.stepId(), g.agent(), g.task(),
                        childResponse, Duration.ZERO));
                break;
            }
        }
        List<PlaybookStep> steps = playbook.steps() != null ? playbook.steps() : List.of();
        for (PlaybookStep step : steps) {
            if (step.id() != null && step.id().equals(c.gatedStepId())
                    && step.extract() != null && !step.extract().isEmpty()) {
                context.putAll(extractFields(step.extract(), childResponse));
                break;
            }
        }

        int effectiveTimeout = resolveTimeout(playbook);
        Instant deadline = Instant.now().plusSeconds(
                Math.max(effectiveTimeout - SYNTHESIS_BUFFER_SECONDS, 60));
        String threadId = c.threadId();
        logger.info("[playbook] Resuming '{}' at step {} of {} after approved gate on '{}'",
                playbook.name(), c.resumeStepIndex(), steps.size(), c.gatedStepId());

        int breakIndex = withCostThread(threadId, () -> runStepLoop(steps, c.resumeStepIndex(),
                context, results, threadId, playbook.name(), false, deadline));
        maybeParkContinuation(playbook, context, results, breakIndex, threadId, c.originalQuery());
        for (StepResult r : results.subList(Math.min(priorSize, results.size()), results.size())) {
            if (r.hitlPending() && r.hitlException() != null) {
                throw r.hitlException();
            }
        }
        return withCostThread(threadId, () -> synthesizeReport(playbook, results, c.originalQuery()));
    }

    private @Nullable CallerContextRegistry callerContextRegistry;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setCallerContextRegistry(@Nullable CallerContextRegistry registry) {
        this.callerContextRegistry = registry;
    }

    /**
     * Detects if a response from an infrastructure agent (kubernetes) indicates the
     * target resource doesn't exist. Only applies to agents in {@link #INFRA_AGENTS}
     * - other agents' "no results" responses (e.g., "no SQL found", "no traces found")
     * are valid investigation findings, not missing resources.
     */
    private boolean isInfraResourceNotFound(String agentKey, String response) {
        if (response == null) return false;
        if (!INFRA_AGENTS.contains(agentKey)) return false;
        // Only check the first 500 chars - the "not found" message is always near the top
        String head = response.substring(0, Math.min(500, response.length()));
        return INFRA_NOT_FOUND_PATTERN.matcher(head).find();
    }

    private List<StepResult> executeParallelBatch(
            List<PlaybookStep> batch,
            Map<String, String> context,
            String threadId,
            String playbookName,
            boolean emitProgress,
            Instant deadline) {
        long timeoutSeconds = Math.max(1, Duration.between(Instant.now(), deadline).getSeconds());

        // First, evaluate conditions and skip steps that don't pass
        List<PlaybookStep> eligible = new ArrayList<>();
        List<StepResult> results = new ArrayList<>();

        for (PlaybookStep step : batch) {
            // Skip if agent already reported "not found"
            String agentUnavailableKey = step.agent() + "_resource_not_found";
            if ("true".equals(context.get(agentUnavailableKey))) {
                results.add(StepResult.skipped(step.id(), step.agent(),
                        resolveTemplate(step.task(), context),
                        step.agent() + " could not find the target resource in a previous step", Duration.ZERO));
                continue;
            }
            if (step.condition() != null && !step.condition().isBlank()) {
                if (!evaluateCondition(step.condition(), context)) {
                    results.add(StepResult.skipped(step.id(), step.agent(),
                            resolveTemplate(step.task(), context),
                            "Condition not met: " + step.condition(), Duration.ZERO));
                    continue;
                }
            }
            eligible.add(step);
        }

        if (eligible.isEmpty()) return results;

        // Execute eligible steps in parallel
        Map<String, CompletableFuture<StepResult>> futures = new LinkedHashMap<>();
        for (PlaybookStep step : eligible) {
            String resolvedTask = resolveTemplate(step.task(), context);

            CompletableFuture<StepResult> future = CompletableFuture.supplyAsync(() -> {
                Instant start = Instant.now();
                try {
                    String response = delegationExecutionService.delegateToAgent(
                            step.agent(), resolvedTask, threadId, context, null, deadline);
                    Duration duration = Duration.between(start, Instant.now());

                    if (isErrorResponse(response)) {
                        return StepResult.error(step.id(), step.agent(), resolvedTask, response, duration);
                    }
                    return StepResult.success(step.id(), step.agent(), resolvedTask, response, duration);
                } catch (ChildAgentHitlException e) {
                    Duration duration = Duration.between(start, Instant.now());
                    ServerSentEvent<String> hitlEvent = childHitlEventBuilder.buildEvent(e);
                    storePendingHitl(e, threadId);
                    return StepResult.hitlPending(step.id(), step.agent(), resolvedTask,
                            "Step paused: child agent requires approval", duration, hitlEvent, e);
                } catch (Exception e) {
                    Duration duration = Duration.between(start, Instant.now());
                    return StepResult.error(step.id(), step.agent(), resolvedTask,
                            "ERROR: " + e.getMessage(), duration);
                }
            });
            futures.put(step.id(), future);
        }

        // Wait for all futures - per-future error handling
        try {
            CompletableFuture.allOf(futures.values().toArray(CompletableFuture[]::new))
                    .get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.warn("Parallel batch wait interrupted: {}", e.getMessage());
        }

        // Collect results and extract fields - first step in batch wins for key collisions
        Set<String> extractedKeys = new HashSet<>();
        for (PlaybookStep step : eligible) {
            CompletableFuture<StepResult> future = futures.get(step.id());
            StepResult result;
            try {
                result = future.getNow(StepResult.error(step.id(), step.agent(),
                        resolveTemplate(step.task(), context), "ERROR: Timeout", Duration.ZERO));
            } catch (Exception e) {
                result = StepResult.error(step.id(), step.agent(),
                        resolveTemplate(step.task(), context), "ERROR: " + e.getMessage(), Duration.ZERO);
            }

            results.add(result);

            // Detect infra "not found" in parallel results - flag agent to skip future steps
            if (result.success && result.response != null && isInfraResourceNotFound(step.agent(), result.response)) {
                logger.warn("[playbook] Parallel step '{}' ({}) - infrastructure resource not found, flagging agent",
                        step.id(), step.agent());
                context.put(step.agent() + "_resource_not_found", "true");
                // Re-create as error so it's reported properly
                result = StepResult.error(step.id(), step.agent(),
                        resolveTemplate(step.task(), context), result.response, result.duration);
                results.set(results.size() - 1, result); // replace last added
            }

            // Emit actual agent response through the sink (same as ReAct path's hook)
            if (emitProgress && result.response != null && (result.success || isInfraResourceNotFound(step.agent(), result.response))) {
                emitAgentResponse(threadId, step.agent(), result.response);
            }

            // Extract fields from successful responses (not "not found")
            if (result.success && step.extract() != null && !step.extract().isEmpty()) {
                Map<String, String> extracted = extractFields(step.extract(), result.response);
                for (Map.Entry<String, String> entry : extracted.entrySet()) {
                    if (extractedKeys.contains(entry.getKey())) {
                        logger.warn("Key '{}' extracted by both step '{}' and a previous step - keeping first",
                                entry.getKey(), step.id());
                    } else {
                        context.put(entry.getKey(), entry.getValue());
                        extractedKeys.add(entry.getKey());
                    }
                }
            }
        }

        return results;
    }


    private String resolveTemplate(String template, Map<String, String> context) {
        if (template == null) return "";
        String result = template;
        Matcher m = CONDITION_FIELD.matcher(template);
        while (m.find()) {
            String field = m.group(1);
            String value = context.getOrDefault(field, "");
            result = result.replace("{" + field + "}", value);
        }
        return result;
    }


    /**
     * Evaluates compound conditions with AND/OR.
     * Split on OR first (lower precedence), then AND (higher precedence).
     */
    boolean evaluateCondition(String condition, Map<String, String> context) {
        if (condition == null || condition.isBlank()) return true;

        // Split on OR (case-insensitive, whitespace-tolerant)
        String[] orClauses = condition.split("(?i)\\s+OR\\s+");
        for (String orClause : orClauses) {
            // Split on AND
            String[] andConditions = orClause.split("(?i)\\s+AND\\s+");
            boolean allTrue = true;
            for (String atomic : andConditions) {
                if (!evaluateAtomic(atomic.trim(), context)) {
                    allTrue = false;
                    break;
                }
            }
            if (allTrue) return true; // Any OR clause true -> whole condition true
        }
        return false;
    }

    private boolean evaluateAtomic(String condition, Map<String, String> context) {
        // {field} > N
        Matcher gt = CONDITION_GT.matcher(condition);
        if (gt.find()) {
            String value = context.getOrDefault(gt.group(1), "");
            try {
                return Double.parseDouble(value) > Double.parseDouble(gt.group(2));
            } catch (NumberFormatException e) {
                return false; // Missing or non-numeric -> false
            }
        }

        // {field} contains X
        Matcher contains = CONDITION_CONTAINS.matcher(condition);
        if (contains.find()) {
            String value = context.getOrDefault(contains.group(1), "");
            return value.toLowerCase().contains(contains.group(2).toLowerCase());
        }

        // {field} is not empty
        Matcher notEmpty = CONDITION_NOT_EMPTY.matcher(condition);
        if (notEmpty.find()) {
            String value = context.getOrDefault(notEmpty.group(1), "");
            return !value.isBlank();
        }

        // {field} is empty
        Matcher empty = CONDITION_EMPTY.matcher(condition);
        if (empty.find()) {
            String value = context.getOrDefault(empty.group(1), "");
            return value.isBlank();
        }

        // {field} == value
        // Special case: when comparing to "false" and field is empty/unknown,
        // treat it as matching (unknown defaults to the "safe" assumption).
        // This prevents skipping steps when upstream data is missing.
        Matcher eq = CONDITION_EQUALS.matcher(condition);
        if (eq.find()) {
            String value = context.getOrDefault(eq.group(1), "");
            String expected = eq.group(2);
            if (value.isBlank()) {
                // Unknown/empty field: treat as "false" for boolean comparisons
                // so "{cpu_throttled} == false" passes when we don't know
                return "false".equalsIgnoreCase(expected);
            }
            return value.equalsIgnoreCase(expected);
        }

        logger.warn("Unrecognized condition format: '{}' - evaluating to false", condition);
        return false;
    }


    private Map<String, String> extractFields(List<String> fieldNames, String response) {
        Map<String, String> result = new HashMap<>();
        if (fieldNames == null || response == null) return result;

        // Fast path: if the response contains a ```json block, try direct JSON parse first
        int jsonStart = response.indexOf("```json");
        if (jsonStart >= 0) {
            int contentStart = response.indexOf('\n', jsonStart) + 1;
            int jsonEnd = response.indexOf("```", contentStart);
            if (jsonEnd > contentStart) {
                try {
                    String jsonBlock = response.substring(contentStart, jsonEnd).trim();
                    Map<String, Object> parsed = objectMapper.readValue(jsonBlock, MAP_TYPE);
                    for (String field : fieldNames) {
                        Object value = parsed.get(field);
                        if (value != null) {
                            result.put(field, String.valueOf(value));
                        }
                    }
                    if (result.size() == fieldNames.size()) return result; // all found
                } catch (Exception e) {
                    logger.debug("JSON extraction failed, falling back to LLM: {}", e.getMessage());
                }
            }
        }

        // LLM-based extraction for any fields not yet populated
        List<String> missing = fieldNames.stream()
                .filter(f -> !result.containsKey(f))
                .toList();
        if (!missing.isEmpty()) {
            try {
                Map<String, String> llmExtracted = extractFieldsWithLlm(missing, response);
                result.putAll(llmExtracted);
                logger.info("[playbook] LLM field extraction: requested={}, populated={} -> {}",
                        missing.size(), llmExtracted.size(), llmExtracted.keySet());
            } catch (Exception e) {
                logger.warn("[playbook] LLM field extraction failed: {}", e.getMessage());
            }
        }

        return result;
    }

    /**
     * Uses the LLM to extract structured fields from a free-form agent response.
     * This is the fallback when JSON blocks and key-value heuristics fail.
     */
    private Map<String, String> extractFieldsWithLlm(List<String> fieldNames, String response)
            throws Exception {
        // Truncate response to limit token usage - generous limit to preserve
        // trace chain hierarchies and source code where class names appear deep in output.
        // Source code from readSourceCode can be 5-15KB per class; with 2-3 classes + trace
        // analysis, responses easily reach 30-50KB. 30KB limit preserves most source code.
        String truncated = response.length() > 30000
                ? response.substring(0, 30000) + "\n... (truncated)"
                : response;

        StringBuilder fieldDesc = new StringBuilder();
        for (String field : fieldNames) {
            String hint = FIELD_EXTRACTION_HINTS.getOrDefault(field, "");
            fieldDesc.append("- ").append(field);
            if (!hint.isEmpty()) fieldDesc.append(" (").append(hint).append(")");
            fieldDesc.append("\n");
        }

        String prompt = """
                Extract structured fields from this investigation response.
                Return ONLY a valid JSON object with the field names as keys.

                Rules:
                - Use concise values derived directly from the response data.
                - For boolean fields (cpu_throttled, memory_pressure), use "true" or "false".
                - For list fields (slow_operations), comma-separate ALL values found.
                - For type fields (dependency_type), you MUST list EVERY type mentioned
                  in the response, not just the dominant one.
                  Example: if the response mentions both SQL and HTTP dependencies,
                  return "SQL, HTTP" - not just "HTTP".
                - If a field genuinely cannot be determined from the response, use null.
                - Do NOT guess or fabricate values not supported by the response.
                - Prefer specific identifiers over descriptions.
                  For slow_operations: use operation/endpoint names, not "HTTP (service-name)".

                Fields to extract:
                %s
                Agent response:
                %s

                JSON:
                """.formatted(fieldDesc.toString(), truncated);

        var llmResponse = chatModel.call(new Prompt(prompt));
        String text = llmResponse.getResult().getOutput().getText().trim();

        // Strip markdown code blocks if present
        if (text.startsWith("```")) {
            int start = text.indexOf('\n') + 1;
            int end = text.lastIndexOf("```");
            if (end > start) text = text.substring(start, end).trim();
        }

        Map<String, Object> parsed = objectMapper.readValue(text, MAP_TYPE);
        Map<String, String> extracted = new HashMap<>();
        for (String field : fieldNames) {
            Object value = parsed.get(field);
            if (value != null && !"null".equals(String.valueOf(value))
                    && !String.valueOf(value).isBlank()) {
                extracted.put(field, String.valueOf(value));
            }
        }
        return extracted;
    }

    /**
     * Hints for the LLM extraction prompt - tells the model what kind of value
     * each field should contain so it extracts the right data.
     */
    private static final Map<String, String> FIELD_EXTRACTION_HINTS = Map.ofEntries(
            Map.entry("dependency_type", "ALL types of downstream calls found - list every type mentioned: SQL, HTTP, Redis, queue, etc. Always comma-separate if multiple"),
            Map.entry("slow_dependency", "name/URL of the slowest downstream dependency"),
            Map.entry("dependency_latency", "latency of the slowest dependency in ms"),
            Map.entry("slow_operations", "specific API operation names or endpoint paths (e.g., POST /api/checkout, GET /api/items). Do NOT use dependency hostnames - those are dependencies, not operations"),
            Map.entry("traced_operations", "fully-qualified Java class.method names from the trace chain. Extract ALL class names mentioned in the trace analysis."),
            Map.entry("latency_spike", "true if a latency spike is confirmed vs baseline"),
            Map.entry("baseline_comparison", "brief comparison of current vs baseline latency"),
            Map.entry("cpu_throttled", "true if CPU throttling is detected on pods"),
            Map.entry("memory_pressure", "true if memory is near limits"),
            Map.entry("pod_resource_usage", "brief summary of pod CPU/memory usage"),
            Map.entry("slow_queries", "SQL statements with high elapsed time"),
            Map.entry("execution_plans", "brief summary of execution plan issues"),
            Map.entry("lock_waits", "true if lock waits or contention detected"),
            Map.entry("db_hostname", "database hostname or connection string from dependency calls"),
            Map.entry("db_connection_info", "database connection strings, JDBC URLs, or DB_HOST values from pod config"),
            Map.entry("db_tables", "table names found in slow SQL queries"),
            Map.entry("source_code", "actual Java source code fetched by readSourceCode tool - include the full class source"),
            Map.entry("schema", "Oracle/database schema name found in source code - from @Table(schema=\"...\"), JDBC URLs, JPA persistence config, or Spring datasource properties"),
            Map.entry("code_issues", "potential code-level issues identified"),
            Map.entry("jvm_metrics", "brief JVM health summary"),
            Map.entry("gc_issues", "true if GC pressure or long pauses detected"),
            Map.entry("thread_status", "brief thread pool status summary")
    );


    /**
     * Emits the actual agent response through the sink - same pattern as the
     * ReAct path's {@code AgentResponseStreamingHook}. This makes playbook
     * output look identical to ReAct output: real agent data in collapsible
     * panels, then a final synthesized report.
     */
    private void emitAgentResponse(String threadId, String agent, String response) {
        try {
            agentResponseSinkRegistry.emit(threadId, agent, response);
        } catch (Exception e) {
            logger.warn("Failed to emit agent response for {}: {}", agent, e.getMessage());
        }
    }


    /**
     * The events that end a playbook run: the approval request if one is pending, otherwise the
     * synthesised report.
     * <p>
     * A run paused on an approval is NOT finished - the steps after the gate never ran. Emitting
     * a synthesised report alongside the approval request showed the user a confident RCA built
     * from partial evidence, so an operator could approve or reject a tool call against
     * conclusions that had already been drawn and displayed. The report is synthesised on resume,
     * once the approved (or rejected) step has actually run.
     * <p>
     * Package-private so the pause is testable without driving the whole executor.
     */
    List<ServerSentEvent<String>> buildTerminalEvents(PlaybookDefinition playbook,
                                                      List<StepResult> results,
                                                      String originalQuery) {
        List<ServerSentEvent<String>> events = new ArrayList<>();
        boolean awaitingApproval = false;
        for (StepResult result : results) {
            if (result.hitlPending) {
                awaitingApproval = true;
                if (result.hitlEvent != null) {
                    events.add(result.hitlEvent);
                }
            }
        }
        if (awaitingApproval) {
            logger.info("[playbook] Paused on a pending approval - withholding synthesis "
                    + "({} step result(s) so far)", results.size());
            return events;
        }
        events.add(buildStreamResponseEvent(synthesizeReport(playbook, results, originalQuery)));
        return events;
    }

    private String synthesizeReport(
            PlaybookDefinition playbook,
            List<StepResult> results,
            String originalQuery) {
        try {
            return synthesizeWithLlm(playbook, results, originalQuery);
        } catch (Exception e) {
            logger.warn("LLM synthesis failed, using template fallback: {}", e.getMessage());
            return buildTemplateFallback(playbook, results);
        }
    }

    private String synthesizeWithLlm(
            PlaybookDefinition playbook,
            List<StepResult> results,
            String originalQuery) {

        StringBuilder context = new StringBuilder();
        context.append("Original query: ").append(originalQuery).append("\n");
        context.append("Investigation: ").append(playbook.name()).append("\n\n");

        for (StepResult result : results) {
            context.append("## Step: ").append(result.stepId)
                    .append(" (").append(result.agent).append(")\n");
            if (result.skipped) {
                context.append("SKIPPED: ").append(result.skipReason).append("\n\n");
            } else if (result.hitlPending) {
                context.append("PAUSED: Requires human approval\n\n");
            } else if (!result.success) {
                context.append("FAILED: ").append(result.response).append("\n\n");
            } else {
                // Truncate very long responses for synthesis - generous limit to preserve
                // trace chain output and source code which users want to see in full.
                // Source code steps return 5-15KB per class; 50KB per step ensures full
                // source code survives into the synthesis prompt without hallucination.
                String resp = result.response;
                if (resp != null && resp.length() > 50000) {
                    resp = resp.substring(0, 50000) + "\n... (truncated)";
                }
                context.append(resp).append("\n\n");
            }
        }

        String prompt = """
                You are analyzing investigation results from a structured diagnostic workflow.
                Synthesize the findings into a clear root cause analysis report.

                Include these sections:
                1. **Summary** - what was found
                2. **Evidence Snapshot** - key metrics (latency, error rates, dependency stats)
                3. **Dependency Chain** - which downstream calls are slow and their latencies
                4. **Trace Chain Analysis** - the full call hierarchy from the trace chain step.
 Include every span with its status ([OK]/[FAIL]), duration, and class.method name.
                   Do NOT omit spans - include the complete hierarchy as provided.
                5. **Source Code** - if readSourceCode results are included, display the actual
                   source code exactly as returned. Use Java code blocks. Do NOT omit code.
                6. **Database ↔ Code Correlation** - if both slow SQL queries and source code
                   were found, map them together:
                   - Which Java entity/repository classes correspond to the slow SQL tables?
                   - Where in the code are the problematic queries being executed?
                   - Are there N+1 query patterns, missing indexes, or inefficient ORM usage
                     visible in the source code that explain the slow SQL?
                7. **Root Cause** - confirmed findings and hypotheses
                8. **Recommendations** - specific remediation steps with code-level fixes
                   (e.g., "Add @BatchSize to Product entity", "Use findAllByIdIn() instead of
                   individual findById() calls")

                CRITICAL RULES:
                - If a step returned trace chain output with span hierarchies, include it VERBATIM.
                - If a step returned source code from readSourceCode, include it in Java code blocks.
                - Do NOT summarize away the trace chain or source code - the user wants to see the details.
                - If both SQL and source code are available, ALWAYS produce the correlation section.
                - If some steps were skipped or failed, note what information is missing.
                - If a step reported "resource not found", suggest the user provide correct details.

                Format as clean markdown with headers.
                Do NOT wrap the report in a code block - output raw markdown.

                """ + context;

        var response = chatModel.call(new Prompt(prompt));
        return response.getResult().getOutput().getText();
    }

    private String buildTemplateFallback(
            PlaybookDefinition playbook,
            List<StepResult> results) {
        StringBuilder report = new StringBuilder();
        report.append("# ").append(playbook.name()).append(" - Results\n\n");

        for (StepResult result : results) {
            report.append("## ").append(result.stepId)
                    .append(" (").append(result.agent).append(")\n");
            if (result.skipped) {
                report.append("*Skipped:* ").append(result.skipReason).append("\n\n");
            } else if (result.hitlPending) {
                report.append("*Paused:* Requires human approval\n\n");
            } else if (!result.success) {
                report.append("*Error:* ").append(result.response).append("\n\n");
            } else {
                report.append(result.response).append("\n\n");
            }
        }

        return report.toString();
    }


    /**
     * Builds an SSE event matching the StreamResponse format the frontend expects.
     * Fields: node, agentName, messageType, content, chunk, promptTokens, completionTokens.
     */
    private ServerSentEvent<String> buildStreamResponseEvent(String content) {
        try {
            Map<String, Object> responseData = new LinkedHashMap<>();
            responseData.put("node", "agent");
            responseData.put("agentName", "orchestrator_agent");
            responseData.put("messageType", "assistant");
            responseData.put("content", content);
            responseData.put("chunk", content);
            responseData.put("promptTokens", null);
            responseData.put("completionTokens", null);
            String json = objectMapper.writeValueAsString(responseData);
            return ServerSentEvent.<String>builder().data(json).build();
        } catch (Exception e) {
            logger.error("Failed to serialize playbook report as StreamResponse", e);
            return ServerSentEvent.<String>builder()
                    .data("{\"messageType\":\"assistant\",\"content\":\"" +
                            content.replace("\"", "\\\"").replace("\n", "\\n") + "\"}")
                    .build();
        }
    }


    private int resolveTimeout(PlaybookDefinition playbook) {
        return playbook.timeout() != null ? playbook.timeout() : DEFAULT_TIMEOUT;
    }


    record StepResult(
            String stepId,
            String agent,
            String task,
            String response,
            boolean success,
            boolean skipped,
            String skipReason,
            Duration duration,
            boolean hitlPending,
            ServerSentEvent<String> hitlEvent,
            ChildAgentHitlException hitlException
    ) {
        static StepResult success(String stepId, String agent, String task,
                                  String response, Duration duration) {
            return new StepResult(stepId, agent, task, response,
                    true, false, null, duration, false, null, null);
        }

        static StepResult skipped(String stepId, String agent, String task,
                                  String skipReason, Duration duration) {
            return new StepResult(stepId, agent, task, null,
                    false, true, skipReason, duration, false, null, null);
        }

        static StepResult error(String stepId, String agent, String task,
                                String response, Duration duration) {
            return new StepResult(stepId, agent, task, response,
                    false, false, null, duration, false, null, null);
        }

        static StepResult hitlPending(String stepId, String agent, String task,
                                      String response, Duration duration,
                                      ServerSentEvent<String> hitlEvent,
                                      ChildAgentHitlException hitlException) {
            return new StepResult(stepId, agent, task, response,
                    false, false, null, duration, true, hitlEvent, hitlException);
        }
    }
}
