package com.#exampleframe#.orchestrator.tools;

import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.agent.a2a.A2aRemoteAgent;
import com.#exampleframe#.orchestrator.config.AgentConfigResolver;
import com.#exampleframe#.orchestrator.config.OrchestratorProperties.RemoteAgentProperties;
import com.#exampleframe#.orchestrator.exception.ChildAgentHitlException;
import com.#exampleframe#.orchestrator.service.A2aCallContext;
import com.#exampleframe#.orchestrator.service.HitlAwareA2aClient;
import com.#exampleframe#.orchestrator.service.InvestigationStateService;
import com.#exampleframe#.orchestrator.tools.ToolFormatUtils.AgentQueryResult;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.#exampleframe#.orchestrator.tools.ToolFormatUtils.*;

/**
 * Invokes remote agents via A2A protocol (HITL-aware path preferred, legacy fallback).
 * Also manages parallel agent queries via a dedicated thread pool.
 */
@Service
public class AgentInvocationService {

    private static final Logger logger = LoggerFactory.getLogger(AgentInvocationService.class);

    private static final Duration AGENT_TIMEOUT = Duration.ofSeconds(1200);
    private static final Duration PARALLEL_TIMEOUT = Duration.ofSeconds(1800);
    private static final int LOG_TRUNCATE_LENGTH = 100;
    private static final int PARALLEL_POOL_SIZE = 4;

    private final AgentConfigResolver agentConfigResolver;
    private final Map<String, A2aRemoteAgent> remoteAgents;
    private final HitlAwareA2aClient hitlAwareA2aClient;
    private final AgentResponseExtractor responseExtractor;
    private final InvestigationStateService investigationStateService;
    private final ExecutorService parallelExecutor;

    public AgentInvocationService(
            AgentConfigResolver agentConfigResolver,
            @Nullable Map<String, A2aRemoteAgent> remoteAgents,
            HitlAwareA2aClient hitlAwareA2aClient,
            AgentResponseExtractor responseExtractor,
            InvestigationStateService investigationStateService) {
        this.agentConfigResolver = agentConfigResolver;
        this.remoteAgents = remoteAgents != null ? remoteAgents : Map.of();
        this.hitlAwareA2aClient = hitlAwareA2aClient;
        this.responseExtractor = responseExtractor;
        this.investigationStateService = investigationStateService;
        this.parallelExecutor = Executors.newFixedThreadPool(PARALLEL_POOL_SIZE, r -> {
            Thread t = new Thread(r, "orchestrator-parallel-agent");
            t.setDaemon(true);
            return t;
        });
    }

    @PostConstruct
    public void init() {
        logger.info("AgentInvocationService initialized with {} remote agents: {}",
                remoteAgents.size(), remoteAgents.keySet());
    }

    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down AgentInvocationService parallel executor");
        parallelExecutor.shutdown();
        try {
            if (!parallelExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                parallelExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            parallelExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }


    /**
     * Invokes an agent, resolving the base URL from AgentConfigResolver.
     * Prefer {@link #invokeAgentWithTracking(A2aRemoteAgent, String, String, String, String)}
     * when the caller already has the resolved URL (e.g., from DynamicDelegationToolFactory config).
     *
     * @throws ChildAgentHitlException if the child agent returns {@code pending_approval}
     */
    public String invokeAgentWithTracking(A2aRemoteAgent agent, String input, String agentName, String originalQuery) {
        return invokeAgentWithTracking(agent, input, agentName, originalQuery, getAgentBaseUrl(agentName));
    }

    /**
     * Invokes an agent using the HITL-aware path with an explicit base URL.
     * Falls back to legacy only when HITL is not configured (hitlAwareA2aClient is null).
     *
     * @param baseUrl the agent's base URL (already resolved by caller), or null
     * @throws ChildAgentHitlException if the child agent returns {@code pending_approval}
     */
    public String invokeAgentWithTracking(A2aRemoteAgent agent, String input, String agentName, String originalQuery, @Nullable String baseUrl) {
        return invokeAgentWithTracking(agent, input, agentName, originalQuery, baseUrl, null);
    }

    /**
     * Variant with an explicit per-call context. When {@code ctx.threadId()} is set it is
     * used for cost/caller attribution AND sent on the wire as the A2A {@code contextId} -
     * so the evidence board, the cost accumulator, the On-Behalf-Of header and the
     * receiving cell's conversation all key on the SAME thread. When null, the thread is
     * resolved from the caller's binding as before (a null resolution degrades to an
     * anonymous call, never to a shared literal key).
     *
     * @throws ChildAgentHitlException if the child agent returns {@code pending_approval}
     */
    public String invokeAgentWithTracking(A2aRemoteAgent agent, String input, String agentName, String originalQuery,
                                          @Nullable String baseUrl,
                                          @Nullable A2aCallContext ctx) {
        Instant startTime = Instant.now();

        try {
            logger.debug("[{}] Invoking agent (baseUrl={}, hitlClient={})",
                    agentName, baseUrl != null, hitlAwareA2aClient != null);

            if (baseUrl != null && hitlAwareA2aClient != null) {
                logger.debug("[{}] Using HitlAwareA2aClient with baseUrl: {}", agentName, baseUrl);

                // One thread identity for everything this call touches: the explicitly
                // passed threadId when the caller has one, else the thread binding.
                String effectiveThreadId = ctx != null && ctx.threadId() != null
                        ? ctx.threadId()
                        : investigationStateService.resolveCurrentThreadId();
                A2aCallContext effectiveCtx =
                        new A2aCallContext(
                                effectiveThreadId,
                                ctx != null ? ctx.rawTask() : null,
                                ctx != null ? ctx.taskParams() : null,
                                ctx != null ? ctx.deadline() : null,
                                configuredTimeoutSeconds(agentName));

                // Set orchestrator threadId so child agent costs get accumulated
                hitlAwareA2aClient.setOrchestratorThreadId(effectiveThreadId);
                try {
                    String response = hitlAwareA2aClient.invokeAgent(agentName, baseUrl, input, effectiveCtx);

                    Duration duration = Duration.between(startTime, Instant.now());
                    logger.info("[{}] Agent responded in {}ms ({} chars)",
                            agentName, duration.toMillis(), response.length());

                    return response;
                } finally {
                    hitlAwareA2aClient.clearOrchestratorThreadId();
                }

            } else if (hitlAwareA2aClient != null) {
                // HITL client exists but no URL - config issue, do NOT silently downgrade
                logger.error("[{}] HITL client is configured but no baseUrl found in " +
                        "#exampleframe#.orchestrator.remote-agents.{}.url. Cannot invoke agent " +
                        "without URL when HITL is enabled.", agentName, agentName);
                return formatAgentError(agentName,
                        "Agent URL not configured. Add 'url' to #exampleframe#.orchestrator.remote-agents."
                                + agentName + " in application.yaml.",
                        getAvailableAgentsList(remoteAgents.keySet()));

            } else {
                // No HITL client at all - legacy mode is intentional
                logger.info("[{}] No HITL client configured, using legacy A2aRemoteAgent", agentName);
                return invokeAgentLegacy(agent, input, agentName);
            }

        } catch (ChildAgentHitlException e) {
            logger.warn("=== [{}] HITL DETECTED - propagating to controller === taskId: {}, tools: {}",
                    agentName, e.getTaskId(), e.getPendingTools().size());
            throw e;

        } catch (com.#exampleframe#.orchestrator.exception.CellInputRequiredException e) {
            logger.info("=== [{}] INPUT REQUIRED - propagating to the forwarding door === playbook '{}', keys {}",
                    agentName, e.getPlaybookId(), e.getMissingKeys());
            throw e;

        } catch (Exception e) {
            logger.error("=== [{}] EXCEPTION: {} ===", agentName, e.getMessage(), e);
            Duration duration = Duration.between(startTime, Instant.now());
            return formatAgentException(agentName, e, duration);
        }
    }

    /**
     * Legacy invocation via A2aRemoteAgent.invoke(). No HITL detection.
     */
    private String invokeAgentLegacy(A2aRemoteAgent agent, String input, String agentName) {
        Instant startTime = Instant.now();

        // The config-driven fallback can pass a null agent + baseUrl; that's safe on the HITL path
        // (routes by URL). It only reaches legacy mode when no HITL client is configured - and
        // legacy has no URL routing, so a null agent can't be invoked. Fail cleanly instead of NPE.
        if (agent == null) {
            logger.error("[{}] No live agent and no HITL/URL routing - cannot invoke", agentName);
            return formatAgentError(agentName,
                    agentName + " agent is not available (no live discovery entry, and HITL/URL routing is not enabled).",
                    getAvailableAgentsList(remoteAgents.keySet()));
        }

        try {
            logger.info("=== [{}] LEGACY agent.invoke() ===", agentName);

            Optional<OverAllState> result = agent.invoke(input);

            logger.info("[{}] LEGACY result.isPresent={}", agentName, result.isPresent());

            Duration duration = Duration.between(startTime, Instant.now());

            if (result.isEmpty()) {
                logger.warn("{} agent returned empty result after {}ms", agentName, duration.toMillis());
                return formatEmptyResponse(agentName, duration);
            }

            OverAllState state = result.get();
            String response = responseExtractor.extractResponseFromState(state, agentName);

            logger.info("[{}] Extracted response length: {} ({}ms)", agentName, response.length(), duration.toMillis());

            return response;

        } catch (Exception e) {
            logger.error("[{}] LEGACY EXCEPTION: {}", agentName, e.getMessage(), e);
            Duration duration = Duration.between(startTime, Instant.now());
            return formatAgentException(agentName, e, duration);
        }
    }


    /**
     * Executes a single agent query. HITL exceptions are returned as results (not propagated).
     *
     * @param threadId the conversation this query belongs to, captured by the CALLER on a
     *                 thread that still has the binding - this method runs on the parallel
     *                 pool where ThreadLocals and per-OS-thread bindings do not follow.
     */
    public AgentQueryResult executeAgentQuery(A2aRemoteAgent agent, String input, String agentDisplayName,
                                              String originalQuery, @Nullable String threadId) {
        Instant startTime = Instant.now();
        String agentKey = agentDisplayName.toLowerCase().replace(" ", "-");

        try {
            String baseUrl = getAgentBaseUrl(agentKey);

            if (baseUrl != null && hitlAwareA2aClient != null) {
                String effectiveThreadId = threadId != null
                        ? threadId : investigationStateService.resolveCurrentThreadId();
                hitlAwareA2aClient.setOrchestratorThreadId(effectiveThreadId);
                try {
                    String response = hitlAwareA2aClient.invokeAgent(agentKey, baseUrl, input,
                            new A2aCallContext(
                                    effectiveThreadId, null, null, null, configuredTimeoutSeconds(agentKey)));
                    Duration duration = Duration.between(startTime, Instant.now());
                    return new AgentQueryResult(agentDisplayName, response, duration, true);
                } finally {
                    hitlAwareA2aClient.clearOrchestratorThreadId();
                }

            } else if (hitlAwareA2aClient != null) {
                // HITL configured but URL missing - config error
                Duration duration = Duration.between(startTime, Instant.now());
                String error = String.format("Agent URL not configured for '%s'. " +
                        "Add 'url' to #exampleframe#.orchestrator.remote-agents.%s in application.yaml.",
                        agentKey, agentKey);
                logger.error("[{}] {}", agentKey, error);
                return new AgentQueryResult(agentDisplayName, "ERROR: " + error, duration, false);

            } else {
                // No HITL client - legacy mode is intentional. The config-driven fallback can pass
                // a null agent (URL routing only works on the HITL path); legacy has no URL routing,
                // so a null agent can't be invoked - fail cleanly instead of NPE.
                if (agent == null) {
                    Duration duration = Duration.between(startTime, Instant.now());
                    return new AgentQueryResult(agentDisplayName,
                            "ERROR: " + agentDisplayName + " agent is not available "
                                    + "(no live discovery entry, and HITL/URL routing is not enabled).",
                            duration, false);
                }
                Optional<OverAllState> result = agent.invoke(input);
                Duration duration = Duration.between(startTime, Instant.now());

                if (result.isEmpty()) {
                    return new AgentQueryResult(agentDisplayName, formatEmptyResponse(agentDisplayName, duration), duration, false);
                }

                String response = responseExtractor.extractResponseFromState(result.get(), agentDisplayName);
                return new AgentQueryResult(agentDisplayName, response, duration, true);
            }

        } catch (ChildAgentHitlException e) {
            Duration duration = Duration.between(startTime, Instant.now());
            String hitlMessage = String.format(
                    "**%s requires approval**\n\n" +
                            "The agent needs to execute a tool that requires your approval.\n" +
                            "Please run a direct query to this agent to see the approval request.\n\n" +
                            "Pending tools: %s",
                    agentDisplayName,
                    e.getPendingTools().stream()
                            .map(t -> String.valueOf(t.get("name")))
                            .collect(Collectors.joining(", "))
            );
            logger.info("[{}] HITL in parallel query - returning as result", agentDisplayName);
            return new AgentQueryResult(agentDisplayName, hitlMessage, duration, false);

        } catch (Exception e) {
            Duration duration = Duration.between(startTime, Instant.now());
            logger.error("{} agent error in parallel query: {}", agentDisplayName, e.getMessage());
            return new AgentQueryResult(agentDisplayName, formatAgentException(agentDisplayName, e, duration), duration, false);
        }
    }

    /**
     * Submits an agent query for async execution on the parallel thread pool.
     *
     * @param threadId the conversation this query belongs to - capture it BEFORE submitting
     *                 (thread bindings do not cross the executor boundary)
     */
    public CompletableFuture<AgentQueryResult> submitParallelQuery(A2aRemoteAgent agent, String input,
                                                                   String agentDisplayName, String originalQuery,
                                                                   @Nullable String threadId) {
        return CompletableFuture.supplyAsync(
                () -> executeAgentQuery(agent, input, agentDisplayName, originalQuery, threadId),
                parallelExecutor
        );
    }

    /** Per-agent configured timeout, or null (= client default) when absent or not positive. */
    public @Nullable Integer configuredTimeoutSeconds(String agentKey) {
        RemoteAgentProperties props = agentConfigResolver.getAgentConfigs().get(agentKey);
        return props != null && props.timeoutSeconds() > 0 ? props.timeoutSeconds() : null;
    }

    /**
     * Waits for all parallel futures and formats the combined result.
     */
    public String collectParallelResults(List<CompletableFuture<AgentQueryResult>> futures, Instant startTime) {
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(PARALLEL_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

            Duration totalDuration = Duration.between(startTime, Instant.now());

            StringBuilder result = new StringBuilder();
            result.append("## Parallel Investigation Results\n\n");
            result.append("_Total execution time: ").append(totalDuration.toMillis()).append("ms_\n\n");

            for (CompletableFuture<AgentQueryResult> future : futures) {
                AgentQueryResult queryResult = future.get();

                result.append("### ").append(queryResult.agentName()).append(" Findings\n\n");

                if (queryResult.success()) {
                    result.append(queryResult.response()).append("\n\n");
                } else {
                    result.append(queryResult.response()).append("\n\n");
                }

                result.append("_Response time: ").append(queryResult.duration().toMillis()).append("ms_\n\n");
            }

            result.append("---\n");
            result.append("**Next Step**: Analyze these findings to identify patterns and correlations. ");
            result.append("Use `correlateFindings` to document your analysis or `recordHypothesis` to document your theory.\n");

            return result.toString();

        } catch (TimeoutException e) {
            logger.error("Parallel agent query timed out after {}s", PARALLEL_TIMEOUT.toSeconds());
            return "ERROR: Parallel query timed out after " + PARALLEL_TIMEOUT.toSeconds() +
                    " seconds. Some agents may be slow or unresponsive.";

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Parallel agent query interrupted");
            return "ERROR: Parallel query was interrupted.";

        } catch (ExecutionException e) {
            logger.error("Parallel agent query execution error: {}", e.getMessage());
            return "ERROR: Parallel query failed - " + e.getMessage();
        }
    }


    /** Configured agent keys (from AgentConfigResolver) - the discovery-empty fallback set. */
    public java.util.Set<String> getConfiguredAgentKeys() {
        return agentConfigResolver.getAgentConfigs().keySet();
    }

    /** Gets the base URL for an agent from config. */
    @Nullable
    public String getAgentBaseUrl(String agentName) {
        RemoteAgentProperties agentProps = agentConfigResolver.getAgentConfigs().get(agentName);
        if (agentProps != null && agentProps.url() != null) {
            logger.debug("[{}] URL resolved via AgentConfigResolver: {}", agentName, agentProps.url());
            return agentProps.url();
        }
        logger.warn("[{}] URL resolution failed - AgentConfigResolver returned no config", agentName);
        return null;
    }

    /**
     * Returns the map of all configured remote agents.
     */
    public Map<String, A2aRemoteAgent> getRemoteAgents() {
        return remoteAgents;
    }
}
