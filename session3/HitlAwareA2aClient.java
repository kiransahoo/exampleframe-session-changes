package com.#exampleframe#.orchestrator.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.#exampleframe#.orchestrator.exception.ChildAgentHitlException;
import com.#exampleframe#.orchestrator.observation.cost.SessionCostAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import java.time.Duration;
import java.util.*;

/**
 * HITL-aware A2A client that detects pending_approval responses from child agents.
 *
 *
 * - A2aRemoteAgent.invoke() returns Optional<OverAllState> and doesn't understand
 *   the "pending_approval" status that child agents return when HITL triggers.
 * - This client makes direct HTTP calls and inspects the response status field.
 * - When pending_approval is detected, it throws ChildAgentHitlException which
 *   propagates up to the controller for SSE delivery to the frontend.
 *
 * Usage:
 * - Injected into OrchestratorTools
 * - Called from invokeAgentWithTracking() instead of A2aRemoteAgent.invoke()
 */
@Component
public class HitlAwareA2aClient {

    private static final Logger logger = LoggerFactory.getLogger(HitlAwareA2aClient.class);

    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    private final SessionCostAccumulator sessionCostAccumulator;

    // ThreadLocal to carry the orchestrator's threadId for cost accumulation
    private static final ThreadLocal<String> currentOrchestratorThreadId = new ThreadLocal<>();

    @Value("${#exampleframe#.a2a.client.timeout-seconds:1200}")
    private int timeoutSeconds;

    @Value("${#exampleframe#.a2a.client.max-memory-mb:10}")
    private int maxMemoryMb;

    public HitlAwareA2aClient(WebClient.Builder webClientBuilder, ObjectMapper objectMapper,
                               @org.springframework.lang.Nullable SessionCostAccumulator sessionCostAccumulator,
                               @org.springframework.beans.factory.annotation.Autowired(required = false)
                               @org.springframework.lang.Nullable
                               com.#exampleframe#.orchestrator.security.ServiceTokenProperties serviceTokenProperties) {
        // clone(): WebClient.Builder is mutable; never taint the shared builder other beans
        // build from. Service-to-service auth is NOT a default header here: the
        // ServiceTokenWebClientCustomizer attaches X-Service-Token per request and only to
        // configured agent/cell hosts, so the token cannot travel to any other URL.
        this.webClient = webClientBuilder.clone()
                .codecs(configurer -> configurer
                        .defaultCodecs()
                        .maxInMemorySize(10 * 1024 * 1024)) // 10MB default
                .build();
        this.objectMapper = objectMapper;
        this.sessionCostAccumulator = sessionCostAccumulator;

        logger.info("HitlAwareA2aClient initialized (costAccumulator: {}, serviceToken: {})",
                sessionCostAccumulator != null,
                serviceTokenProperties != null && serviceTokenProperties.isEnabled());
    }

    /**
     * Set the orchestrator's threadId for cost accumulation.
     * Call this before invokeAgent/resumeAgent so child agent costs get attributed to the right session.
     */
    /** Header carrying the acting user's stable identity (oid) to agents/cells. */
    public static final String ON_BEHALF_OF = "X-On-Behalf-Of";
    /** Comma-separated roles of the acting user (ROLE_OPERATOR lets an approver resume another user's HITL). */
    public static final String ON_BEHALF_OF_ROLES = "X-On-Behalf-Of-Roles";

    private com.#exampleframe#.orchestrator.security.CallerContextRegistry callerContextRegistry;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setCallerContextRegistry(com.#exampleframe#.orchestrator.security.CallerContextRegistry registry) {
        this.callerContextRegistry = registry;
    }

    /**
     * Forwards who this call is made for, so agents can refuse a body/path userId that
     * contradicts the real caller. Resolved from the caller bound at the HTTP boundary for
     * the current orchestrator thread; nothing is sent when no authenticated caller is bound.
     */
    private void addOnBehalfOf(org.springframework.http.HttpHeaders headers) {
        if (callerContextRegistry == null) {
            return;
        }
        com.#exampleframe#.orchestrator.security.CallerContext caller =
                callerContextRegistry.resolve(currentOrchestratorThreadId.get());
        if (caller != null && caller.authenticated()
                && !caller.hasRole(com.#exampleframe#.orchestrator.security.Roles.SERVICE)) {
            headers.set(ON_BEHALF_OF, caller.name());
            headers.set(ON_BEHALF_OF_ROLES, String.join(",", new java.util.TreeSet<>(caller.roles())));
        }
    }

    public void setOrchestratorThreadId(String threadId) {
        currentOrchestratorThreadId.set(threadId);
    }

    /**
     * Clear the orchestrator threadId after the call completes.
     */
    public void clearOrchestratorThreadId() {
        currentOrchestratorThreadId.remove();
    }

    /**
     * Invokes a child agent via A2A protocol, detecting HITL pending_approval.
     *
     * @param agentName Name of the child agent (for logging/identification)
     * @param baseUrl Base URL of the child agent (e.g., http://oracle-agent:8080)
     * @param input The query/input to send to the child agent
     * @param ctx Per-call context: conversation identity (sent as {@code contextId} so a
     *            receiving cell can retain conversation state), the bare task + structured
     *            params for deterministic playbook routing, and the timeout budget. Null =
     *            anonymous call with default timeouts.
     * @return The agent's response text if completed successfully
     * @throws ChildAgentHitlException if agent returns pending_approval status
     * @throws RuntimeException for other errors
     */
    public String invokeAgent(String agentName, String baseUrl, String input, @org.springframework.lang.Nullable A2aCallContext ctx) {
        String url = baseUrl + "/a2a/message";
        String requestId = "orch-" + System.currentTimeMillis();
        int effectiveTimeout = effectiveTimeoutSeconds(ctx);

        logger.info("=== [{}] HITL-aware A2A invoke === url: {} (timeout {}s{})", agentName, url,
                effectiveTimeout, ctx != null && ctx.threadId() != null ? ", contextId " + ctx.threadId() : "");
        logger.debug("[{}] Input: {}", agentName, truncate(input, 200));

        // Build A2A protocol request
        Map<String, Object> request = buildA2aRequest(requestId, input, ctx);

        try {
            long startTime = System.currentTimeMillis();

            // Make HTTP call
            String responseBody = webClient.post()
                    .uri(url)
                    .headers(this::addOnBehalfOf)
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(request)
                    .retrieve()
                    .bodyToMono(String.class)
                    .timeout(Duration.ofSeconds(effectiveTimeout))
                    .block();

            long duration = System.currentTimeMillis() - startTime;
            logger.debug("[{}] Response received in {}ms: {}", agentName, duration, truncate(responseBody, 500));

            return processResponse(agentName, baseUrl, responseBody);

        } catch (ChildAgentHitlException e) {
            // Re-throw HITL exceptions as-is
            throw e;
        } catch (com.#exampleframe#.orchestrator.exception.CellInputRequiredException e) {
            // Re-throw as-is: wrapping it here turns the relay into an error string
            // downstream and the whole input_required loop is dead on the wire.
            throw e;
        } catch (WebClientResponseException e) {
            logger.error("[{}] HTTP error {}: {}", agentName, e.getStatusCode(), e.getMessage());
            throw new RuntimeException("Child agent HTTP error: " + e.getStatusCode(), e);
        } catch (Exception e) {
            if (e.getCause() instanceof java.util.concurrent.TimeoutException) {
                logger.error("[{}] Request timed out after {}s", agentName, effectiveTimeout);
                throw new RuntimeException("Child agent timeout after " + effectiveTimeout + "s", e);
            }
            logger.error("[{}] A2A call failed: {}", agentName, e.getMessage(), e);
            throw new RuntimeException("Failed to call " + agentName + ": " + e.getMessage(), e);
        }
    }

    /**
     * The timeout for one call: per-agent configured value when the caller resolved one
     * (explicitly configured values are honored even when SHORTER than the client default -
     * fail-fast configs are deliberate), else this client's global default; in both cases
     * capped at the caller's remaining deadline (a playbook step must not outlive its budget).
     */
    private int effectiveTimeoutSeconds(@org.springframework.lang.Nullable A2aCallContext ctx) {
        int t = timeoutSeconds;
        if (ctx != null && ctx.timeoutSeconds() != null && ctx.timeoutSeconds() > 0) {
            t = ctx.timeoutSeconds();
        }
        if (ctx != null && ctx.deadline() != null) {
            long remaining = Duration.between(java.time.Instant.now(), ctx.deadline()).getSeconds();
            t = (int) Math.max(1, Math.min(t, remaining));
        }
        return t;
    }

    /**
     * Resumes a child agent after HITL approval.
     *
     * @param agentName Name of the child agent
     * @param baseUrl Base URL of the child agent
     * @param taskId The task ID returned in pending_approval response
     * @param toolFeedbacks List of approval decisions from the user
     * @return The agent's response text after resuming
     * @throws ChildAgentHitlException if agent returns ANOTHER pending_approval (chained HITL)
     */
    public String resumeAgent(String agentName, String baseUrl, String taskId,
                              List<Map<String, Object>> toolFeedbacks) {
        return resumeAgent(agentName, baseUrl, taskId, toolFeedbacks, null);
    }

    /**
     * Variant with the agent's configured timeout - a resume finishes the SAME long
     * operation the initial (per-agent-timed) call started, so it must not fall back
     * to the shorter global default and time out right after the human approved.
     */
    public String resumeAgent(String agentName, String baseUrl, String taskId,
                              List<Map<String, Object>> toolFeedbacks,
                              @org.springframework.lang.Nullable Integer agentTimeoutSeconds) {
        String url = baseUrl + "/a2a/resume";
        String requestId = "orch-resume-" + System.currentTimeMillis();
        int effectiveTimeout = agentTimeoutSeconds != null && agentTimeoutSeconds > 0
                ? agentTimeoutSeconds : timeoutSeconds;

        logger.info("=== [{}] HITL-aware A2A resume === taskId: {}, feedbacks: {} (timeout {}s)",
                agentName, taskId, toolFeedbacks.size(), effectiveTimeout);

        // Build resume request
        Map<String, Object> params = new HashMap<>();
        params.put("taskId", taskId);
        params.put("toolFeedbacks", toolFeedbacks);

        Map<String, Object> request = new HashMap<>();
        request.put("jsonrpc", "2.0");
        request.put("id", requestId);
        request.put("method", "task/resume");
        request.put("params", params);

        try {
            long startTime = System.currentTimeMillis();

            String responseBody = webClient.post()
                    .uri(url)
                    .headers(this::addOnBehalfOf)
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(request)
                    .retrieve()
                    .bodyToMono(String.class)
                    .timeout(Duration.ofSeconds(effectiveTimeout))
                    .block();

            long duration = System.currentTimeMillis() - startTime;
            logger.debug("[{}] Resume response in {}ms: {}", agentName, duration, truncate(responseBody, 500));

            return processResponse(agentName, baseUrl, responseBody);

        } catch (ChildAgentHitlException e) {
            // Re-throw (chained HITL)
            throw e;
        } catch (com.#exampleframe#.orchestrator.exception.CellInputRequiredException e) {
            throw e;
        } catch (Exception e) {
            logger.error("[{}] Resume failed: {}", agentName, e.getMessage(), e);
            throw new RuntimeException("Failed to resume " + agentName + ": " + e.getMessage(), e);
        }
    }

    // RESPONSE PROCESSING

    @SuppressWarnings("unchecked")
    private String processResponse(String agentName, String baseUrl, String responseBody) {
        try {
            Map<String, Object> response = objectMapper.readValue(responseBody,
                    new TypeReference<Map<String, Object>>() {});

            // Check for JSON-RPC error
            if (response.containsKey("error")) {
                Map<String, Object> error = (Map<String, Object>) response.get("error");
                String errorMsg = String.valueOf(error.getOrDefault("message", "Unknown error"));
                int errorCode = error.get("code") != null ? ((Number) error.get("code")).intValue() : -32000;
                logger.error("[{}] A2A error (code {}): {}", agentName, errorCode, errorMsg);
                throw new RuntimeException("Child agent error: " + errorMsg);
            }

            // Get result
            Map<String, Object> result = (Map<String, Object>) response.get("result");
            if (result == null) {
                logger.warn("[{}] No 'result' in response", agentName);
                return "No response from " + agentName;
            }

            String status = String.valueOf(result.getOrDefault("status", "completed"));
            String taskId = result.get("taskId") != null ? String.valueOf(result.get("taskId")) : null;

            logger.info("[{}] Response status: {}, taskId: {}", agentName, status, taskId);

            // CRITICAL: Detect pending_approval and throw exception
            if ("pending_approval".equals(status)) {
                List<Map<String, Object>> pendingTools =
                        (List<Map<String, Object>>) result.get("pendingTools");

                if (pendingTools == null) {
                    pendingTools = new ArrayList<>();
                }

                logger.info("[{}] === HITL DETECTED === taskId: {}, pendingTools: {}",
                        agentName, taskId, pendingTools.size());

                for (Map<String, Object> tool : pendingTools) {
                    logger.info("[{}]   - Tool: {} (id: {})",
                            agentName, tool.get("name"), tool.get("id"));
                }

                throw new ChildAgentHitlException(agentName, taskId, pendingTools, baseUrl);
            }

            // A delegated CELL matched a playbook but needs values only the user can supply
            // (the delegated path cannot prompt). Relay exactly like pending_approval: the
            // gateway asks the user and re-forwards with the reply merged in.
            if ("input_required".equals(status)) {
                @SuppressWarnings("unchecked")
                List<String> missingKeys = result.get("missingKeys") instanceof List<?> l
                        ? (List<String>) l : List.of();
                String playbookId = result.get("playbookId") != null
                        ? String.valueOf(result.get("playbookId")) : "";
                String playbookName = result.get("playbookName") != null
                        ? String.valueOf(result.get("playbookName")) : playbookId;
                Map<String, String> prompts = new LinkedHashMap<>();
                // Optional keys the cell's own door would offer in the same question. Cells
                // before this field send neither list nor flag - both reads tolerate that.
                List<String> optionalKeys = new ArrayList<>();
                if (result.get("optionalKeys") instanceof List<?> ol) {
                    for (Object o : ol) {
                        if (o != null && !optionalKeys.contains(String.valueOf(o))) {
                            optionalKeys.add(String.valueOf(o));
                        }
                    }
                }
                if (result.get("missingParams") instanceof List<?> pairs) {
                    for (Object o : pairs) {
                        if (o instanceof Map<?, ?> pair && pair.get("key") != null && pair.get("prompt") != null) {
                            String key = String.valueOf(pair.get("key"));
                            prompts.put(key, String.valueOf(pair.get("prompt")));
                            if (Boolean.parseBoolean(String.valueOf(pair.get("optional")))
                                    && !optionalKeys.contains(key)) {
                                optionalKeys.add(key);
                            }
                        }
                    }
                }
                // Cell-supplied strings reach a log here before the gateway sanitizes them:
                // strip line breaks so a hostile key cannot forge a log line.
                logger.info("[{}] === INPUT REQUIRED === playbook '{}', missing keys: {} (optional: {})",
                        agentName, logSafe(playbookId), logSafe(missingKeys), logSafe(optionalKeys));
                throw new com.#exampleframe#.orchestrator.exception.CellInputRequiredException(
                        agentName, taskId, playbookId, playbookName, missingKeys, prompts, optionalKeys);
            }

            // Extract child agent cost data and accumulate into orchestrator session
            extractAndAccumulateChildCost(agentName, result);

            // Normal completion - extract output
            return extractOutput(result, agentName);

        } catch (ChildAgentHitlException e) {
            throw e;
        } catch (com.#exampleframe#.orchestrator.exception.CellInputRequiredException e) {
            throw e;
        } catch (Exception e) {
            logger.error("[{}] Failed to parse response: {}", agentName, e.getMessage());
            throw new RuntimeException("Failed to parse response from " + agentName, e);
        }
    }

    // CHILD COST EXTRACTION

    @SuppressWarnings("unchecked")
    /** Log operand from the wire: line breaks and tabs collapsed so it cannot forge a log line. */
    private static String logSafe(Object value) {
        return String.valueOf(value).replaceAll("[\\r\\n\\t]", " ");
    }

    private void extractAndAccumulateChildCost(String agentName, Map<String, Object> result) {
        if (sessionCostAccumulator == null) return;

        String orchestratorThreadId = currentOrchestratorThreadId.get();
        if (orchestratorThreadId == null) {
            logger.debug("[{}] No orchestrator threadId set, skipping child cost accumulation", agentName);
            return;
        }

        try {
            Map<String, Object> costData = (Map<String, Object>) result.get("costData");
            if (costData == null) {
                logger.debug("[{}] No costData in A2A response", agentName);
                return;
            }

            long inputTokens = costData.get("inputTokens") != null
                    ? ((Number) costData.get("inputTokens")).longValue() : 0;
            long outputTokens = costData.get("outputTokens") != null
                    ? ((Number) costData.get("outputTokens")).longValue() : 0;
            int llmCalls = costData.get("llmCalls") != null
                    ? ((Number) costData.get("llmCalls")).intValue() : 0;
            String totalCost = costData.get("totalCost") != null
                    ? String.valueOf(costData.get("totalCost")) : null;
            boolean isolated = Boolean.TRUE.equals(costData.get("isolated"));

            sessionCostAccumulator.recordChildAgentCost(
                    orchestratorThreadId, agentName, inputTokens, outputTokens, llmCalls, totalCost, isolated);

            logger.info("[{}] Child agent cost accumulated (isolated={}) - tokens: {}/{}, calls: {}, cost: {}",
                    agentName, isolated, inputTokens, outputTokens, llmCalls, totalCost);

        } catch (Exception e) {
            logger.warn("[{}] Failed to extract child agent cost data: {}", agentName, e.getMessage());
        }
    }

    // REQUEST BUILDING

    static Map<String, Object> buildA2aRequest(String requestId, String input,
                                               @org.springframework.lang.Nullable A2aCallContext ctx) {
        // Build message in A2A format
        Map<String, Object> textPart = new HashMap<>();
        textPart.put("type", "text");
        textPart.put("text", input);

        Map<String, Object> message = new HashMap<>();
        message.put("role", "user");
        message.put("parts", List.of(textPart));

        // Build params. The extra fields (contextId, rawTask, taskParams) are read only by
        // #ExampleFrame# cells - conversation retention and deterministic playbook routing on
        // the delegated path; plain agents ignore them.
        Map<String, Object> params = new HashMap<>();
        params.put("message", message);
        if (ctx != null) {
            if (ctx.threadId() != null && !ctx.threadId().isBlank()) {
                params.put("contextId", ctx.threadId());
            }
            if (ctx.rawTask() != null && !ctx.rawTask().isBlank()) {
                params.put("rawTask", ctx.rawTask());
            }
            if (ctx.taskParams() != null && !ctx.taskParams().isEmpty()) {
                params.put("taskParams", ctx.taskParams());
            }
        }

        // Build full request
        Map<String, Object> request = new HashMap<>();
        request.put("jsonrpc", "2.0");
        request.put("id", requestId);
        request.put("method", "message/send");
        request.put("params", params);

        return request;
    }

    // OUTPUT EXTRACTION

    @SuppressWarnings("unchecked")
    private String extractOutput(Map<String, Object> result, String agentName) {
        // Try artifacts[].parts[].text (A2A standard format)
        List<Map<String, Object>> artifacts = (List<Map<String, Object>>) result.get("artifacts");
        if (artifacts != null && !artifacts.isEmpty()) {
            Map<String, Object> firstArtifact = artifacts.get(0);
            List<Map<String, Object>> parts = (List<Map<String, Object>>) firstArtifact.get("parts");
            if (parts != null && !parts.isEmpty()) {
                for (Map<String, Object> part : parts) {
                    if ("text".equals(part.get("type"))) {
                        Object text = part.get("text");
                        if (text != null && !text.toString().isBlank()) {
                            String output = text.toString();
                            logger.info("[{}] Extracted output from artifacts: {} chars", agentName, output.length());
                            return output;
                        }
                    }
                }
            }
        }

        // Fallback: try direct output field
        Object output = result.get("output");
        if (output != null && !output.toString().isBlank()) {
            logger.info("[{}] Extracted output from 'output' field: {} chars", agentName, output.toString().length());
            return output.toString();
        }

        // Fallback: try message field
        Object message = result.get("message");
        if (message != null && !message.toString().isBlank()) {
            return message.toString();
        }

        logger.warn("[{}] Could not extract output from result keys: {}", agentName, result.keySet());
        return "Completed but no output extracted";
    }

    // HELPERS

    private String truncate(String s, int maxLen) {
        if (s == null) return "null";
        return s.length() > maxLen ? s.substring(0, maxLen) + "..." : s;
    }
}