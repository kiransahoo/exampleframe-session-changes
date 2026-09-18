package com.#exampleframe#.orchestrator.api.controller;



import org.jspecify.annotations.Nullable;
import com.#exampleframe#.orchestrator.service.DelegationExecutionService;
import com.#exampleframe#.orchestrator.security.CallerContext;
import com.#exampleframe#.orchestrator.security.CallerContextRegistry;
import org.springframework.security.access.prepost.PreAuthorize;
import com.alibaba.cloud.ai.graph.NodeOutput;
import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.RunnableConfig;
import com.alibaba.cloud.ai.graph.action.InterruptionMetadata;
import com.alibaba.cloud.ai.graph.agent.Agent;
import com.alibaba.cloud.ai.graph.agent.ReactAgent;
import com.alibaba.cloud.ai.graph.exception.GraphRunnerException;
import com.alibaba.cloud.ai.graph.streaming.StreamingOutput;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.#exampleframe#.orchestrator.agent.loader.AgentLoader;
import com.#exampleframe#.orchestrator.api.dto.AgentResumeRequest;
import com.#exampleframe#.orchestrator.api.dto.AgentRunRequest;
import com.#exampleframe#.orchestrator.api.dto.ChildAgentResumeRequest;
import com.#exampleframe#.orchestrator.config.AgentConfigResolver;
import com.#exampleframe#.orchestrator.exception.ChildAgentHitlException;
import com.#exampleframe#.orchestrator.observation.cost.SessionCostAccumulator;
import com.#exampleframe#.orchestrator.runnableconfig.RunnableConfigConstants;
import com.#exampleframe#.orchestrator.thread.history.store.ThreadStore;
import com.#exampleframe#.orchestrator.service.AgentResponseSinkRegistry;
import com.#exampleframe#.orchestrator.service.ChildHitlEventBuilder;
import com.#exampleframe#.orchestrator.service.HitlAwareA2aClient;
import com.#exampleframe#.orchestrator.service.InvestigationStateService;
import com.#exampleframe#.orchestrator.service.PlaybookGateway;
import com.#exampleframe#.orchestrator.tools.AgentInvocationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.metadata.Usage;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.lang.NonNull;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;
import com.#exampleframe#.orchestrator.exception.ChildAgentHitlRuntimeException;

import java.net.SocketException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Controller handling orchestrator agent execution endpoints.
 * Provides streaming endpoints for running and resuming agent conversations.
 *
 * Supports Human-in-the-Loop (HITL) for both:
 * - Orchestrator-level tools (delegateToKubernetesAgent, etc.)
 * - Child agent tools (killSession, flushSharedPool, etc.) via propagation
 */
@RestController
@RequestMapping("/api/agents/{agentName}")
@CrossOrigin(origins = "*")
@Tag(name = "Agent Execution", description = "Execute and resume AI agent conversations with real-time SSE streaming")
public class OrchestratorController {

    private static final Logger logger = LoggerFactory.getLogger(OrchestratorController.class);
    private static final ServerSentEvent<String> EMPTY_SSE = ServerSentEvent.<String>builder().data("{}").build();

    private final ObjectMapper mapper = new ObjectMapper();
    private final AgentLoader agentLoader;
    private final HitlAwareA2aClient hitlAwareA2aClient;
    private final AgentConfigResolver agentConfigResolver;
    private final AgentInvocationService agentInvocationService;
    private final AgentResponseSinkRegistry agentResponseSinkRegistry;
    private final InvestigationStateService investigationStateService;
    private final ChildHitlEventBuilder childHitlEventBuilder;
    private final PlaybookGateway playbookGateway;

    // Playbook HITL continuation (optional): finishes a paused playbook after its gate is
    // answered, instead of ending the conversation with the child's answer alone.
    private com.#exampleframe#.orchestrator.service.PlaybookExecutor playbookExecutor;
    private com.#exampleframe#.orchestrator.service.PlaybookContinuationService playbookContinuationService;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setPlaybookExecutor(com.#exampleframe#.orchestrator.service.PlaybookExecutor executor) {
        this.playbookExecutor = executor;
    }

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setPlaybookContinuationService(com.#exampleframe#.orchestrator.service.PlaybookContinuationService service) {
        this.playbookContinuationService = service;
    }
    private final SessionCostAccumulator sessionCostAccumulator;
    private final ThreadStore threadStore;
    private final String activeModelName;

    /**
     * Descriptions for orchestrator-level tools that require approval.
     * Built dynamically from agent configuration at startup.
     */
    private final Map<String, String> toolApprovalDescriptions;

    private final @Nullable DelegationExecutionService delegationExecutionService;

    public OrchestratorController(AgentLoader agentLoader,
                                  HitlAwareA2aClient hitlAwareA2aClient,
                                  AgentConfigResolver agentConfigResolver,
                                  AgentInvocationService agentInvocationService,
                                  AgentResponseSinkRegistry agentResponseSinkRegistry,
                                  InvestigationStateService investigationStateService,
                                  ChildHitlEventBuilder childHitlEventBuilder,
                                  PlaybookGateway playbookGateway,
                                  SessionCostAccumulator sessionCostAccumulator,
                                  ThreadStore threadStore,
                                  @org.springframework.beans.factory.annotation.Autowired(required = false)
                                  @Nullable DelegationExecutionService delegationExecutionService,
                                  @org.springframework.beans.factory.annotation.Value("${#exampleframe#.orchestrator.agent.provider:OPENAI}") String provider,
                                  @org.springframework.beans.factory.annotation.Value("${spring.ai.anthropic.chat.options.model:}") String anthropicModel,
                                  @org.springframework.beans.factory.annotation.Value("${spring.ai.openai.chat.options.model:}") String openaiModel) {
        this.delegationExecutionService = delegationExecutionService;
        this.agentLoader = agentLoader;
        this.hitlAwareA2aClient = hitlAwareA2aClient;
        this.agentConfigResolver = agentConfigResolver;
        this.agentInvocationService = agentInvocationService;
        this.agentResponseSinkRegistry = agentResponseSinkRegistry;
        this.investigationStateService = investigationStateService;
        this.childHitlEventBuilder = childHitlEventBuilder;
        this.playbookGateway = playbookGateway;
        this.sessionCostAccumulator = sessionCostAccumulator;
        this.threadStore = threadStore;
        this.activeModelName = "ANTHROPIC".equalsIgnoreCase(provider) ? anthropicModel : openaiModel;
        this.toolApprovalDescriptions = buildToolApprovalDescriptions();
    }

    /**
     * Builds tool approval descriptions dynamically from YAML agent configuration.
     * Each agent with a {@code toolName} gets an auto-generated approval description.
     */
    private Map<String, String> buildToolApprovalDescriptions() {
        Map<String, String> descriptions = new HashMap<>();

        // Dynamic delegation tool descriptions from config (uses 3-layer fallback)
        var agents = agentConfigResolver.getAgentConfigs();
        if (agents != null) {
            for (var entry : agents.entrySet()) {
                var config = entry.getValue();
                if (config.toolName() != null) {
                    String agentLabel = formatAgentLabel(entry.getKey());
                    String desc = "Delegating a task to the " + agentLabel + " agent";
                    if (config.layer() != null) {
                        desc += " for " + config.layer() + " operations";
                    }
                    desc += ".";
                    descriptions.put(config.toolName(), desc);
                }
            }
        }

        // Static tool descriptions (non-delegation tools)
        descriptions.put("parallelAgentQuery",
                "Executing parallel queries to multiple agents for cross-domain analysis.");

        return Collections.unmodifiableMap(descriptions);
    }

    private static String formatAgentLabel(String agentKey) {
        String[] words = agentKey.split("-");
        StringBuilder label = new StringBuilder();
        for (String word : words) {
            if (!label.isEmpty()) label.append(" ");
            if (!word.isEmpty()) {
                label.append(Character.toUpperCase(word.charAt(0))).append(word.substring(1));
            }
        }
        return label.toString();
    }


    /**
     * Executes an agent run and streams the resulting events using Server-Sent Events (SSE).
     */
    @Operation(
            summary = "Run an agent conversation",
            description = """
                    Initiates a new agent conversation turn and streams the response using Server-Sent Events (SSE).

                    The orchestrator agent analyzes requests and delegates to appropriate domain agents:
                    - Kubernetes agent for infrastructure operations
                    - Oracle agent for database operations
                    - Azure Monitor agent for telemetry analysis

                    SSE Event Types:
                    - (default): Normal streaming response chunks
                    - tool-confirm: Orchestrator tool needs approval
                    - child-tool-confirm: Child agent tool needs approval
                    - error: An error occurred
                    """
    )
    @ApiResponses({
            @ApiResponse(
                    responseCode = "200",
                    description = "SSE stream opened successfully",
                    content = @Content(
                            mediaType = MediaType.TEXT_EVENT_STREAM_VALUE,
                            examples = {
                                    @ExampleObject(
                                            name = "Streaming Response",
                                            summary = "Normal streaming token response",
                                            value = """
                                                    data: {"node":"agent","agentName":"orchestrator_agent","content":"Analyzing your request...","chunk":"Analyzing"}
                                                    """
                                    )
                            }
                    )
            ),
            @ApiResponse(
                    responseCode = "404",
                    description = "Agent not found",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE)
            )
    })
    @PreAuthorize("@authz.viewer()")
    @PostMapping(value = "/run", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<String>> runAgent(
            @Parameter(description = "Name of the agent to execute", required = true, example = "orchestrator_agent")
            @PathVariable String agentName,
            @Valid @RequestBody AgentRunRequest request) {

        logger.info("POST /api/agents/{}/run - message: {}", agentName, truncate(request.getMessage(), 100));

        try {
            String threadId = request.getThreadId() != null ? request.getThreadId() : UUID.randomUUID().toString();
            String namespace = request.getNamespace() != null ? request.getNamespace() : "default";
            // Identity: the client-supplied userId is advisory only. When a caller is
            // authenticated, the server-verified principal wins (audit + thread ownership).
            String userId = effectiveUserId(request.getUserId());
            bindCaller(threadId);
            logger.info("[audit] run by '{}' (uid {}) thread={} agent={}", com.#exampleframe#.orchestrator.security.CurrentPrincipal.displayName(), userId, threadId, agentName);

            Agent agent = agentLoader.loadAgent(agentName);
            if (!(agent instanceof ReactAgent reactAgent)) {
                return Flux.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
                        "Agent is not a ReactAgent: " + agentName));
            }

            // Initialize investigation state for this thread
            investigationStateService.initThread(threadId);

            // Try deterministic playbook first - pass raw request namespace (may be null).
            // LLM extraction takes priority; request namespace is a fallback for API callers
            // that explicitly specify a namespace.
            Optional<Flux<ServerSentEvent<String>>> playbookResult =
                    playbookGateway.tryExecute(request.getMessage(), threadId, userId, request.getNamespace());
            if (playbookResult.isPresent()) {
                logger.info("Playbook matched - bypassing ReAct for threadId={}", threadId);
                return playbookResult.get();
            }

            RunnableConfig config = RunnableConfig.builder()
                    .threadId(threadId)
                    .addMetadata(RunnableConfigConstants.USER_ID_KEY, userId)
                    .addMetadata(RunnableConfigConstants.NAMESPACE_KEY, namespace)
                    .build();

            UserMessage userMessage = new UserMessage(request.getMessage());
            return executeAgent(userMessage, reactAgent, config);

        } catch (NoSuchElementException e) {
            logger.error("Agent not found: {}", agentName);
            return Flux.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "Agent not found: " + agentName));
        } catch (Exception e) {
            return handleExecutionError(request.getThreadId(), e);
        }
    }


    /**
     * Resumes an agent after human-in-the-loop interruption.
     */
    @Operation(
            summary = "Resume an interrupted agent",
            description = """
                    Resumes an agent conversation that was interrupted for tool approval.

                    When an agent requests to use a tool that requires approval, the `/run` endpoint
                    will return a tool-confirm event. Use this endpoint to provide feedback and continue.

                    Note: This is for ORCHESTRATOR-level tool approvals. For child agent approvals,
                    use the `/resume-child` endpoint.
                    """
    )
    @ApiResponses({
            @ApiResponse(
                    responseCode = "200",
                    description = "Agent resumed successfully",
                    content = @Content(mediaType = MediaType.TEXT_EVENT_STREAM_VALUE)
            ),
            @ApiResponse(
                    responseCode = "404",
                    description = "Agent or thread not found",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE)
            )
    })
    @PreAuthorize("@authz.operator()")
    @PostMapping(value = "/resume", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<String>> resumeAgent(
            @Parameter(description = "Name of the agent to resume", required = true, example = "orchestrator_agent")
            @PathVariable String agentName,
            @Valid @RequestBody AgentResumeRequest request) {

        logger.info("POST /api/agents/{}/resume - threadId: {}", agentName, request.getThreadId());

        try {
            String namespace = request.getNamespace() != null ? request.getNamespace() : "default";

            Agent agent = agentLoader.loadAgent(agentName);
            if (!(agent instanceof ReactAgent reactAgent)) {
                return Flux.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
                        "Agent is not a ReactAgent: " + agentName));
            }

            InterruptionMetadata interruptionMetadata = buildInterruptionMetadata(request.getToolFeedbacks());

            String approver = effectiveUserId(request.getUserId());
            bindCaller(request.getThreadId());
            logger.info("[audit] HITL resume by '{}' thread={} agent={} feedbacks={}", approver,
                    request.getThreadId(), agentName,
                    request.getToolFeedbacks() != null ? request.getToolFeedbacks().size() : 0);

            RunnableConfig config = RunnableConfig.builder()
                    .threadId(request.getThreadId())
                    .addMetadata(RunnableConfigConstants.USER_ID_KEY, approver)
                    .addMetadata(RunnableConfigConstants.NAMESPACE_KEY, namespace)
                    .addHumanFeedback(interruptionMetadata)
                    .build();

            return executeAgent(null, reactAgent, config);

        } catch (NoSuchElementException e) {
            logger.error("Agent not found: {}", agentName);
            return Flux.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "Agent not found: " + agentName));
        } catch (Exception e) {
            return handleExecutionError(request.getThreadId(), e);
        }
    }


    /**
     * Resumes a child agent after HITL approval.
     *
     * This endpoint is called when the frontend approves a child agent's tool request.
     * The child agent (e.g., Oracle) returned pending_approval, which was propagated
     * to the frontend. Now the user has approved/rejected, and we send the decision
     * back to the child agent.
     */
    @Operation(
            summary = "Resume a child agent after HITL approval",
            description = """
                    Resumes a child agent (Kubernetes, Oracle, Azure Monitor) that was interrupted
                    for tool approval.

                    When a child agent requires approval for a destructive operation (e.g., kill session),
                    the orchestrator propagates this to the frontend via a 'child-tool-confirm' SSE event.
                    Use this endpoint to send the user's approval decision back to the child agent.

                    Request body should include:
                    - agentName: The child agent name (e.g., "oracle")
                    - taskId: The task ID from the child-tool-confirm event
                    - toolFeedbacks: Array of approval decisions
                    """
    )
    @ApiResponses({
            @ApiResponse(
                    responseCode = "200",
                    description = "Child agent resumed successfully",
                    content = @Content(mediaType = MediaType.TEXT_EVENT_STREAM_VALUE)
            ),
            @ApiResponse(
                    responseCode = "400",
                    description = "Invalid request (missing agentName or taskId)",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE)
            )
    })
    @PreAuthorize("@authz.operator()")
    @PostMapping(value = "/resume-child", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<String>> resumeChildAgent(
            @Parameter(description = "Name of the orchestrator agent", required = true, example = "orchestrator_agent")
            @PathVariable String agentName,
            @Valid @RequestBody ChildAgentResumeRequest request) {

        logger.info("POST /api/agents/{}/resume-child - childAgent: {}, taskId: {}",
                agentName, request.getAgentName(), request.getTaskId());
        logger.info("[audit] child HITL approval by '{}' childAgent={} taskId={}",
                com.#exampleframe#.orchestrator.security.CurrentPrincipal.name(),
                request.getAgentName(), request.getTaskId());
        bindCaller(request.getOrchestratorThreadId());

        try {
            // Validate child agent name
            String childAgentName = request.getAgentName();
            if (childAgentName == null || childAgentName.isBlank()) {
                return Flux.just(buildErrorSseEvent(new IllegalArgumentException("agentName is required")));
            }

            // A resume is an approval of a privileged action inside a domain. The chat path
            // checks domain access before a task enters a cell; this path must too, or an
            // operator of one domain could approve a write in another.
            String resumeDenied = delegationExecutionService == null ? null
                    : delegationExecutionService.checkDomainAccess(childAgentName.toLowerCase(), request.getOrchestratorThreadId());
            if (resumeDenied != null) {
                return Flux.just(buildErrorSseEvent(new SecurityException(resumeDenied)));
            }

            // Validate taskId / toolFeedbacks up front: a malformed request must not consume
            // the pending approval.
            if (request.getTaskId() == null || request.getTaskId().isBlank()) {
                return Flux.just(buildErrorSseEvent(new IllegalArgumentException("taskId is required")));
            }
            if (request.getToolFeedbacks() == null || request.getToolFeedbacks().isEmpty()) {
                return Flux.just(buildErrorSseEvent(new IllegalArgumentException("toolFeedbacks is required")));
            }

            String ownerDenied = investigationStateService.checkHitlOwner(request.getTaskId());
            if (ownerDenied != null) {
                return Flux.just(buildErrorSseEvent(new SecurityException(ownerDenied)));
            }

            // Atomically CLAIM (consume) the pending approval: from here no concurrent
            // approval and no replay of this taskId can act on it. The claim carries the URL
            // of the instance that raised the gate - no fallback to the agent's default URL:
            // an approval that matches no pending task is not an approval, and forwarding it
            // would let a caller post arbitrary tool feedback at any configured agent.
            InvestigationStateService.ClaimedHitl claimed =
                    investigationStateService.claimPendingHitl(request.getTaskId());
            if (claimed == null) {
                return Flux.just(buildErrorSseEvent(new IllegalArgumentException(
                        "No pending approval for taskId '" + request.getTaskId() + "' - it may have "
                        + "expired, already been answered, or belong to another conversation.")));
            }
            String baseUrl = claimed.childBaseUrl();
            if (baseUrl == null) {
                investigationStateService.restorePendingHitl(request.getTaskId(), claimed);
                return Flux.just(buildErrorSseEvent(new IllegalArgumentException(
                        "No pending approval for taskId '" + request.getTaskId() + "' - it may have "
                        + "expired, already been answered, or belong to another conversation.")));
            }

            // Convert tool feedbacks to map format expected by HitlAwareA2aClient. Only an
            // explicit APPROVED authorizes the tool - absent, unknown, and EDITED (no channel
            // for edited arguments on this wire) all deny.
            List<Map<String, Object>> toolFeedbacks = request.getToolFeedbacks().stream()
                    .map(tf -> {
                        Map<String, Object> map = new HashMap<>();
                        map.put("id", tf.getId());
                        map.put("name", tf.getName());
                        map.put("arguments", tf.getArguments());
                        map.put("result", tf.getResult() == ChildAgentResumeRequest.Decision.APPROVED
                                ? "APPROVED" : "REJECTED");
                        return map;
                    })
                    .collect(Collectors.toList());

            logger.info("Resuming child agent {} with {} tool feedbacks", childAgentName, toolFeedbacks.size());

            String response;
            try {
                // Set orchestrator threadId for cost accumulation during resume
                hitlAwareA2aClient.setOrchestratorThreadId(claimed.threadId());
                try {
                    response = hitlAwareA2aClient.resumeAgent(
                            childAgentName,
                            baseUrl,
                            request.getTaskId(),
                            toolFeedbacks,
                            agentInvocationService.configuredTimeoutSeconds(childAgentName.toLowerCase()));
                } finally {
                    hitlAwareA2aClient.clearOrchestratorThreadId();
                }
            } catch (ChildAgentHitlException e) {
                // The answered tool ran; the child immediately raised the NEXT gate. Replace
                // the consumed record with the new pending task (same owner and thread), or
                // the follow-up approval would find nothing to act on.
                investigationStateService.chainClaimedHitl(
                        claimed, e.getTaskId(), e.getAgentName(), e.getAgentBaseUrl());
                if (playbookContinuationService != null) {
                    playbookContinuationService.rekey(request.getTaskId(), e.getTaskId());
                }
                logger.info("Child agent {} has chained HITL - {} more tools pending",
                        e.getAgentName(), e.getPendingTools().size());
                return Flux.just(childHitlEventBuilder.buildEvent(e));
            } catch (Exception e) {
                // Delivery failed: put the approval back so the operator can retry it.
                investigationStateService.restorePendingHitl(request.getTaskId(), claimed);
                throw e;
            }

            // Record the response for the orphan fixer and evidence board; the pending
            // record stays consumed so the same approval cannot be replayed.
            investigationStateService.resolveClaimedHitl(request.getTaskId(), claimed, response);

            logger.info("Child agent {} completed after resume, response: {} chars",
                    childAgentName, response.length());

            // The gate paused a PLAYBOOK step: run the remaining steps and synthesise, so the
            // operator gets the investigation they approved - not a dead conversation.
            if (playbookContinuationService != null && playbookExecutor != null) {
                var continuation = playbookContinuationService.claim(request.getTaskId());
                if (continuation != null) {
                    try {
                        String report = playbookExecutor.resumeContinuation(continuation, response);
                        logger.info("Playbook '{}' completed after resume - {} chars synthesised",
                                continuation.playbook().name(), report.length());
                        return Flux.just(buildChildCompleteResponse("orchestrator_agent", report));
                    } catch (ChildAgentHitlException next) {
                        // A later step raised its own gate; its pending record and re-parked
                        // continuation were stored by the executor.
                        return Flux.just(childHitlEventBuilder.buildEvent(next));
                    } catch (Exception ex) {
                        logger.warn("Playbook continuation after resume failed - returning the "
                                + "child's answer alone: {}", ex.getMessage(), ex);
                    }
                }
            }
            return Flux.just(buildChildCompleteResponse(childAgentName, response));

        } catch (Exception e) {
            logger.error("Failed to resume child agent: {}", e.getMessage(), e);
            return Flux.just(buildErrorSseEvent(e));
        }
    }


    /**
     * Synchronous invoke endpoint that returns the complete OverAllState.
     * Useful for simple request/response patterns without streaming.
     */
    @Operation(
            summary = "Invoke agent synchronously",
            description = """
                    Invokes the agent synchronously and returns the complete OverAllState response.

                    Unlike the streaming /run endpoint, this waits for complete execution
                    before returning the full state including all outputs.

                    Note: This endpoint does NOT support HITL. If the agent needs approval,
                    it will fail. Use the streaming /run endpoint for HITL support.
                    """
    )
    @ApiResponses({
            @ApiResponse(
                    responseCode = "200",
                    description = "Agent invocation successful",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE)
            ),
            @ApiResponse(
                    responseCode = "404",
                    description = "Agent not found",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE)
            )
    })
    @PreAuthorize("@authz.viewer()")
    @PostMapping(value = "/invoke", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<InvokeResponse> invokeAgent(
            @Parameter(description = "Name of the agent to execute", required = true, example = "orchestrator_agent")
            @PathVariable String agentName,
            @Valid @RequestBody AgentRunRequest request) {

        logger.info("POST /api/agents/{}/invoke - message: {}", agentName, truncate(request.getMessage(), 100));

        try {
            Agent agent = agentLoader.loadAgent(agentName);
            if (!(agent instanceof ReactAgent reactAgent)) {
                throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
                        "Agent is not a ReactAgent: " + agentName);
            }

            String threadId = request.getThreadId() != null ? request.getThreadId() : UUID.randomUUID().toString();
            String namespace = request.getNamespace() != null ? request.getNamespace() : "default";
            String userId = effectiveUserId(request.getUserId());
            bindCaller(threadId);
            logger.info("[audit] invoke by '{}' (uid {}) thread={} agent={}", com.#exampleframe#.orchestrator.security.CurrentPrincipal.displayName(), userId, threadId, agentName);

            RunnableConfig config = RunnableConfig.builder()
                    .threadId(threadId)
                    .addMetadata(RunnableConfigConstants.USER_ID_KEY, userId)
                    .addMetadata(RunnableConfigConstants.NAMESPACE_KEY, namespace)
                    .build();

            // Synchronous invoke returning OverAllState
            Optional<OverAllState> result = reactAgent.invoke(request.getMessage(), config);

            if (result.isEmpty()) {
                return ResponseEntity.ok(new InvokeResponse(
                        threadId,
                        agentName,
                        "completed",
                        null,
                        Map.of(),
                        "Agent returned no state"
                ));
            }

            OverAllState state = result.get();

            // Extract the main output from state
            String output = extractOutput(state);

            // Convert state data to serializable map
            Map<String, Object> stateData = new HashMap<>();
            for (Map.Entry<String, Object> entry : state.data().entrySet()) {
                Object value = entry.getValue();
                // Convert Message objects to strings for JSON serialization
                if (value instanceof AssistantMessage am) {
                    stateData.put(entry.getKey(), am.getText());
                } else if (value instanceof List<?> list && !list.isEmpty() && list.get(0) instanceof Message) {
                    List<String> messages = new ArrayList<>();
                    for (Object item : list) {
                        if (item instanceof Message msg) {
                            messages.add(msg.getText());
                        }
                    }
                    stateData.put(entry.getKey(), messages);
                } else {
                    stateData.put(entry.getKey(), value);
                }
            }

            return ResponseEntity.ok(new InvokeResponse(
                    threadId,
                    agentName,
                    "completed",
                    output,
                    stateData,
                    null
            ));

        } catch (NoSuchElementException e) {
            logger.error("Agent not found: {}", agentName);
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Agent not found: " + agentName);
        } catch (GraphRunnerException e) {
            logger.error("GraphRunnerException during invoke: {}", e.getMessage());
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
                    "Agent execution failed: " + e.getMessage());
        } catch (Exception e) {
            logger.error("Error during agent invoke: {}", e.getMessage(), e);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
                    "Agent invoke failed: " + e.getMessage());
        }
    }


    /**
     * Executes the agent and returns an SSE stream.
     *
     * Handles both:
     * - Orchestrator-level HITL (InterruptionMetadata in stream)
     * - Child agent HITL (ChildAgentHitlException propagated from tools)
     */
    @NonNull
    private Flux<ServerSentEvent<String>> executeAgent(UserMessage userMessage,
                                                       ReactAgent agent,
                                                       RunnableConfig runnableConfig) throws GraphRunnerException {

        // Extract threadId for agent-response sink registration
        String threadId = runnableConfig.threadId().orElse(null);

        // Register a sink so the AgentResponseStreamingHook can push raw responses.
        // An active sink means another request is already streaming this conversation -
        // running two graph executions on one thread corrupts its checkpointed history
        // and crosses their SSE events, so reject this one instead.
        if (threadId != null && !agentResponseSinkRegistry.register(threadId)) {
            return Flux.just(buildErrorSseEvent(new IllegalStateException(
                    "Another request is already streaming for this conversation (threadId " + threadId
                            + "). Wait for it to finish, or start a new thread.")));
        }

        Flux<NodeOutput> agentStream;

        // Set threadId for the observation filter to attribute costs to this session.
        // ReactorContextPropagationConfig auto-propagates this ThreadLocal across
        // Reactor async boundaries (Netty event loops where ObservationFilter fires).
        if (threadId != null) {
            SessionCostAccumulator.setCurrentThreadId(threadId);
        }

        try {
            if (userMessage != null) {
                agentStream = agent.stream(userMessage, runnableConfig);
            } else {
                agentStream = agent.stream("", runnableConfig);
            }
        } catch (Exception e) {
            // Synchronous failure: the doFinally cleanup below was never installed, so
            // without this the sink stays registered forever - permanently rejecting
            // every later request on this thread - and the cost ThreadLocal leaks into
            // whatever runs next on this request thread.
            if (threadId != null) {
                agentResponseSinkRegistry.unregister(threadId);
                SessionCostAccumulator.clearCurrentThreadId();
            }
            throw e;
        }

        // Retry transient OpenAI failures (connection reset, socket timeout, 5xx).
        // These are infrastructure-level errors where the LLM call never completed;
        // retrying with exponential backoff typically succeeds on the next attempt.
        agentStream = agentStream.retryWhen(Retry.backoff(3, Duration.ofSeconds(2))
                .maxBackoff(Duration.ofSeconds(10))
                .filter(OrchestratorController::isTransientLlmError)
                .doBeforeRetry(signal -> logger.warn(
                        "Transient LLM error (attempt {}/3), retrying: {}",
                        signal.totalRetries() + 1, signal.failure().getMessage()))
                .onRetryExhaustedThrow((spec, signal) -> signal.failure()));

        Flux<ServerSentEvent<String>> agentSSE = agentStream.map(nodeOutput -> {
                    String node = nodeOutput.node();
                    String agentName = nodeOutput.agent();
                    Usage tokenUsage = nodeOutput.tokenUsage();

                    // Handle InterruptionMetadata (Orchestrator-level HITL approval)
                    if (nodeOutput instanceof InterruptionMetadata interruptionMetadata) {
                        return buildToolConfirmResponse(node, agentName, interruptionMetadata);
                    }

                    // Cost tracking is handled by ChatModelCostObservationFilter via ThreadLocal.
                    // It fires once per actual LLM API call with real token counts,
                    // unlike streaming chunks which have empty Usage objects.

                    StreamResponse response = buildResponse(nodeOutput, node, agentName, tokenUsage, threadId);
                    if (response == null) {
                        return EMPTY_SSE;
                    }
                    return serializeResponse(response);
                })
                .onErrorResume(error -> {
                    // Walk the cause chain for HITL exceptions
                    Throwable cause = error;
                    while (cause != null) {
                        // Check for the runtime wrapper
                        if (cause instanceof ChildAgentHitlRuntimeException runtimeHitl) {
                            // Store pending HITL so the orphan fixer can use the actual
                            // resume result instead of a generic "interrupted" synthetic response
                            investigationStateService.storePendingHitl(
                                    runtimeHitl.getTaskId(), threadId, runtimeHitl.getAgentName(),
                                    runtimeHitl.getHitlException().getAgentBaseUrl());
                            logger.info("Child agent HITL detected (runtime wrapper) - agent: {}, taskId: {}",
                                    runtimeHitl.getAgentName(), runtimeHitl.getTaskId());
                            return Flux.just(childHitlEventBuilder.buildEvent(runtimeHitl.getHitlException()));
                        }
                        // Also check for direct exception (just in case)
                        if (cause instanceof ChildAgentHitlException childHitl) {
                            investigationStateService.storePendingHitl(
                                    childHitl.getTaskId(), threadId, childHitl.getAgentName(), childHitl.getAgentBaseUrl());
                            logger.info("Child agent HITL detected - agent: {}, taskId: {}",
                                    childHitl.getAgentName(), childHitl.getTaskId());
                            return Flux.just(childHitlEventBuilder.buildEvent(childHitl));
                        }
                        cause = cause.getCause();
                    }
                    return Flux.just(buildErrorSseEvent(error));
                });

        // Merge agent SSE stream with raw agent-response events from the hook.
        //
        // IMPORTANT: Flux.merge completes only when ALL sources complete.
        // agentResponseFlux (from the unicast sink) won't complete until
        // sink.tryEmitComplete() is called. If we only call that in doFinally,
        // we have a deadlock: doFinally waits for merge to complete, merge waits
        // for agentResponseFlux, agentResponseFlux waits for doFinally.
        //
        // Fix: when agentSSE completes/errors/cancels, immediately complete the
        // sink so agentResponseFlux terminates and Flux.merge can finish.
        if (threadId != null) {
            Flux<ServerSentEvent<String>> agentResponseFlux = agentResponseSinkRegistry.getFlux(threadId);

            Flux<ServerSentEvent<String>> terminatingAgentSSE = agentSSE
                    .doFinally(signal -> {
                        // Complete the sink as soon as the main agent stream ends
                        // (whether by completion, error, or cancellation).
                        // This unblocks Flux.merge so the SSE connection can close.
                        agentResponseSinkRegistry.unregister(threadId);
                    });

            return Flux.merge(terminatingAgentSSE, agentResponseFlux)
                    .doFinally(signal -> {
                        // Defensive: ensure cleanup even if the above didn't fire
                        agentResponseSinkRegistry.unregister(threadId);

                        // Fallback: persist cost (MessagePersistenceHook.afterAgent handles primary path)
                        agentResponseSinkRegistry.persistCostSnapshot(threadId);

                        // Clean up ThreadLocals to prevent leaks
                        SessionCostAccumulator.clearCurrentThreadId();
                        InvestigationStateService.CURRENT_THREAD_ID.remove();
                        investigationStateService.unbindCurrentThread(threadId);
                    });
        }

        return agentSSE.doFinally(signal -> {
            if (threadId != null) {
                agentResponseSinkRegistry.persistCostSnapshot(threadId);
            }
            SessionCostAccumulator.clearCurrentThreadId();
        });
    }


    /**
     * Gets the base URL for a child agent.
     * Delegates to AgentInvocationService which handles both @ConfigurationProperties
     * and @Value fallback URL resolution.
     */
    private String getChildAgentBaseUrl(String agentKey) {
        return agentInvocationService.getAgentBaseUrl(agentKey);
    }

    /**
     * Gets the list of available agent keys from the A2A remote agents bean.
     * Uses the injected AgentInvocationService's remote agents map (populated by A2AConfig)
     * which is always reliable, unlike @ConfigurationProperties map binding.
     */
    private String getAvailableAgentKeys() {
        Map<String, ?> agents = agentInvocationService.getRemoteAgents();
        if (agents != null && !agents.isEmpty()) {
            return String.join(", ", agents.keySet());
        }
        // Fallback: try AgentConfigResolver (3-layer config resolution)
        var resolvedAgents = agentConfigResolver.getAgentConfigs();
        if (!resolvedAgents.isEmpty()) {
            return String.join(", ", resolvedAgents.keySet());
        }
        return "none";
    }

    /**
     * Extracts the primary output from OverAllState.
     */
    private String extractOutput(OverAllState state) {
        List<String> outputKeys = List.of("output", "messages", "result", "response");

        for (String key : outputKeys) {
            Optional<Object> value = state.value(key);
            if (value.isPresent()) {
                Object val = value.get();
                if (val instanceof AssistantMessage am) {
                    return am.getText();
                }
                if (val instanceof List<?> list && !list.isEmpty()) {
                    Object last = list.get(list.size() - 1);
                    if (last instanceof AssistantMessage am) {
                        return am.getText();
                    }
                    if (last instanceof Message msg) {
                        return msg.getText();
                    }
                    return last.toString();
                }
                String result = val.toString();
                if (!result.isBlank()) {
                    return result;
                }
            }
        }
        return null;
    }

    /**
     * Builds InterruptionMetadata from tool feedback list for orchestrator resume.
     */
    private InterruptionMetadata buildInterruptionMetadata(List<AgentResumeRequest.ToolFeedback> toolFeedbacks) {
        InterruptionMetadata.Builder metadataBuilder = InterruptionMetadata.builder();

        if (toolFeedbacks != null && !toolFeedbacks.isEmpty()) {
            for (AgentResumeRequest.ToolFeedback feedback : toolFeedbacks) {
                // Approval semantics mirror the hardened A2A resume path exactly:
                // - an omitted decision is a REJECTION, never an approval - the human's
                //   explicit yes is the whole point of the gate;
                // - EDITED is not accepted: the framework substitutes the caller's
                //   arguments into the pending tool call without comparing them to what
                //   was shown for approval - an edit must go back through the model as a
                //   new call, not through the approval;
                // - arguments always come from the checkpointed tool call (null here),
                //   never from the approval request, so the approver can only ever
                //   approve exactly what the card showed.
                InterruptionMetadata.ToolFeedback.FeedbackResult result =
                        feedback.getResult() == AgentResumeRequest.ToolFeedback.FeedbackResult.APPROVED
                                ? InterruptionMetadata.ToolFeedback.FeedbackResult.APPROVED
                                : InterruptionMetadata.ToolFeedback.FeedbackResult.REJECTED;

                InterruptionMetadata.ToolFeedback.Builder feedbackBuilder =
                        InterruptionMetadata.ToolFeedback.builder()
                                .id(feedback.getId())
                                .name(feedback.getName())
                                .arguments(null)
                                .result(result);

                if (feedback.getDescription() != null) {
                    feedbackBuilder.description(feedback.getDescription());
                }

                metadataBuilder.addToolFeedback(feedbackBuilder.build());
                logger.info("Adding tool feedback: {} result={}", feedback.getName(), result);
            }
        }
        return metadataBuilder.build();
    }

    /**
     * Resets the in-memory cost accumulator for a given session.
     * Called by the frontend [RESET] button so the cost bar starts fresh.
     */
    @PreAuthorize("@authz.operator()")
    @PostMapping(value = "/cost/reset", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> resetSessionCost(
            @RequestParam(required = false) String threadId) {
        if (threadId != null && !threadId.isBlank()) {
            // IDOR gate (CWE-639): the threadId is caller-supplied and the accumulator is
            // globally keyed, so resetting is an action ON that thread - it needs the thread's
            // own principal (admin may override); operator role alone is not ownership.
            if (callerContextRegistry != null) {
                callerContextRegistry.assertAccess(threadId,
                        com.#exampleframe#.orchestrator.security.CurrentPrincipal.name(),
                        com.#exampleframe#.orchestrator.security.CurrentPrincipal.hasRole(
                                com.#exampleframe#.orchestrator.security.Roles.ADMIN));
            }
            sessionCostAccumulator.resetSession(threadId);
            logger.info("Cost accumulator reset for session: {}", threadId);
        }
        return ResponseEntity.ok(Map.of("status", "ok"));
    }

    private Flux<ServerSentEvent<String>> handleExecutionError(String threadId, Exception e) {
        logger.error("Error during agent run for session {}", threadId, e);
        return Flux.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Agent run failed: " + e.getMessage(), e));
    }


    /**
     * Builds SSE response for orchestrator-level tool confirmation.
     */
    private ServerSentEvent<String> buildToolConfirmResponse(String node, String agentName,
                                                             InterruptionMetadata interruptionMetadata) {
        List<ToolConfirmInfo> pendingTools = extractPendingTools(interruptionMetadata);

        ToolConfirmResponse response = new ToolConfirmResponse(
                node,
                agentName,
                "tool-confirm",
                pendingTools
        );

        try {
            String jsonData = mapper.writeValueAsString(response);
            return ServerSentEvent.<String>builder().data(jsonData).build();
        } catch (Exception e) {
            logger.error("Failed to serialize tool-confirm response", e);
            return ServerSentEvent.<String>builder()
                    .data("{\"messageType\":\"tool-confirm\",\"error\":\"Failed to serialize\"}")
                    .build();
        }
    }

    private List<ToolConfirmInfo> extractPendingTools(InterruptionMetadata interruptionMetadata) {
        List<ToolConfirmInfo> tools = new ArrayList<>();

        logger.info("InterruptionMetadata received - node: {}", interruptionMetadata.node());

        try {
            List<InterruptionMetadata.ToolFeedback> toolFeedbacks = interruptionMetadata.toolFeedbacks();
            if (toolFeedbacks != null && !toolFeedbacks.isEmpty()) {
                logger.info("Found {} pending tool feedbacks", toolFeedbacks.size());

                for (InterruptionMetadata.ToolFeedback feedback : toolFeedbacks) {
                    String name = feedback.getName();
                    String id = feedback.getId();
                    String arguments = feedback.getArguments();
                    String desc = feedback.getDescription();

                    if (desc == null || desc.isBlank()) {
                        desc = getToolApprovalDescription(name);
                    }

                    tools.add(new ToolConfirmInfo(id, name, arguments, desc));
                    logger.info("Pending tool: {} (id={}, args={})", name, id, arguments);
                }
            }
        } catch (Exception e) {
            logger.warn("Error extracting tool feedbacks: {} - {}", e.getClass().getSimpleName(), e.getMessage());
        }

        return tools;
    }

    private String getToolApprovalDescription(String toolName) {
        String description = toolApprovalDescriptions.get(toolName);
        if (description != null) {
            return description;
        }
        return "The AI is requesting to use the tool: " + toolName + ". Do you approve?";
    }


    /**
     * Builds SSE response when child agent completes after HITL approval.
     */
    private ServerSentEvent<String> buildChildCompleteResponse(String agentName, String response) {
        Map<String, Object> data = new HashMap<>();
        data.put("messageType", "child-complete");
        data.put("agentName", agentName);
        data.put("content", response);

        try {
            String jsonData = mapper.writeValueAsString(data);
            return ServerSentEvent.<String>builder()
                    .event("child-complete")
                    .data(jsonData)
                    .build();
        } catch (Exception e) {
            logger.error("Failed to serialize child-complete response", e);
            return ServerSentEvent.<String>builder()
                    .event("child-complete")
                    .data("{\"messageType\":\"child-complete\",\"agentName\":\"" + agentName +
                            "\",\"content\":\"" + truncate(response, 100).replace("\"", "\\\"") + "\"}")
                    .build();
        }
    }


    private StreamResponse buildResponse(NodeOutput nodeOutput, String node, String agentName, Usage tokenUsage, String threadId) {
        if (nodeOutput instanceof StreamingOutput<?> streamingOutput) {
            Message message = streamingOutput.message();
            if (message == null) {
                return null;
            }

            String content = message.getText();
            String chunk = "";

            if (message instanceof AssistantMessage assistantMessage) {
                chunk = assistantMessage.hasToolCalls() ? "" : assistantMessage.getText();
            }

            // Skip events where both content and chunk are null/empty - avoids sending
            // "null" to the UI during tool-execution phases
            if ((content == null || content.isEmpty()) && (chunk == null || chunk.isEmpty())) {
                return null;
            }

            // Get accumulated session cost snapshot
            SessionCostAccumulator.SessionCostSnapshot costSnapshot = threadId != null
                    ? sessionCostAccumulator.getSnapshot(threadId)
                    : SessionCostAccumulator.SessionCostSnapshot.EMPTY;

            if (costSnapshot.inputTokens() > 0 || costSnapshot.llmCalls() > 0) {
                logger.info("SSE cost snapshot for {}: in={}, out={}, calls={}, cost={}",
                        threadId, costSnapshot.inputTokens(), costSnapshot.outputTokens(),
                        costSnapshot.llmCalls(), costSnapshot.totalCost());
            }

            return new StreamResponse(
                    node,
                    agentName,
                    message.getMessageType().name().toLowerCase(),
                    content != null ? content : "",
                    chunk != null ? chunk : "",
                    tokenUsage != null ? tokenUsage.getPromptTokens() : null,
                    tokenUsage != null ? tokenUsage.getCompletionTokens() : null,
                    costSnapshot.inputTokens(),
                    costSnapshot.outputTokens(),
                    costSnapshot.llmCalls(),
                    costSnapshot.totalCost()
            );
        }
        return null;
    }

    private ServerSentEvent<String> serializeResponse(StreamResponse response) {
        try {
            String jsonData = mapper.writeValueAsString(response);
            return ServerSentEvent.<String>builder().data(jsonData).build();
        } catch (Exception e) {
            logger.error("Failed to serialize response to JSON", e);
            return ServerSentEvent.<String>builder()
                    .data("{\"error\":\"Failed to serialize response\"}")
                    .build();
        }
    }

    /**
     * Determines if an error is a transient LLM/network failure that is safe to retry.
     * Walks the exception cause chain looking for connection resets, socket timeouts,
     * and WebClient request errors to the LLM provider.
     */
    private static boolean isTransientLlmError(Throwable error) {
        Throwable cause = error;
        while (cause != null) {
            // HITL exceptions are NOT transient - they require user action
            if (cause instanceof ChildAgentHitlException
                    || cause instanceof ChildAgentHitlRuntimeException) {
                return false;
            }
            String msg = cause.getMessage();
            if (msg != null) {
                String lower = msg.toLowerCase();
                // Connection reset by OpenAI/provider
                if (cause instanceof SocketException && lower.contains("connection reset")) {
                    return true;
                }
                // WebClient request exceptions (connection refused, timeout, reset)
                if (cause.getClass().getSimpleName().contains("WebClientRequestException")
                        && (lower.contains("connection reset") || lower.contains("connection refused")
                            || lower.contains("connection timed out"))) {
                    return true;
                }
                // HTTP 5xx from provider (rate limit, server error)
                if (lower.contains("500 internal server error")
                        || lower.contains("502 bad gateway")
                        || lower.contains("503 service unavailable")
                        || lower.contains("429 too many requests")) {
                    return true;
                }
            }
            cause = cause.getCause();
        }
        return false;
    }

    private ServerSentEvent<String> buildErrorSseEvent(Throwable error) {
        logger.error("Error occurred during agent stream execution", error);
        String errorMessage = error.getMessage() != null ? error.getMessage() : "Unknown error occurred";
        String errorType = error.getClass().getSimpleName();
        String errorJson = String.format(
                "{\"error\":true,\"errorType\":\"%s\",\"errorMessage\":\"%s\"}",
                errorType.replace("\"", "\\\""),
                errorMessage.replace("\"", "\\\"").replace("\n", "\\n")
        );
        return ServerSentEvent.<String>builder().event("error").data(errorJson).build();
    }

    private String truncate(String text, int maxLength) {
        if (text == null) return "";
        if (text.length() <= maxLength) return text;
        return text.substring(0, maxLength) + "...";
    }


    @Schema(description = "Streaming response from agent execution")
    public record StreamResponse(
            @Schema(description = "Current graph node", example = "agent")
            String node,
            @Schema(description = "Agent name", example = "orchestrator_agent")
            String agentName,
            @Schema(description = "Message type", example = "assistant")
            String messageType,
            @Schema(description = "Full content so far")
            String content,
            @Schema(description = "New chunk of content")
            String chunk,
            @Schema(description = "Prompt tokens used in this call")
            Integer promptTokens,
            @Schema(description = "Completion tokens used in this call")
            Integer completionTokens,
            @Schema(description = "Session accumulated input tokens")
            Long sessionInputTokens,
            @Schema(description = "Session accumulated output tokens")
            Long sessionOutputTokens,
            @Schema(description = "Session total LLM calls")
            Integer sessionLlmCalls,
            @Schema(description = "Session accumulated estimated cost (USD)")
            String sessionTotalCost
    ) {}

    @Schema(description = "Tool confirmation response requiring human approval (orchestrator level)")
    public record ToolConfirmResponse(
            @Schema(description = "Current graph node", example = "_AGENT_HOOK_HITL")
            String node,
            @Schema(description = "Agent name", example = "orchestrator_agent")
            String agentName,
            @Schema(description = "Message type - always 'tool-confirm' for this response", example = "tool-confirm")
            String messageType,
            @Schema(description = "List of tools pending approval")
            List<ToolConfirmInfo> pendingTools
    ) {}

    @Schema(description = "Child agent tool confirmation response requiring human approval")
    public record ChildToolConfirmResponse(
            @Schema(description = "Child agent name that requires approval", example = "oracle")
            String agentName,
            @Schema(description = "Task ID to use when resuming the child agent", example = "oracle:task:abc123")
            String taskId,
            @Schema(description = "Message type - always 'child-tool-confirm'", example = "child-tool-confirm")
            String messageType,
            @Schema(description = "List of tools pending approval")
            List<ToolConfirmInfo> pendingTools,
            @Schema(description = "Human-readable message about the approval request")
            String message
    ) {}

    @Schema(description = "Tool information for approval request")
    public record ToolConfirmInfo(
            @Schema(description = "Tool call ID - use this in resume request", example = "call_abc123")
            String id,
            @Schema(description = "Tool name", example = "delegateToKubernetesAgent")
            String name,
            @Schema(description = "Tool arguments as JSON string")
            String arguments,
            @Schema(description = "Human-readable description of what this tool will do")
            String description
    ) {}

    @Schema(description = "Synchronous invoke response containing OverAllState")
    public record InvokeResponse(
            @Schema(description = "Thread ID for this conversation", example = "550e8400-e29b-41d4-a716-446655440000")
            String threadId,
            @Schema(description = "Agent name", example = "orchestrator_agent")
            String agentName,
            @Schema(description = "Execution status", example = "completed")
            String status,
            @Schema(description = "Primary output from the agent")
            String output,
            @Schema(description = "Full state data from OverAllState")
            Map<String, Object> state,
            @Schema(description = "Error message if any")
            String error
    ) {}

    /**
     * Server-verified identity when authenticated (Entra UPN / "service"), else the
     * client-supplied userId (legacy / auth-disabled), else "anonymous". Prevents a
     * caller from impersonating another user's threads once auth is on.
     */
    private @org.jspecify.annotations.Nullable CallerContextRegistry callerContextRegistry;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setCallerContextRegistry(@org.jspecify.annotations.Nullable CallerContextRegistry registry) {
        this.callerContextRegistry = registry;
    }

    /** Snapshot the caller for this thread so delegations made later (on worker threads) are authorised against them. */
    private void bindCaller(@org.jspecify.annotations.Nullable String threadId) {
        if (callerContextRegistry != null && threadId != null) {
            callerContextRegistry.bind(threadId, CallerContext.capture());
        }
    }

    private static String effectiveUserId(String requestedUserId) {
        String principal = com.#exampleframe#.orchestrator.security.CurrentPrincipal.name();
        if (!"anonymous".equals(principal) && !"unknown".equals(principal)) {
            return principal;
        }
        return requestedUserId != null && !requestedUserId.isBlank() ? requestedUserId : "anonymous";
    }
}
