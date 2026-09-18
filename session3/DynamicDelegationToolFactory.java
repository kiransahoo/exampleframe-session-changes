package com.#exampleframe#.orchestrator.tools;

import com.alibaba.cloud.ai.graph.agent.a2a.A2aRemoteAgent;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.#exampleframe#.orchestrator.config.AgentConfigResolver;
import com.#exampleframe#.orchestrator.config.OrchestratorProperties.RemoteAgentProperties;
import com.#exampleframe#.orchestrator.service.DelegationExecutionService;
import com.#exampleframe#.orchestrator.service.InvestigationStateService;
import jakarta.annotation.PostConstruct;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.ai.tool.definition.ToolDefinition;
import org.springframework.ai.tool.execution.ToolExecutionException;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

import static com.#exampleframe#.orchestrator.tools.ToolFormatUtils.*;

/**
 * Dynamically generates {@link ToolCallback} instances for each remote agent
 * defined in {@code #exampleframe#.orchestrator.remote-agents.*} YAML configuration.
 * <p>
 * Adding a new agent requires only a YAML config block with {@code tool-name},
 * {@code parameters}, and {@code parameter-defaults}. No Java code changes needed.
 * <p>
 * Each generated tool:
 * <ul>
 *   <li>Has a name from {@code tool-name} (e.g., {@code delegateToKubernetesAgent})</li>
 *   <li>Has a description composed from agent config fields</li>
 *   <li>Has a JSON schema built from the {@code parameters} map</li>
 *   <li>Applies {@code parameter-defaults} for missing/blank values at invocation time</li>
 *   <li>Delegates to {@link AgentInvocationService#invokeAgentWithTracking}</li>
 * </ul>
 */
@Component
public class DynamicDelegationToolFactory {

    private static final Logger logger = LoggerFactory.getLogger(DynamicDelegationToolFactory.class);

    private final AgentConfigResolver agentConfigResolver;
    private final Map<String, A2aRemoteAgent> remoteAgents;
    private final AgentInvocationService invocationService;
    private final InvestigationStateService investigationStateService;
    private final DelegationExecutionService delegationExecutionService;
    private final ObjectMapper objectMapper;
    private final List<ToolCallback> delegationCallbacks;

    public DynamicDelegationToolFactory(
            AgentConfigResolver agentConfigResolver,
            @Nullable Map<String, A2aRemoteAgent> remoteAgents,
            AgentInvocationService invocationService,
            InvestigationStateService investigationStateService,
            DelegationExecutionService delegationExecutionService) {
        this.agentConfigResolver = agentConfigResolver;
        this.remoteAgents = remoteAgents != null ? remoteAgents : Map.of();
        this.invocationService = invocationService;
        this.investigationStateService = investigationStateService;
        this.delegationExecutionService = delegationExecutionService;
        this.objectMapper = new ObjectMapper();
        this.delegationCallbacks = buildDelegationTools();
    }

    @PostConstruct
    public void init() {
        logger.info("DynamicDelegationToolFactory created {} delegation tools: {}",
                delegationCallbacks.size(),
                delegationCallbacks.stream()
                        .map(cb -> cb.getToolDefinition().name())
                        .collect(Collectors.joining(", ")));
    }

    /**
     * Returns the list of dynamically generated delegation tool callbacks.
     */
    public List<ToolCallback> getDelegationToolCallbacks() {
        return Collections.unmodifiableList(delegationCallbacks);
    }


    private List<ToolCallback> buildDelegationTools() {
        List<ToolCallback> callbacks = new ArrayList<>();
        Map<String, RemoteAgentProperties> agents = agentConfigResolver.getAgentConfigs();

        if (agents.isEmpty()) {
            logger.warn("No remote agents configured - no delegation tools will be created");
            return callbacks;
        }

        for (Map.Entry<String, RemoteAgentProperties> entry : agents.entrySet()) {
            String agentKey = entry.getKey();
            RemoteAgentProperties config = entry.getValue();

            if (config.toolName() == null || config.toolName().isBlank()) {
                logger.debug("Skipping agent '{}': no toolName configured", agentKey);
                continue;
            }

            try {
                ToolCallback callback = createDelegationCallback(agentKey, config);
                callbacks.add(callback);
                logger.info("Created delegation tool '{}' for agent '{}' (layer: {})",
                        config.toolName(), agentKey, config.layer());
            } catch (Exception e) {
                logger.error("Failed to create delegation tool for agent '{}': {}", agentKey, e.getMessage(), e);
            }
        }

        return callbacks;
    }

    private ToolCallback createDelegationCallback(String agentKey, RemoteAgentProperties config) {
        String description = buildToolDescription(agentKey, config);
        String inputSchema = buildInputSchema(config);

        ToolDefinition toolDef = ToolDefinition.builder()
                .name(config.toolName())
                .description(description)
                .inputSchema(inputSchema)
                .build();

        return new DynamicDelegationToolCallback(
                toolDef, agentKey, config, remoteAgents, invocationService,
                investigationStateService, delegationExecutionService, objectMapper);
    }


    private String buildToolDescription(String agentKey, RemoteAgentProperties config) {
        StringBuilder desc = new StringBuilder();

        desc.append("Delegates an investigation task or remediation action to the ")
                .append(capitalizeFirst(agentKey)).append(" agent.\n\n");

        if (config.description() != null) {
            desc.append(config.description()).append("\n\n");
        }

        if (config.whenToUse() != null && !config.whenToUse().isBlank()) {
            desc.append("**When to use:** ").append(config.whenToUse().strip()).append("\n\n");
        }

        if (config.capabilities() != null && !config.capabilities().isEmpty()) {
            desc.append("**Capabilities:** ").append(String.join(", ", config.capabilities())).append("\n\n");
        }

        // Add example queries (limited to keep description manageable for LLM)
        if (config.exampleQueries() != null && !config.exampleQueries().isEmpty()) {
            desc.append("**Example queries:**\n");
            int exampleCount = 0;
            for (Map.Entry<String, List<String>> category : config.exampleQueries().entrySet()) {
                for (String example : category.getValue()) {
                    if (exampleCount >= 8) break;
                    desc.append("- \"").append(example).append("\"\n");
                    exampleCount++;
                }
                if (exampleCount >= 8) break;
            }
            desc.append("\n");
        }

        return desc.toString().trim();
    }


    private String buildInputSchema(RemoteAgentProperties config) {
        try {
            ObjectNode schema = objectMapper.createObjectNode();
            schema.put("type", "object");

            ObjectNode props = schema.putObject("properties");
            Map<String, String> params = config.parameters();
            if (params != null) {
                for (Map.Entry<String, String> param : params.entrySet()) {
                    ObjectNode prop = props.putObject(param.getKey());
                    prop.put("type", "string");
                    prop.put("description", param.getValue());
                }
            }

            // Built-in: priorFindings parameter for cross-agent context propagation.
            // This lets the orchestrator pass relevant findings from previously queried
            // agents so the child agent can focus its investigation.
            ObjectNode priorFindingsParam = props.putObject("priorFindings");
            priorFindingsParam.put("type", "string");
            priorFindingsParam.put("description",
                    "Key findings from previously queried agents. CRITICAL: You MUST preserve " +
                    "ALL fully-qualified identifiers EXACTLY as they appear - never shorten or " +
                    "abbreviate them. This includes: fully-qualified Java class.method names " +
                    "(fully-qualified class.method from traces), database " +
                    "table/schema names, SQL IDs, Kubernetes pod/" +
                    "namespace names, error codes, and exact performance numbers (P50/P95/P99). " +
                    "These identifiers are used by downstream agents for telemetry lookups - " +
                    "shortened names cause lookup failures. " +
                    "Example: 'The main hotspot has P95=4200ms, 80% of latency in SQL dependency calls to " +
                    "TRANSACTIONS table. Kubernetes pods healthy, no restarts. Evidence points to database.' " +
                    "Leave empty if this is the first agent being queried.");

            // 'task' is always required; scope-parameters are also required
            ArrayNode required = schema.putArray("required");
            required.add("task");
            if (config.scopeParameters() != null) {
                for (String scopeParam : config.scopeParameters()) {
                    if (!"task".equals(scopeParam)) {
                        required.add(scopeParam);
                    }
                }
            }

            schema.put("additionalProperties", false);

            return objectMapper.writeValueAsString(schema);
        } catch (Exception e) {
            logger.error("Failed to build input schema, using fallback: {}", e.getMessage());
            return "{\"type\":\"object\",\"properties\":{\"task\":{\"type\":\"string\"," +
                    "\"description\":\"The investigation query\"}},\"required\":[\"task\"]}";
        }
    }


    /**
     * A {@link ToolCallback} that delegates to a remote agent via A2A protocol.
     * Created dynamically from YAML configuration - one instance per agent.
     * <p>
     * Records agent results to {@link InvestigationStateService} for the evidence board,
     * and supports a built-in {@code priorFindings} parameter for cross-agent context propagation.
     */
    private static class DynamicDelegationToolCallback implements ToolCallback {

        private static final Logger logger = LoggerFactory.getLogger(DynamicDelegationToolCallback.class);
        private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

        private final ToolDefinition toolDefinition;
        private final String agentKey;
        private final RemoteAgentProperties config;
        private final Map<String, A2aRemoteAgent> remoteAgents;
        private final AgentInvocationService invocationService;
        private final InvestigationStateService investigationStateService;
        private final DelegationExecutionService delegationExecutionService;
        private final ObjectMapper objectMapper;

        DynamicDelegationToolCallback(
                ToolDefinition toolDefinition,
                String agentKey,
                RemoteAgentProperties config,
                Map<String, A2aRemoteAgent> remoteAgents,
                AgentInvocationService invocationService,
                InvestigationStateService investigationStateService,
                DelegationExecutionService delegationExecutionService,
                ObjectMapper objectMapper) {
            this.toolDefinition = toolDefinition;
            this.agentKey = agentKey;
            this.config = config;
            this.remoteAgents = remoteAgents;
            this.invocationService = invocationService;
            this.investigationStateService = investigationStateService;
            this.delegationExecutionService = delegationExecutionService;
            this.objectMapper = objectMapper;
        }

        @Override
        public ToolDefinition getToolDefinition() {
            return toolDefinition;
        }

        @Override
        public String call(String toolInput) {
            logger.info("[{}] Dynamic delegation tool invoked", agentKey);
            try {
                Map<String, Object> params = objectMapper.readValue(toolInput, MAP_TYPE);
                String task = asString(params.get("task"));
                if (task == null || task.isBlank()) {
                    return "ERROR: 'task' parameter is required for " +
                            capitalizeFirst(agentKey) + " agent delegation.";
                }
                // Convert Map<String, Object> to Map<String, String>, keeping ONLY keys this
                // tool's schema declares. additionalProperties:false is enforced by the
                // provider at best-effort; some providers pass undeclared keys straight
                // through, and an undeclared free-text key (e.g. a hallucinated 'service' on
                // a cell tool) would silently re-scope the delegation. Dropping here makes
                // the schema the contract regardless of provider enforcement - and means a
                // 'service' param on a cell delegation can only originate from deterministic
                // code paths (the playbook forward), never from the model.
                Map<String, String> declared = config.parameters() != null
                        ? config.parameters() : Map.of();
                Map<String, String> paramMap = new HashMap<>();
                for (Map.Entry<String, Object> entry : params.entrySet()) {
                    String val = asString(entry.getValue());
                    if (val == null) {
                        continue;
                    }
                    if (!declared.containsKey(entry.getKey())
                            && !"task".equals(entry.getKey())
                            && !"priorFindings".equals(entry.getKey())) {
                        logger.warn("[{}] Dropping undeclared tool argument '{}' - not in the "
                                + "delegation schema", agentKey, entry.getKey());
                        continue;
                    }
                    paramMap.put(entry.getKey(), val);
                }
                String threadId = investigationStateService.resolveCurrentThreadId();
                return delegationExecutionService.delegateToAgent(
                        agentKey, task, threadId, paramMap, asString(params.get("priorFindings")));
            } catch (com.#exampleframe#.orchestrator.exception.ChildAgentHitlException e) {
                throw new ToolExecutionException(toolDefinition, e);
            } catch (Exception e) {
                logger.error("[{}] Error in dynamic delegation: {}", agentKey, e.getMessage(), e);
                return "ERROR: Failed to invoke " + capitalizeFirst(agentKey) +
                        " agent: " + e.getMessage();
            }
        }

        private static String asString(Object value) {
            if (value == null) return null;
            String str = String.valueOf(value);
            return "null".equals(str) ? null : str;
        }
    }
}
