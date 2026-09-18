package com.#exampleframe#.orchestrator.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.#exampleframe#.orchestrator.config.AgentConfigResolver;
import com.#exampleframe#.orchestrator.config.OrchestratorProperties.RemoteAgentProperties;
import org.junit.jupiter.api.Test;
import org.springframework.ai.tool.ToolCallback;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A cell delegation tool must not offer the LLM a free-text {@code service} slot.
 * The slot was optional, so the model filled it when the user named no service -
 * silently narrowing a namespace-wide question to one service and reporting the
 * scoped negative as global. Scope comes from the verbatim task text, which the
 * receiving cell resolves itself; the deterministic playbook forward passes its
 * service param outside this schema entirely.
 */
class CellDelegationSchemaTest {

    /** The meta profile's shipped cell entries must not declare a service parameter. */
    @Test
    @SuppressWarnings("unchecked")
    void shippedCellConfigDeclaresNoServiceParameter() {
        InputStream in = getClass().getResourceAsStream("/application-meta.yaml");
        assertNotNull(in, "application-meta.yaml must be on the classpath");
        Map<String, Object> root = new Yaml().load(in);
        Map<String, Object> agents = (Map<String, Object>) path(root,
                "#exampleframe#", "orchestrator", "remote-agents");
        assertNotNull(agents, "remote-agents must exist in the meta profile");
        assertFalse(agents.isEmpty(), "meta profile must declare cells");
        for (Map.Entry<String, Object> cell : agents.entrySet()) {
            Map<String, Object> cfg = (Map<String, Object>) cell.getValue();
            Map<String, Object> params = (Map<String, Object>) cfg.get("parameters");
            assertNotNull(params, cell.getKey() + " must declare parameters");
            assertTrue(params.containsKey("task"), cell.getKey() + " must declare 'task'");
            assertFalse(params.containsKey("service"),
                    cell.getKey() + " must NOT declare a free-text 'service' parameter - "
                    + "the cell derives scope from the task text");
        }
    }

    /** Door-realistic: the schema the LLM actually sees has no service property and is closed. */
    @Test
    void builtToolSchemaIsClosedAndHasNoServiceSlot() throws Exception {
        RemoteAgentProperties cell = new RemoteAgentProperties(
                "http://localhost:8081", "orchestrator_agent__claims", "Claims cell",
                60, 3, List.of("full-domain-investigation"), "domain", 1,
                "Any claims investigation", "delegateToClaimsCell",
                Map.of("task", "The investigation to run in the claims cell",
                       "timeRange", "Time window the user stated, verbatim"),
                Map.of(), Map.of(), List.of(), Map.of());

        AgentConfigResolver resolver = mock(AgentConfigResolver.class);
        when(resolver.getAgentConfigs()).thenReturn(Map.of("claims-cell", cell));

        DynamicDelegationToolFactory factory = new DynamicDelegationToolFactory(
                resolver, Map.of(), null, null, null);
        List<ToolCallback> tools = factory.getDelegationToolCallbacks();
        assertEquals(1, tools.size(), "one cell -> one delegation tool");

        JsonNode schema = new ObjectMapper().readTree(
                tools.get(0).getToolDefinition().inputSchema());
        assertFalse(schema.get("properties").has("service"),
                "no service property in the LLM-facing schema");
        assertTrue(schema.get("properties").has("task"));
        assertTrue(schema.get("properties").has("timeRange"));
        assertFalse(schema.get("additionalProperties").asBoolean(),
                "schema must be closed - and the callback drops undeclared keys besides");
        JsonNode required = schema.get("required");
        assertEquals(1, required.size());
        assertEquals("task", required.get(0).asText());
    }

    /**
     * additionalProperties:false is provider-best-effort - some providers pass undeclared
     * keys straight through. The callback is the choke point: an undeclared 'service' on a
     * cell tool must be dropped before it can re-scope the delegation, while declared keys
     * and the built-ins pass.
     */
    @Test
    void callbackDropsUndeclaredKeysBeforeDelegating() throws Exception {
        RemoteAgentProperties cell = new RemoteAgentProperties(
                "http://localhost:8081", "orchestrator_agent__claims", "Claims cell",
                60, 3, List.of("full-domain-investigation"), "domain", 1,
                "Any claims investigation", "delegateToClaimsCell",
                Map.of("task", "The investigation to run in the claims cell",
                       "timeRange", "Time window the user stated, verbatim"),
                Map.of(), Map.of(), List.of(), Map.of());

        AgentConfigResolver resolver = mock(AgentConfigResolver.class);
        when(resolver.getAgentConfigs()).thenReturn(Map.of("claims-cell", cell));

        com.#exampleframe#.orchestrator.service.DelegationExecutionService delegation =
                mock(com.#exampleframe#.orchestrator.service.DelegationExecutionService.class);
        com.#exampleframe#.orchestrator.service.InvestigationStateService state =
                mock(com.#exampleframe#.orchestrator.service.InvestigationStateService.class);
        when(state.resolveCurrentThreadId()).thenReturn("t-1");
        when(delegation.delegateToAgent(org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any()))
                .thenReturn("ok");

        DynamicDelegationToolFactory factory = new DynamicDelegationToolFactory(
                resolver, Map.of(), null, state, delegation);
        ToolCallback tool = factory.getDelegationToolCallbacks().get(0);

        tool.call("{\"task\":\"list pods in the default namespace\","
                + "\"service\":\"esign\",\"timeRange\":\"2h\"}");

        @SuppressWarnings("unchecked")
        org.mockito.ArgumentCaptor<Map<String, String>> paramsCaptor =
                org.mockito.ArgumentCaptor.forClass(Map.class);
        org.mockito.Mockito.verify(delegation).delegateToAgent(
                org.mockito.ArgumentMatchers.eq("claims-cell"),
                org.mockito.ArgumentMatchers.eq("list pods in the default namespace"),
                org.mockito.ArgumentMatchers.eq("t-1"),
                paramsCaptor.capture(), org.mockito.ArgumentMatchers.isNull());
        Map<String, String> forwarded = paramsCaptor.getValue();
        assertFalse(forwarded.containsKey("service"),
                "undeclared 'service' must be dropped at the callback");
        assertEquals("2h", forwarded.get("timeRange"), "declared keys must pass");
        assertTrue(forwarded.containsKey("task"));
    }

    private static Object path(Map<String, Object> root, String... keys) {
        Object cur = root;
        for (String k : keys) {
            if (!(cur instanceof Map<?, ?> m)) return null;
            cur = m.get(k);
        }
        return cur;
    }
}
