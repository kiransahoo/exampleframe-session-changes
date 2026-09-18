package com.#exampleframe#.orchestrator.api.controller;

import com.#exampleframe#.orchestrator.config.AgentConfigResolver;
import com.#exampleframe#.orchestrator.config.OrchestratorProperties;
import com.#exampleframe#.orchestrator.config.PlaybookProperties.PlaybookDefinition;
import com.#exampleframe#.orchestrator.incident.InvestigationRunner;
import com.#exampleframe#.orchestrator.incident.OrchestratorInvestigationRunner;
import com.#exampleframe#.orchestrator.service.InvestigationStateService;
import com.#exampleframe#.orchestrator.service.PlaybookExecutor;
import com.#exampleframe#.orchestrator.service.PlaybookResolver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * The delegated door's deterministic pre-route: same engine the UI door tries first.
 * First turn + strong match + all required params known -> the playbook runs blocking;
 * anything less (follow-up turn, soft match, missing params, no match) -> ReAct, and a
 * caller-supplied contextId maps to a STABLE cell threadId so conversation state survives
 * across delegated turns.
 */
@ExtendWith(MockitoExtension.class)
class A2aIngressPlaybookTest {

    @Mock OrchestratorInvestigationRunner runner;
    @Mock AgentConfigResolver agentConfigResolver;
    @Mock PlaybookResolver resolver;
    @Mock PlaybookExecutor executor;
    @Mock com.alibaba.cloud.ai.graph.checkpoint.BaseCheckpointSaver checkpointSaver;
    @Mock com.#exampleframe#.orchestrator.thread.history.store.ThreadStore threadStore;

    private A2aIngressController ingress;
    private InvestigationStateService state;

    private static final PlaybookDefinition DEFINITION = new PlaybookDefinition(
            "Service Performance Investigation", "desc", null, null, null, null, null, null, null, null);

    @BeforeEach
    void setUp() {
        state = new InvestigationStateService(agentConfigResolver, null);
        ingress = new A2aIngressController(runner, new OrchestratorProperties(), null, state, null,
                resolver, executor, checkpointSaver, threadStore, null /* costAccumulator */);
    }

    private static Map<String, Object> message(String text, Map<String, Object> extraParams) {
        Map<String, Object> params = new java.util.HashMap<>();
        params.put("message", Map.of("parts", List.of(Map.of("type", "text", "text", text))));
        params.putAll(extraParams);
        return Map.of("id", "req-1", "params", params);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> result(Map<String, Object> body) {
        return (Map<String, Object>) body.get("result");
    }

    @Test
    void strongMatchWithAllParamsRunsThePlaybookAndKeepsAStableThread() throws Exception {
        Map<String, String> params = Map.of("service", "claims-service", "timeRange", "2h");
        when(resolver.resolve(eq("why is claims-service slow"), anyMap())).thenReturn(Optional.of(
                new PlaybookResolver.Resolution("service-slow", DEFINITION, false, params, List.of())));
        when(executor.executeBlocking(eq(DEFINITION), eq(params), anyString(), eq("why is claims-service slow")))
                .thenReturn("# RCA report");

        Map<String, Object> request = message("[Service: claims-service] why is claims-service slow",
                Map.of("contextId", "meta-thread-1",
                        "rawTask", "why is claims-service slow",
                        "taskParams", Map.of("service", "claims-service", "timeRange", "2h")));
        Map<String, Object> done = result(ingress.handleMessage(request).getBody());

        assertThat(done.get("status")).isEqualTo("completed");
        assertThat(done.get("output")).isEqualTo("# RCA report");
        // Stable, cell-scoped thread derived from the contextId - not a random UUID
        assertThat((String) done.get("taskId")).isEqualTo("a2a-meta-thread-1");
        verifyNoInteractions(runner);

        ArgumentCaptor<String> threadCaptor = ArgumentCaptor.forClass(String.class);
        verify(executor).executeBlocking(any(), anyMap(), threadCaptor.capture(), anyString());
        assertThat(threadCaptor.getValue()).isEqualTo("a2a-meta-thread-1");
    }

    @Test
    void followUpTurnOnTheSameContextGoesToReactWithTheSameThread() throws Exception {
        // Turn 1 created state for the derived thread
        state.initThread("a2a-meta-thread-1");
        when(runner.investigate(any(InvestigationRunner.InvestigationTask.class))).thenReturn("continued");

        Map<String, Object> done = result(ingress.handleMessage(
                message("and what about the database?", Map.of("contextId", "meta-thread-1"))).getBody());

        assertThat(done.get("status")).isEqualTo("completed");
        assertThat((String) done.get("taskId")).isEqualTo("a2a-meta-thread-1");
        verifyNoInteractions(executor);
        verify(resolver, never()).resolve(anyString(), anyMap());

        ArgumentCaptor<InvestigationRunner.InvestigationTask> task =
                ArgumentCaptor.forClass(InvestigationRunner.InvestigationTask.class);
        verify(runner).investigate(task.capture());
        assertThat(task.getValue().threadId()).isEqualTo("a2a-meta-thread-1");
    }

    @Test
    void missingRequiredParamsFallBackToReact() throws Exception {
        when(resolver.resolve(anyString(), anyMap())).thenReturn(Optional.of(
                new PlaybookResolver.Resolution("service-slow", DEFINITION, false, Map.of("service", "x"),
                        List.of(new PlaybookResolver.MissingParam("timeRange", "How far back?", false)))));
        when(runner.investigate(any(InvestigationRunner.InvestigationTask.class))).thenReturn("react answer");

        Map<String, Object> done = result(ingress.handleMessage(
                message("investigate x being slow", Map.of("contextId", "meta-thread-2"))).getBody());

        assertThat(done.get("output")).isEqualTo("react answer");
        verifyNoInteractions(executor);
    }

    @Test
    void followUpTurnWithAForwardedPlaybookIdStillRunsThePlaybook() throws Exception {
        // The meta's playbook door forwards deterministically and says so via taskParams.playbookId.
        // The first-turn gate must not demote that to ReAct just because this conversation
        // touched the cell before - and the ID, not text matching, selects the playbook.
        state.initThread("a2a-meta-thread-1");   // prior turn exists
        Map<String, String> params = Map.of("service", "claims-service", "timeRange", "2h");
        when(resolver.resolveById(eq("service-slow"), eq("run the service-slow playbook for claims-service"), anyMap()))
                .thenReturn(Optional.of(
                        new PlaybookResolver.Resolution("service-slow", DEFINITION, false, params, List.of())));
        when(executor.executeBlocking(eq(DEFINITION), eq(params), anyString(), anyString()))
                .thenReturn("# RCA report");

        Map<String, Object> done = result(ingress.handleMessage(
                message("run the service-slow playbook for claims-service",
                        Map.of("contextId", "meta-thread-1",
                                "rawTask", "run the service-slow playbook for claims-service",
                                "taskParams", Map.of("playbookId", "service-slow",
                                        "service", "claims-service", "timeRange", "2h")))).getBody());

        assertThat(done.get("status")).isEqualTo("completed");
        assertThat(done.get("output")).isEqualTo("# RCA report");
        verifyNoInteractions(runner);
        verify(resolver, never()).resolve(anyString(), anyMap());
    }

    @Test
    void followUpTurnWithTheDeterministicForwardMarkerRunsTheCellsOwnRouting() throws Exception {
        // A meta's SCORED forward pins no playbookId - the cell routes the text itself -
        // but it does mark the turn as a deliberate forward. The first-turn gate must
        // honor the marker, or every scored forward after turn 1 silently becomes ReAct.
        state.initThread("a2a-meta-thread-1");   // prior turn exists
        Map<String, String> params = Map.of("service", "claims-service", "timeRange", "2h");
        when(resolver.resolve(eq("why is claims-service slow over the last 2h"), anyMap()))
                .thenReturn(Optional.of(
                        new PlaybookResolver.Resolution("service-slow", DEFINITION, false, params, List.of())));
        when(executor.executeBlocking(eq(DEFINITION), eq(params), anyString(), anyString()))
                .thenReturn("# RCA report");

        Map<String, Object> done = result(ingress.handleMessage(
                message("why is claims-service slow over the last 2h",
                        Map.of("contextId", "meta-thread-1",
                                "rawTask", "why is claims-service slow over the last 2h",
                                "taskParams", Map.of("deterministicForward", "true",
                                        "service", "claims-service", "timeRange", "2h")))).getBody());

        assertThat(done.get("status")).isEqualTo("completed");
        assertThat(done.get("output")).isEqualTo("# RCA report");
        verifyNoInteractions(runner);   // playbook path, not ReAct
        verify(resolver).resolve(anyString(), anyMap());   // the CELL's router picked, no pin
    }

    @Test
    void forwardedPlaybookIdSelectsThePlaybookEvenWhenTheTextNamesAnotherOne() throws Exception {
        // Config drift or overlapping references: the ID pins the selection; text matching
        // never runs, so it cannot silently execute a different playbook than the one named.
        Map<String, String> params = Map.of("service", "claims-service", "timeRange", "2h");
        when(resolver.resolveById(eq("service-errors"), anyString(), anyMap()))
                .thenReturn(Optional.of(
                        new PlaybookResolver.Resolution("service-errors", DEFINITION, false, params, List.of())));
        when(executor.executeBlocking(eq(DEFINITION), eq(params), anyString(), anyString()))
                .thenReturn("# errors report");

        Map<String, Object> done = result(ingress.handleMessage(
                message("run the service-slow playbook for claims-service",
                        Map.of("contextId", "meta-thread-7",
                                "rawTask", "run the service-slow playbook for claims-service",
                                "taskParams", Map.of("playbookId", "service-errors",
                                        "service", "claims-service", "timeRange", "2h")))).getBody());

        assertThat(done.get("output")).isEqualTo("# errors report");
        verify(resolver, never()).resolve(anyString(), anyMap());
        verifyNoInteractions(runner);
    }

    @Test
    void unknownForwardedPlaybookIdDefersToReactInsteadOfTextMatching() throws Exception {
        when(resolver.resolveById(eq("no-such-playbook"), anyString(), anyMap()))
                .thenReturn(Optional.empty());
        when(runner.investigate(any(InvestigationRunner.InvestigationTask.class))).thenReturn("react answer");

        Map<String, Object> done = result(ingress.handleMessage(
                message("run the mystery playbook",
                        Map.of("contextId", "meta-thread-8",
                                "rawTask", "run the mystery playbook",
                                "taskParams", Map.of("playbookId", "no-such-playbook")))).getBody());

        assertThat(done.get("output")).isEqualTo("react answer");
        verifyNoInteractions(executor);
        verify(resolver, never()).resolve(anyString(), anyMap());
    }

    @Test
    void softMatchFallsBackToReact() throws Exception {
        when(resolver.resolve(anyString(), anyMap())).thenReturn(Optional.of(
                new PlaybookResolver.Resolution("service-slow", DEFINITION, true, Map.of(), List.of())));
        when(runner.investigate(any(InvestigationRunner.InvestigationTask.class))).thenReturn("react answer");

        Map<String, Object> done = result(ingress.handleMessage(
                message("something vague", Map.of("contextId", "meta-thread-3"))).getBody());

        assertThat(done.get("output")).isEqualTo("react answer");
        verifyNoInteractions(executor);
    }

    @Test
    void noContextIdMintsAFreshRandomThreadAndSkipsNothing() throws Exception {
        lenient().when(resolver.resolve(anyString(), anyMap())).thenReturn(Optional.empty());
        when(runner.investigate(any(InvestigationRunner.InvestigationTask.class))).thenReturn("ok");

        Map<String, Object> done = result(ingress.handleMessage(message("hello", Map.of())).getBody());

        String taskId = (String) done.get("taskId");
        assertThat(taskId).startsWith("a2a-");
        assertThat(taskId).hasSizeGreaterThan(20); // random UUID form, not a derived key
    }

    @Test
    void unsafeContextIdIsHashedNotUsedVerbatim() throws Exception {
        when(resolver.resolve(anyString(), anyMap())).thenReturn(Optional.empty());
        when(runner.investigate(any(InvestigationRunner.InvestigationTask.class))).thenReturn("ok");

        Map<String, Object> done = result(ingress.handleMessage(
                message("hi", Map.of("contextId", "we/ird id\nwith junk"))).getBody());

        String taskId = (String) done.get("taskId");
        assertThat(taskId).matches("a2a-[0-9a-f]{16}");
    }

    @Test
    void overlappingRequestOnTheSameConversationIsRejected() throws Exception {
        // Simulate a long-running first request by re-entering from within the runner
        when(runner.investigate(any(InvestigationRunner.InvestigationTask.class))).thenAnswer(inv -> {
            Map<String, Object> second = ingress.handleMessage(
                    message("retry of the same conversation", Map.of("contextId", "meta-thread-9"))).getBody();
            assertThat(second).containsKey("error");
            assertThat(second.get("error").toString()).contains("already running");
            return "first answer";
        });

        Map<String, Object> done = result(ingress.handleMessage(
                message("first request", Map.of("contextId", "meta-thread-9"))).getBody());
        assertThat(done.get("output")).isEqualTo("first answer");
    }

    @Test
    void checkpointedHistorySurvivingAStateRestartStillGatesToReact() throws Exception {
        // In-memory state is gone (restart / TTL) but the checkpoint saver remembers the thread
        when(checkpointSaver.get(any(com.alibaba.cloud.ai.graph.RunnableConfig.class)))
                .thenReturn(Optional.of(org.mockito.Mockito.mock(
                        com.alibaba.cloud.ai.graph.checkpoint.Checkpoint.class)));
        when(runner.investigate(any(InvestigationRunner.InvestigationTask.class))).thenReturn("continued");

        Map<String, Object> done = result(ingress.handleMessage(
                message("claims-api is slow again, last 2h", Map.of("contextId", "meta-thread-42"))).getBody());

        assertThat(done.get("output")).isEqualTo("continued");
        verifyNoInteractions(executor);
        verify(resolver, never()).resolve(anyString(), anyMap());
    }

    @Test
    void playbookTurnIsPersistedAsADurableFirstTurnMarker() throws Exception {
        when(resolver.resolve(anyString(), anyMap())).thenReturn(Optional.of(
                new PlaybookResolver.Resolution("service-slow", DEFINITION, false,
                        Map.of("service", "claims-service", "timeRange", "2h"), List.of())));
        when(executor.executeBlocking(any(), anyMap(), anyString(), anyString())).thenReturn("# report");

        ingress.handleMessage(message("why is claims-service slow, last 2h",
                Map.of("contextId", "meta-thread-77")));

        // Playbooks bypass the graph and write no checkpoint - the thread-history store
        // carries the durable marker (and the conversation's recorded turn).
        verify(threadStore).getOrCreateThread(
                com.#exampleframe#.orchestrator.thread.history.model.ThreadKey.of(
                        "orchestrator_agent", "a2a-ingress", "a2a-meta-thread-77"));
        verify(threadStore, org.mockito.Mockito.times(2)).appendMessage(
                eq(com.#exampleframe#.orchestrator.thread.history.model.ThreadKey.of(
                        "orchestrator_agent", "a2a-ingress", "a2a-meta-thread-77")),
                any(com.#exampleframe#.orchestrator.thread.history.model.Message.class));
    }

    @Test
    void persistedPlaybookTurnAloneGatesTheNextMessageToReact() throws Exception {
        // No in-memory state, no checkpoint - only the thread-history marker survives
        when(threadStore.threadExists(
                com.#exampleframe#.orchestrator.thread.history.model.ThreadKey.of(
                        "orchestrator_agent", "a2a-ingress", "a2a-meta-thread-88"))).thenReturn(true);
        when(runner.investigate(any(InvestigationRunner.InvestigationTask.class))).thenReturn("continued");

        Map<String, Object> done = result(ingress.handleMessage(
                message("claims-api is slow again, last 2h", Map.of("contextId", "meta-thread-88"))).getBody());

        assertThat(done.get("output")).isEqualTo("continued");
        verifyNoInteractions(executor);
        verify(resolver, never()).resolve(anyString(), anyMap());
    }

    // ---- input_required: the delegated door's answer to missing user-only params ----

    /**
     * A deterministic forward whose playbook lacks required values must NOT silently
     * degrade to ReAct: the forwarding meta can relay a question to the user, so the
     * cell answers input_required naming the keys - the pending_approval contract's
     * sibling - and the meta re-forwards with the reply merged.
     */
    @Test
    @SuppressWarnings("unchecked")
    void missingParamsOnADeterministicForwardReturnInputRequired() throws Exception {
        when(resolver.resolve(eq("why is esign slow"), anyMap())).thenReturn(Optional.of(
                new PlaybookResolver.Resolution("service-slow", DEFINITION, false,
                        Map.of("service", "esign", "timeRange", "2h"),
                        List.of(new PlaybookResolver.MissingParam(
                                "schema", "Which Oracle schema?", false)))));

        Map<String, Object> request = message("why is esign slow",
                Map.of("contextId", "meta-thread-ir",
                        "rawTask", "why is esign slow",
                        "taskParams", Map.of("deterministicForward", "true", "acceptsInputRequired", "true")));
        Map<String, Object> res = result(ingress.handleMessage(request).getBody());

        assertThat(res.get("status")).isEqualTo("input_required");
        assertThat((List<String>) res.get("missingKeys")).containsExactly("schema");
        assertThat(res.get("playbookId")).isEqualTo("service-slow");
        assertThat(res.get("playbookName")).isEqualTo("Service Performance Investigation");
        assertThat((List<Map<String, String>>) res.get("missingParams"))
                .containsExactly(Map.of("key", "schema", "prompt", "Which Oracle schema?"));
        assertThat(String.valueOf(res.get("output"))).contains("schema");
        verifyNoInteractions(executor);
        verify(runner, never()).investigate(any());
    }

    /**
     * Version skew: an OLDER meta sends the marker but not the capability param, and a
     * pin-only third party sets the marker via the pin alone - both must keep the ReAct
     * fallback, because they render an unknown status as garbage instead of a prompt.
     */
    @Test
    void markerWithoutTheCapabilityParamKeepsTheReActFallback() throws Exception {
        when(resolver.resolve(eq("why is esign slow"), anyMap())).thenReturn(Optional.of(
                new PlaybookResolver.Resolution("service-slow", DEFINITION, false,
                        Map.of("service", "esign"),
                        List.of(new PlaybookResolver.MissingParam(
                                "schema", "Which Oracle schema?", false)))));
        when(runner.investigate(any(InvestigationRunner.InvestigationTask.class)))
                .thenReturn("react answer");

        Map<String, Object> request = message("why is esign slow",
                Map.of("contextId", "old-meta-1",
                        "rawTask", "why is esign slow",
                        "taskParams", Map.of("deterministicForward", "true")));
        Map<String, Object> res = result(ingress.handleMessage(request).getBody());

        assertThat(res.get("status")).isEqualTo("completed");
        assertThat(res.get("output")).isEqualTo("react answer");
        verifyNoInteractions(executor);
    }

    /** A plain A2A caller has no user to relay a prompt to - the ReAct fallback stands. */
    @Test
    void missingParamsWithoutTheMarkerStillDeferToReAct() throws Exception {
        when(resolver.resolve(eq("why is esign slow"), anyMap())).thenReturn(Optional.of(
                new PlaybookResolver.Resolution("service-slow", DEFINITION, false,
                        Map.of("service", "esign"),
                        List.of(new PlaybookResolver.MissingParam(
                                "schema", "Which Oracle schema?", false)))));
        when(runner.investigate(any(InvestigationRunner.InvestigationTask.class)))
                .thenReturn("react answer");

        Map<String, Object> request = message("why is esign slow",
                Map.of("contextId", "plain-caller-1", "rawTask", "why is esign slow"));
        Map<String, Object> res = result(ingress.handleMessage(request).getBody());

        assertThat(res.get("status")).isEqualTo("completed");
        assertThat(res.get("output")).isEqualTo("react answer");
        verifyNoInteractions(executor);
    }
}
