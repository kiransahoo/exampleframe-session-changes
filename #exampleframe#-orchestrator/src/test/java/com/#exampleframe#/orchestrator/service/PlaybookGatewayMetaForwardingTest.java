package com.#exampleframe#.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.#exampleframe#.orchestrator.config.AgentConfigResolver;
import com.#exampleframe#.orchestrator.config.AppContextResolver;
import com.#exampleframe#.orchestrator.config.PlaybookProperties;
import com.#exampleframe#.orchestrator.config.PlaybookProperties.ParamDefinition;
import com.#exampleframe#.orchestrator.config.PlaybookProperties.ParamRef;
import com.#exampleframe#.orchestrator.config.PlaybookProperties.PlaybookDefinition;
import com.#exampleframe#.orchestrator.exception.ChildAgentHitlException;
import com.#exampleframe#.orchestrator.federation.DomainCellRouter;
import com.#exampleframe#.orchestrator.federation.FederationProperties;
import com.#exampleframe#.orchestrator.thread.history.store.ThreadStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.model.Generation;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The meta door end to end at the gateway level: verbatim forwarding to the owning cell with
 * playbookId/service/domain task params, infra params left for the cell, the domain question's
 * answer kept separate from the service, HITL registration for /resume-child, and sink cleanup.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class PlaybookGatewayMetaForwardingTest {

    @Mock PlaybookRouter router;
    @Mock PlaybookExecutor executor;
    @Mock PlaybookProperties playbookProperties;
    @Mock PlaybookResolver resolver;
    @Mock InvestigationStateService investigationStateService;
    @Mock AgentResponseSinkRegistry sinkRegistry;
    @Mock ThreadStore threadStore;
    @Mock ChatModel chatModel;
    @Mock DomainCellRouter domainCellRouter;
    @Mock DelegationExecutionService delegationExecutionService;
    @Mock AppContextResolver appContextResolver;
    @Mock AgentConfigResolver agentConfigResolver;
    @Mock ChildHitlEventBuilder childHitlEventBuilder;

    private PlaybookGateway gateway;

    private static final PlaybookDefinition SERVICE_SLOW = new PlaybookDefinition(
            "Service Performance Investigation", null, null, null, null, null, null, 1500,
            List.of(new ParamRef("service", true), new ParamRef("timeRange", true),
                    new ParamRef("namespace", true)),
            null);

    private static final Map<String, ParamDefinition> PARAM_DEFS = Map.of(
            "service", new ParamDefinition("Which service?", "", true),
            "timeRange", new ParamDefinition("How far back?", "", true),
            "namespace", new ParamDefinition("Which namespace?", "", true));

    private static final FederationProperties FEDERATION = new FederationProperties(true, Map.of(
            "claims", new FederationProperties.DomainCell("claims-cell", null),
            "payments", new FederationProperties.DomainCell("payments-cell", null)), null, true);

    @BeforeEach
    void setUp() {
        gateway = new PlaybookGateway(router, executor, playbookProperties, resolver,
                investigationStateService, sinkRegistry, threadStore, chatModel, new ObjectMapper(),
                FEDERATION, domainCellRouter, delegationExecutionService, appContextResolver,
                agentConfigResolver, childHitlEventBuilder);

        when(playbookProperties.paramDefinitions()).thenReturn(PARAM_DEFS);
        when(resolver.agentsAvailable(any())).thenReturn(false);   // a meta has only cells
        when(resolver.resolveByExplicitIdAnyAgents(anyString())).thenReturn(Optional.empty());
        // Mimic the real missing-param logic: every required key not yet filled.
        when(resolver.findMissingParams(any(), anyMap(), any())).thenAnswer(inv -> {
            Map<String, String> filled = inv.getArgument(1);
            List<PlaybookResolver.MissingParam> missing = new ArrayList<>();
            for (String key : List.of("service", "timeRange", "namespace")) {
                String v = filled.get(key);
                if (v == null || v.isBlank()) {
                    missing.add(new PlaybookResolver.MissingParam(key, key + "?", false));
                }
            }
            return missing;
        });
        when(router.extractParameters(anyString(), any(), any())).thenReturn(new HashMap<>());
        when(appContextResolver.analyzeText(anyString()))
                .thenReturn(new AppContextResolver.TextAnalysis(null, false, false));
        when(sinkRegistry.register(anyString())).thenReturn(true);
        when(sinkRegistry.getFlux(anyString())).thenReturn(Flux.empty());
        when(agentConfigResolver.getAgentConfigs()).thenReturn(Map.of());
        when(domainCellRouter.owningDomain(any())).thenReturn(Optional.empty());
    }

    private void explicit(String message, String playbookId) {
        when(resolver.resolveByExplicitIdAnyAgents(eq(message)))
                .thenReturn(Optional.of(Map.entry(playbookId, SERVICE_SLOW)));
    }

    private String run(String message, String threadId) {
        Optional<Flux<ServerSentEvent<String>>> result =
                gateway.tryExecute(message, threadId, "user-1", null);
        assertThat(result).isPresent();
        List<ServerSentEvent<String>> events = result.get().collectList().block(Duration.ofSeconds(10));
        assertThat(events).isNotEmpty();
        StringBuilder all = new StringBuilder();
        for (ServerSentEvent<String> e : events) all.append(e.data()).append('\n');
        return all.toString();
    }

    private void llmExtracts(String keyValues) {
        when(chatModel.call(any(Prompt.class))).thenReturn(
                new ChatResponse(List.of(new Generation(new AssistantMessage(keyValues)))));
    }

    @Test
    void ownedServiceDelegatesTheVerbatimQuestionToTheOwningCell() {
        String q = "run the service-slow playbook for payments-api over the last 24h";
        explicit(q, "service-slow");
        when(router.extractParameters(anyString(), any(), any())).thenReturn(new HashMap<>(
                Map.of("service", "payments-api", "timeRange", "24h")));
        when(domainCellRouter.owningDomain("payments-api")).thenReturn(Optional.of("payments"));
        when(delegationExecutionService.delegateToAgentPinned(anyString(), anyString(), anyString(), anyMap(), isNull()))
                .thenReturn("CELL REPORT");

        String out = run(q, "t-owned");

        assertThat(out).contains("CELL REPORT");
        verify(delegationExecutionService).delegateToAgentPinned(eq("payments-cell"), eq(q), eq("t-owned"),
                argThat(m -> "service-slow".equals(m.get("playbookId"))
                        && "payments-api".equals(m.get("service"))
                        && "payments".equals(m.get("domain"))), isNull());
        verify(sinkRegistry).emit("t-owned", "delegateToPaymentsCell", "CELL REPORT");
        verify(sinkRegistry).register("t-owned");
        verify(sinkRegistry, org.mockito.Mockito.atLeastOnce()).unregister("t-owned");
    }

    @Test
    void infrastructureParamsAreLeftForTheCellToFill() {
        // namespace is required and unfilled - but it is the CELL's mapping that knows it.
        // The meta must prompt only for timeRange, then delegate with namespace still absent.
        String q = "run the service-slow playbook for payments-api";
        explicit(q, "service-slow");
        when(router.extractParameters(anyString(), any(), any())).thenReturn(new HashMap<>(
                Map.of("service", "payments-api")));
        when(domainCellRouter.owningDomain("payments-api")).thenReturn(Optional.of("payments"));

        String prompt = run(q, "t-infra");
        assertThat(prompt).contains("timeRange").doesNotContain("namespace");
        verify(delegationExecutionService, never()).delegateToAgentPinned(anyString(), anyString(), anyString(), anyMap(), any());

        llmExtracts("timeRange=24h");
        when(delegationExecutionService.delegateToAgentPinned(anyString(), anyString(), anyString(), anyMap(), isNull()))
                .thenReturn("CELL REPORT");
        String out = run("last 24 hours", "t-infra");

        assertThat(out).contains("CELL REPORT");
        verify(delegationExecutionService).delegateToAgentPinned(eq("payments-cell"), eq(q), eq("t-infra"),
                argThat(m -> !m.containsKey("namespace") && "24h".equals(m.get("timeRange"))), isNull());
    }

    @Test
    void bareDomainAnswerCollectsTheServiceBeforeDelegating() {
        String q = "run the service-slow playbook";
        explicit(q, "service-slow");

        String ask = run(q, "t-bare");
        assertThat(ask).contains("Which domain should run it?");

        // "payments" answers the DOMAIN question - it must not become the service.
        String prompt = run("payments", "t-bare");
        assertThat(prompt).contains("service").contains("timeRange");
        verify(delegationExecutionService, never()).delegateToAgentPinned(anyString(), anyString(), anyString(), anyMap(), any());

        llmExtracts("service=payments-api\ntimeRange=24h");
        when(domainCellRouter.owningDomain("payments-api")).thenReturn(Optional.of("payments"));
        when(delegationExecutionService.delegateToAgentPinned(anyString(), anyString(), anyString(), anyMap(), isNull()))
                .thenReturn("CELL REPORT");
        String out = run("payments-api, last 24 hours", "t-bare");

        assertThat(out).contains("CELL REPORT");
        verify(delegationExecutionService).delegateToAgentPinned(eq("payments-cell"), eq(q), eq("t-bare"),
                argThat(m -> "payments-api".equals(m.get("service")) && "payments".equals(m.get("domain"))), isNull());
    }

    @Test
    void unmappedServiceSurvivesTheDomainAnswer() {
        String q = "run the service-slow playbook for ledger-sync-svc over the last 24h";
        explicit(q, "service-slow");
        when(router.extractParameters(anyString(), any(), any())).thenReturn(new HashMap<>(
                Map.of("service", "ledger-sync-svc", "timeRange", "24h")));

        String ask = run(q, "t-unmapped");
        assertThat(ask).contains("ledger-sync-svc").contains("Which domain should run it?");

        when(delegationExecutionService.delegateToAgentPinned(anyString(), anyString(), anyString(), anyMap(), isNull()))
                .thenReturn("CELL REPORT");
        String out = run("payments", "t-unmapped");

        assertThat(out).contains("CELL REPORT");
        // The domain answer routed the request; the original service was NOT overwritten by it.
        verify(delegationExecutionService).delegateToAgentPinned(eq("payments-cell"), eq(q), eq("t-unmapped"),
                argThat(m -> "ledger-sync-svc".equals(m.get("service")) && "payments".equals(m.get("domain"))), isNull());
    }

    @Test
    void hitlFromTheCellIsStoredForResume() {
        String q = "run the service-slow playbook for payments-api over the last 24h";
        explicit(q, "service-slow");
        when(router.extractParameters(anyString(), any(), any())).thenReturn(new HashMap<>(
                Map.of("service", "payments-api", "timeRange", "24h")));
        when(domainCellRouter.owningDomain("payments-api")).thenReturn(Optional.of("payments"));
        when(delegationExecutionService.delegateToAgentPinned(anyString(), anyString(), anyString(), anyMap(), isNull()))
                .thenThrow(new ChildAgentHitlException("kubernetes_agent", "task-9",
                        List.of(Map.of("tool", "scaleDeployment")), "http://cell:8086"));
        when(childHitlEventBuilder.buildEvent(any(ChildAgentHitlException.class)))
                .thenReturn(ServerSentEvent.<String>builder().data("{\"type\":\"child-hitl\"}").build());

        String out = run(q, "t-hitl");

        assertThat(out).contains("child-hitl");
        // Without this record /resume-child answers "No pending approval for this task".
        verify(investigationStateService).storePendingHitlAs(
                eq("task-9"), eq("t-hitl"), eq("kubernetes_agent"), eq("http://cell:8086"), anyString());
        verify(sinkRegistry, org.mockito.Mockito.atLeastOnce()).unregister("t-hitl");
    }

    @Test
    void aSlowCellTurnEmitsKeepalivesAndTheStreamStillCompletes() throws Exception {
        // The reported defect: one blocking A2A call emitted nothing for the whole
        // investigation, so the browser's 300s idle-cancel (and the ingress 300s
        // proxy-read-timeout) killed the request before the answer arrived. Comment frames
        // carry no data - the UI drops every non-"data:" line - but they are bytes, which is
        // all either timer needs. Termination is the easy thing to get wrong: an unbounded
        // interval would stop Flux.merge from ever completing, so assert completion too.
        String q = "run the service-slow playbook for payments-api over the last 24h";
        explicit(q, "service-slow");
        when(router.extractParameters(anyString(), any(), any())).thenReturn(new HashMap<>(
                Map.of("service", "payments-api", "timeRange", "24h")));
        when(domainCellRouter.owningDomain("payments-api")).thenReturn(Optional.of("payments"));
        when(delegationExecutionService.delegateToAgentPinned(anyString(), anyString(), anyString(), anyMap(), isNull()))
                .thenAnswer(inv -> {                       // outlast two keepalive ticks
                    Thread.sleep(32_000);
                    return "CELL REPORT";
                });

        List<ServerSentEvent<String>> events = gateway
                .tryExecute(q, "t-keepalive", "user-1", null).orElseThrow()
                .collectList().block(Duration.ofSeconds(60));   // completes, or this times out

        assertThat(events).isNotNull();
        long keepalives = events.stream().filter(e -> e.data() == null && e.comment() != null).count();
        assertThat(keepalives).as("keepalive frames while the cell worked").isGreaterThanOrEqualTo(2);
        assertThat(events.stream().anyMatch(e -> e.data() != null && e.data().contains("CELL REPORT")))
                .as("the answer still arrives").isTrue();
    }

    @Test
    void aFastCellTurnEmitsNoKeepaliveAtAll() {
        // Guard against the keepalive leaking into ordinary turns (and into the parity contract).
        String q = "run the service-slow playbook for payments-api over the last 24h";
        explicit(q, "service-slow");
        when(router.extractParameters(anyString(), any(), any())).thenReturn(new HashMap<>(
                Map.of("service", "payments-api", "timeRange", "24h")));
        when(domainCellRouter.owningDomain("payments-api")).thenReturn(Optional.of("payments"));
        when(delegationExecutionService.delegateToAgentPinned(anyString(), anyString(), anyString(), anyMap(), isNull()))
                .thenReturn("CELL REPORT");

        List<ServerSentEvent<String>> events = gateway
                .tryExecute(q, "t-fast", "user-1", null).orElseThrow()
                .collectList().block(Duration.ofSeconds(10));

        assertThat(events).isNotNull();
        assertThat(events.stream().noneMatch(e -> e.comment() != null)).isTrue();
    }

    @Test
    void aPlaybookIdIsNeverForwardedAsTheServiceName() {
        // Seen live: "run the service-slow playbook" with no service named had the PLAYBOOK ID
        // extracted into the service slot. The meta asks which domain either way - the damage is
        // on the NEXT turn, where that nonsense service is forwarded: it matches no mapping, the
        // cell finds no namespace/schema, abandons the playbook and runs an open-ended ReAct
        // investigation that outlives the turn. A service is never a playbook id.
        String q = "run the service-slow playbook";
        explicit(q, "service-slow");
        when(playbookProperties.playbooks())
                .thenReturn(new java.util.LinkedHashMap<>(Map.of("service-slow", SERVICE_SLOW)));
        when(router.extractParameters(anyString(), any(), any()))
                .thenReturn(new HashMap<>(Map.of("service", "service-slow")));   // the bad extraction

        String ask = run(q, "t-pbid");
        assertThat(ask).contains("Which domain should run it?");

        // turn 2: the user picks a domain. With the bogus service discarded there is no service
        // left, so the meta must ASK for one rather than forward a playbook id as the target.
        String prompt = run("payments", "t-pbid");
        assertThat(prompt).contains("service");

        // turn 3: the real service arrives and only now is anything forwarded
        llmExtracts("service=payments-api\ntimeRange=24h");
        when(domainCellRouter.owningDomain("payments-api")).thenReturn(Optional.of("payments"));
        when(delegationExecutionService.delegateToAgentPinned(anyString(), anyString(), anyString(), anyMap(), isNull()))
                .thenReturn("CELL REPORT");
        run("payments-api, last 24 hours", "t-pbid");

        verify(delegationExecutionService).delegateToAgentPinned(eq("payments-cell"), eq(q), eq("t-pbid"),
                argThat(m -> "payments-api".equals(m.get("service"))), isNull());
    }

    @Test
    void noExplicitReferencePassesThroughToReact() {
        when(router.match(anyString())).thenReturn(Optional.empty());

        assertThat(gateway.tryExecute("why is payments-api slow", "t-pass", "user-1", null)).isEmpty();
        verify(delegationExecutionService, never()).delegateToAgentPinned(anyString(), anyString(), anyString(), anyMap(), any());
    }

    @Test
    void hallucinatedExtractionCannotOverrideTheTextNamedService() {
        // Extraction claims claims-api; the user's own words name payments-api. The text wins,
        // so the playbook cannot be routed to a domain the user never asked for.
        String q = "run the service-slow playbook for payments-api over the last 24h";
        explicit(q, "service-slow");
        when(router.extractParameters(anyString(), any(), any())).thenReturn(new HashMap<>(
                Map.of("service", "claims-api", "timeRange", "24h")));
        when(appContextResolver.analyzeText(eq(q)))
                .thenReturn(new AppContextResolver.TextAnalysis("payments-api", false, false));
        when(domainCellRouter.owningDomain("payments-api")).thenReturn(Optional.of("payments"));
        when(delegationExecutionService.delegateToAgentPinned(anyString(), anyString(), anyString(), anyMap(), isNull()))
                .thenReturn("CELL REPORT");

        String out = run(q, "t-halluc");

        assertThat(out).contains("CELL REPORT");
        verify(delegationExecutionService).delegateToAgentPinned(eq("payments-cell"), eq(q), eq("t-halluc"),
                argThat(m -> "payments-api".equals(m.get("service"))), isNull());
    }

    @Test
    void metaMappingInfraValuesAreNotForwardedButCallerStatedOnesAre() {
        String q = "run the service-slow playbook for payments-api over the last 24h in namespace edge";
        explicit(q, "service-slow");
        // namespace 'edge' came from the user's own words (extraction); applyAppContext then
        // adds mapping-derived schema - only the user's namespace may travel.
        when(router.extractParameters(anyString(), any(), any())).thenReturn(new HashMap<>(
                Map.of("service", "payments-api", "timeRange", "24h", "namespace", "edge")));
        org.mockito.Mockito.doAnswer(inv -> {
            Map<String, String> p = inv.getArgument(0);
            p.putIfAbsent("schema", "META_SCHEMA");
            return null;
        }).when(resolver).applyAppContext(anyMap(), anyString());
        when(domainCellRouter.owningDomain("payments-api")).thenReturn(Optional.of("payments"));
        when(delegationExecutionService.delegateToAgentPinned(anyString(), anyString(), anyString(), anyMap(), isNull()))
                .thenReturn("CELL REPORT");

        run(q, "t-prov");

        verify(delegationExecutionService).delegateToAgentPinned(eq("payments-cell"), eq(q), eq("t-prov"),
                argThat(m -> "edge".equals(m.get("namespace")) && !m.containsKey("schema")), isNull());
    }

    @Test
    void volunteeredParamsInAPromptReplyAreCapturedWithUserProvenance() {
        String q = "run the service-slow playbook for payments-api";
        explicit(q, "service-slow");
        when(router.extractParameters(anyString(), any(), any())).thenReturn(new HashMap<>(
                Map.of("service", "payments-api")));
        when(domainCellRouter.owningDomain("payments-api")).thenReturn(Optional.of("payments"));

        String prompt = run(q, "t-volunteer");
        assertThat(prompt).contains("timeRange");

        // The reply answers timeRange AND volunteers a namespace - both are the user's words.
        llmExtracts("timeRange=24h\nnamespace=staging");
        when(delegationExecutionService.delegateToAgentPinned(anyString(), anyString(), anyString(), anyMap(), isNull()))
                .thenReturn("CELL REPORT");
        run("last 24 hours in namespace staging", "t-volunteer");

        verify(delegationExecutionService).delegateToAgentPinned(eq("payments-cell"), eq(q), eq("t-volunteer"),
                argThat(m -> "staging".equals(m.get("namespace")) && "24h".equals(m.get("timeRange"))), isNull());
    }

    @Test
    void busyRefusalKeepsTheForwardForTheNextMessage() {
        String q = "run the service-slow playbook for payments-api over the last 24h";
        explicit(q, "service-slow");
        when(router.extractParameters(anyString(), any(), any())).thenReturn(new HashMap<>(
                Map.of("service", "payments-api", "timeRange", "24h")));
        when(domainCellRouter.owningDomain("payments-api")).thenReturn(Optional.of("payments"));
        when(sinkRegistry.register(anyString())).thenReturn(false).thenReturn(true);
        when(delegationExecutionService.delegateToAgentPinned(anyString(), anyString(), anyString(), anyMap(), isNull()))
                .thenReturn("CELL REPORT");

        String refused = run(q, "t-busy");
        assertThat(refused).contains("already streaming");
        verify(delegationExecutionService, never()).delegateToAgentPinned(anyString(), anyString(), anyString(), anyMap(), any());

        String out = run("try again", "t-busy");
        assertThat(out).contains("CELL REPORT");
        verify(delegationExecutionService).delegateToAgentPinned(eq("payments-cell"), eq(q), eq("t-busy"),
                anyMap(), isNull());
    }

    @Test
    void aFullQuestionAfterTheAskIsNotSwallowedIntoTheStaleForward() {
        String q = "run the service-slow playbook";
        explicit(q, "service-slow");
        String ask = run(q, "t-pivot");
        assertThat(ask).contains("Which domain should run it?");

        // A topic pivot - even one naming a known service - is its own request.
        when(appContextResolver.analyzeText(anyString()))
                .thenReturn(new AppContextResolver.TextAnalysis("checkout-service", false, false));
        when(router.match(anyString())).thenReturn(Optional.empty());
        Optional<Flux<ServerSentEvent<String>>> result = gateway.tryExecute(
                "forget the run - why is checkout-service suddenly throwing 500s today?", "t-pivot", "user-1", null);

        assertThat(result).isEmpty();   // fell through to ReAct
        verify(delegationExecutionService, never()).delegateToAgentPinned(anyString(), anyString(), anyString(), anyMap(), any());
    }

    @Test
    void cancelDropsThePendingAskWithAnAcknowledgement() {
        String q = "run the service-slow playbook";
        explicit(q, "service-slow");
        run(q, "t-cancel");

        String ack = run("cancel that", "t-cancel");
        assertThat(ack).contains("dropped the pending");

        // Pending is gone: the next message routes normally.
        when(router.match(anyString())).thenReturn(Optional.empty());
        assertThat(gateway.tryExecute("hello there", "t-cancel", "user-1", null)).isEmpty();
        verify(delegationExecutionService, never()).delegateToAgentPinned(anyString(), anyString(), anyString(), anyMap(), any());
    }

    @Test
    void conversationalFillerIsNotTakenAsAServiceName() {
        String q = "run the service-slow playbook";
        explicit(q, "service-slow");
        run(q, "t-junk");

        String again = run("ok", "t-junk");
        assertThat(again).contains("didn't recognize");
        verify(delegationExecutionService, never()).delegateToAgentPinned(anyString(), anyString(), anyString(), anyMap(), any());
    }
}
