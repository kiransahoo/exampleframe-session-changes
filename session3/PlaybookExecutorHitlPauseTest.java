package com.#exampleframe#.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.#exampleframe#.orchestrator.config.PlaybookProperties.PlaybookDefinition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.http.codec.ServerSentEvent;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * A playbook paused on a human approval must not also answer.
 * <p>
 * The executor used to append a synthesised report unconditionally, so the user saw a finished
 * RCA - built only from the steps that ran BEFORE the gate - while the tool was still awaiting a
 * decision. An operator could then approve or reject a privileged call against conclusions that
 * had already been drawn and shown to them. The report belongs after the approved step runs.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class PlaybookExecutorHitlPauseTest {

    @Mock DelegationExecutionService delegationExecutionService;
    @Mock com.#exampleframe#.orchestrator.tools.AgentInvocationService agentInvocationService;
    @Mock InvestigationStateService investigationStateService;
    @Mock AgentResponseSinkRegistry agentResponseSinkRegistry;
    @Mock ChildHitlEventBuilder childHitlEventBuilder;
    @Mock ChatModel chatModel;

    private static final PlaybookDefinition PLAYBOOK = new PlaybookDefinition(
            "Service Performance Investigation", null, null, null, null, null, null, 1500, List.of(), null);

    private PlaybookExecutor executor() {
        return new PlaybookExecutor(delegationExecutionService, agentInvocationService,
                investigationStateService, agentResponseSinkRegistry, childHitlEventBuilder,
                chatModel, new ObjectMapper());
    }

    @Test
    void aPendingApprovalEmitsOnlyTheApprovalRequest() {
        ServerSentEvent<String> hitl = ServerSentEvent.<String>builder()
                .data("{\"messageType\":\"tool-confirm\"}").build();

        List<PlaybookExecutor.StepResult> results = List.of(
                PlaybookExecutor.StepResult.success("s1", "kubernetes", "check pods",
                        "3 pods restarting", Duration.ofSeconds(2)),
                PlaybookExecutor.StepResult.hitlPending("s2", "kubernetes", "scale deployment",
                        "Step paused: child agent requires approval", Duration.ofSeconds(1), hitl, null));

        List<ServerSentEvent<String>> events =
                executor().buildTerminalEvents(PLAYBOOK, results, "why are pods restarting");

        assertThat(events).containsExactly(hitl);
        // the decisive assertion: no report was produced from the partial evidence
        verifyNoInteractions(chatModel);
    }

    @Test
    void aCompletedRunStillSynthesisesItsReport() {
        // Guard against over-correcting: with nothing pending, the report is still produced.
        List<PlaybookExecutor.StepResult> results = List.of(
                PlaybookExecutor.StepResult.success("s1", "kubernetes", "check pods",
                        "3 pods restarting", Duration.ofSeconds(2)));

        List<ServerSentEvent<String>> events =
                executor().buildTerminalEvents(PLAYBOOK, results, "why are pods restarting");

        assertThat(events).hasSize(1);
        assertThat(events.get(0).data()).isNotNull();
    }

    // ---- Continuation: the resume half the pause always promised ----

    private static final com.#exampleframe#.orchestrator.config.PlaybookProperties.PlaybookStep AZURE =
            new com.#exampleframe#.orchestrator.config.PlaybookProperties.PlaybookStep(
                    "azure-traces", "azure-monitor", "get traces for {service}", null, null, false);
    private static final com.#exampleframe#.orchestrator.config.PlaybookProperties.PlaybookStep K8S =
            new com.#exampleframe#.orchestrator.config.PlaybookProperties.PlaybookStep(
                    "k8s-diagnostics", "kubernetes", "diagnose {service}", null, null, false);
    private static final com.#exampleframe#.orchestrator.config.PlaybookProperties.PlaybookStep ORACLE =
            new com.#exampleframe#.orchestrator.config.PlaybookProperties.PlaybookStep(
                    "oracle-check", "oracle", "check db for {service}", null, null, false);
    private static final PlaybookDefinition THREE_STEPS = new PlaybookDefinition(
            "Service Performance Investigation", null, null, null, null, null, null, 1500,
            List.of(), List.of(AZURE, K8S, ORACLE));

    private static com.#exampleframe#.orchestrator.exception.ChildAgentHitlException gate(String taskId) {
        return new com.#exampleframe#.orchestrator.exception.ChildAgentHitlException(
                "kubernetes", taskId, List.of(java.util.Map.of("name", "execCommand")),
                "http://localhost:8097");
    }

    /**
     * A gate at step 2 of 3 parks a continuation at index 2; resuming it runs step 3 and
     * synthesises from ALL steps - the operator gets the investigation they approved,
     * never a dead conversation ending at the child's answer.
     */
    @Test
    void approvedGateResumesRemainingStepsAndSynthesises() throws Exception {
        PlaybookContinuationService continuations = new PlaybookContinuationService();
        PlaybookExecutor executor = executor();
        executor.setContinuationService(continuations);

        org.mockito.Mockito.when(delegationExecutionService.delegateToAgent(
                        org.mockito.ArgumentMatchers.eq("azure-monitor"), org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyMap(),
                        org.mockito.ArgumentMatchers.isNull(), org.mockito.ArgumentMatchers.any()))
                .thenReturn("azure evidence");
        org.mockito.Mockito.when(delegationExecutionService.delegateToAgent(
                        org.mockito.ArgumentMatchers.eq("kubernetes"), org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyMap(),
                        org.mockito.ArgumentMatchers.isNull(), org.mockito.ArgumentMatchers.any()))
                .thenThrow(gate("task-gate-1"));
        org.mockito.Mockito.when(childHitlEventBuilder.buildEvent(org.mockito.ArgumentMatchers.any()))
                .thenReturn(ServerSentEvent.<String>builder().data("{}").build());

        try {
            executor.executeBlocking(THREE_STEPS, new java.util.HashMap<>(
                    java.util.Map.of("service", "esign")), "thread-1", "why is esign slow");
            org.junit.jupiter.api.Assertions.fail("expected the gate to propagate");
        } catch (com.#exampleframe#.orchestrator.exception.ChildAgentHitlException expected) {
            assertThat(expected.getTaskId()).isEqualTo("task-gate-1");
        }

        PlaybookContinuationService.PlaybookContinuation parked = continuations.claim("task-gate-1");
        assertThat(parked).isNotNull();
        assertThat(parked.resumeStepIndex()).isEqualTo(2);
        assertThat(parked.gatedStepId()).isEqualTo("k8s-diagnostics");

        org.mockito.Mockito.when(delegationExecutionService.delegateToAgent(
                        org.mockito.ArgumentMatchers.eq("oracle"), org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyMap(),
                        org.mockito.ArgumentMatchers.isNull(), org.mockito.ArgumentMatchers.any()))
                .thenReturn("oracle evidence");

        String report = executor.resumeContinuation(parked, "k8s evidence after approval");

        org.mockito.Mockito.verify(delegationExecutionService).delegateToAgent(
                org.mockito.ArgumentMatchers.eq("oracle"), org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyMap(),
                org.mockito.ArgumentMatchers.isNull(), org.mockito.ArgumentMatchers.any());
        assertThat(report).isNotBlank();
        assertThat(report).contains("azure evidence").contains("k8s evidence after approval")
                .contains("oracle evidence");
    }

    /** A later step's own gate re-parks the run under the NEW taskId and rethrows. */
    @Test
    void aLaterGateDuringResumeParksAFreshContinuation() throws Exception {
        PlaybookContinuationService continuations = new PlaybookContinuationService();
        PlaybookExecutor executor = executor();
        executor.setContinuationService(continuations);

        org.mockito.Mockito.when(childHitlEventBuilder.buildEvent(org.mockito.ArgumentMatchers.any()))
                .thenReturn(ServerSentEvent.<String>builder().data("{}").build());
        org.mockito.Mockito.when(delegationExecutionService.delegateToAgent(
                        org.mockito.ArgumentMatchers.eq("azure-monitor"), org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyMap(),
                        org.mockito.ArgumentMatchers.isNull(), org.mockito.ArgumentMatchers.any()))
                .thenReturn("azure evidence");
        org.mockito.Mockito.when(delegationExecutionService.delegateToAgent(
                        org.mockito.ArgumentMatchers.eq("kubernetes"), org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyMap(),
                        org.mockito.ArgumentMatchers.isNull(), org.mockito.ArgumentMatchers.any()))
                .thenThrow(gate("task-gate-1"));

        try {
            executor.executeBlocking(THREE_STEPS, new java.util.HashMap<>(
                    java.util.Map.of("service", "esign")), "thread-2", "why is esign slow");
            org.junit.jupiter.api.Assertions.fail("expected the gate to propagate");
        } catch (com.#exampleframe#.orchestrator.exception.ChildAgentHitlException ignored) {
        }
        PlaybookContinuationService.PlaybookContinuation parked = continuations.claim("task-gate-1");
        assertThat(parked).isNotNull();

        org.mockito.Mockito.when(delegationExecutionService.delegateToAgent(
                        org.mockito.ArgumentMatchers.eq("oracle"), org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyMap(),
                        org.mockito.ArgumentMatchers.isNull(), org.mockito.ArgumentMatchers.any()))
                .thenThrow(new com.#exampleframe#.orchestrator.exception.ChildAgentHitlException(
                        "oracle", "task-gate-2", List.of(java.util.Map.of("name", "killSession")),
                        "http://localhost:8093"));

        try {
            executor.resumeContinuation(parked, "k8s evidence after approval");
            org.junit.jupiter.api.Assertions.fail("expected the second gate to propagate");
        } catch (com.#exampleframe#.orchestrator.exception.ChildAgentHitlException second) {
            assertThat(second.getTaskId()).isEqualTo("task-gate-2");
        }
        PlaybookContinuationService.PlaybookContinuation reparked = continuations.claim("task-gate-2");
        assertThat(reparked).isNotNull();
        assertThat(reparked.resumeStepIndex()).isEqualTo(3);
    }
}
