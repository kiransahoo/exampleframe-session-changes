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
}
