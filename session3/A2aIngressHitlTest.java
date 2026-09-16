package com.#exampleframe#.orchestrator.api.controller;

import com.#exampleframe#.orchestrator.config.AgentConfigResolver;
import com.#exampleframe#.orchestrator.config.OrchestratorProperties;
import com.#exampleframe#.orchestrator.exception.ChildAgentHitlException;
import com.#exampleframe#.orchestrator.exception.ChildAgentHitlRuntimeException;
import com.#exampleframe#.orchestrator.incident.InvestigationRunner;
import com.#exampleframe#.orchestrator.incident.OrchestratorInvestigationRunner;
import com.#exampleframe#.orchestrator.service.HitlAwareA2aClient;
import com.#exampleframe#.orchestrator.service.InvestigationStateService;
import com.#exampleframe#.orchestrator.tools.AgentInvocationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * HITL across the federation boundary: a cell whose child agent needs approval answers the
 * meta exactly like an agent (pending_approval + taskId + pendingTools), and resumes that
 * child - at the instance URL that raised the gate - when the meta calls /a2a/resume.
 */
@ExtendWith(MockitoExtension.class)
class A2aIngressHitlTest {

    @Mock OrchestratorInvestigationRunner runner;
    @Mock HitlAwareA2aClient hitlClient;
    @Mock AgentInvocationService invocationService;
    @Mock AgentConfigResolver agentConfigResolver;

    private A2aIngressController ingress;
    private InvestigationStateService state;

    private static final List<Map<String, Object>> PENDING = List.of(
            Map.of("id", "call-1", "name", "restartDeployment", "arguments", "{\"name\":\"payments-api\"}"));

    @BeforeEach
    void setUp() {
        state = new InvestigationStateService(agentConfigResolver, null);
        ingress = new A2aIngressController(runner, new OrchestratorProperties(), hitlClient, state, invocationService,
                null /* playbookResolver */, null /* playbookExecutor */, null /* checkpointSaver */, null /* threadStore */, null /* costAccumulator */);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> result(Map<String, Object> body) {
        return (Map<String, Object>) body.get("result");
    }

    @Test
    void childApprovalInsideTheCellIsReturnedAsPendingApprovalAndThenResumedAtTheSameInstance() throws Exception {
        // 1) meta -> cell message; the cell's routed k8s instance (payments-prod) hits a write gate
        // The real ReAct path surfaces HITL as ChildAgentHitlRuntimeException (thrown by the
        // tool-execution exception processor), often nested inside the graph runner's own
        // wrappers - never as the raw checked-style exception the old catch expected.
        when(runner.investigate(any(InvestigationRunner.InvestigationTask.class)))
                .thenThrow(new RuntimeException("node failed", new ChildAgentHitlRuntimeException(
                        new ChildAgentHitlException("kubernetes", "child-task-42", PENDING, "http://k8s-agent-payments-prod:8082"))));

        Map<String, Object> msg = Map.of("id", "req-1", "params", Map.of("message",
                Map.of("parts", List.of(Map.of("type", "text", "text", "restart payments-api in payments-prod")))));
        Map<String, Object> pending = result(ingress.handleMessage(msg).getBody());

        assertThat(pending.get("status")).isEqualTo("pending_approval");
        assertThat(pending.get("taskId")).isEqualTo("child-task-42");
        assertThat((List<?>) pending.get("pendingTools")).hasSize(1);
        assertThat(pending.get("childAgent")).isEqualTo("kubernetes");

        // 2) meta -> cell resume with the operator's approval: the cell resumes the child at the
        //    instance URL that raised the gate (not the agent's default URL)
        when(hitlClient.resumeAgent(eq("kubernetes"), eq("http://k8s-agent-payments-prod:8082"), eq("child-task-42"), anyList(), any()))
                .thenReturn("Deployment payments-api restarted; 2/2 pods Ready");
        Map<String, Object> resume = Map.of("id", "req-2", "method", "task/resume", "params",
                Map.of("taskId", "child-task-42", "toolFeedbacks",
                        List.of(Map.of("id", "call-1", "name", "restartDeployment", "result", "APPROVED"))));
        Map<String, Object> done = result(ingress.resume(resume).getBody());

        assertThat(done.get("status")).isEqualTo("completed");
        assertThat(done.toString()).contains("payments-api restarted");
        verify(hitlClient).resumeAgent(eq("kubernetes"), eq("http://k8s-agent-payments-prod:8082"), eq("child-task-42"), anyList(), any());
        verify(invocationService, org.mockito.Mockito.never()).getAgentBaseUrl(any());
    }

    @Test
    void chainedApprovalIsReturnedAsPendingAgain() throws Exception {
        when(runner.investigate(any(InvestigationRunner.InvestigationTask.class)))
                .thenThrow(new ChildAgentHitlException("kubernetes", "t-1", PENDING, "http://k8s:8082"));
        ingress.handleMessage(Map.of("id", "r", "params", Map.of("message",
                Map.of("parts", List.of(Map.of("type", "text", "text", "do it"))))));
        when(hitlClient.resumeAgent(eq("kubernetes"), eq("http://k8s:8082"), eq("t-1"), anyList(), any()))
                .thenThrow(new ChildAgentHitlException("kubernetes", "t-1", PENDING, "http://k8s:8082"));
        Map<String, Object> again = result(ingress.resume(Map.of("id", "r2", "params",
                Map.of("taskId", "t-1", "toolFeedbacks", List.of(Map.of("id", "call-1", "result", "APPROVED"))))).getBody());
        assertThat(again.get("status")).isEqualTo("pending_approval");
    }

    @Test
    void unknownTaskIsAClearError() {
        Map<String, Object> body = ingress.resume(Map.of("id", "r3", "params",
                Map.of("taskId", "never-seen", "toolFeedbacks", List.of()))).getBody();
        assertThat(body).containsKey("error");
        assertThat(body.get("error").toString()).contains("No pending approval");
    }

    @Test
    void fallsBackToTheDefaultAgentUrlWhenNoInstanceUrlWasRecorded() throws Exception {
        when(runner.investigate(any(InvestigationRunner.InvestigationTask.class)))
                .thenThrow(new ChildAgentHitlException("oracle", "t-9", PENDING, null));
        ingress.handleMessage(Map.of("id", "r", "params", Map.of("message",
                Map.of("parts", List.of(Map.of("type", "text", "text", "kill session"))))));
        lenient().when(invocationService.getAgentBaseUrl("oracle")).thenReturn("http://oracle-agent:8083");
        when(hitlClient.resumeAgent(eq("oracle"), eq("http://oracle-agent:8083"), eq("t-9"), anyList(), any())).thenReturn("killed");
        Map<String, Object> done = result(ingress.resume(Map.of("id", "r2", "params",
                Map.of("taskId", "t-9", "toolFeedbacks", List.of(Map.of("id", "call-1", "result", "APPROVED"))))).getBody());
        assertThat(done.get("status")).isEqualTo("completed");
    }

    @Test
    void aResumedApprovalCannotBeReplayed() throws Exception {
        when(runner.investigate(any(InvestigationRunner.InvestigationTask.class)))
                .thenThrow(new ChildAgentHitlException("kubernetes", "t-once", PENDING, "http://k8s:8082"));
        ingress.handleMessage(Map.of("id", "r", "params", Map.of("message",
                Map.of("parts", List.of(Map.of("type", "text", "text", "do it"))))));
        when(hitlClient.resumeAgent(eq("kubernetes"), eq("http://k8s:8082"), eq("t-once"), anyList(), any()))
                .thenReturn("done");
        Map<String, Object> resume = Map.of("id", "r2", "params",
                Map.of("taskId", "t-once", "toolFeedbacks", List.of(Map.of("id", "call-1", "result", "APPROVED"))));
        assertThat(result(ingress.resume(resume).getBody()).get("status")).isEqualTo("completed");
        // the record was consumed with the approval: replaying the same taskId is not an approval
        Map<String, Object> replay = ingress.resume(resume).getBody();
        assertThat(replay).containsKey("error");
        assertThat(replay.get("error").toString()).contains("No pending approval");
    }

    @Test
    void chainedApprovalConsumesTheOldTaskAndRegistersTheNew() throws Exception {
        when(runner.investigate(any(InvestigationRunner.InvestigationTask.class)))
                .thenThrow(new ChildAgentHitlException("kubernetes", "t-chain-1", PENDING, "http://k8s:8082"));
        ingress.handleMessage(Map.of("id", "r", "params", Map.of("message",
                Map.of("parts", List.of(Map.of("type", "text", "text", "do it"))))));
        // approving t-chain-1 runs the tool; the child raises the NEXT gate as t-chain-2
        when(hitlClient.resumeAgent(eq("kubernetes"), eq("http://k8s:8082"), eq("t-chain-1"), anyList(), any()))
                .thenThrow(new ChildAgentHitlException("kubernetes", "t-chain-2", PENDING, "http://k8s:8082"));
        Map<String, Object> again = result(ingress.resume(Map.of("id", "r2", "params",
                Map.of("taskId", "t-chain-1", "toolFeedbacks",
                        List.of(Map.of("id", "call-1", "result", "APPROVED"))))).getBody());
        assertThat(again.get("status")).isEqualTo("pending_approval");
        assertThat(again.get("taskId")).isEqualTo("t-chain-2");
        // the old task is consumed...
        Map<String, Object> stale = ingress.resume(Map.of("id", "r3", "params",
                Map.of("taskId", "t-chain-1", "toolFeedbacks",
                        List.of(Map.of("id", "call-1", "result", "APPROVED"))))).getBody();
        assertThat(stale).containsKey("error");
        // ...and the new one is registered and resumable
        when(hitlClient.resumeAgent(eq("kubernetes"), eq("http://k8s:8082"), eq("t-chain-2"), anyList(), any()))
                .thenReturn("second tool done");
        Map<String, Object> done = result(ingress.resume(Map.of("id", "r4", "params",
                Map.of("taskId", "t-chain-2", "toolFeedbacks",
                        List.of(Map.of("id", "call-2", "result", "APPROVED"))))).getBody());
        assertThat(done.get("status")).isEqualTo("completed");
    }

    @Test
    void failedDeliveryLeavesTheApprovalRetryable() throws Exception {
        when(runner.investigate(any(InvestigationRunner.InvestigationTask.class)))
                .thenThrow(new ChildAgentHitlException("kubernetes", "t-retry", PENDING, "http://k8s:8082"));
        ingress.handleMessage(Map.of("id", "r", "params", Map.of("message",
                Map.of("parts", List.of(Map.of("type", "text", "text", "do it"))))));
        when(hitlClient.resumeAgent(eq("kubernetes"), eq("http://k8s:8082"), eq("t-retry"), anyList(), any()))
                .thenThrow(new RuntimeException("connection reset"))
                .thenReturn("done after retry");
        Map<String, Object> resume = Map.of("id", "r2", "params",
                Map.of("taskId", "t-retry", "toolFeedbacks", List.of(Map.of("id", "call-1", "result", "APPROVED"))));
        assertThat(ingress.resume(resume).getBody()).containsKey("error");
        // the delivery failed, so the approval was NOT consumed - the retry goes through
        assertThat(result(ingress.resume(resume).getBody()).get("status")).isEqualTo("completed");
    }
}
