package com.#exampleframe#.kubernetes.agent.interceptor;

import com.alibaba.cloud.ai.graph.agent.interceptor.ModelCallHandler;
import com.alibaba.cloud.ai.graph.agent.interceptor.ModelRequest;
import com.alibaba.cloud.ai.graph.agent.interceptor.ModelResponse;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SlowPodRcaGuardInterceptorTest {

    private final SlowPodRcaGuardInterceptor interceptor = new SlowPodRcaGuardInterceptor();

    @Test
    void shouldInjectGuardForSlowPodPrompts() {
        AtomicReference<ModelRequest> observed = new AtomicReference<>();
        ModelRequest request = request("Why is this java pod slow and high CPU?");

        ModelCallHandler handler = req -> {
            observed.set(req);
            return response("ok");
        };

        interceptor.interceptModel(request, handler);

        ModelRequest guarded = observed.get();
        assertTrue(guarded.getMessages().stream()
                        .filter(m -> m.getMessageType() == org.springframework.ai.chat.messages.MessageType.SYSTEM)
                        .map(m -> m.getText() == null ? "" : m.getText())
                        .anyMatch(text -> text.contains("[SLOW_POD_RCA_GUARD]")),
                "Slow pod prompts should include RCA guard instructions");
    }

    @Test
    void shouldNotInjectGuardForGeneralPrompts() {
        AtomicReference<ModelRequest> observed = new AtomicReference<>();
        ModelRequest request = request("List pods in default namespace");

        ModelCallHandler handler = req -> {
            observed.set(req);
            return response("ok");
        };

        interceptor.interceptModel(request, handler);

        ModelRequest forwarded = observed.get();
        assertFalse(forwarded.getMessages().stream()
                        .filter(m -> m.getMessageType() == org.springframework.ai.chat.messages.MessageType.SYSTEM)
                        .map(m -> m.getText() == null ? "" : m.getText())
                        .anyMatch(text -> text.contains("[SLOW_POD_RCA_GUARD]")),
                "Non-RCA prompts should not get slow-pod guard instructions");
    }

    @Test
    void shouldStaySinglePassForSlowPodPrompts() {
        AtomicInteger calls = new AtomicInteger();
        List<ModelRequest> seenRequests = new ArrayList<>();
        ModelRequest request = request("why is pod slow in namespace #exampleframe#-jvm-test?");

        ModelCallHandler handler = req -> {
            seenRequests.add(req);
            calls.incrementAndGet();
            if (calls.get() == 1) {
                return response("""
                        ### Executive Summary
                        first
                        ### Executive Summary
                        duplicate
                        """);
            }
            return response("TOOL_CALL: listProblemPods");
        };

        ModelResponse modelResponse = interceptor.interceptModel(request, handler);

        assertEquals(2, calls.get(), "Slow pod RCA prompts should trigger one corrective retry");
        assertTrue(seenRequests.get(1).getMessages().stream()
                        .filter(m -> m.getMessageType() == org.springframework.ai.chat.messages.MessageType.SYSTEM)
                        .map(m -> m.getText() == null ? "" : m.getText())
                        .anyMatch(text -> text.contains("[SLOW_POD_RCA_TOOL_FIRST_RETRY]")));
        AssistantMessage assistant = (AssistantMessage) modelResponse.getMessage();
        assertTrue(assistant.getText().contains("TOOL_CALL"));
    }

    @Test
    void validateResponse_shouldDetectMissingSections() {
        List<String> violations = interceptor.validateResponse("""
                ### Executive Summary
                Something is slow
                ### Findings
                Confirmed: High CPU
                """);

        assertTrue(violations.contains("missing_kubernetes_evidence_section"));
        assertTrue(violations.contains("missing_jvm_evidence_section"));
        assertTrue(violations.contains("missing_recommendations_section"));
        assertTrue(violations.contains("missing_evidence_gaps_section"));
    }

    @Test
    void validateResponse_shouldRequireToolEvidenceRowsAndEvidenceRefs() {
        List<String> violations = interceptor.validateResponse("""
                ### Executive Summary
                order pod latency spike
                ### Kubernetes Evidence
                pod was running
                ### JVM Evidence
                analyzeJvm was called
                ### Findings
                Confirmed
                payment path timed out repeatedly
                ### Recommended Actions
                tune client timeout
                ### Evidence Gaps
                none
                """);

        assertTrue(violations.contains("missing_kubernetes_tool_evidence_rows"));
        assertTrue(violations.contains("missing_jvm_tool_evidence_rows"));
        assertTrue(violations.contains("confirmed_findings_without_evidence_refs"));
    }

    private static ModelRequest request(String userPrompt) {
        List<Message> messages = new ArrayList<>();
        SystemMessage systemMessage = new SystemMessage("system");
        messages.add(systemMessage);
        messages.add(new UserMessage(userPrompt));

        return ModelRequest.builder()
                .systemMessage(systemMessage)
                .messages(messages)
                .build();
    }

    private static ModelResponse response(String text) {
        return ModelResponse.of(new AssistantMessage(text));
    }

    /**
     * The guard is an automated quality bar: neither its primary contract nor its
     * corrective retry may push the model into approval-gated tools. Doing so turned a
     * "why is X slow" on a pod without approval-free JVM access into sequential HITL
     * prompts on a healthy pod. Both injected instructions must carry the constraint.
     */
    @Test
    void guardInstructionsForbidApprovalGatedToolCalls() {
        AtomicReference<ModelRequest> primary = new AtomicReference<>();
        // compareAndSet: capture the FIRST (primary) request even if a corrective retry
        // issues a second call - the primary-block assertions must not silently test the
        // retry request instead.
        ModelCallHandler ok = req -> { primary.compareAndSet(null, req); return response("ok"); };
        interceptor.interceptModel(request("Why is this java pod slow and high CPU?"), ok);
        // Whitespace-normalized: the text blocks wrap phrases across lines.
        String primaryText = primary.get().getMessages().stream()
                .filter(m -> m.getMessageType() == org.springframework.ai.chat.messages.MessageType.SYSTEM)
                .map(m -> m.getText() == null ? "" : m.getText())
                .reduce("", (a, b) -> a + "\n" + b)
                .replaceAll("\\s+", " ");
        assertTrue(primaryText.contains("never raise a human-approval prompt"),
                "primary guard contract must forbid approval prompts");
        assertTrue(primaryText.contains("Do NOT call execCommand at all"),
                "primary prohibition must be unconditional - the model cannot know where exec is gated");
        assertTrue(primaryText.contains("EXCEPTION: if the user's own message explicitly requested"),
                "a USER-requested gated operation must stay callable (in-flow approval is the design)");
        assertTrue(primaryText.contains("calling it is approval-free"),
                "JFR must be stated approval-free, not conditioned on an unknowable precondition");
        assertTrue(primaryText.contains("- Tool=analyzeJvm | Evidence="),
                "the gap-row format must match the validator's TOOL_EVIDENCE_ROW pattern");

        String retryText = interceptor.addToolFirstRetryInstruction(
                        request("Why is this java pod slow and high CPU?"))
                .getMessages().stream()
                .filter(m -> m.getMessageType() == org.springframework.ai.chat.messages.MessageType.SYSTEM)
                .map(m -> m.getText() == null ? "" : m.getText())
                .reduce("", (a, b) -> a + "\n" + b)
                .replaceAll("\\s+", " ");
        assertTrue(retryText.contains("never raise a human-approval prompt"),
                "the corrective pass must forbid approval prompts");
        assertTrue(retryText.contains("Do NOT call execCommand at all"),
                "the corrective prohibition must be unconditional");
        assertTrue(retryText.contains("EXCEPTION: a gated operation the user's own message explicitly requested"),
                "a USER-requested gated operation must stay callable on the retry too");
        assertTrue(retryText.contains("- Tool=analyzeJvm | Evidence="),
                "the retry's gap-row format must match the validator's TOOL_EVIDENCE_ROW pattern");
    }
}
