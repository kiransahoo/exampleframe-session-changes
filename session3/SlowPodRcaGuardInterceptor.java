package com.#exampleframe#.kubernetes.agent.interceptor;

import com.alibaba.cloud.ai.graph.agent.interceptor.ModelCallHandler;
import com.alibaba.cloud.ai.graph.agent.interceptor.ModelInterceptor;
import com.alibaba.cloud.ai.graph.agent.interceptor.ModelRequest;
import com.alibaba.cloud.ai.graph.agent.interceptor.ModelResponse;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.MessageType;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Single-pass quality guard for slow-pod RCA responses.
 *
 * <p>It does not bypass ReAct or tools. It appends stricter instructions before model calls
 * for slowdown prompts and validates drafts only for telemetry/logging purposes.
 */
public class SlowPodRcaGuardInterceptor extends ModelInterceptor {

    private static final Logger log = LoggerFactory.getLogger(SlowPodRcaGuardInterceptor.class);

    private static final Pattern EXEC_SUMMARY_HEADING = Pattern.compile("(?mi)^#{2,3}\\s*Executive Summary\\b");
    private static final Pattern K8S_EVIDENCE_HEADING = Pattern.compile("(?mi)^#{2,3}\\s*Kubernetes Evidence\\b");
    private static final Pattern JVM_EVIDENCE_HEADING = Pattern.compile("(?mi)^#{2,3}\\s*JVM Evidence\\b");
    private static final Pattern FINDINGS_HEADING = Pattern.compile("(?mi)^#{2,3}\\s*Findings\\b");
    private static final Pattern RECOMMENDATIONS_HEADING = Pattern.compile("(?mi)^#{2,3}\\s*(Recommended Actions|Recommendations)\\b");
    private static final Pattern EVIDENCE_GAPS_HEADING = Pattern.compile("(?mi)^#{2,3}\\s*Evidence Gaps\\b");
    private static final Pattern TOOL_EVIDENCE_ROW = Pattern.compile("(?mi)^\\s*[-*]\\s*Tool\\s*=\\s*[^|\\n]+\\|\\s*Evidence\\s*=.+$");
    private static final Pattern CONFIRMED_SUBSECTION = Pattern.compile("(?mi)\\bConfirmed\\b");
    private static final Pattern EVIDENCE_REFERENCE = Pattern.compile("(?mi)(EvidenceRef\\s*=\\s*\\[[^\\]]+\\]|Tool\\s*=\\s*[^|\\n]+)");

    private static final String GUARD_MARKER = "[SLOW_POD_RCA_GUARD]";
    private static final String TOOL_FIRST_RETRY_MARKER = "[SLOW_POD_RCA_TOOL_FIRST_RETRY]";

    @Override
    public @NonNull ModelResponse interceptModel(
            @NonNull ModelRequest request,
            @NonNull ModelCallHandler handler) {
        String userPrompt = extractLastUserPrompt(request);
        ModelRequest guardedRequest = isSlowPodPrompt(userPrompt)
                ? addPrimaryGuardInstruction(request)
                : request;

        ModelResponse response = handler.call(guardedRequest);

        AssistantMessage assistant = extractAssistantMessage(response);
        String responseText = assistant != null ? assistant.getText() : null;

        if (!shouldValidate(userPrompt, assistant, responseText)) {
            return response;
        }

        List<String> violations = validateResponse(responseText);
        if (!violations.isEmpty()) {
            log.warn("Slow pod RCA quality issues detected (single-pass mode); violations={}", violations);
            if (shouldRetryForToolFirst(userPrompt, assistant, violations)) {
                ModelRequest retryRequest = addToolFirstRetryInstruction(guardedRequest);
                log.warn("Slow pod RCA tool-first retry triggered; issuing one corrective model pass");
                return handler.call(retryRequest);
            }
        }

        return response;
    }

    ModelRequest addPrimaryGuardInstruction(ModelRequest request) {
        String guardInstruction = """
                %s
                Output contract for slow-pod/root-cause analysis:
                1) Return one final non-duplicated report only.
                2) MANDATORY tool sequence - you MUST call ALL of these before producing a final report:
                   a) getPodDetails - get pod status, containers, and events
                   b) getPodLogsSince or getPodLogs - get current container logs
                   c) searchPodLogs with pattern 'ERROR,Exception,FATAL,OOM,killed' - search for errors
                   d) analyzeJvm - run JVM diagnostics (thread dump, heap histogram, GC stats)
                   If any tool fails, include the exact error under Evidence Gaps but still call the others.
                   If previous logs are checked, report them separately from current logs.
                   Never say "logs unavailable" when current logs were retrieved successfully.
                3) For Java latency/CPU investigations, attempt `captureAndAnalyzeJfr` - calling it
                   is approval-free; if it fails or reports that an approval-gated operation would
                   be needed, record the exact error as an evidence row (format in item 4).
                   If JVM tooling fails, include exact tool error under Evidence Gaps.
                4) This contract is an automated quality bar, NOT a user request: satisfying it must
                   never raise a human-approval prompt. Do NOT call execCommand at all to satisfy
                   this contract (in some environments it is approval-gated and you cannot tell
                   which), and never deployDiagnosticsSidecar or captureHeapDump either. A
                   diagnostic that cannot run approval-free - permissions error, missing
                   in-container JVM tooling, or it reports a gated operation is needed - appears
                   BOTH under Evidence Gaps and as a bulleted row in the JVM Evidence section, in
                   exactly this format:
                   - Tool=analyzeJvm | Evidence=<exact reason it could not run approval-free>
                   then continue, and RECOMMEND the gated operation in the report - approving it is
                   the user's decision, not this quality pass's.
                   EXCEPTION: if the user's own message explicitly requested a gated operation
                   (heap dump, diagnostics sidecar, exec), CALL it - its approval prompt is the
                   user's designed control point. This prohibition only bars gated calls made to
                   satisfy this automated contract.
                5) Use sections exactly:
                   Executive Summary
                   Kubernetes Evidence
                   JVM Evidence
                   Findings (Confirmed/Hypothesis)
                   Recommended Actions
                   Evidence Gaps
                5.1) In both Kubernetes Evidence and JVM Evidence, include evidence rows in this format:
                     - Tool=<toolName> | Evidence=<key output, metric, or exact failure reason>
                5.2) In Findings/Confirmed, include explicit evidence references:
                     - <finding text> | EvidenceRef=[toolName]
                6) Do not fabricate tool outputs. If data is missing, state exactly what failed and continue with available evidence.
                7) IMPORTANT: If the user asked about a specific service (e.g., "order service") and no pods matching that service name exist in the cluster, report "No pods matching '<service-name>' were found" and list the available pods. Do NOT investigate unrelated pods. The RCA format sections above are NOT required when no matching pods exist.
                """.formatted(GUARD_MARKER);

        return appendGuardInstruction(request, guardInstruction, GUARD_MARKER);
    }

    ModelRequest addToolFirstRetryInstruction(ModelRequest request) {
        String retryInstruction = """
                %s
                Tool-first enforcement for this turn:
                - Do not return a narrative answer yet.
                - You MUST call ALL of these tools before producing a final report:
                  1) getPodDetails
                  2) getPodLogsSince or getPodLogs
                  3) searchPodLogs with pattern 'ERROR,Exception,FATAL,OOM,killed'
                  4) analyzeJvm
                - For Java latency/CPU investigations, also attempt captureAndAnalyzeJfr - calling it
                  is approval-free; if it errors that a gated operation would be needed, record the
                  exact error as an evidence row instead.
                - This corrective pass is an automated quality bar, NOT a user request: it must never
                  raise a human-approval prompt. Do NOT call execCommand at all to satisfy the list
                  above (in some environments it is approval-gated and you cannot tell which), and
                  never deployDiagnosticsSidecar or captureHeapDump either. A diagnostic that cannot
                  run approval-free appears BOTH under Evidence Gaps and as a bulleted JVM Evidence
                  row in exactly this format:
                  - Tool=analyzeJvm | Evidence=<exact reason it could not run approval-free>
                  and RECOMMEND the gated operation in the report for the user to decide.
                  EXCEPTION: a gated operation the user's own message explicitly requested is called
                  normally - its approval prompt is the user's designed control point.
                - If any tool fails, report the error but still call the remaining tools.
                - Then return one final report with required sections and tool evidence rows.
                - EXCEPTION: If no pods match the user's service name query, you may return a text response stating "No pods matching '<service-name>' were found" without calling diagnostic tools. Do NOT investigate unrelated pods.
                """.formatted(TOOL_FIRST_RETRY_MARKER);
        return appendGuardInstruction(request, retryInstruction, TOOL_FIRST_RETRY_MARKER);
    }

    List<String> validateResponse(String responseText) {
        List<String> violations = new ArrayList<>();
        if (responseText == null || responseText.isBlank()) {
            return violations;
        }

        // If the agent reports no matching pods, the full RCA format is not required
        if (isNoMatchingPodsResponse(responseText)) {
            log.info("Skipping RCA format validation: response indicates no matching pods found");
            return violations;
        }

        if (countMatches(EXEC_SUMMARY_HEADING, responseText) > 1) {
            violations.add("duplicate_final_report");
        }
        if (!K8S_EVIDENCE_HEADING.matcher(responseText).find()) {
            violations.add("missing_kubernetes_evidence_section");
        } else {
            String k8sEvidence = extractSection(responseText, "Kubernetes Evidence");
            if (!TOOL_EVIDENCE_ROW.matcher(k8sEvidence).find()) {
                violations.add("missing_kubernetes_tool_evidence_rows");
            }
        }
        if (!JVM_EVIDENCE_HEADING.matcher(responseText).find()) {
            violations.add("missing_jvm_evidence_section");
        } else {
            String jvmEvidence = extractSection(responseText, "JVM Evidence");
            if (!TOOL_EVIDENCE_ROW.matcher(jvmEvidence).find()) {
                violations.add("missing_jvm_tool_evidence_rows");
            }
        }
        if (!FINDINGS_HEADING.matcher(responseText).find()) {
            violations.add("missing_findings_section");
        } else {
            String findingsSection = extractSection(responseText, "Findings");
            if (CONFIRMED_SUBSECTION.matcher(findingsSection).find()) {
                String confirmedBlock = extractSubSection(
                        findingsSection,
                        "confirmed",
                        List.of("hypothesis", "recommended actions", "recommendations", "evidence gaps"));
                if (!confirmedBlock.isBlank() && !EVIDENCE_REFERENCE.matcher(confirmedBlock).find()) {
                    violations.add("confirmed_findings_without_evidence_refs");
                }
            }
        }
        if (!RECOMMENDATIONS_HEADING.matcher(responseText).find()) {
            violations.add("missing_recommendations_section");
        }
        if (!EVIDENCE_GAPS_HEADING.matcher(responseText).find()) {
            violations.add("missing_evidence_gaps_section");
        }

        return violations;
    }

    private String extractSection(String text, String sectionName) {
        if (text == null || text.isBlank()) {
            return "";
        }
        Pattern sectionPattern = Pattern.compile(
                "(?is)^#{2,3}\\s*" + Pattern.quote(sectionName) + "\\b(.*?)(?=^#{2,3}\\s+|\\z)",
                Pattern.MULTILINE);
        Matcher matcher = sectionPattern.matcher(text);
        return matcher.find() ? matcher.group(1) : "";
    }

    private String extractSubSection(String sectionText, String startMarker, List<String> endMarkers) {
        if (sectionText == null || sectionText.isBlank()) {
            return "";
        }
        String lower = sectionText.toLowerCase(Locale.ROOT);
        int start = lower.indexOf(startMarker.toLowerCase(Locale.ROOT));
        if (start < 0) {
            return "";
        }
        int contentStart = start + startMarker.length();
        int end = lower.length();
        for (String marker : endMarkers) {
            int markerIndex = lower.indexOf(marker.toLowerCase(Locale.ROOT), contentStart);
            if (markerIndex >= 0 && markerIndex < end) {
                end = markerIndex;
            }
        }
        return sectionText.substring(contentStart, end);
    }

    private ModelRequest appendGuardInstruction(ModelRequest request, String guardInstruction, String dedupeMarker) {
        List<Message> updatedMessages = new ArrayList<>(request.getMessages() != null
                ? request.getMessages()
                : List.of());

        boolean updatedSystem = false;
        for (int i = 0; i < updatedMessages.size(); i++) {
            Message msg = updatedMessages.get(i);
            if (msg.getMessageType() == MessageType.SYSTEM) {
                String current = msg.getText() != null ? msg.getText() : "";
                if (current.contains(dedupeMarker)) {
                    return request;
                }
                updatedMessages.set(i, new SystemMessage(current + "\n\n" + guardInstruction));
                updatedSystem = true;
                break;
            }
        }
        if (!updatedSystem) {
            updatedMessages.add(0, new SystemMessage(guardInstruction));
        }

        ModelRequest.Builder builder = ModelRequest.builder(request).messages(updatedMessages);
        if (request.getSystemMessage() != null) {
            String existing = request.getSystemMessage().getText() != null
                    ? request.getSystemMessage().getText()
                    : "";
            if (!existing.contains(dedupeMarker)) {
                builder.systemMessage(new SystemMessage(existing + "\n\n" + guardInstruction));
            }
        }
        return builder.build();
    }

    String extractLastUserPrompt(ModelRequest request) {
        if (request.getMessages() == null || request.getMessages().isEmpty()) {
            return "";
        }
        List<Message> messages = request.getMessages();
        for (int i = messages.size() - 1; i >= 0; i--) {
            Message message = messages.get(i);
            if (message instanceof UserMessage || message.getMessageType() == MessageType.USER) {
                return message.getText() != null ? message.getText() : "";
            }
        }
        return "";
    }

    private static boolean shouldValidate(String userPrompt, AssistantMessage assistant, String responseText) {
        if (assistant == null || assistant.hasToolCalls()) {
            return false;
        }
        if (responseText == null || responseText.isBlank()) {
            return false;
        }
        return isSlowPodPrompt(userPrompt);
    }

    private static boolean isSlowPodPrompt(String userPrompt) {
        String prompt = userPrompt != null ? userPrompt.toLowerCase(Locale.ROOT) : "";
        boolean hasPerformanceSignal = prompt.contains("slow")
                || prompt.contains("slowness")
                || prompt.contains("latency")
                || prompt.contains("high cpu")
                || prompt.contains("high memory")
                || prompt.contains("memory pressure")
                || prompt.contains("rca")
                || prompt.contains("root cause")
                || prompt.contains("diagnos")
                || prompt.contains("investigate")
                || prompt.contains("analyze")
                || prompt.contains("troubleshoot");
        boolean podScoped = prompt.contains("pod")
                || prompt.contains("deployment")
                || prompt.contains("workload")
                || prompt.contains("namespace")
                || prompt.contains("container")
                || prompt.contains("service")
                || prompt.contains("[context:");
        return hasPerformanceSignal && podScoped;
    }

    private static boolean shouldRetryForToolFirst(String userPrompt,
                                                   AssistantMessage assistant,
                                                   List<String> violations) {
        if (!isSlowPodPrompt(userPrompt)) {
            return false;
        }
        if (assistant == null || assistant.hasToolCalls()) {
            return false;
        }
        if (violations == null || violations.isEmpty()) {
            return false;
        }

        // If the agent explicitly reports no matching pods, do NOT force a retry.
        // The prompt guardrail tells the agent to report "no matching pods" when the
        // user's service name doesn't match any pod in the cluster. Forcing a retry
        // would cause the agent to investigate unrelated pods (hallucination).
        String responseText = assistant.getText();
        if (responseText != null && isNoMatchingPodsResponse(responseText)) {
            log.info("Skipping tool-first retry: agent reports no matching pods for user's service query");
            return false;
        }

        return violations.contains("duplicate_final_report")
                || violations.contains("missing_kubernetes_evidence_section")
                || violations.contains("missing_jvm_evidence_section")
                || violations.contains("missing_findings_section")
                || violations.contains("missing_recommendations_section")
                || violations.contains("missing_evidence_gaps_section")
                || violations.contains("missing_kubernetes_tool_evidence_rows")
                || violations.contains("missing_jvm_tool_evidence_rows");
    }

    /** Check if the response indicates no pods matched the query. */
    private static boolean isNoMatchingPodsResponse(String responseText) {
        String lower = responseText.toLowerCase(Locale.ROOT);
        return lower.contains("no pods matching")
                || lower.contains("no matching pods")
                || lower.contains("no pods found matching")
                || lower.contains("no pods related to")
                || lower.contains("could not find any pods")
                || lower.contains("no pod with")
                || lower.contains("no pods named")
                || (lower.contains("not found") && lower.contains("pod"));
    }

    private static AssistantMessage extractAssistantMessage(ModelResponse response) {
        if (response == null) {
            return null;
        }
        if (response.getChatResponse() != null
                && response.getChatResponse().getResult() != null
                && response.getChatResponse().getResult().getOutput() != null) {
            return response.getChatResponse().getResult().getOutput();
        }
        Object message = response.getMessage();
        if (message instanceof AssistantMessage assistantMessage) {
            return assistantMessage;
        }
        return null;
    }

    private static int countMatches(Pattern pattern, String text) {
        if (text == null || text.isBlank()) {
            return 0;
        }
        int count = 0;
        Matcher matcher = pattern.matcher(text);
        while (matcher.find()) {
            count++;
        }
        return count;
    }

    @Override
    public String getName() {
        return "slow-pod-rca-guard";
    }
}
