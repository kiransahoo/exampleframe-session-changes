package com.#exampleframe#.orchestrator.exception;

import java.util.List;

/**
 * A delegated cell matched a playbook but cannot run it without values only the user can
 * supply - and the delegated path cannot prompt. The cell answers the A2A call with
 * {@code status: input_required} naming the missing param keys (the exact shape of the
 * {@code pending_approval} contract HITL already uses), the meta's client raises this, and
 * the meta's gateway asks the user the same question the cell's own door would have asked,
 * then re-forwards the original request with the reply merged in as caller-provided params.
 * <p>
 * Without this, a forwarded "why is X slow" whose mapping lacks e.g. {@code oracle.schema}
 * silently degraded to a single-agent ReAct answer while the direct door prompted and ran
 * the full playbook ({@code A2aIngressController}: "missing required params on the
 * delegated path - deferring to ReAct").
 * <p>
 * Unchecked, mirroring {@link ChildAgentHitlException}: it must traverse
 * {@code HitlAwareA2aClient -> AgentInvocationService -> DelegationExecutionService}
 * unwrapped; every catch-all on that path rethrows it explicitly.
 */
public class CellInputRequiredException extends RuntimeException {

    private final String agentName;
    private final String taskId;
    private final String playbookId;
    private final String playbookName;
    private final List<String> missingKeys;
    /** Cell-authored prompt per missing key (insertion-ordered); may be empty for old cells. */
    private final java.util.Map<String, String> missingPrompts;

    public CellInputRequiredException(String agentName, String taskId, String playbookId,
                                      String playbookName, List<String> missingKeys) {
        this(agentName, taskId, playbookId, playbookName, missingKeys, java.util.Map.of());
    }

    public CellInputRequiredException(String agentName, String taskId, String playbookId,
                                      String playbookName, List<String> missingKeys,
                                      java.util.Map<String, String> missingPrompts) {
        super("Cell '" + agentName + "' needs user input for playbook '" + playbookId
                + "': missing " + missingKeys);
        this.agentName = agentName;
        this.taskId = taskId;
        this.playbookId = playbookId;
        this.playbookName = playbookName;
        this.missingKeys = missingKeys == null ? List.of() : List.copyOf(missingKeys);
        this.missingPrompts = missingPrompts == null
                ? java.util.Map.of()
                : java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(missingPrompts));
    }

    public String getAgentName() {
        return agentName;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getPlaybookId() {
        return playbookId;
    }

    public String getPlaybookName() {
        return playbookName;
    }

    public List<String> getMissingKeys() {
        return missingKeys;
    }

    public java.util.Map<String, String> getMissingPrompts() {
        return missingPrompts;
    }
}
