package com.#exampleframe#.orchestrator.service;

import com.#exampleframe#.orchestrator.config.AgentConfigResolver;
import com.#exampleframe#.orchestrator.config.PlaybookProperties.PlaybookDefinition;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * The ONE definition of "can this deployment run this playbook itself": every agent an
 * UNCONDITIONAL step delegates to must exist in the resolved agent configs. Conditional
 * steps may be skipped at runtime, so a missing agent there does not disqualify; absent
 * or empty configs mean "cannot judge" and never filter.
 *
 * <p>Shared by {@link PlaybookRouter} (scored-match candidate filter) and
 * {@link PlaybookResolver} (explicit-reference and forward decisions) - the meta's
 * forwarding gate compares exactly this judgement from both classes, so the rule must
 * never fork between them.
 */
final class PlaybookAgentAvailability {

    private static final Logger logger = LoggerFactory.getLogger(PlaybookAgentAvailability.class);

    private PlaybookAgentAvailability() {}

    /**
     * @param label what to call the playbook in the log line (id or display name)
     */
    static boolean available(@Nullable PlaybookDefinition definition,
                             @Nullable AgentConfigResolver agentConfigResolver,
                             String label) {
        if (agentConfigResolver == null || definition == null || definition.steps() == null) {
            return true;
        }
        Set<String> have = agentConfigResolver.getAgentConfigs().keySet();
        if (have.isEmpty()) {
            return true;                       // configs not resolved yet - do not filter blindly
        }
        for (var step : definition.steps()) {
            boolean conditional = step.condition() != null && !step.condition().isBlank();
            if (!conditional && step.agent() != null && !have.contains(step.agent())) {
                logger.info("Playbook '{}' not runnable on this deployment: unconditional step '{}' needs "
                        + "agent '{}', this deployment has {}", label, step.id(), step.agent(), have);
                return false;
            }
        }
        return true;
    }
}
