package com.#exampleframe#.orchestrator.config;

/**
 * The single derivation of a cell's A2A identity: {@code <agent>__<cell-name>}.
 * <p>
 * Two places announce this orchestrator's agent name to the outside world - the
 * {@code /.well-known/agent.json} response and the {@link io.a2a.spec.AgentCard}
 * bean that {@code A2aServerRegistryAutoConfiguration} registers into Nacos. Both
 * MUST derive the name here. They once used two independent derivations: the
 * well-known endpoint suffixed the cell name while the registered card kept the
 * static {@code spring.ai.alibaba.a2a.server.card.name} literal, so a cell
 * advertised {@code orchestrator_agent__claims} over HTTP while registering
 * {@code orchestrator_agent} in Nacos - and a meta resolving cells by name through
 * the registry found nothing (worse, every cell registered the SAME literal, so
 * name lookups collided across cells).
 */
public final class CellAgentNames {

    private CellAgentNames() {
    }

    /**
     * @param baseName the agent's base name (e.g. {@code orchestrator_agent})
     * @param cellName {@code #exampleframe#.orchestrator.federation.cell-name}; blank on a
     *                 meta or single-cell deployment
     * @return {@code baseName__cellName}, or {@code baseName} unchanged when this is not
     *         a named cell or the suffix is already present (idempotent)
     */
    public static String cellCardName(String baseName, String cellName) {
        if (baseName == null || baseName.isBlank() || cellName == null || cellName.isBlank()) {
            return baseName;
        }
        String suffix = "__" + cellName.trim();
        return baseName.endsWith(suffix) ? baseName : baseName + suffix;
    }
}
