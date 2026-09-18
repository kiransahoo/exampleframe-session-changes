package com.#exampleframe#.orchestrator.config;

import com.alibaba.cloud.ai.graph.agent.a2a.A2aRemoteAgent;
import com.alibaba.cloud.ai.graph.agent.a2a.AgentCardProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.AbstractEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A2A (Agent-to-Agent) Configuration.
 * <p>
 * Dynamically creates A2aRemoteAgent beans by reading agent definitions from
 * {@code #exampleframe#.orchestrator.remote-agents.*} in application.yaml.
 * <p>
 * To add a new agent, add its entry to {@code #exampleframe#.orchestrator.remote-agents} in
 * application.yaml. No code changes required.
 *
 * <h2>Resilience</h2>
 * Discovery is hardened in two layers because Nacos and the child agents start
 * asynchronously and an agent's Nacos registration can be dropped and re-acquired at
 * runtime (pod restart, heartbeat lapse, Nacos blip):
 * <ol>
 *   <li><b>Startup retry</b> - {@link #remoteAgents()} retries discovery so an
 *       orchestrator that boots before Nacos/agents are ready does not end up
 *       permanently agent-less.</li>
 *   <li><b>Runtime self-heal</b> - {@link #selfHealDiscovery()} re-runs discovery on a
 *       fixed interval and updates the <i>same</i> shared map in place, so an agent that
 *       (re)registers after startup is re-acquired automatically with no orchestrator
 *       restart. The bean is a {@link ConcurrentHashMap}, so every consumer that holds
 *       the injected reference sees the refresh.</li>
 * </ol>
 */
@Configuration
public class A2AConfig {

    private static final Logger logger = LoggerFactory.getLogger(A2AConfig.class);

    private static final String DEFAULT_INSTRUCTION = "Execute the following task: {input}";
    private static final String REMOTE_AGENTS_PREFIX = "#exampleframe#.orchestrator.remote-agents.";

    /** Marker attempt number for periodic self-heal passes - suppresses per-agent startup logs. */
    private static final int SELF_HEAL_ATTEMPT = -1;

    private final Environment environment;

    @Value("${#exampleframe#.orchestrator.a2a.enabled:true}")
    private boolean a2aEnabled;

    /**
     * Max startup discovery attempts. When the orchestrator boots first it can see
     * "Nacos version too low" (501, registry not ready) or "Agent not found" (404, agent
     * not registered yet). We retry so a startup race does not leave the orchestrator
     * permanently agent-less until a manual restart.
     */
    @Value("${#exampleframe#.orchestrator.a2a.discovery.max-attempts:10}")
    private int discoveryMaxAttempts;

    /** Delay between startup discovery attempts (ms). 10 × 6s = up to ~60s for the registry to converge. */
    @Value("${#exampleframe#.orchestrator.a2a.discovery.retry-delay-ms:6000}")
    private long discoveryRetryDelayMs;

    @Autowired(required = false)
    private AgentCardProvider agentCardProvider;

    /**
     * The live, shared remote-agent map. A single mutable instance is exposed as the
     * {@code remoteAgents} bean and mutated in place by {@link #selfHealDiscovery()} so
     * refreshes propagate to every holder of the injected reference.
     */
    private final Map<String, A2aRemoteAgent> liveAgents = new ConcurrentHashMap<>();

    /** Agent keys discovered from configuration at startup - the set the self-heal pass re-resolves. */
    private volatile Set<String> agentKeys = Collections.emptySet();

    public A2AConfig(Environment environment) {
        this.environment = environment;
    }

    @Bean
    public Map<String, A2aRemoteAgent> remoteAgents() {
        logger.info("A2A Configuration - enabled: {}, AgentCardProvider: {}",
                a2aEnabled, agentCardProvider != null ? agentCardProvider.getClass().getSimpleName() : "null");

        Set<String> keys = discoverAgentKeys();
        Set<String> disabled = DisabledAgents.from(environment);
        if (!disabled.isEmpty()) {
            keys.removeIf(disabled::contains);
            logger.info("A2A: agents disabled for this deployment: {} - not discovered", disabled);
        }
        this.agentKeys = keys;
        logger.info("Discovered {} remote agent keys: {}", keys.size(), keys);

        if (keys.isEmpty()) {
            logger.warn("No remote agents configured in '{}'", REMOTE_AGENTS_PREFIX);
            return liveAgents;
        }

        // URL fallback mode (a2a disabled or no card provider): building an agent needs no
        // registry round-trip, so one pass is definitive - an agent missing after it (e.g. a
        // key with no 'name' property) will never appear on retry. The loop below would burn
        // the full retry budget on every boot and emit the discovery-failure WARN signature
        // for a condition that is not a failure.
        if (!a2aEnabled || agentCardProvider == null) {
            liveAgents.putAll(buildAgentsOnce(keys, 1));
            logger.info("A2A URL fallback mode - discovery skipped; {} of {} agents created: {}",
                    liveAgents.size(), keys.size(), liveAgents.keySet());
            return liveAgents;
        }

        // STARTUP RETRY: retry until either (a) every configured agent resolves, or (b) the
        // resolved count plateaus with at least one agent found (so genuinely-absent agents -
        // e.g. dbt not deployed - don't force a full timeout). An empty result keeps retrying
        // for the full budget, since 0 agents almost always means Nacos isn't ready yet.
        int previousCount = -1;
        for (int attempt = 1; attempt <= discoveryMaxAttempts; attempt++) {
            Map<String, A2aRemoteAgent> resolved = buildAgentsOnce(keys, attempt);
            liveAgents.putAll(resolved);

            if (liveAgents.size() == keys.size()) {
                logger.info("A2A discovery: all {} agents resolved on attempt {}/{}",
                        liveAgents.size(), attempt, discoveryMaxAttempts);
                break;
            }
            if (liveAgents.size() > 0 && liveAgents.size() == previousCount) {
                logger.warn("A2A discovery: count plateaued at {}/{} after attempt {} - proceeding. "
                                + "Unresolved keys (likely not deployed yet): {}. Runtime self-heal will keep retrying.",
                        liveAgents.size(), keys.size(), attempt, missing(keys, liveAgents));
                break;
            }
            previousCount = liveAgents.size();
            if (attempt < discoveryMaxAttempts) {
                logger.warn("A2A discovery attempt {}/{}: resolved {}/{} (missing {}). "
                                + "Nacos registry or agent registration not ready - retrying in {}ms.",
                        attempt, discoveryMaxAttempts, liveAgents.size(), keys.size(),
                        missing(keys, liveAgents), discoveryRetryDelayMs);
                sleepQuietly(discoveryRetryDelayMs);
            }
        }

        logger.info("Total A2A remote agents created: {} - {}", liveAgents.size(), liveAgents.keySet());
        return liveAgents;
    }

    /**
     * Runtime self-heal: re-runs discovery on a fixed interval and refreshes the shared
     * {@link #liveAgents} map in place. Re-acquires any agent that (re)registers after
     * startup - e.g. after a pod restart or a Nacos heartbeat lapse - so the orchestrator
     * recovers without a manual restart.
     *
     * <p>Newly-resolved agents are added and existing ones are refreshed with a fresh bean.
     * Agents that fail to resolve in a given pass are <i>not</i> removed: that avoids
     * flapping on a transient blip, and the primary delegation path invokes agents by their
     * configured URL anyway, so a momentarily-stale Nacos entry is harmless.
     */
    @Scheduled(
            fixedDelayString = "${#exampleframe#.orchestrator.a2a.discovery.refresh-interval-ms:30000}",
            initialDelayString = "${#exampleframe#.orchestrator.a2a.discovery.refresh-initial-delay-ms:30000}")
    public void selfHealDiscovery() {
        if (!a2aEnabled || agentCardProvider == null) {
            return; // fallback (URL) mode - nothing to re-resolve from Nacos
        }
        Set<String> keys = this.agentKeys;
        if (keys.isEmpty()) {
            return;
        }

        Map<String, A2aRemoteAgent> resolved = buildAgentsOnce(keys, SELF_HEAL_ATTEMPT);
        if (resolved.isEmpty()) {
            return; // registry not ready this pass - keep existing liveAgents
        }

        Set<String> newlyAcquired = new LinkedHashSet<>(resolved.keySet());
        newlyAcquired.removeAll(liveAgents.keySet());

        liveAgents.putAll(resolved); // refresh existing beans + add new ones, in place

        if (!newlyAcquired.isEmpty()) {
            logger.info("A2A self-heal: re-acquired agent(s) {} - now {}/{} live: {}",
                    newlyAcquired, liveAgents.size(), keys.size(), liveAgents.keySet());
        }
    }

    /** One full discovery pass - builds every configured agent, skipping ones that fail this attempt. */
    private Map<String, A2aRemoteAgent> buildAgentsOnce(Set<String> keys, int attempt) {
        Map<String, A2aRemoteAgent> agents = new HashMap<>();
        for (String agentKey : keys) {
            String name = environment.getProperty(REMOTE_AGENTS_PREFIX + agentKey + ".name");
            String description = environment.getProperty(REMOTE_AGENTS_PREFIX + agentKey + ".description", agentKey + " agent");
            String url = environment.getProperty(REMOTE_AGENTS_PREFIX + agentKey + ".url");

            if (name == null || name.isBlank()) {
                if (attempt == 1) {
                    logger.warn("Skipping agent '{}': no 'name' property configured", agentKey);
                }
                continue;
            }

            try {
                A2aRemoteAgent.Builder builder = A2aRemoteAgent.builder()
                        .name(name)
                        .description(description)
                        .instruction(DEFAULT_INSTRUCTION);

                if (a2aEnabled && agentCardProvider != null) {
                    builder.agentCardProvider(agentCardProvider);
                }

                agents.put(agentKey, builder.build());
                if (attempt == 1) {
                    logger.info("Created A2aRemoteAgent '{}' ({}) {}", agentKey, name,
                            (a2aEnabled && agentCardProvider != null) ? "via Nacos discovery" : "(fallback URL: " + url + ")");
                }
            } catch (Exception e) {
                // Expected during startup races (501 Nacos-not-ready / 404 not-registered-yet)
                // and for genuinely-undeployed agents. Only log during the startup retry pass -
                // the every-30s self-heal pass would otherwise spam this for absent agents.
                if (attempt != SELF_HEAL_ATTEMPT) {
                    logger.debug("Attempt {}: A2aRemoteAgent '{}' not resolvable: {}", attempt, agentKey, e.getMessage());
                }
            }
        }
        return agents;
    }

    private static Set<String> missing(Set<String> all, Map<String, ?> resolved) {
        Set<String> m = new LinkedHashSet<>(all);
        m.removeAll(resolved.keySet());
        return m;
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Discovers agent keys by scanning all property sources for keys matching
     * {@code #exampleframe#.orchestrator.remote-agents.<key>.*}.
     */
    private Set<String> discoverAgentKeys() {
        Set<String> keys = new LinkedHashSet<>();

        if (environment instanceof AbstractEnvironment abstractEnv) {
            for (org.springframework.core.env.PropertySource<?> ps : abstractEnv.getPropertySources()) {
                if (ps instanceof EnumerablePropertySource<?> eps) {
                    for (String propName : eps.getPropertyNames()) {
                        if (propName.startsWith(REMOTE_AGENTS_PREFIX)) {
                            // Extract the agent key: #exampleframe#.orchestrator.remote-agents.<key>.xxx
                            String remainder = propName.substring(REMOTE_AGENTS_PREFIX.length());
                            int dot = remainder.indexOf('.');
                            if (dot > 0) {
                                keys.add(remainder.substring(0, dot));
                            }
                        }
                    }
                }
            }
        }

        return keys;
    }
}
