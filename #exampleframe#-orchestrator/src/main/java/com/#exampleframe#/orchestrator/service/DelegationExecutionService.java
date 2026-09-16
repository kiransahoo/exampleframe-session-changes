package com.#exampleframe#.orchestrator.service;




import com.#exampleframe#.orchestrator.security.CallerContext;
import com.#exampleframe#.orchestrator.security.CallerContextRegistry;
import com.#exampleframe#.orchestrator.federation.DomainAccessPolicy;
import com.#exampleframe#.orchestrator.federation.DomainCellRouter;
import com.alibaba.cloud.ai.graph.agent.a2a.A2aRemoteAgent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.#exampleframe#.orchestrator.config.AgentConfigResolver;
import com.#exampleframe#.orchestrator.config.AppContextResolver;
import com.#exampleframe#.orchestrator.config.AppContextResolver.TextAnalysis;
import com.#exampleframe#.orchestrator.config.OrchestratorProperties.RemoteAgentProperties;
import com.#exampleframe#.orchestrator.exception.ChildAgentHitlException;
import com.#exampleframe#.orchestrator.federation.FederationProperties;
import com.#exampleframe#.orchestrator.health.AgentHealthRegistry;
import com.#exampleframe#.orchestrator.routing.InstanceRoutingService;
import com.#exampleframe#.orchestrator.tools.AgentInvocationService;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.#exampleframe#.orchestrator.tools.ToolFormatUtils.*;

/**
 * Centralized delegation logic - scope validation, context propagation, agent
 * invocation, and evidence board recording. Used by both tool callbacks and playbooks.
 */
@Service
public class DelegationExecutionService {

    private static final Logger logger = LoggerFactory.getLogger(DelegationExecutionService.class);

    private static final Pattern TIME_RANGE_PATTERN = Pattern.compile(
            "(?:last |past |in the (?:last |past )?)(\\d+)\\s*" +
                    "(hours?|hrs?|h|days?|d|minutes?|mins?|m|weeks?|w)",
            Pattern.CASE_INSENSITIVE);

    /** Detects "Multiple tools with the same name" errors from Spring AI framework. */
    private static final Pattern MULTIPLE_TOOLS_ERROR = Pattern.compile(
            "(?i)multiple tools with the same name|duplicate.*tool.*name|" +
                    "more than one tool.*(same|matching).*name");

    private final AgentInvocationService invocationService;
    private final InvestigationStateService investigationStateService;
    private final Map<String, A2aRemoteAgent> remoteAgents;
    private final AgentConfigResolver agentConfigResolver;
    private final ObjectMapper objectMapper;
    private final @Nullable AppContextResolver appContextResolver;
    private final @Nullable AgentHealthRegistry healthRegistry;
    private final @Nullable InstanceRoutingService instanceRoutingService;
    private final @Nullable FederationProperties federationProperties;

    public DelegationExecutionService(
            AgentInvocationService invocationService,
            InvestigationStateService investigationStateService,
            @Nullable Map<String, A2aRemoteAgent> remoteAgents,
            AgentConfigResolver agentConfigResolver,
            ObjectMapper objectMapper,
            @Nullable AppContextResolver appContextResolver,
            @org.springframework.beans.factory.annotation.Autowired(required = false)
            @Nullable AgentHealthRegistry healthRegistry,
            @org.springframework.beans.factory.annotation.Autowired(required = false)
            @Nullable InstanceRoutingService instanceRoutingService,
            @org.springframework.beans.factory.annotation.Autowired(required = false)
            @Nullable FederationProperties federationProperties) {
        this.invocationService = invocationService;
        this.investigationStateService = investigationStateService;
        this.remoteAgents = remoteAgents != null ? remoteAgents : Map.of();
        this.agentConfigResolver = agentConfigResolver;
        this.objectMapper = objectMapper;
        this.appContextResolver = appContextResolver;
        this.healthRegistry = healthRegistry;
        this.instanceRoutingService = instanceRoutingService;
        this.federationProperties = federationProperties;
    }

    private @Nullable DomainAccessPolicy domainAccessPolicy;
    private @Nullable DomainCellRouter domainCellRouter;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setDomainCellRouter(@Nullable DomainCellRouter router) {
        this.domainCellRouter = router;
    }
    private @Nullable CallerContextRegistry callerContextRegistry;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setDomainAccessPolicy(@Nullable DomainAccessPolicy policy) {
        this.domainAccessPolicy = policy;
    }

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setCallerContextRegistry(@Nullable CallerContextRegistry registry) {
        this.callerContextRegistry = registry;
    }

    /** Non-null denial message when {@code agentKey} is a domain cell the bound caller may not use. */
    /**
     * Ownership check: a service belongs to exactly one domain, so an investigation for it must
     * run in that domain's cell. The model chooses the delegation tool, and it can choose wrong -
     * a payments question sent to the claims cell would be answered with claims agents against
     * claims backends. Refusing (rather than silently redirecting) keeps the decision auditable
     * and lets the model retry with the right cell.
     *
     * @return an error message to return to the model, or null when the delegation may proceed
     */
    public @Nullable String checkDomainOwnership(String agentKey, @Nullable String task,
                                                 @Nullable String threadId, Map<String, String> extraParams) {
        if (federationProperties == null || !federationProperties.enabled()
                || !federationProperties.isDomainCell(agentKey) || domainCellRouter == null) {
            return null;
        }
        String targetDomain = federationProperties.domainForAgentKey(agentKey);
        String service = explicitServiceName(task, extraParams);
        if (targetDomain == null || service == null || service.isBlank()) {
            return null;                       // nothing to compare against - existing behaviour
        }
        String owningDomain;
        try {
            owningDomain = domainCellRouter.owningDomain(service).orElse(null);
        } catch (RuntimeException e) {
            // the caller is not authorised for the owning domain: that is checkDomainAccess's
            // decision to make on the target cell, not a reason to fail this lookup
            logger.debug("[{}] ownership lookup for service '{}' could not resolve: {}",
                    agentKey, service, e.getMessage());
            return null;
        }
        if (owningDomain == null) {
            // A KNOWN service whose mapping names no owning domain: a guess would silently
            // land the investigation in some domain's cell. Fail closed and make the model
            // ask - an unknown service never reaches this point (explicitServiceName is null).
            logger.warn("[audit] delegation for service '{}' to cell '{}' REFUSED - the service "
                    + "mapping has no 'domain' key, so no cell can be chosen", service, agentKey);
            return "ERROR: '" + service + "' is a known service but its mapping has no 'domain' "
                    + "key, so its owning cell cannot be determined. EVERY cell returns this "
                    + "same error for this service - do not retry any delegation tool. Your "
                    + "ONLY next step: ask the user which domain owns '" + service + "', then "
                    + "delegate to that domain's cell. (An operator can make the answer "
                    + "permanent with 'domain: <name>' in the service mapping.)";
        }
        if (owningDomain.equals(targetDomain)) {
            return null;
        }
        String owningCell = federationProperties.domainsOrEmpty().get(owningDomain) != null
                ? federationProperties.domainsOrEmpty().get(owningDomain).agentKey() : owningDomain + "-cell";
        logger.warn("[audit] delegation for service '{}' (domain '{}') to cell '{}' REFUSED - "
                + "wrong domain; owning cell is '{}'", service, owningDomain, agentKey, owningCell);
        return "ERROR: '" + service + "' belongs to the '" + owningDomain + "' domain, not '"
                + targetDomain + "'. Send this investigation to " + owningCell
                + " instead - each domain's agents only see their own systems.";
    }

    public @Nullable String checkDomainAccess(String agentKey, @Nullable String threadId) {
        if (federationProperties == null || domainAccessPolicy == null || !federationProperties.isDomainCell(agentKey)) {
            return null;
        }
        String domain = federationProperties.domainForAgentKey(agentKey);
        if (domain == null) {
            return null;
        }
        CallerContext caller = callerContextRegistry != null
                ? callerContextRegistry.resolve(threadId)
                : CallerContext.capture();
        if (domainAccessPolicy.canAccess(domain, caller)) {
            return null;
        }
        logger.warn("[audit] delegation to cell '{}' (domain '{}') DENIED for caller '{}' thread={}",
                agentKey, domain, caller.name(), threadId);
        return "Access denied: this account is not authorised for the '" + domain
                + "' domain, so the request was not sent to " + agentKey
                + ". Ask a domain owner for access or investigate a service you own.";
    }

    /** Union of configured + discovered agent keys - the controlled catalog for user-facing lists. */
    private java.util.Set<String> availableAgentKeys() {
        java.util.Set<String> keys = new java.util.LinkedHashSet<>(agentConfigResolver.getAgentConfigs().keySet());
        keys.addAll(remoteAgents.keySet());
        return keys;
    }

    /** Validates scope-required parameters. Returns error message if missing, null if valid. */
    @Nullable
    public String validateScope(String agentKey, Map<String, String> params) {
        RemoteAgentProperties config = agentConfigResolver.getAgentConfigs().get(agentKey);
        if (config == null) return null;

        List<String> scopeParams = config.scopeParameters();
        if (scopeParams == null || scopeParams.isEmpty()) return null;

        List<String> missing = new ArrayList<>();
        for (String scopeParam : scopeParams) {
            String value = params.get(scopeParam);
            if (value == null || value.isBlank()) {
                Map<String, String> defaults = config.parameterDefaults();
                String defaultVal = (defaults != null) ? defaults.get(scopeParam) : null;
                if (defaultVal == null || defaultVal.isBlank()) {
                    missing.add(scopeParam);
                }
            }
        }

        if (!missing.isEmpty()) {
            return "ERROR: Cannot invoke " + capitalizeFirst(agentKey) +
                    " agent without scope. Please ask the user to specify: " +
                    String.join(", ", missing) +
                    ". Do not guess or infer these values.";
        }
        return null;
    }

    /**
     * Invokes an agent with full context propagation and evidence board recording.
     * @throws ChildAgentHitlException if the child agent requires approval
     */
    public String delegateToAgent(
            String agentKey,
            String task,
            String threadId,
            Map<String, String> extraParams,
            @Nullable String priorFindings) throws ChildAgentHitlException {
        return delegateToAgent(agentKey, task, threadId, extraParams, priorFindings, null);
    }

    /**
     * Variant with an absolute deadline for the remote call - playbook steps pass their
     * remaining budget so a child call can never outlive the playbook that started it.
     * @throws ChildAgentHitlException if the child agent requires approval
     */
    public String delegateToAgent(
            String agentKey,
            String task,
            String threadId,
            Map<String, String> extraParams,
            @Nullable String priorFindings,
            @Nullable Instant deadline) throws ChildAgentHitlException {
        return delegateToAgent(agentKey, task, threadId, extraParams, priorFindings, deadline, false);
    }

    /**
     * Pinned-route variant for callers that resolved the target cell MECHANICALLY - the meta's
     * playbook door routes by the service mapping, or by the user's own answer to "which
     * domain?". The wrong-tool-choice ownership guard ({@link #checkDomainOwnership}) exists
     * for the model's free choice of delegation tool; on this path it would refuse the very
     * answer it demands (a known service with no 'domain' key fails closed with "ask the user
     * which domain" - which the user just answered). Access control still applies unchanged.
     * @throws ChildAgentHitlException if the child agent requires approval
     */
    public String delegateToAgentPinned(
            String agentKey,
            String task,
            String threadId,
            Map<String, String> extraParams,
            @Nullable String priorFindings) throws ChildAgentHitlException {
        return delegateToAgent(agentKey, task, threadId, extraParams, priorFindings, null, true);
    }

    private String delegateToAgent(
            String agentKey,
            String task,
            String threadId,
            Map<String, String> extraParams,
            @Nullable String priorFindings,
            @Nullable Instant deadline,
            boolean pinnedRoute) throws ChildAgentHitlException {
        // An agent type this deployment does not have (disabled, or a meta that only routes to
        // cells) must fail immediately with what IS available - not wait out discovery self-heal.
        if (!agentConfigResolver.getAgentConfigs().containsKey(agentKey)) {
            logger.warn("[{}] delegation refused: not an agent of this deployment (available: {})",
                    agentKey, availableAgentKeys());
            return "ERROR: '" + agentKey + "' is not an agent of this deployment. Available: "
                    + String.join(", ", availableAgentKeys()) + ".";
        }

        // Validate scope
        String scopeError = validateScope(agentKey, extraParams);
        if (scopeError != null) {
            return scopeError;
        }

        // Per-domain access: a delegation into a domain cell is authorised against the caller
        // bound to this thread at the HTTP boundary (not the worker thread's empty security
        // context). Denied = the tool returns a clear message and nothing leaves this cell.
        String domainDenied = checkDomainAccess(agentKey, threadId);
        if (domainDenied != null) {
            return domainDenied;
        }
        if (!pinnedRoute) {
            String wrongDomain = checkDomainOwnership(agentKey, task, threadId, extraParams);
            if (wrongDomain != null) {
                return wrongDomain;
            }
        }

        // Build full agent input with context prefixes
        String input = buildAgentInput(agentKey, task, threadId, extraParams, priorFindings);

        // Cell routing (per-domain architecture): if the service mapping carries a routing
        // key (e.g. kubernetes.cluster: claims-prod), delegate to that specific agent
        // instance by URL. Health-aware with failover handled inside the router - an empty
        // route means "use the default path below", which behaves exactly as before.
        if (instanceRoutingService != null) {
            String routedService = resolveServiceNameForRouting(task, threadId, extraParams);
            InstanceRoutingService.ResolvedRoute route =
                    instanceRoutingService.route(agentKey, routedService, task).orElse(null);
            if (route != null) {
                // Routed delegations invoke by URL on the HITL path - they don't depend on
                // the default agent's discovery entry or health probe.
                logger.info("[{}] Delegation routed to instance '{}' at {} (service '{}')",
                        agentKey, route.instanceKey(), route.url(), routedService);
                String response = invocationService.invokeAgentWithTracking(
                        remoteAgents.get(agentKey), input, agentKey, task, route.url(),
                        callContext(threadId, task, extraParams, deadline));
                if (isMultipleToolsError(response)) {
                    logger.warn("[{}] Detected 'multiple tools' error - returning clarification prompt",
                            agentKey);
                    return buildClarificationResponse(agentKey);
                }
                recordToEvidenceBoard(threadId, agentKey, response);
                return response;
            }
        }

        // Look up the A2aRemoteAgent. If it isn't in the live discovery map yet, it may be
        // mid-recovery: its registration dropped and the orchestrator self-heal re-acquires it
        // every ~30s (while the agent reconciler re-registers it). Wait briefly for that
        // re-acquisition before failing, so a transient discovery gap is invisible to the user
        // instead of surfacing "agent not available". Does NOT bypass discovery - it only waits
        // for the normal self-heal to repopulate the map.
        A2aRemoteAgent agent = remoteAgents.get(agentKey);
        if (agent == null) {
            // Fail fast for an agent that is confirmed dead: not in discovery AND its health probe
            // says DOWN. The 30s reacquire-wait + URL fallback only make sense for an agent that is
            // reachable but temporarily de-registered (health UP) - a partial Nacos drop or full
            // Nacos outage. A configured-but-undeployed agent (health DOWN) would just burn the full
            // wait and then fail, so return immediately with a clear offline message. Health unknown
            // (null) still gets the wait+fallback - we only short-circuit on a confirmed-DOWN probe.
            if (healthRegistry != null) {
                AgentHealthRegistry.AgentHealth h = healthRegistry.snapshot().get(agentKey);
                // Snapshot says DOWN, but it can be up to one refresh interval (~30s) stale - an
                // agent that just recovered would be wrongly blocked. Confirm with a single live
                // probe and only fail fast if it is *currently* unreachable.
                if (h != null && !h.up() && !healthRegistry.probeNow(agentKey)) {
                    logger.warn("[{}] not in discovery AND live health probe DOWN ({}) - failing "
                            + "fast, skipping reacquire-wait/URL fallback", agentKey, h.reason());
                    String error = capitalizeFirst(agentKey) + " agent is offline (" + h.reason()
                            + "). Not retrying - the agent is not deployed or not reachable.";
                    return formatAgentError(agentKey, error, getAvailableAgentsList(availableAgentKeys()));
                }
            }
            agent = waitForAgentReacquire(agentKey);
        }

        // Resolve the configured Service-DNS URL - used both as the routing target and as the
        // last-resort fallback when Nacos discovery never produced a card for this agent.
        RemoteAgentProperties config = agentConfigResolver.getAgentConfigs().get(agentKey);
        String baseUrl = (config != null) ? config.url() : null;

        if (agent == null) {
            // LAST-RESORT config-driven fallback: the agent isn't in the Nacos-discovered map even
            // after waiting for self-heal. If we have its configured Service-DNS URL, invoke via it
            // directly. This is NOT a new bypass: the HITL path already invokes through
            // HitlAwareA2aClient (a WebClient POST to the agent's /a2a endpoint) using baseUrl and
            // never touches the A2aRemoteAgent object - so a null agent is safe on that path. Keeps
            // known/healthy agents reachable even if their Nacos registration never succeeds.
            if (baseUrl == null || baseUrl.isBlank()) {
                String error = capitalizeFirst(agentKey) +
                        " agent is not available. Check agent configuration and connectivity.";
                return formatAgentError(agentKey, error,
                        getAvailableAgentsList(availableAgentKeys()));
            }
            logger.warn("[{}] not in Nacos discovery map after wait - config-driven fallback, "
                    + "routing directly to configured URL {}", agentKey, baseUrl);
        }

        logger.info("[{}] Delegating with full input ({} chars):\n{}", agentKey,
                input.length(), input);
        logger.debug("[{}] Delegating via HITL path with baseUrl={}", agentKey, baseUrl);

        // Throws ChildAgentHitlException - NOT wrapped here; caller decides wrapping strategy
        String response = invocationService.invokeAgentWithTracking(
                agent, input, agentKey, task, baseUrl,
                callContext(threadId, task, extraParams, deadline));

        // Detect "Multiple tools with the same name" errors - convert to user-friendly clarification
        if (isMultipleToolsError(response)) {
            logger.warn("[{}] Detected 'multiple tools' error - returning clarification prompt", agentKey);
            return buildClarificationResponse(agentKey);
        }

        // Record result to evidence board
        recordToEvidenceBoard(threadId, agentKey, response);

        return response;
    }

    /**
     * Per-call context for the outbound A2A invocation: the conversation thread (context
     * retention on cells + unified cost/caller attribution), the bare task + structured
     * params (deterministic playbook routing on cells - the labeled text envelope is not
     * parseable back apart), and the call deadline. {@code task}/{@code priorFindings}
     * entries are internal to the envelope and never sent as task params.
     */
    /** Wire task params stay small: scope values (service, timeRange, namespace, ...), never
     *  long extracted analysis text - playbook context maps can carry multi-KB fields
     *  (source code, trace output) that no receiver reads as a parameter. */
    private static final int TASK_PARAM_MAX_VALUE_CHARS = 500;
    private static final int TASK_PARAM_MAX_ENTRIES = 24;

    private static @Nullable A2aCallContext callContext(@Nullable String threadId, String task,
                                                        @Nullable Map<String, String> extraParams,
                                                        @Nullable Instant deadline) {
        Map<String, String> taskParams = new java.util.LinkedHashMap<>();
        if (extraParams != null) {
            for (Map.Entry<String, String> e : extraParams.entrySet()) {
                if ("task".equals(e.getKey()) || "priorFindings".equals(e.getKey())) continue;
                if (e.getValue() == null || e.getValue().isBlank()) continue;
                if (e.getValue().length() > TASK_PARAM_MAX_VALUE_CHARS) continue;
                taskParams.put(e.getKey(), e.getValue());
                if (taskParams.size() >= TASK_PARAM_MAX_ENTRIES) break;
            }
        }
        return new A2aCallContext(threadId, task, taskParams.isEmpty() ? null : taskParams, deadline, null);
    }

    /** Max time to wait for a missing agent to be re-acquired before failing the delegation. */
    private static final long AGENT_WAIT_MAX_MS = 30_000L;
    /** Poll interval while waiting for re-acquisition. */
    private static final long AGENT_WAIT_POLL_MS = 3_000L;

    /**
     * Waits briefly for an agent to (re)appear in the live discovery map, polling until the
     * deadline. Covers the window where an agent's registration dropped and the orchestrator
     * self-heal (every ~30s) is re-acquiring it - so a transient discovery gap is hidden from the
     * user instead of surfacing as "agent not available". Returns the agent once available, or
     * {@code null} on timeout. Does not trigger discovery itself; it only waits for the self-heal.
     */
    @Nullable
    private A2aRemoteAgent waitForAgentReacquire(String agentKey) {
        long deadline = System.currentTimeMillis() + AGENT_WAIT_MAX_MS;
        logger.info("[{}] not in live discovery map - waiting up to {}ms for self-heal re-acquisition",
                agentKey, AGENT_WAIT_MAX_MS);
        while (System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(AGENT_WAIT_POLL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return remoteAgents.get(agentKey);
            }
            A2aRemoteAgent agent = remoteAgents.get(agentKey);
            if (agent != null) {
                logger.info("[{}] re-acquired after waiting - proceeding with delegation", agentKey);
                return agent;
            }
        }
        logger.warn("[{}] still not in live discovery map after {}ms - failing delegation",
                agentKey, AGENT_WAIT_MAX_MS);
        return null;
    }

    /**
     * Resolves the service name for instance routing, mirroring the app-context analysis
     * in {@link #buildAgentInput} but read-only (no activeService mutation): explicit
     * {@code service} param, else known service in the task text, else - for follow-up
     * turns with no topic signal - the thread's active service.
     */
    @Nullable
    /**
     * Dry run of the routing decision for a task, exactly as {@link #delegateToAgent} makes it:
     * which service the text resolves to and which instance (if any) it would be sent to. Used
     * by the Settings UI so an operator can check "where would this question go?" without
     * spending an LLM turn.
     */
    public Map<String, Object> previewRoute(String agentKey, String task, @Nullable String threadId) {
        Map<String, Object> out = new java.util.LinkedHashMap<>();
        String service = resolveServiceNameForRouting(task, threadId, Map.of());
        out.put("agentKey", agentKey);
        out.put("resolvedService", service);
        // Meta level: which domain cell owns the service (federation), before any instance routing
        if (domainCellRouter != null && federationProperties != null && federationProperties.enabled()) {
            try {
                var cell = domainCellRouter.route(service, callerContextRegistry != null
                        ? callerContextRegistry.resolve(threadId) : CallerContext.capture()).orElse(null);
                if (cell != null) {
                    out.put("domain", cell.domain());
                    out.put("cellAgentKey", cell.cellAgentKey());
                    var cellCfg = agentConfigResolver.getAgentConfigs().get(cell.cellAgentKey());
                    out.put("cellUrl", cellCfg != null ? cellCfg.url() : null);
                    out.put("routed", true);
                    out.put("reason", "service '" + service + "' belongs to domain '" + cell.domain()
                            + "' - the whole investigation is delegated to its cell; that cell does its own instance routing");
                    return out;
                }
            } catch (DomainCellRouter.DomainAccessDeniedException denied) {
                out.put("routed", false);
                out.put("denied", true);
                out.put("reason", denied.getMessage());
                return out;
            }
        }
        InstanceRoutingService.ResolvedRoute route = instanceRoutingService != null
                ? instanceRoutingService.route(agentKey, service, task).orElse(null) : null;
        if (route != null) {
            out.put("routed", true);
            out.put("instanceKey", route.instanceKey());
            out.put("url", route.url());
            out.put("registeredName", route.registeredName());
            boolean mentioned = instanceRoutingService.isMentioned(agentKey, task, route.instanceKey());
            String mappedTo = service != null
                    ? instanceRoutingService.route(agentKey, service).map(InstanceRoutingService.ResolvedRoute::instanceKey).orElse(null) : null;
            out.put("reason", mentioned && mappedTo != null && !mappedTo.equals(route.instanceKey())
                    ? "the task names instance '" + route.instanceKey() + "' (overrides service '" + service + "' mapping '" + mappedTo + "')"
                    : mentioned && mappedTo == null ? "the task names instance '" + route.instanceKey() + "'"
                    : "service '" + service + "' maps to instance '" + route.instanceKey() + "'");
        } else {
            out.put("routed", false);
            var cfg = agentConfigResolver.getAgentConfigs().get(agentKey);
            out.put("url", cfg != null ? cfg.url() : null);
            String intended = instanceRoutingService != null
                    ? instanceRoutingService.intendedInstance(agentKey, service, task).orElse(null) : null;
            if (intended != null) {
                out.put("intendedInstance", intended);
                out.put("reason", instanceRoutingService.explainMiss(agentKey, intended));
            } else {
                out.put("reason", service == null
                        ? "no known service or instance name in the task - default agent URL"
                        : "service '" + service + "' has no routing key for " + agentKey + " - default agent URL");
            }
        }
        return out;
    }

    /**
     * The service this task explicitly names - the {@code service} parameter, or a known service
     * in the task text. Deliberately excludes the thread's active service and the conversation
     * fallback that {@link #resolveServiceNameForRouting} uses for routing: those are good
     * guesses for choosing a backend, but too weak to refuse a delegation over.
     */
    private @Nullable String explicitServiceName(@Nullable String task, Map<String, String> params) {
        if (appContextResolver == null) {
            return null;
        }
        String explicit = params == null ? null : params.get("service");
        String text = (explicit != null && !explicit.isBlank()) ? explicit : task;
        if (text == null || text.isBlank()) {
            return null;
        }
        return appContextResolver.analyzeText(text).knownServiceName();
    }

    private String resolveServiceNameForRouting(String task, @Nullable String threadId,
                                                Map<String, String> params) {
        if (appContextResolver == null) {
            return null;
        }
        String textForAnalysis = (params.get("service") != null && !params.get("service").isBlank())
                ? params.get("service") : task;
        TextAnalysis analysis = appContextResolver.analyzeText(textForAnalysis);
        if (analysis.knownServiceName() != null) {
            return analysis.knownServiceName();
        }
        if (analysis.isNewTopic()) {
            return null;
        }
        String activeService = threadId != null
                ? investigationStateService.getActiveService(threadId) : null;
        if (activeService != null) {
            return activeService;
        }
        // Last resort: the LLM-composed task text often drops the service name the
        // user gave ("run this SQL" with no service mention). Look for a known
        // service in the recent conversation turns so the delegation still routes
        // to the cell/instance the conversation is about.
        String conversation = buildConversationContext(threadId);
        if (conversation != null && !conversation.isBlank()) {
            TextAnalysis conversationAnalysis = appContextResolver.analyzeText(conversation);
            if (conversationAnalysis.knownServiceName() != null) {
                logger.debug("Routing service resolved from conversation context: '{}'",
                        conversationAnalysis.knownServiceName());
                return conversationAnalysis.knownServiceName();
            }
        }
        return null;
    }

    /** Builds agent input with conversation history, prior findings, and labeled context params. */
    /**
     * Params that name WHAT to investigate rather than WHERE the service lives. A value the
     * caller supplies for these is typically evidence discovered upstream (a table named in
     * the service's source code, a build id from the failing pipeline) - forwarding it is the
     * point of cross-agent work - so it outranks the mapping's default even in authoritative
     * mode. Identity and infrastructure params (schema, namespace, workload, workspace,
     * cloudRoleName, ...) stay mapping-authoritative: a hallucinated one misdirects the agent
     * entirely. Applies uniformly to every agent, since all delegation input flows through
     * {@link #buildAgentInput}.
     */
    private static final java.util.Set<String> INVESTIGATION_TARGET_PARAMS =
            java.util.Set.of("tables", "buildId", "definitionId");

    /** One target item: a plain identifier that cannot break the [Param: value] envelope. */
    private static final java.util.regex.Pattern TARGET_ITEM =
            java.util.regex.Pattern.compile("[A-Za-z0-9][A-Za-z0-9_$#.\\-]{0,63}");
    private static final java.util.Set<String> TARGET_SENTINELS =
            java.util.Set.of("all", "unknown", "none", "any", "*", "%");
    private static final int MAX_TARGET_ITEMS = 8;

    /**
     * A caller value for an investigation-target param may outrank the mapping only when it
     * is EVIDENCE, not invention. The tool call is composed by the orchestrator's model, so
     * each comma-separated item must (a) be a plain identifier - no wildcards, sentinels,
     * brackets or newlines that could corrupt the labeled envelope or inject instructions -
     * and (b) appear verbatim in the current task or the prior findings the child agent will
     * see, the two trusted carriers of upstream discovery. Failing items are dropped with an
     * audit log; returns null when nothing survives, so the caller falls back to the mapping.
     */
    private @Nullable String groundedTargets(String agentKey, String paramName, String rawValue,
                                             String task, @Nullable String priorFindings) {
        String haystack = (task + "\n" + (priorFindings == null ? "" : priorFindings))
                .toLowerCase(java.util.Locale.ROOT);
        List<String> kept = new ArrayList<>();
        for (String item : rawValue.split(",")) {
            String t = item.trim();
            if (t.isEmpty()) continue;
            if (kept.size() >= MAX_TARGET_ITEMS) {
                logger.info("[audit] [{}] {} target list capped at {} items - rest dropped",
                        agentKey, paramName, MAX_TARGET_ITEMS);
                break;
            }
            if (!TARGET_ITEM.matcher(t).matches()
                    || TARGET_SENTINELS.contains(t.toLowerCase(java.util.Locale.ROOT))) {
                logger.info("[audit] [{}] dropping invalid {} target '{}' - not a plain identifier",
                        agentKey, paramName,
                        t.replaceAll("[^A-Za-z0-9_$#.\\-]", "?").substring(0, Math.min(60, t.length())));
                continue;
            }
            if (!haystack.contains(t.toLowerCase(java.util.Locale.ROOT))) {
                logger.info("[audit] [{}] dropping ungrounded {} target '{}' - it appears in "
                        + "neither the task nor the forwarded prior findings", agentKey, paramName, t);
                continue;
            }
            if (!kept.contains(t)) kept.add(t);
        }
        return kept.isEmpty() ? null : String.join(",", kept);
    }

    public String buildAgentInput(
            String agentKey,
            String task,
            String threadId,
            Map<String, String> params,
            @Nullable String priorFindings) {

        StringBuilder input = new StringBuilder();

        // Data-governance boundary (cell-per-domain): a delegation to a domain cell is
        // findings-confined - no meta conversation history, no other domains' findings,
        // no global hypothesis. Claims evidence stays in the Claims cell.
        boolean isolatedCell = federationProperties != null
                && federationProperties.isIsolatedCell(agentKey);

        // Prepend conversation history (never across a domain-cell boundary)
        String conversationContext = isolatedCell ? null : buildConversationContext(threadId);
        if (conversationContext != null && !conversationContext.isBlank()) {
            input.append("[Conversation history:\n")
                    .append(conversationContext.trim())
                    .append("]\n");
        }

        // Prepend prior findings - use explicit if provided, else auto-fill from evidence board
        String priorFindingsSource;
        if (isolatedCell) {
            // The boundary must hold structurally: the priorFindings tool argument is
            // LLM-composed at the meta and can carry another domain's evidence, so for
            // cell targets it is ALWAYS dropped and replaced with the cell's own
            // recorded findings - regardless of what the caller supplied.
            if (priorFindings != null && !priorFindings.isBlank()) {
                logger.warn("[{}] Findings isolation: dropping caller-supplied prior findings "
                                + "({} chars) - only the cell's own evidence is forwarded",
                        agentKey, priorFindings.length());
            }
            priorFindings = buildOwnCellFindings(threadId, agentKey);
            priorFindingsSource = "evidence-board (own cell only)";
        } else if (priorFindings == null || priorFindings.isBlank()) {
            priorFindings = buildPriorFindings(threadId, agentKey);
            priorFindingsSource = "evidence-board";
        } else {
            priorFindingsSource = "llm-generated";
        }
        if (priorFindings != null && !priorFindings.isBlank()) {
            input.append("[Prior findings: ").append(priorFindings.trim()).append("] ");
            logger.info("[{}] Prior findings ({}, {} chars): {}", agentKey,
                    priorFindingsSource, priorFindings.length(), priorFindings);
        } else {
            logger.info("[{}] No prior findings (first agent call)", agentKey);
        }

        // --- App-context resolution (text-based, non-LLM-dependent) ---
        Map<String, String> appContextDefaults = Map.of();
        boolean appContextIsAuthoritative = false;

        if (appContextResolver != null) {
            // When the caller provides an explicit "service" param (e.g., from playbook context),
            // analyze THAT instead of the full task text. The resolved task may contain parameter
            // values like "Schema=orders" that cause false alias matches (e.g., "orders" -> "order-service").
            String explicitService = params.get("service") != null && !params.get("service").isBlank()
                    ? params.get("service").trim() : null;
            String textForAnalysis = explicitService != null ? explicitService : task;
            TextAnalysis analysis = appContextResolver.analyzeText(textForAnalysis);

            if (analysis.knownServiceName() != null) {
                // Known service in task -> authoritative mode, update activeService
                if (threadId != null) {
                    investigationStateService.setActiveService(threadId, analysis.knownServiceName());
                }
                appContextDefaults = appContextResolver.getAgentContext(analysis.knownServiceName(), agentKey);
                appContextIsAuthoritative = true;
                logger.info("[{}] App-context (authoritative) for '{}': {}",
                        agentKey, analysis.knownServiceName(), appContextDefaults);

            } else if (analysis.isNewTopic()) {
                // Unknown service or entity detected -> new topic -> clear activeService
                if (threadId != null) {
                    investigationStateService.setActiveService(threadId, null);
                }
                logger.info("[{}] New topic detected in task (unknown service or entity) - clearing activeService",
                        agentKey);

            } else if (explicitService != null) {
                // The caller NAMED a service and it is not in the mapping. The previous
                // subject's scope must not be lent to it: filling namespace/workload/schema
                // from whatever was active before would silently investigate a different
                // service's infrastructure under this one's name. An unmapped service simply
                // has no app-context - the caller's own values stand.
                logger.info("[{}] Explicit service '{}' has no mapping - no app-context "
                        + "(previous subject's scope deliberately NOT applied)", agentKey, explicitService);

            } else {
                // No signal -> follow-up turn -> try activeService in suggestion mode
                String activeService = threadId != null
                        ? investigationStateService.getActiveService(threadId) : null;
                if (activeService != null) {
                    appContextDefaults = appContextResolver.getAgentContext(activeService, agentKey);
                    if (!appContextDefaults.isEmpty()) {
                        logger.info("[{}] App-context (suggestion) for '{}' via activeService: {}",
                                agentKey, activeService, appContextDefaults);
                    }
                }
            }
        }

        // Apply parameter defaults and build labeled context parameters
        RemoteAgentProperties config = agentConfigResolver.getAgentConfigs().get(agentKey);
        if (config != null) {
            Map<String, String> paramDefs = config.parameters();
            if (paramDefs != null) {
                for (String paramName : paramDefs.keySet()) {
                    if ("task".equals(paramName)) continue;

                    String value;
                    String targetNote = null;
                    String ctxValue = appContextDefaults.get(paramName);

                    if (appContextIsAuthoritative && ctxValue != null && !ctxValue.isBlank()) {
                        String llmValue = params.get(paramName);
                        String grounded = null;
                        if (INVESTIGATION_TARGET_PARAMS.contains(paramName)
                                && llmValue != null && !llmValue.isBlank() && !llmValue.equals(ctxValue)) {
                            grounded = groundedTargets(agentKey, paramName, llmValue, task, priorFindings);
                        }
                        if (grounded != null) {
                            // A grounded investigation TARGET is evidence a previous agent
                            // discovered (a table the service's source code touches) -
                            // forwarding it is the point of cross-agent work, so it outranks
                            // the mapping's default scope. The precedence is stated in the
                            // composed input so the receiving agent acts on the target first
                            // and treats the mapping's list as fallback scope only.
                            value = grounded;
                            targetNote = " (investigation target from the caller - analyze this first;"
                                    + " mapping default: " + ctxValue + ")";
                            logger.info("[{}] Grounded investigation target '{}'='{}' outranks "
                                    + "the mapping's default '{}'", agentKey, paramName, grounded, ctxValue);
                        } else {
                            // AUTHORITATIVE: app-context wins - identity and infrastructure
                            // scope come from the mapping, never from a model's guess.
                            value = ctxValue;
                            if (llmValue != null && !llmValue.isBlank() && !llmValue.equals(ctxValue)) {
                                logger.info("[{}] App-context override for '{}': LLM had '{}', using '{}'",
                                        agentKey, paramName, llmValue, ctxValue);
                            }
                        }
                    } else {
                        // SUGGESTION or no app-context: LLM > app-context > parameterDefaults
                        value = params.get(paramName);

                        // A model-composed target is only usable if it survives the same
                        // grounding and identifier checks as in authoritative mode - the
                        // envelope must never carry invented or injected values, whichever
                        // mode this delegation happens to be in.
                        if (INVESTIGATION_TARGET_PARAMS.contains(paramName)
                                && value != null && !value.isBlank()) {
                            value = groundedTargets(agentKey, paramName, value, task, priorFindings);
                        }

                        // Telemetry identity belongs to the mapping, even in suggestion mode.
                        // On a follow-up hop the model composes its task from the PREVIOUS agent's
                        // findings, so its idea of "the service" is whatever that agent talked
                        // about - the pod it inspected. That produced [PackageName: java-healthy]
                        // (a Kubernetes workload) for a service whose telemetry identity is
                        // order-service, and the query matched nothing. Nulling here lets the
                        // fallback below supply ctxValue; the complementary guard that follows
                        // covers the opposite case (no mapping at all).
                        //
                        // Provenance, stated honestly: packageName/cloudRoleName are in no
                        // playbook param-definition and no playbook's refs, so no PROMPTED user
                        // value can reach here - but a user CAN name a package in free text on a
                        // follow-up turn and the model will pass it through, and this overrides
                        // it. Narrower than it sounds: packageName values are auto-indexed as
                        // service aliases (AppContextResolver.addImplicitAlias), so naming a
                        // MAPPED package switches the subject and takes the authoritative path.
                        // Only a package prefix in no mapping is overridden, and it is logged.
                        if ("azure-monitor".equals(agentKey)
                                && ctxValue != null && !ctxValue.isBlank()
                                && value != null && !value.isBlank() && !value.equals(ctxValue)
                                && ("packageName".equalsIgnoreCase(paramName)
                                    || "cloudRoleName".equalsIgnoreCase(paramName))) {
                            logger.info("[{}] Telemetry identity '{}' comes from the mapping: "
                                    + "model had '{}', using '{}'", agentKey, paramName, value, ctxValue);
                            value = null;
                        }

                        // Identity params the model INVENTED are worse than none: an imagined
                        // cloudRoleName/packageName filters real telemetry down to zero rows.
                        // For a service with no app-context mapping for this agent, drop the
                        // model's value and let the service-name fallback below supply identity.
                        if ("azure-monitor".equals(agentKey)
                                && appContextDefaults.isEmpty()
                                && value != null && !value.isBlank()
                                && ("packageName".equalsIgnoreCase(paramName)
                                    || "cloudRoleName".equalsIgnoreCase(paramName))) {
                            String serviceName = params.get("service");
                            if (serviceName == null || !value.trim().equalsIgnoreCase(serviceName.trim())) {
                                logger.info("[audit] [{}] dropping unmapped identity param '{}'='{}' "
                                        + "(service not in app-context; the model may have invented it)",
                                        agentKey, paramName, value);
                                value = null;
                            }
                        }
                        if (value == null || value.isBlank()) {
                            if (ctxValue != null && !ctxValue.isBlank()) {
                                value = ctxValue;
                            } else {
                                Map<String, String> defaults = config.parameterDefaults();
                                if (defaults != null) {
                                    value = defaults.get(paramName);
                                }
                            }
                        }
                    }

                    // For timeRange: extract from conversation history (unchanged behavior)
                    if ("timeRange".equalsIgnoreCase(paramName)
                            && (value == null || value.isBlank()
                                || (config.parameterDefaults() != null
                                    && value.equals(config.parameterDefaults().get(paramName))))) {
                        if (conversationContext != null && !conversationContext.isBlank()) {
                            String extracted = extractTimeRangeFromHistory(conversationContext);
                            if (extracted != null) {
                                logger.info("[{}] Overriding default timeRange '{}' with '{}' " +
                                        "extracted from conversation history", agentKey, value, extracted);
                                value = extracted;
                            }
                        }
                    }

                    // Normalize empty namespace to "all" for kubernetes agent (local variable, no map mutation)
                    if ("namespace".equalsIgnoreCase(paramName) && "kubernetes".equals(agentKey)) {
                        if (value == null || value.isBlank()) {
                            value = "all";
                            logger.info("[{}] Normalized empty namespace to 'all' for cross-namespace search", agentKey);
                        }
                    }

                    // For azure-monitor: if packageName or cloudRoleName is still empty,
                    // fall back to the service name as-is. This ensures unknown services
                    // (not in app-context) still get explicit identity passed to the agent,
                    // preventing the LLM from guessing a different service from prior findings.
                    //
                    // Gated on appContextDefaults.isEmpty(): only fall back for services with NO
                    // app-context mapping for this agent. A service mapped cloudRoleName-only (no
                    // packageName) must NOT have its missing packageName filled with the service
                    // name - that would add a redundant/over-restrictive KQL clause. Leave it empty
                    // and let the precise cloud_RoleName filter scope the query.
                    if ("azure-monitor".equals(agentKey)
                            && (value == null || value.isBlank())
                            && appContextDefaults.isEmpty()
                            && ("packageName".equalsIgnoreCase(paramName)
                                || "cloudRoleName".equalsIgnoreCase(paramName))) {
                        String serviceName = params.get("service");
                        if (serviceName != null && !serviceName.isBlank()) {
                            value = serviceName.trim();
                            logger.info("[{}] Falling back '{}' to service name '{}' (not in app-context)",
                                    agentKey, paramName, value);
                        }
                    }

                    if (value != null && !value.isBlank()) {
                        input.append("[").append(capitalizeFirst(paramName))
                                .append(": ").append(value.trim());
                        if (targetNote != null) {
                            input.append(targetNote);
                        }
                        input.append("] ");
                    }
                }
            }
        }

        input.append(task);
        return input.toString();
    }

    /** Records result to evidence board, filtering out error responses. */
    public void recordToEvidenceBoard(String threadId, String agentKey, String response) {
        try {
            if (response == null || response.isBlank()) return;

            // Don't record error responses as "findings" - they're noise
            if (isErrorResponse(response)) {
                logger.debug("[{}] Skipping evidence board recording - response is an error",
                        agentKey);
                return;
            }

            if (threadId != null) {
                investigationStateService.recordAgentResult(threadId, agentKey, response);
                logger.debug("[{}] Recorded result to evidence board ({} chars)",
                        agentKey, response.length());
            } else {
                logger.warn("[{}] Cannot record to evidence board - no threadId available",
                        agentKey);
            }
        } catch (Exception e) {
            // Never let evidence board recording fail the actual invocation
            logger.warn("[{}] Failed to record result to evidence board: {}",
                    agentKey, e.getMessage());
        }
    }

    /** Extracts KQL-compatible time range from conversation text (e.g., "50 hrs" -> "50h"). */
    @Nullable
    public static String extractTimeRangeFromHistory(@Nullable String conversationContext) {
        if (conversationContext == null || conversationContext.isBlank()) return null;

        Matcher matcher = TIME_RANGE_PATTERN.matcher(conversationContext);
        if (!matcher.find()) return null;

        String num = matcher.group(1);
        String unit = matcher.group(2).toLowerCase();

        return switch (unit.charAt(0)) {
            case 'h' -> num + "h";
            case 'd' -> num + "d";
            case 'm' -> num + "m";
            case 'w' -> (Integer.parseInt(num) * 7) + "d";
            default -> null;
        };
    }

    /**
     * Findings-confined variant for domain cells: only the target cell's OWN prior
     * findings from this thread (continuity across turns - cells run fresh context per
     * task), never another domain's findings or the thread's global hypothesis.
     */
    @Nullable
    private String buildOwnCellFindings(@Nullable String threadId, String cellAgentKey) {
        if (threadId == null) {
            return null;
        }
        String own = investigationStateService.getFindings(threadId).get(cellAgentKey);
        if (own == null || own.isBlank()) {
            return null;
        }
        return own.length() > 2000 ? own.substring(0, 1997) + "..." : own;
    }

    /** Builds prior findings from evidence board, excluding the target agent. Capped at 2000 chars. */
    @Nullable
    public String buildPriorFindings(String threadId, String excludeAgent) {
        if (threadId == null) return null;

        Map<String, String> findings = investigationStateService.getFindings(threadId);
        String hypothesis = investigationStateService.getHypothesisSummary(threadId, 280);
        if (findings.isEmpty() && (hypothesis == null || hypothesis.isBlank())) return null;

        // Filter out this agent's own findings - only include OTHER agents
        StringBuilder sb = new StringBuilder();
        if (hypothesis != null && !hypothesis.isBlank()) {
            sb.append(hypothesis);
        }
        for (Map.Entry<String, String> entry : findings.entrySet()) {
            if (entry.getKey().equals(excludeAgent)) continue;
            if (!sb.isEmpty()) sb.append(" | ");
            sb.append(capitalizeFirst(entry.getKey())).append(": ").append(entry.getValue());
        }

        String result = sb.toString();
        // Cap at 2000 chars
        if (result.length() > 2000) {
            result = result.substring(0, 1997) + "...";
        }
        return result.isBlank() ? null : result;
    }

    /** Builds conversation context from investigation state. */
    @Nullable
    public String buildConversationContext(String threadId) {
        if (threadId == null) return null;
        return investigationStateService.getConversationContext(threadId, 800);
    }


    /** Detects "Multiple tools with the same name" errors from Spring AI. */
    private boolean isMultipleToolsError(@Nullable String response) {
        if (response == null) return false;
        return MULTIPLE_TOOLS_ERROR.matcher(response).find();
    }

    /** User-friendly clarification prompt when duplicate tool errors occur. */
    private String buildClarificationResponse(String agentKey) {
        return switch (agentKey) {
            case "kubernetes" -> """
                    Your query is broad and matched multiple investigation areas. \
                    Please specify what you'd like to investigate:

                    - **Pod health** - pod status, restarts, OOM kills, resource usage
                    - **Deployments** - rollout status, replica count, recent changes
                    - **Logs** - container logs, error patterns, previous container logs
                    - **Events** - Kubernetes warnings, FailedScheduling, FailedMount
                    - **JVM diagnostics** - thread dumps, heap analysis, GC metrics
                    - **Cluster overview** - node resources, pending pods, HPA status

                    Try a more specific question like:
                    - "Show all pods with restarts > 0"
                    - "What's the CPU/memory usage across all pods?"
                    - "Are there any warning events in the last 30 minutes?"
                    - "Show cluster node resource utilization"
                    """;
            case "oracle" -> """
                    Your query is broad and matched multiple investigation areas. \
                    Please specify what you'd like to investigate:

                    - **Sessions** - active sessions, blocking tree, long-running queries
                    - **SQL performance** - top SQL by elapsed time, execution plans
                    - **Locks** - row lock contention, deadlocks, TX enqueue waits
                    - **Storage** - tablespace usage, datafile I/O, temp usage
                    - **Database health** - overview, wait events, alert log errors

                    Try a more specific question like:
                    - "Show active sessions and what they're executing"
                    - "Find top 10 SQL by elapsed time"
                    - "Check for blocking sessions"
                    - "Show tablespace usage over 80%"
                    """;
            case "azure-monitor" -> """
                    Your query is broad and matched multiple investigation areas. \
                    Please specify what you'd like to investigate:

                    - **Exceptions** - error rates, stack traces, exception types
                    - **Performance** - latency hotspots, P95/P99, slow operations
                    - **Dependencies** - downstream call failures, latency by dependency
                    - **Traces** - end-to-end request tracing, trace chain analysis
                    - **Source code** - fetch and analyze application source code

                    Try a more specific question like:
                    - "Show exceptions in the last hour grouped by type"
                    - "What are the slowest operations by P95 latency?"
                    - "Show failed dependency calls in the last 30 minutes"
                    """;
            default -> """
                    Your query is broad and matched multiple investigation areas on the %s agent. \
                    Please be more specific about what you'd like to investigate.
                    """.formatted(capitalizeFirst(agentKey));
        };
    }
}
