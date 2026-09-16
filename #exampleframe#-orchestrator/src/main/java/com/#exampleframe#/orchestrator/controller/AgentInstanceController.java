package com.#exampleframe#.orchestrator.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.#exampleframe#.orchestrator.config.AgentConfigResolver;
import com.#exampleframe#.orchestrator.config.OrchestratorProperties.RemoteAgentProperties;
import com.#exampleframe#.orchestrator.health.AgentHealthRegistry;
import com.#exampleframe#.orchestrator.routing.AgentInstanceRegistry;
import com.#exampleframe#.orchestrator.routing.AgentInstanceRegistry.RuntimeInstance;
import com.#exampleframe#.orchestrator.provisioning.KubernetesProvisioner;
import com.#exampleframe#.orchestrator.routing.RoutingKeyResolver;
import com.#exampleframe#.orchestrator.security.CurrentPrincipal;
import com.#exampleframe#.orchestrator.service.DelegationExecutionService;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Onboard a cluster / database from the Settings UI: register where an agent instance
 * listens, without an orchestrator restart. Read is for any viewer; add/remove is
 * ADMIN-only because the orchestrator will present the shared service token to the
 * URL. On add, the URL must answer the A2A agent card and identify itself as
 * {@code <agentName>__<instanceKey>} - proof it is a #ExampleFrame# agent deployed with that
 * {@code AGENT_INSTANCE_ID}, not a typo or an arbitrary host.
 *
 * @author kiransahoo
 */
@RestController
@RequestMapping("/api/settings/agent-instances")
@CrossOrigin(origins = "*")
public class AgentInstanceController {

    private static final Logger log = LoggerFactory.getLogger(AgentInstanceController.class);
    private static final Duration CARD_TIMEOUT = Duration.ofSeconds(6);

    private final AgentInstanceRegistry registry;
    private final AgentConfigResolver agentConfigResolver;
    private final @Nullable AgentHealthRegistry healthRegistry;
    private final @Nullable RoutingKeyResolver routingKeyResolver;
    private final @Nullable KubernetesProvisioner provisioner;
    private @Nullable DelegationExecutionService delegationExecutionService;
    private final WebClient webClient;
    private final ObjectMapper objectMapper;

    public AgentInstanceController(AgentInstanceRegistry registry,
                                   AgentConfigResolver agentConfigResolver,
                                   @Autowired(required = false) @Nullable AgentHealthRegistry healthRegistry,
                                   @Autowired(required = false) @Nullable RoutingKeyResolver routingKeyResolver,
                                   @Autowired(required = false) @Nullable KubernetesProvisioner provisioner,
                                   WebClient.Builder webClientBuilder,
                                   ObjectMapper objectMapper) {
        this.registry = registry;
        this.agentConfigResolver = agentConfigResolver;
        this.healthRegistry = healthRegistry;
        this.routingKeyResolver = routingKeyResolver;
        this.provisioner = provisioner;
        this.webClient = webClientBuilder.clone().build();
        this.objectMapper = objectMapper;
    }

    @Autowired(required = false)
    public void setDelegationExecutionService(
            @Nullable DelegationExecutionService delegationExecutionService) {
        this.delegationExecutionService = delegationExecutionService;
    }

    /** Everything routable: yaml and runtime instances per agent, with health and provenance. */
    @PreAuthorize("@authz.viewer()")
    @GetMapping
    public Map<String, Object> list() {
        Map<String, RemoteAgentProperties> configs = agentConfigResolver.getAgentConfigs();
        Map<String, AgentHealthRegistry.AgentHealth> health = healthRegistry != null
                ? healthRegistry.snapshot() : Map.of();
        Map<String, Map<String, RuntimeInstance>> runtime = registry.byAgent();

        List<Map<String, Object>> agentTypes = new ArrayList<>();
        List<Map<String, Object>> instances = new ArrayList<>();
        for (Map.Entry<String, RemoteAgentProperties> e : new TreeMap<>(configs).entrySet()) {
            RemoteAgentProperties props = e.getValue();
            Map<String, Object> t = new LinkedHashMap<>();
            t.put("agentKey", e.getKey());
            t.put("agentName", props.name());
            t.put("defaultUrl", props.url());
            t.put("routingKey", routingKeyFor(e.getKey()));
            // health of the agent type itself (its default URL) - a domain view rendered from
            // this response, e.g. the meta's per-domain agent bar, has no other source for it
            AgentHealthRegistry.AgentHealth th = health.get(e.getKey());
            t.put("health", th == null ? "UNKNOWN" : (th.up() ? "UP" : "DOWN"));
            agentTypes.add(t);
            for (String key : new TreeMap<>(props.instancesOrEmpty()).keySet()) {
                RemoteAgentProperties inst = props.forInstance(key);
                RuntimeInstance ri = runtime.getOrDefault(e.getKey(), Map.of()).get(key);
                boolean yaml = agentConfigResolver.isYamlInstance(e.getKey(), key);
                Map<String, Object> v = new LinkedHashMap<>();
                v.put("agentKey", e.getKey());
                v.put("instanceKey", key);
                v.put("registeredName", inst.name());
                v.put("url", inst.url());
                v.put("source", yaml ? "yaml" : "runtime");
                AgentHealthRegistry.AgentHealth h = health.get(AgentHealthRegistry.instanceHealthKey(e.getKey(), key));
                v.put("health", h == null ? "UNKNOWN" : (h.up() ? "UP" : "DOWN"));
                v.put("healthReason", h == null ? null : h.reason());
                v.put("checkedAt", h == null ? null : h.checkedAt());
                if (ri != null && !yaml) {
                    v.put("addedBy", ri.addedBy());
                    v.put("addedAt", ri.addedAt());
                }
                instances.add(v);
            }
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("agentTypes", agentTypes);
        body.put("instances", instances);
        body.put("persistent", registry.persistent());
        boolean prov = provisioner != null && provisioner.enabled();
        body.put("provisioningEnabled", prov);
        body.put("provisionableAgents", prov ? provisioner.provisionableAgents() : List.of());
        body.put("provisioningNamespace", prov ? provisioner.namespace() : null);
        return body;
    }

    /** Body: {"agentKey":"kubernetes","instanceKey":"payments-prod","url":"http://k8s-agent-payments-prod:8082"} */
    @PreAuthorize("@authz.admin()")
    @PostMapping
    public ResponseEntity<Map<String, Object>> add(@RequestBody Map<String, String> body) {
        String agentKey;
        String instanceKey;
        String url;
        try {
            agentKey = AgentInstanceRegistry.validateAgentKey(body.get("agentKey"));
            instanceKey = AgentInstanceRegistry.validateInstanceKey(body.get("instanceKey"));
            url = registry.validateUrl(body.get("url"));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
        RemoteAgentProperties props = agentConfigResolver.getAgentConfigs().get(agentKey);
        if (props == null) {
            return ResponseEntity.badRequest().body(Map.of("error",
                    "Unknown agent '" + agentKey + "'. Instances can only be added for configured agent types."));
        }
        if (agentConfigResolver.isYamlInstance(agentKey, instanceKey)) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error",
                    "Instance '" + instanceKey + "' is defined in yaml; change it there."));
        }
        String expectedName = props.name() + "__" + instanceKey;
        // Card verification is MANDATORY - it used to be skippable with "skipVerify": true in the
        // body, and that flag was a credential-disclosure hole rather than a convenience.
        // Registering makes this host "internal" to ServiceTokenWebClientCustomizer, which decides
        // what to attach X-Service-Token to by enumerating configured agent URLs (runtime
        // instances included). So an unverified registration meant the very next call - the health
        // probe below - carried the shared service token to a caller-supplied URL. Requiring the
        // card first means a host can only become internal after proving it serves this exact
        // agent identity, which an arbitrary endpoint cannot fake. Nothing used the flag: it
        // appeared in no UI, script or doc. Health probes stay public on the agents, so the
        // pre-registration card fetch needs no token either.
        String problem = verifyAgentCard(url, expectedName);
        if (problem != null) {
            return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(Map.of(
                    "error", problem,
                    "expectedName", expectedName,
                    "hint", "Deploy the " + agentKey + " agent with AGENT_INSTANCE_ID=" + instanceKey
                            + " and make sure " + url + " is reachable from the orchestrator."));
        }
        RuntimeInstance ri = registry.add(agentKey, instanceKey, url, CurrentPrincipal.displayName());
        boolean up = healthRegistry != null && healthRegistry.probeInstanceNow(agentKey, instanceKey);
        log.info("[audit] agent instance {}/{} -> {} added by '{}' (card verified, up={})",
                agentKey, instanceKey, url, CurrentPrincipal.displayName(), up);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("agentKey", ri.agentKey());
        out.put("instanceKey", ri.instanceKey());
        out.put("url", ri.url());
        out.put("registeredName", expectedName);
        out.put("source", "runtime");
        out.put("verified", true);
        out.put("health", up ? "UP" : "DOWN");
        out.put("persistent", registry.persistent());
        out.put("routingKey", routingKeyFor(agentKey));
        out.put("message", "Instance registered. Put '" + routingKeyFor(agentKey) + ": " + instanceKey
                + "' on the services it serves.");
        return ResponseEntity.ok(out);
    }

    @PreAuthorize("@authz.admin()")
    @DeleteMapping("/{agentKey}/{instanceKey}")
    public ResponseEntity<Map<String, Object>> remove(@PathVariable String agentKey,
                                                      @PathVariable String instanceKey) {
        if (agentConfigResolver.isYamlInstance(agentKey, instanceKey)) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error",
                    "Instance '" + instanceKey + "' is defined in yaml and cannot be removed here."));
        }
        boolean removed = registry.remove(agentKey, instanceKey, CurrentPrincipal.displayName());
        if (!removed) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "No such runtime instance"));
        }
        return ResponseEntity.ok(Map.of("removed", true, "agentKey", agentKey, "instanceKey", instanceKey));
    }

    /** Live probe of one instance (operator): refreshes its health entry immediately. */
    @PreAuthorize("@authz.operator()")
    @PostMapping("/{agentKey}/{instanceKey}/probe")
    public ResponseEntity<Map<String, Object>> probe(@PathVariable String agentKey,
                                                     @PathVariable String instanceKey) {
        if (healthRegistry == null) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of("error", "health registry not available"));
        }
        boolean up = healthRegistry.probeInstanceNow(agentKey, instanceKey);
        return ResponseEntity.ok(Map.of("agentKey", agentKey, "instanceKey", instanceKey, "health", up ? "UP" : "DOWN"));
    }

    /** Dry run: where would a question be delegated for this agent? (no LLM call, no delegation) */
    @PreAuthorize("@authz.viewer()")
    @GetMapping("/route-preview")
    public ResponseEntity<Map<String, Object>> routePreview(
            @org.springframework.web.bind.annotation.RequestParam(defaultValue = "kubernetes") String agentKey,
            @org.springframework.web.bind.annotation.RequestParam String text,
            @org.springframework.web.bind.annotation.RequestParam(required = false) @Nullable String threadId) {
        if (delegationExecutionService == null) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of("error", "delegation service not available"));
        }
        return ResponseEntity.ok(delegationExecutionService.previewRoute(agentKey, text, threadId));
    }

    // ---- provisioning: upload a kubeconfig, get a running registered instance ------------------

    /** Parse an uploaded kubeconfig: contexts, servers, and anything that would make it unusable in a pod. */
    @PreAuthorize("@authz.admin()")
    @PostMapping(value = "/provision/inspect", consumes = org.springframework.http.MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<Map<String, Object>> inspectKubeconfig(
            @org.springframework.web.bind.annotation.RequestPart("kubeconfig") org.springframework.web.multipart.MultipartFile file)
            throws java.io.IOException {
        if (provisioner == null || !provisioner.enabled()) {
            return provisioningDisabled();
        }
        KubernetesProvisioner.KubeconfigInspection insp = provisioner.inspect(file.getBytes());
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("contexts", insp.contexts());
        out.put("currentContext", insp.currentContext());
        out.put("servers", insp.serversByContext());
        out.put("problems", insp.problems());
        out.put("usable", insp.problems().isEmpty());
        return ResponseEntity.ok(out);
    }

    /**
     * Create the agent instance for this cluster: Secret(kubeconfig) + Deployment + Service in the
     * platform namespace, wait for its agent card, register it. Async: poll {@code /provision/{a}/{k}/status}.
     */
    @PreAuthorize("@authz.admin()")
    @PostMapping(value = "/provision", consumes = org.springframework.http.MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<Map<String, Object>> provision(
            @org.springframework.web.bind.annotation.RequestPart("agentKey") String agentKey,
            @org.springframework.web.bind.annotation.RequestPart("instanceKey") String instanceKey,
            @org.springframework.web.bind.annotation.RequestPart(value = "context", required = false) @Nullable String context,
            @org.springframework.web.bind.annotation.RequestPart("kubeconfig") org.springframework.web.multipart.MultipartFile file)
            throws java.io.IOException {
        if (provisioner == null || !provisioner.enabled()) {
            return provisioningDisabled();
        }
        try {
            KubernetesProvisioner.ProvisionStatus st = provisioner.provision(
                    agentKey, instanceKey, file.getBytes(), context, CurrentPrincipal.displayName());
            log.info("[audit] provision requested {}/{} by '{}' (context={})", agentKey, instanceKey,
                    CurrentPrincipal.displayName(), context);
            return ResponseEntity.accepted().body(statusView(st));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of("error", e.getMessage()));
        }
    }

    @PreAuthorize("@authz.viewer()")
    @GetMapping("/provision/{agentKey}/{instanceKey}/status")
    public ResponseEntity<Map<String, Object>> provisionStatus(@PathVariable String agentKey,
                                                               @PathVariable String instanceKey) {
        if (provisioner == null) {
            return provisioningDisabled();
        }
        return provisioner.status(agentKey, instanceKey)
                .map(st -> ResponseEntity.ok(statusView(st)))
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "no provisioning in progress")));
    }

    /** Tear down a provisioned instance (Deployment, Service, Secret) and unregister it. */
    @PreAuthorize("@authz.admin()")
    @DeleteMapping("/provision/{agentKey}/{instanceKey}")
    public ResponseEntity<Map<String, Object>> deprovision(@PathVariable String agentKey,
                                                           @PathVariable String instanceKey) {
        if (provisioner == null || !provisioner.enabled()) {
            return provisioningDisabled();
        }
        try {
            boolean any = provisioner.deprovision(agentKey, instanceKey, CurrentPrincipal.displayName());
            return ResponseEntity.ok(Map.of("agentKey", agentKey, "instanceKey", instanceKey, "resourcesDeleted", any));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of("error", e.getMessage()));
        }
    }

    private static ResponseEntity<Map<String, Object>> provisioningDisabled() {
        return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).body(Map.of("error",
                "Provisioning is not enabled on this orchestrator (#exampleframe#.orchestrator.provisioning.enabled). "
                        + "Deploy the agent yourself and register its URL instead."));
    }

    private static Map<String, Object> statusView(KubernetesProvisioner.ProvisionStatus st) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("agentKey", st.agentKey());
        m.put("instanceKey", st.instanceKey());
        m.put("phase", st.phase());
        m.put("message", st.message());
        m.put("url", st.url());
        m.put("registeredName", st.registeredName());
        m.put("updatedAt", st.updatedAt());
        return m;
    }

    // ---- helpers ------------------------------------------------------------------------

    /** Null when the card at {@code url} is reachable and names {@code expectedName}; else the problem. */
    private @Nullable String verifyAgentCard(String url, String expectedName) {
        try {
            String card = webClient.get()
                    .uri(url + "/a2a/.well-known/agent.json")
                    .retrieve()
                    .bodyToMono(String.class)
                    .timeout(CARD_TIMEOUT)
                    .block();
            if (card == null || card.isBlank()) {
                return "No agent card at " + url + "/a2a/.well-known/agent.json";
            }
            JsonNode node = objectMapper.readTree(card);
            String name = node.path("name").asText(null);
            if (name == null || name.isBlank()) {
                return "The agent card at " + url + " has no name; is this a #ExampleFrame# agent?";
            }
            if (!expectedName.equals(name)) {
                return "The agent at " + url + " identifies as '" + name + "', expected '" + expectedName
                        + "'. Set AGENT_INSTANCE_ID on that deployment to match the instance key.";
            }
            return null;
        } catch (Exception e) {
            return "Could not reach " + url + " (" + rootMessage(e) + ")";
        }
    }

    private static String rootMessage(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null && c.getCause() != c) {
            c = c.getCause();
        }
        return c.getMessage() == null ? c.getClass().getSimpleName() : c.getMessage();
    }

    /** The mapping key that routes a service to an instance of this agent (from RoutingKeyResolver). */
    private String routingKeyFor(String agentKey) {
        return routingKeyResolver != null ? routingKeyResolver.preferredMappingKey(agentKey) : agentKey + ".instance";
    }
}
