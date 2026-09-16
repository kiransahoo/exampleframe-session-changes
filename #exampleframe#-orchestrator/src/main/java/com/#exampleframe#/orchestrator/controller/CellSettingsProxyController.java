package com.#exampleframe#.orchestrator.controller;

import com.#exampleframe#.orchestrator.config.AgentConfigResolver;
import com.#exampleframe#.orchestrator.config.OrchestratorProperties.RemoteAgentProperties;
import com.#exampleframe#.orchestrator.federation.DomainAccessPolicy;
import com.#exampleframe#.orchestrator.federation.FederationProperties;
import com.#exampleframe#.orchestrator.security.CallerContext;
import com.#exampleframe#.orchestrator.security.CurrentPrincipal;
import com.#exampleframe#.orchestrator.security.Roles;
import jakarta.servlet.http.HttpServletRequest;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * One URL for everyone: the meta proxies a domain cell's <em>settings</em> API so a team
 * administers its own domain (service mappings, agent instances, topology, routing dry-run)
 * from the meta's UI, without the cell ever being exposed outside the cluster.
 * <p>
 * {@code /api/cells/{domain}/settings/**} -> {@code {cellUrl}/api/settings/**}. The service
 * token is attached by the usual customizer (the cell URL is a configured agent host) and the
 * acting user is forwarded. Only the settings surface is proxied - never {@code /api/agents/**}
 * or {@code /a2a/**}, so this cannot be used to bypass the meta's own delegation path.
 * <p>
 * Authorisation is layered: the endpoint needs a VIEWER (reads) or OPERATOR (writes) at the
 * meta, <em>and</em> the caller must pass {@link DomainAccessPolicy} for that domain - a claims
 * operator cannot administer the sales cell.
 *
 * @author kiransahoo
 */
@RestController
@RequestMapping("/api/cells")
@CrossOrigin(origins = "*")
public class CellSettingsProxyController {

    private static final Logger log = LoggerFactory.getLogger(CellSettingsProxyController.class);
    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    private final @Nullable FederationProperties federationProperties;
    private final @Nullable DomainAccessPolicy domainAccessPolicy;
    private final AgentConfigResolver agentConfigResolver;
    private final WebClient webClient;

    public CellSettingsProxyController(@Autowired(required = false) @Nullable FederationProperties federationProperties,
                                       @Autowired(required = false) @Nullable DomainAccessPolicy domainAccessPolicy,
                                       AgentConfigResolver agentConfigResolver,
                                       WebClient.Builder webClientBuilder) {
        this.federationProperties = federationProperties;
        this.domainAccessPolicy = domainAccessPolicy;
        this.agentConfigResolver = agentConfigResolver;
        this.webClient = webClientBuilder.clone().build();
    }

    /** Domains this caller may administer, with their cell keys - drives the UI's domain picker. */
    @PreAuthorize("@authz.viewer()")
    @GetMapping
    public Map<String, Object> domains() {
        Map<String, Object> body = new LinkedHashMap<>();
        List<Map<String, Object>> list = new java.util.ArrayList<>();
        if (federationProperties != null && federationProperties.enabled()) {
            CallerContext caller = CallerContext.capture();
            for (var e : new TreeMap<>(federationProperties.domainsOrEmpty()).entrySet()) {
                String domain = e.getKey();
                boolean allowed = domainAccessPolicy == null || domainAccessPolicy.canAccess(domain, caller);
                RemoteAgentProperties cfg = e.getValue() == null ? null
                        : agentConfigResolver.getAgentConfigs().get(e.getValue().agentKey());
                Map<String, Object> d = new LinkedHashMap<>();
                d.put("domain", domain);
                d.put("cellAgentKey", e.getValue() != null ? e.getValue().agentKey() : null);
                d.put("reachable", cfg != null);
                d.put("allowed", allowed);
                list.add(d);
            }
        }
        body.put("federationEnabled", federationProperties != null && federationProperties.enabled());
        body.put("domains", list);
        return body;
    }

    /** Reads: mappings, agent instances, topology, routing dry-run inside the domain's cell. */
    @PreAuthorize("@authz.viewer()")
    @GetMapping("/{domain}/settings/**")
    public ResponseEntity<String> get(@PathVariable String domain, HttpServletRequest request) {
        return proxy(domain, HttpMethod.GET, request, null);
    }

    /** Writes: save a mapping, register/provision an instance. Operator at the meta + domain access. */
    @PreAuthorize("@authz.operator()")
    @RequestMapping(value = "/{domain}/settings/**", method = {RequestMethod.POST, RequestMethod.PUT, RequestMethod.DELETE})
    public ResponseEntity<String> write(@PathVariable String domain, HttpServletRequest request,
                                        @RequestBody(required = false) @Nullable String body) {
        return proxy(domain, HttpMethod.valueOf(request.getMethod()), request, body);
    }

    private ResponseEntity<String> proxy(String domain, HttpMethod method, HttpServletRequest request,
                                         @Nullable String body) {
        if (federationProperties == null || !federationProperties.enabled()) {
            return json(HttpStatus.NOT_IMPLEMENTED, "{\"error\":\"This orchestrator is not a meta (federation disabled)\"}");
        }
        FederationProperties.DomainCell cell = federationProperties.domainsOrEmpty().get(domain);
        if (cell == null) {
            return json(HttpStatus.NOT_FOUND, "{\"error\":\"Unknown domain '" + domain + "'\"}");
        }
        CallerContext caller = CallerContext.capture();
        if (domainAccessPolicy != null && !domainAccessPolicy.canAccess(domain, caller)) {
            log.warn("[audit] cell settings DENIED: principal='{}' domain='{}'", CurrentPrincipal.displayName(), domain);
            return json(HttpStatus.FORBIDDEN, "{\"error\":\"Not authorised for domain '" + domain + "'\"}");
        }
        RemoteAgentProperties cfg = agentConfigResolver.getAgentConfigs().get(cell.agentKey());
        if (cfg == null || cfg.url() == null || cfg.url().isBlank()) {
            return json(HttpStatus.SERVICE_UNAVAILABLE,
                    "{\"error\":\"No URL configured for cell '" + cell.agentKey() + "'\"}");
        }
        // Everything after /api/cells/{domain}/settings maps to the cell's /api/settings/...
        String path = request.getRequestURI();
        int marker = path.indexOf("/settings");
        String suffix = marker >= 0 ? path.substring(marker) : "/settings";
        String query = request.getQueryString();
        String target = cfg.url() + "/api" + suffix + (query != null && !query.isBlank() ? "?" + query : "");
        try {
            // The user's own bearer goes with the call: cells share the meta's Entra app
            // registration, so the cell validates the same token and sees the same oid/roles.
            // (The shared service token deliberately does not open a cell's user API.)
            String authorization = request.getHeader("Authorization");
            WebClient.RequestBodySpec spec = webClient.method(method).uri(java.net.URI.create(target))
                    .header("X-On-Behalf-Of", caller.authenticated() ? caller.name() : "")
                    .header("X-On-Behalf-Of-Roles", String.join(",", new java.util.TreeSet<>(caller.roles())));
            if (authorization != null && !authorization.isBlank()) {
                spec = spec.header("Authorization", authorization);
            }
            WebClient.RequestHeadersSpec<?> readySpec = (body == null || body.isBlank())
                    ? spec
                    : spec.contentType(contentType(request)).bodyValue(body);
            // Relay the cell's status, not a flat 200: a cell-side rejection (validation
            // 400, domain-ownership 409, authz 403) must fail at the meta too - the UI
            // reads the status to decide between "saved" and the error message.
            ResponseEntity<String> upstream = readySpec.exchangeToMono(r -> r.toEntity(String.class))
                    .timeout(TIMEOUT).block();
            log.info("[audit] cell settings {} {} by '{}' -> {} {}", method, logSafe(suffix),
                    CurrentPrincipal.displayName(), domain, upstream == null ? "-" : upstream.getStatusCode());
            if (upstream == null) {
                return json(HttpStatus.BAD_GATEWAY, "{\"error\":\"Cell '" + domain + "' returned no response\"}");
            }
            return ResponseEntity.status(upstream.getStatusCode())
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(upstream.getBody() != null ? upstream.getBody() : "{}");
        } catch (Exception e) {
            log.warn("Cell settings proxy failed ({} {}): {}", method, logSafe(target), logSafe(e.getMessage()));
            return json(HttpStatus.BAD_GATEWAY, "{\"error\":\"Cell '" + domain + "' unreachable: "
                    + (e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage().replace('"', '\'')) + "\"}");
        }
    }

    private static MediaType contentType(HttpServletRequest request) {
        String ct = request.getContentType();
        try {
            return ct == null || ct.isBlank() ? MediaType.APPLICATION_JSON : MediaType.parseMediaType(ct);
        } catch (Exception e) {
            return MediaType.APPLICATION_JSON;
        }
    }

    private static ResponseEntity<String> json(HttpStatus status, String body) {
        return ResponseEntity.status(status).contentType(MediaType.APPLICATION_JSON).body(body);
    }

    /**
     * Request-derived text for log lines: CR/LF (and other control characters) in a crafted
     * URI or query string would forge log entries, so they are replaced and the value capped.
     */
    static String logSafe(String s) {
        if (s == null) {
            return "";
        }
        String cleaned = s.replaceAll("[\\r\\n\\t\\p{Cntrl}]", "_");
        return cleaned.length() > 200 ? cleaned.substring(0, 200) + "..." : cleaned;
    }
}
