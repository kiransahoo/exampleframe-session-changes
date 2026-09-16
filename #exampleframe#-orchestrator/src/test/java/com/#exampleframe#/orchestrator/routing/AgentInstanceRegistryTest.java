package com.#exampleframe#.orchestrator.routing;

import com.#exampleframe#.orchestrator.config.AgentConfigResolver;
import com.#exampleframe#.orchestrator.config.OrchestratorProperties;
import com.#exampleframe#.orchestrator.config.OrchestratorProperties.AgentInstanceProperties;
import com.#exampleframe#.orchestrator.config.OrchestratorProperties.RemoteAgentProperties;
import com.#exampleframe#.orchestrator.service.context.ServiceContextStore;
import com.#exampleframe#.orchestrator.service.context.StoredServiceContext;
import org.junit.jupiter.api.Test;
import org.springframework.core.env.StandardEnvironment;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Onboarding a cluster from the UI: a runtime instance persists through the service
 * context store, survives a reload, and shows up in the agent configs everyone reads,
 * beneath the yaml ones. Bad keys and URLs never get in.
 */
class AgentInstanceRegistryTest {

    /** Minimal in-memory store standing in for Mongo/Cassandra. */
    static class MemStore implements ServiceContextStore {
        final Map<String, Map<String, String>> docs = new LinkedHashMap<>();
        @Override public void save(String serviceName, Map<String, String> config) { docs.put(serviceName, new LinkedHashMap<>(config)); }
        @Override public Optional<StoredServiceContext> findByServiceName(String serviceName) {
            return Optional.ofNullable(docs.get(serviceName)).map(c -> new StoredServiceContext(serviceName, c, Instant.now(), Instant.now()));
        }
        @Override public List<StoredServiceContext> findAll() {
            List<StoredServiceContext> out = new ArrayList<>();
            docs.forEach((k, v) -> out.add(new StoredServiceContext(k, v, Instant.now(), Instant.now())));
            return out;
        }
        @Override public boolean delete(String serviceName) { return docs.remove(serviceName) != null; }
        @Override public long count() { return docs.size(); }
        @Override public String storeType() { return "mem"; }
    }

    private static AgentInstanceRegistry registry(MemStore store) {
        AgentInstanceRegistry r = new AgentInstanceRegistry(store, "");
        r.load();
        return r;
    }

    @Test
    void addPersistsAndReloadsFromTheStore() {
        MemStore store = new MemStore();
        AgentInstanceRegistry r = registry(store);
        r.add("kubernetes", "payments-prod", "http://k8s-agent-payments-prod:8082/", "admin@x");
        r.add("oracle", "payments-db", "https://oracle-payments:8083", "admin@x");

        assertThat(store.docs).containsKey(AgentInstanceRegistry.RESERVED_DOC);
        assertThat(r.byAgent().get("kubernetes").get("payments-prod").url()).isEqualTo("http://k8s-agent-payments-prod:8082");

        AgentInstanceRegistry fresh = registry(store);   // restart
        assertThat(fresh.byAgent()).containsKeys("kubernetes", "oracle");
        assertThat(fresh.get("oracle", "payments-db")).isPresent()
                .get().extracting(AgentInstanceRegistry.RuntimeInstance::addedBy).isEqualTo("admin@x");

        assertThat(fresh.remove("kubernetes", "payments-prod", "admin@x")).isTrue();
        assertThat(fresh.remove("kubernetes", "payments-prod", "admin@x")).isFalse();
        assertThat(registry(store).byAgent()).containsOnlyKeys("oracle");
    }

    @Test
    void rejectsBadKeysAndUrls() {
        AgentInstanceRegistry r = registry(new MemStore());
        assertThatThrownBy(() -> r.add("kubernetes", "bad key!", "http://h:1", null)).hasMessageContaining("instanceKey");
        assertThatThrownBy(() -> r.add("Kubernetes", "ok", "http://h:1", null)).hasMessageContaining("agentKey");
        assertThatThrownBy(() -> r.add("kubernetes", "ok", "ftp://h:1", null)).hasMessageContaining("http");
        assertThatThrownBy(() -> r.add("kubernetes", "ok", "http://user:pw@h:1", null)).hasMessageContaining("credentials");
        assertThatThrownBy(() -> r.add("kubernetes", "ok", "http://h:1/a2a/message", null)).hasMessageContaining("path");
        assertThatThrownBy(() -> r.add("kubernetes", "ok", "http://h:1?x=1", null)).hasMessageContaining("path");
        assertThat(r.byAgent()).isEmpty();
    }

    @Test
    void urlAllowPatternIsEnforcedWhenConfigured() {
        AgentInstanceRegistry r = new AgentInstanceRegistry(new MemStore(), "https?://[a-z0-9-]+\\.tb\\.svc(:\\d+)?");
        r.load();
        r.add("kubernetes", "a", "http://k8s-agent-a.tb.svc:8082", null);
        assertThatThrownBy(() -> r.add("kubernetes", "b", "http://evil.example.com:8082", null))
                .hasMessageContaining("allowed pattern");
    }

    @Test
    void runtimeInstancesAppearInAgentConfigsBeneathYaml() {
        RemoteAgentProperties k8s = new RemoteAgentProperties("http://k8s-agent:8082", "kubernetes_agent", "d",
                60, 3, List.of(), null, 99, null, "delegateToKubernetesAgent", Map.of(), Map.of(), Map.of(), List.of(),
                Map.of("claims-prod", new AgentInstanceProperties("http://yaml-claims:8082", null)));
        OrchestratorProperties props = new OrchestratorProperties(new OrchestratorProperties.AgentProperties(),
                new OrchestratorProperties.A2aProperties(), Map.of("kubernetes", k8s));
        AgentConfigResolver resolver = new AgentConfigResolver(props, new StandardEnvironment(), null);

        AgentInstanceRegistry r = registry(new MemStore());
        resolver.setInstanceRegistry(r);
        assertThat(resolver.getAgentConfigs().get("kubernetes").instancesOrEmpty()).containsOnlyKeys("claims-prod");

        r.add("kubernetes", "payments-prod", "http://k8s-payments:8082", "admin");
        r.add("kubernetes", "claims-prod", "http://runtime-tries-to-override:8082", "admin"); // yaml wins
        r.add("unknown-agent", "x", "http://x:1", "admin");                                    // ignored

        Map<String, AgentInstanceProperties> merged = resolver.getAgentConfigs().get("kubernetes").instancesOrEmpty();
        assertThat(merged).containsOnlyKeys("claims-prod", "payments-prod");
        assertThat(merged.get("claims-prod").url()).isEqualTo("http://yaml-claims:8082");
        assertThat(resolver.getAgentConfigs().get("kubernetes").forInstance("payments-prod").name())
                .isEqualTo("kubernetes_agent__payments-prod");
        assertThat(resolver.isYamlInstance("kubernetes", "claims-prod")).isTrue();
        assertThat(resolver.isYamlInstance("kubernetes", "payments-prod")).isFalse();
        assertThat(resolver.getAgentConfigs()).doesNotContainKey("unknown-agent");

        r.remove("kubernetes", "payments-prod", "admin");
        assertThat(resolver.getAgentConfigs().get("kubernetes").instancesOrEmpty()).containsOnlyKeys("claims-prod");
    }

    @Test
    void cellsSharingOneStoreDoNotSeeEachOthersInstances() {
        // claims onboards a cluster; the payments cell runs against the SAME store (one keyspace
        // is the common platform setup) and must not inherit a route into the claims cluster
        MemStore store = new MemStore();
        AgentInstanceRegistry claims = new AgentInstanceRegistry(store, "", "claims");
        claims.add("kubernetes", "claims-uat", "http://k8s-claims-uat:8082", "admin");

        AgentInstanceRegistry payments = new AgentInstanceRegistry(store, "", "payments");
        payments.load();
        assertThat(payments.byAgent()).isEmpty();

        AgentInstanceRegistry claimsAgain = new AgentInstanceRegistry(store, "", "claims");
        claimsAgain.load();
        assertThat(claimsAgain.get("kubernetes", "claims-uat")).isPresent();

        // a single (non-federated) deployment keeps the unscoped document
        assertThat(store.docs.keySet()).containsExactly(AgentInstanceRegistry.RESERVED_DOC + "@claims");
    }

    @Test
    void instanceUrlsMayNeverAimAtLinkLocalOrWildcardAddresses() {
        // 169.254.169.254 is the cloud metadata service - the classic SSRF credential
        // target - and wildcard/multicast addresses are never a usable agent origin.
        AgentInstanceRegistry r = new AgentInstanceRegistry(null, "");
        for (String url : new String[]{
                "http://169.254.169.254", "http://169.254.10.20:8080", "http://0.0.0.0:8082"}) {
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> r.validateUrl(url))
                    .as(url)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("link-local");
        }
        // ...while loopback and RFC1918 remain valid by default: agents live on private
        // networks, and on localhost in local development (this very stack).
        assertThat(r.validateUrl("http://localhost:8093")).isEqualTo("http://localhost:8093");
        assertThat(r.validateUrl("http://10.1.2.3:8082")).isEqualTo("http://10.1.2.3:8082");
    }

    @Test
    void productionCanForbidPrivateAndLoopbackInstanceUrls() {
        AgentInstanceRegistry r = new AgentInstanceRegistry(null, "", "", false, false);
        for (String url : new String[]{"http://127.0.0.1:8082", "http://192.168.1.5:8082", "http://10.0.0.9"}) {
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> r.validateUrl(url))
                    .as(url)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("allow-private-urls");
        }
    }

    @Test
    void productionChartValueLocksOutPrivateAndLoopbackOrigins() {
        // The chart now sets allow-private-urls: false in-cluster. Registering a URL makes that
        // host internal - the service token is attached to it - so an admin must not be able to
        // aim that egress at the pod network. Local development keeps the permissive default.
        AgentInstanceRegistry locked = new AgentInstanceRegistry(null, "", "", false, false);
        for (String url : new String[]{"http://127.0.0.1:8082", "http://10.1.2.3:8082", "http://192.168.0.7"}) {
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> locked.validateUrl(url))
                    .as(url).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("allow-private-urls");
        }
        AgentInstanceRegistry dev = new AgentInstanceRegistry(null, "");
        assertThat(dev.validateUrl("http://localhost:8093")).isEqualTo("http://localhost:8093");
    }
}
