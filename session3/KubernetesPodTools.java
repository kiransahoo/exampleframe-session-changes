package com.#exampleframe#.kubernetes.tools.kubernetes;

import com.#exampleframe#.kubernetes.tools.SmartContainerResolver;
import com.#exampleframe#.kubernetes.tools.SmartContainerResolver.ContainerResolution;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Component
public class KubernetesPodTools extends KubernetesToolSupport {

    private static final Logger logger = LoggerFactory.getLogger(KubernetesPodTools.class);

    public KubernetesPodTools(
            @Nullable KubernetesClient client,
            @Value("${#exampleframe#.kubernetes.client.default-namespace:default}") String defaultNamespace,
            @Value("${#exampleframe#.kubernetes.agent.enable-write-operations:false}") boolean writeOperationsEnabled,
            @Value("${#exampleframe#.kubernetes.agent.enable-human-approval:true}") boolean humanApprovalEnabled,
            @Nullable SmartContainerResolver smartContainerResolver) {
        super(client, defaultNamespace, writeOperationsEnabled, humanApprovalEnabled, smartContainerResolver);
    }

    @Tool(description = "Lists all pods in a namespace with their status, ready count, restarts, and age. Use namespace='all' to list across all namespaces.")
    public String listPods(
            @ToolParam(description = "Kubernetes namespace (defaults to 'default'). Use 'all' or '*' for all namespaces.") String namespace) {

        String requestedNamespace = namespace != null ? namespace.trim() : "";
        boolean allNamespaces = isAllNamespaces(requestedNamespace);
        String ns = allNamespaces ? "<all-namespaces>" : (requestedNamespace.isEmpty() ? defaultNamespace : requestedNamespace);
        logger.info("Listing pods in scope: {}", ns);

        String err = requireClient();
        if (err != null) return err;

        try {
            List<Pod> pods = allNamespaces
                    ? client.pods().inAnyNamespace().list().getItems()
                    : client.pods().inNamespace(ns).list().getItems();

            if (pods.isEmpty()) {
                return allNamespaces
                        ? "No pods found across all namespaces."
                        : "No pods found in namespace: " + ns;
            }

            StringBuilder sb = new StringBuilder();
            if (allNamespaces) {
                sb.append("Pods across all namespaces:\n\n");
                sb.append(String.format("%-24s %-40s %-12s %-10s %-10s %-15s%n",
                        "NAMESPACE", "NAME", "STATUS", "READY", "RESTARTS", "AGE"));
            } else {
                sb.append("Pods in namespace '").append(ns).append("':\n\n");
                sb.append(String.format("%-40s %-12s %-10s %-10s %-15s%n",
                        "NAME", "STATUS", "READY", "RESTARTS", "AGE"));
            }
            sb.append("-".repeat(110)).append("\n");

            for (Pod pod : pods) {
                String name = pod.getMetadata().getName();
                String status = pod.getStatus().getPhase();
                String ready = getReadyCount(pod);
                int restarts = getTotalRestarts(pod);
                String age = getAge(pod.getMetadata().getCreationTimestamp());

                if (allNamespaces) {
                    String podNs = pod.getMetadata().getNamespace();
                    sb.append(String.format("%-24s %-40s %-12s %-10s %-10d %-15s%n",
                            truncate(podNs, 24), truncate(name, 40), status, ready, restarts, age));
                } else {
                    sb.append(String.format("%-40s %-12s %-10s %-10d %-15s%n",
                            truncate(name, 40), status, ready, restarts, age));
                }
            }

            return sb.toString();

        } catch (Exception e) {
            logger.error("Failed to list pods: {}", e.getMessage(), e);
            return "Error listing pods: " + e.getMessage();
        }
    }

    @Tool(description = """
            Lists problematic pods in a namespace for incident triage.

            Flags pods as problematic when any of these conditions are true:
            - Pod phase is not Running/Succeeded
            - Any container is not Ready
            - Restart count crosses the threshold
            - Waiting/termination reason indicates failure (CrashLoopBackOff, OOMKilled, ImagePullBackOff, etc.)
            """)
    public String listProblemPods(
            @ToolParam(description = "Kubernetes namespace (defaults to 'default')") String namespace,
            @ToolParam(description = "Minimum restart count to flag (default: 3)") Integer restartThreshold) {

        String requestedNamespace = namespace != null ? namespace.trim() : "";
        boolean allNamespaces = requestedNamespace.isBlank() || isAllNamespaces(requestedNamespace);
        String ns = allNamespaces ? "<all-namespaces>" : requestedNamespace;
        int threshold = restartThreshold != null ? Math.max(1, Math.min(restartThreshold, 1000)) : 3;
        logger.info("Listing problematic pods in scope: {} (restart threshold: {})", ns, threshold);

        String err = requireClient();
        if (err != null) return err;

        try {
            List<Pod> pods = allNamespaces
                    ? client.pods().inAnyNamespace().list().getItems()
                    : client.pods().inNamespace(ns).list().getItems();
            if (pods.isEmpty()) {
                return allNamespaces
                        ? "No pods found across all namespaces."
                        : "No pods found in namespace: " + ns;
            }

            StringBuilder sb = new StringBuilder();
            if (allNamespaces) {
                sb.append("Problematic pods across all namespaces (restart threshold: ")
                        .append(threshold).append("):\n\n");
                sb.append(String.format("%-24s %-32s %-17s %-7s %-9s %s%n",
                        "NAMESPACE", "NAME", "STATUS", "READY", "RESTARTS", "ISSUES"));
            } else {
                sb.append("Problematic pods in namespace '").append(ns).append("' (restart threshold: ")
                        .append(threshold).append("):\n\n");
                sb.append(String.format("%-36s %-17s %-7s %-9s %s%n",
                        "NAME", "STATUS", "READY", "RESTARTS", "ISSUES"));
            }
            sb.append("-".repeat(110)).append("\n");

            int problemCount = 0;
            for (Pod pod : pods) {
                List<String> issues = collectPodIssues(pod, threshold);
                if (issues.isEmpty()) {
                    continue;
                }

                String name = pod.getMetadata().getName();
                String status = pod.getStatus() != null && pod.getStatus().getPhase() != null
                        ? pod.getStatus().getPhase() : "Unknown";
                String ready = getReadyCount(pod);
                int restarts = getTotalRestarts(pod);
                String podNamespace = pod.getMetadata() != null ? pod.getMetadata().getNamespace() : "unknown";

                if (allNamespaces) {
                    sb.append(String.format("%-24s %-32s %-17s %-7s %-9d %s%n",
                            truncate(podNamespace, 24),
                            truncate(name, 32),
                            truncate(status, 17),
                            ready,
                            restarts,
                            truncate(String.join(", ", issues), 200)));
                } else {
                    sb.append(String.format("%-36s %-17s %-7s %-9d %s%n",
                            truncate(name, 36),
                            truncate(status, 17),
                            ready,
                            restarts,
                            truncate(String.join(", ", issues), 200)));
                }
                problemCount++;
            }

            if (problemCount == 0) {
                return allNamespaces
                        ? """
                        No problematic pods found across all namespaces using restart threshold %d.
                        All pods are Running/Succeeded with containers Ready and no high-restart indicators.
                        """.formatted(threshold)
                        : """
                        No problematic pods found in namespace '%s' using restart threshold %d.
                        All pods are Running/Succeeded with containers Ready and no high-restart indicators.
                        """.formatted(ns, threshold);
            }

            sb.append("\nSuggested next checks:\n");
            sb.append("1. getPodDetails(podName, namespace)\n");
            sb.append("2. getEvents(namespace, podName)\n");
            sb.append("3. getPodPreviousLogs(podName, namespace, container, lines)\n");
            sb.append("4. Use jvm-diagnostics skill for Java pods with CPU/memory symptoms\n");
            return sb.toString();

        } catch (Exception e) {
            logger.error("Failed to list problematic pods: {}", e.getMessage(), e);
            return "Error listing problematic pods: " + e.getMessage();
        }
    }

    @Tool(description = "Lists all namespaces in the Kubernetes cluster")
    public String listNamespaces() {
        logger.info("Listing all namespaces");

        String err = requireClient();
        if (err != null) return err;

        try {
            List<Namespace> namespaces = client.namespaces().list().getItems();
            StringBuilder sb = new StringBuilder("Namespaces in cluster:\n\n");
            for (Namespace ns : namespaces) {
                sb.append("  - ").append(ns.getMetadata().getName()).append("\n");
            }
            return sb.toString();
        } catch (Exception e) {
            return "Error listing namespaces: " + e.getMessage();
        }
    }

    /** How many pods to recommend per workload for diagnostic sampling. */
    private static final int MAX_RECOMMENDED_PODS = 3;

    @Tool(description = """
            Resolves a workload (Deployment/StatefulSet/DaemonSet) to its CURRENT pods via the
            workload's selector. Call FIRST when you have a workload/deployment/service name and
            need a concrete podName for downstream tools (JVM diagnostics, logs, exec, ...).

            Returns ALL pods plus up to 3 RECOMMENDED pods (high restarts first, then failing,
            then one healthy Running+Ready baseline). Run diagnostics on EACH recommended pod
            and compare against the baseline - the diff is usually where the root cause is.

            If no controller matches, deterministically falls back to a pod of that exact name,
            then to pods labeled app=<name>, each marked as such with ownership stated - proceed
            on what it returns; only a not-found or zero-pods result means stop.
            """)
    public String resolvePodsForWorkload(
            @ToolParam(description = "Workload name - Deployment / StatefulSet / DaemonSet (NOT a pod name)") String workloadName,
            @ToolParam(description = "Kubernetes namespace") String namespace) {

        String ns = (namespace != null && !namespace.isBlank()) ? namespace.trim() : defaultNamespace;
        if (workloadName == null || workloadName.isBlank()) {
            return "Error: workloadName is required.";
        }
        logger.info("Resolving pods for workload {}/{}", ns, workloadName);

        String err = requireClient();
        if (err != null) return err;

        try {
            // The FULL selector (matchLabels + matchExpressions) - matchLabels alone would
            // route an expressions-only controller into the fallback and let the tool claim
            // "no controller exists" about its own pods.
            LabelSelector fullSelector = null;
            String kind = null;

            // Try Deployment first
            io.fabric8.kubernetes.api.model.apps.Deployment dep =
                    client.apps().deployments().inNamespace(ns).withName(workloadName).get();
            if (dep != null && dep.getSpec() != null && dep.getSpec().getSelector() != null) {
                fullSelector = dep.getSpec().getSelector();
                kind = "Deployment";
            }

            // Then StatefulSet
            if (fullSelector == null) {
                io.fabric8.kubernetes.api.model.apps.StatefulSet sts =
                        client.apps().statefulSets().inNamespace(ns).withName(workloadName).get();
                if (sts != null && sts.getSpec() != null && sts.getSpec().getSelector() != null) {
                    fullSelector = sts.getSpec().getSelector();
                    kind = "StatefulSet";
                }
            }

            // Then DaemonSet
            if (fullSelector == null) {
                io.fabric8.kubernetes.api.model.apps.DaemonSet ds =
                        client.apps().daemonSets().inNamespace(ns).withName(workloadName).get();
                if (ds != null && ds.getSpec() != null && ds.getSpec().getSelector() != null) {
                    fullSelector = ds.getSpec().getSelector();
                    kind = "DaemonSet";
                }
            }

            // No controller with that name: fall back to PODS, deterministically. Test/demo
            // namespaces often run controller-less pods, and a service mapping's `workload`
            // sometimes (wrongly, but commonly) carries a POD's name. Leaving this case to
            // the calling LLM made behavior flip run to run - sometimes it analyzed the
            // like-named pod, sometimes it refused. The TOOL decides now: exact pod-name
            // match first, then pods labeled app=<workloadName>; only when both are empty is
            // the workload reported as not found. Ownership is CHECKED, never assumed - a
            // pod named like a workload is usually controller-owned (web-0, a copied full
            // pod name), and claiming "bare" about an owned pod would be false and its
            // "convert to a Deployment" advice harmful. Lookups degrade to not-found on
            // errors (RBAC etc.) instead of surfacing a raw exception with no directive.
            String bareWarning = null;
            List<Pod> pods;
            if (fullSelector == null) {
                Pod exact = null;
                try {
                    exact = client.pods().inNamespace(ns).withName(workloadName).get();
                } catch (Exception lookupErr) {
                    logger.warn("Bare-pod fallback lookup failed for {}/{}: {}", ns, workloadName,
                            lookupErr.getMessage());
                }
                if (exact != null) {
                    pods = List.of(exact);
                    List<OwnerReference> owners = exact.getMetadata() != null
                            && exact.getMetadata().getOwnerReferences() != null
                            ? exact.getMetadata().getOwnerReferences() : List.of();
                    if (owners.isEmpty()) {
                        kind = "BARE POD (no controller)";
                        bareWarning = String.format(
                                "NOTE: '%s' is a bare pod - no controller owns it, so it will not be "
                                + "rescheduled or scaled. Proceed with diagnostics on this pod (logs and "
                                + "describe always; exec-based tools only while it is Running). The "
                                + "mapping's `workload` should name a controller; for a permanent setup "
                                + "convert this pod to a Deployment or fix the mapping.%n%n", workloadName);
                    } else {
                        String ownerDesc = owners.stream()
                                .map(o -> o.getKind() + "/" + o.getName())
                                .collect(Collectors.joining(", "));
                        boolean viaReplicaSet = owners.stream()
                                .anyMatch(o -> "ReplicaSet".equals(o.getKind()));
                        kind = "POD (owned by " + ownerDesc + ")";
                        bareWarning = String.format(
                                "NOTE: '%s' is a POD NAME, not a workload - it is owned by %s%s. The "
                                + "mapping's `workload` should name that controller. Proceeding with THIS "
                                + "pod only; sibling pods and the healthy-baseline comparison are "
                                + "unavailable until the mapping names the controller.%n%n",
                                workloadName, ownerDesc,
                                viaReplicaSet ? " (a ReplicaSet's Deployment is the right workload name)" : "");
                    }
                } else {
                    List<Pod> labeled = List.of();
                    boolean validLabelValue = workloadName.length() <= 63
                            && workloadName.matches("[A-Za-z0-9]([A-Za-z0-9_.-]*[A-Za-z0-9])?");
                    if (validLabelValue) {
                        try {
                            labeled = client.pods().inNamespace(ns)
                                    .withLabel("app", workloadName).list().getItems().stream()
                                    .filter(p -> !"Succeeded".equalsIgnoreCase(podPhase(p)))
                                    .collect(Collectors.toList());
                        } catch (Exception lookupErr) {
                            logger.warn("Label fallback lookup failed for {}/app={}: {}", ns, workloadName,
                                    lookupErr.getMessage());
                        }
                    }
                    if (!labeled.isEmpty()) {
                        pods = labeled;
                        Set<String> ownerNames = new LinkedHashSet<>();
                        for (Pod p : labeled) {
                            if (p.getMetadata() != null && p.getMetadata().getOwnerReferences() != null) {
                                for (OwnerReference o : p.getMetadata().getOwnerReferences()) {
                                    ownerNames.add(o.getKind() + "/" + o.getName());
                                }
                            }
                        }
                        if (ownerNames.isEmpty()) {
                            kind = "BARE PODS (label app=" + workloadName + ", no controller)";
                            bareWarning = String.format(
                                    "NOTE: no controller named '%s' exists; these ownerless pods matched by "
                                    + "label app=%s only - tell the user they were label-matched. Proceed "
                                    + "with diagnostics. The mapping's `workload` should name a controller "
                                    + "for a permanent setup.%n%n", workloadName, workloadName);
                        } else {
                            kind = "PODS (label app=" + workloadName + ")";
                            bareWarning = String.format(
                                    "NOTE: no controller named '%s' exists; these label-matched pods have "
                                    + "owners among: %s (some may be ownerless). The mapping's `workload` "
                                    + "should name the right controller. "
                                    + "Proceeding with the matched pods - tell the user they were matched by "
                                    + "label, not by workload name.%n%n",
                                    workloadName, String.join(", ", ownerNames));
                        }
                    } else {
                        return String.format(
                                "Workload '%s' not found in namespace '%s' (checked Deployment, StatefulSet, "
                                + "DaemonSet, a pod of that exact name, and pods labeled app=%s).%n"
                                + "Verify the workload name with `listDeployments(namespace=\"%s\")` or "
                                + "`listPods(namespace=\"%s\")`. "
                                + "Do NOT proceed with diagnostics on this workload until resolved.",
                                workloadName, ns, workloadName, ns, ns);
                    }
                }
            } else {
                pods = client.pods().inNamespace(ns).withLabelSelector(fullSelector).list().getItems();
            }
            if (pods.isEmpty()) {
                return String.format(
                        "%s '%s' exists in namespace '%s' but currently has ZERO matching pods "
                        + "(label selector: %s). The workload may be scaled to 0 or all pods are "
                        + "still terminating. Tell the user - do NOT pick an arbitrary pod.",
                        kind, workloadName, ns,
                        fullSelector != null && fullSelector.getMatchLabels() != null
                                && !fullSelector.getMatchLabels().isEmpty()
                                ? fullSelector.getMatchLabels() : fullSelector);
            }

            // Score each pod: higher score = more likely problematic / more useful to sample.
            List<ScoredPod> scored = new ArrayList<>();
            for (Pod p : pods) {
                scored.add(new ScoredPod(p, diagnosticScore(p), diagnosticReason(p)));
            }

            // Sort by score DESC. Among healthy pods we keep stable order so the baseline is
            // deterministic across calls.
            scored.sort((a, b) -> Integer.compare(b.score(), a.score()));

            // Pick up to MAX_RECOMMENDED_PODS, ensuring at least one healthy baseline is
            // included when we have a mix of problematic + healthy pods.
            List<ScoredPod> recommended = pickRecommended(scored, MAX_RECOMMENDED_PODS);
            Set<String> recommendedNames = new LinkedHashSet<>();
            for (ScoredPod sp : recommended) {
                recommendedNames.add(sp.pod().getMetadata().getName());
            }

            StringBuilder out = new StringBuilder();
            if (bareWarning != null) {
                out.append(bareWarning);
            }
            out.append(String.format("%s: **%s** in namespace `%s` -> %d pod(s)%n%n",
                    kind, workloadName, ns, pods.size()));
            out.append("| Pod | Phase | Ready | Restarts | Recommended | Why |\n");
            out.append("|---|---|---|---|---|---|\n");
            // Output rows in score order for clarity
            for (ScoredPod sp : scored) {
                Pod p = sp.pod();
                String name = p.getMetadata() != null ? p.getMetadata().getName() : "?";
                String phase = podPhase(p);
                String ready = readyString(p);
                int restarts = totalRestarts(p);
 String rec = recommendedNames.contains(name) ? "[OK]" : "";
                out.append(String.format("| `%s` | %s | %s | %d | %s | %s |%n",
                        name, phase, ready, restarts, rec, sp.reason()));
            }

            out.append(String.format("%n**Recommended pods for diagnostics (run each, then compare):**%n"));
            int rank = 1;
            for (ScoredPod sp : recommended) {
                out.append(String.format("  %d. `%s` - %s%n",
                        rank++, sp.pod().getMetadata().getName(), sp.reason()));
            }
            out.append(String.format("%nRun the diagnostic tool (e.g. `analyzeJvm`) on EACH of the %d "
                    + "recommended pods above, then compare findings. The diff between a problematic "
                    + "pod and the healthy baseline is usually where the root cause shows up.%n",
                    recommended.size()));
            return out.toString();

        } catch (Exception e) {
            logger.error("resolvePodsForWorkload failed for {}/{}: {}", ns, workloadName, e.getMessage(), e);
            return "Error resolving workload pods: " + e.getMessage();
        }
    }

    /**
     * Diagnostic-priority score. Higher = more interesting to investigate.
     * Roughly: failures > high-restart Running > recent-restart Running > healthy.
     */
    private int diagnosticScore(Pod p) {
        int score = 0;
        String phase = podPhase(p);

        // Non-Running pods are very interesting (Failed, Pending, Unknown)
        if (!"Running".equalsIgnoreCase(phase) && !"Succeeded".equalsIgnoreCase(phase)) {
            score += 1000;
        }
        // Running but not Ready - probe failing, draining, etc.
        if ("Running".equalsIgnoreCase(phase) && !isRunningAndReady(p)) {
            score += 500;
        }
        // Restart count is the strongest signal of repeated failure
        int restarts = totalRestarts(p);
        score += Math.min(restarts, 50) * 10;   // cap at 500 so a 1000-restart loop doesn't dominate

        // Recent OOMKilled / Error in last termination
        if (lastTerminationIsBad(p)) {
            score += 200;
        }
        return score;
    }

    /** One-line human-readable reason for the score, surfaced in the output table. */
    private String diagnosticReason(Pod p) {
        String phase = podPhase(p);
        int restarts = totalRestarts(p);
        if (!"Running".equalsIgnoreCase(phase)) {
            return "phase=" + phase + " - non-Running";
        }
        if (!isRunningAndReady(p)) {
            return "Running but not Ready";
        }
        if (restarts >= 3) {
            return "Running+Ready but " + restarts + " restarts";
        }
        if (lastTerminationIsBad(p)) {
            return "recent abnormal termination";
        }
        return "healthy (baseline candidate)";
    }

    private boolean lastTerminationIsBad(Pod p) {
        if (p.getStatus() == null || p.getStatus().getContainerStatuses() == null) return false;
        for (ContainerStatus cs : p.getStatus().getContainerStatuses()) {
            if (cs.getLastState() != null && cs.getLastState().getTerminated() != null) {
                String reason = cs.getLastState().getTerminated().getReason();
                if (reason != null && (reason.contains("OOM") || reason.contains("Error")
                        || reason.contains("Crash"))) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Pick up to {@code max} pods to recommend. If we have a mix of problematic and healthy pods,
     * ensure at least one healthy baseline ends up in the list so the LLM has a reference point.
     */
    private List<ScoredPod> pickRecommended(List<ScoredPod> sortedDesc, int max) {
        if (sortedDesc.size() <= max) {
            return new ArrayList<>(sortedDesc);
        }
        List<ScoredPod> picked = new ArrayList<>(sortedDesc.subList(0, max));
        // If everything we picked is problematic AND there's a healthy pod available, swap the
        // lowest-scoring picked entry for the healthiest remaining pod.
        boolean anyHealthyPicked = picked.stream().anyMatch(sp -> sp.score() == 0);
        if (!anyHealthyPicked) {
            ScoredPod healthiest = sortedDesc.get(sortedDesc.size() - 1);
            if (healthiest.score() == 0) {
                picked.set(picked.size() - 1, healthiest);
            }
        }
        return picked;
    }

    /** Bundles a Pod with its computed diagnostic score and human-readable reason. */
    private record ScoredPod(Pod pod, int score, String reason) {}

    private boolean isRunningAndReady(Pod p) {
        if (!"Running".equalsIgnoreCase(podPhase(p))) return false;
        if (p.getStatus() == null || p.getStatus().getConditions() == null) return false;
        return p.getStatus().getConditions().stream()
                .anyMatch(c -> "Ready".equalsIgnoreCase(c.getType()) && "True".equalsIgnoreCase(c.getStatus()));
    }

    private String podPhase(Pod p) {
        return p.getStatus() != null && p.getStatus().getPhase() != null ? p.getStatus().getPhase() : "Unknown";
    }

    private String readyString(Pod p) {
        if (p.getStatus() == null || p.getStatus().getContainerStatuses() == null) return "0/0";
        long ready = p.getStatus().getContainerStatuses().stream().filter(ContainerStatus::getReady).count();
        long total = p.getStatus().getContainerStatuses().size();
        return ready + "/" + total;
    }

    private int totalRestarts(Pod p) {
        if (p.getStatus() == null || p.getStatus().getContainerStatuses() == null) return 0;
        return p.getStatus().getContainerStatuses().stream()
                .mapToInt(ContainerStatus::getRestartCount).sum();
    }

    @Tool(description = "Gets detailed information about a specific pod including containers, status, events, and resource usage")
    public String getPodDetails(
            @ToolParam(description = "Name of the pod") String podName,
            @ToolParam(description = "Kubernetes namespace") String namespace) {

        String ns = namespace != null ? namespace : defaultNamespace;
        logger.info("Getting pod details: {}/{}", ns, podName);

        String err = requireClient();
        if (err != null) return err;

        try {
            Pod pod = client.pods().inNamespace(ns).withName(podName).get();

            if (pod == null) {
                return "Pod not found: " + podName + " in namespace " + ns;
            }

            StringBuilder sb = new StringBuilder();
            sb.append("Pod: ").append(podName).append("\n");
            sb.append("Namespace: ").append(ns).append("\n");
            sb.append("Status: ").append(pod.getStatus().getPhase()).append("\n");
            sb.append("Node: ").append(pod.getSpec().getNodeName()).append("\n");
            sb.append("IP: ").append(pod.getStatus().getPodIP()).append("\n");
            sb.append("Created: ").append(pod.getMetadata().getCreationTimestamp()).append("\n\n");

            // Containers
            sb.append("Containers:\n");
            int containerCount = 0;
            if (pod.getStatus().getContainerStatuses() != null) {
                containerCount = pod.getStatus().getContainerStatuses().size();
                for (ContainerStatus cs : pod.getStatus().getContainerStatuses()) {
                    sb.append("  - ").append(cs.getName()).append(": ");
                    sb.append(cs.getReady() ? "Ready" : "Not Ready");
                    sb.append(", Restarts: ").append(cs.getRestartCount()).append("\n");
                }
            }
            if (containerCount > 1) {
                sb.append("\nNOTE: This pod has multiple containers. Call selectContainer(podName=\"")
                  .append(podName).append("\", namespace=\"").append(ns)
                  .append("\") to determine the target container for logs and diagnostics.\n");
            }

            // Events
            sb.append("\nRecent Events:\n");
            List<Event> events = client.v1().events().inNamespace(ns)
                    .withField("involvedObject.name", podName)
                    .list().getItems();

            for (Event event : events.stream().limit(5).toList()) {
                sb.append("  [").append(event.getType()).append("] ");
                sb.append(event.getReason()).append(": ");
                sb.append(event.getMessage()).append("\n");
            }

            return sb.toString();

        } catch (Exception e) {
            logger.error("Failed to get pod details: {}", e.getMessage(), e);
            return "Error getting pod details: " + e.getMessage();
        }
    }

    @Tool(description = "Selects the best application container for a multi-container pod. "
            + "Call this when getPodDetails shows multiple containers, before calling log or diagnostic tools. "
            + "Returns the recommended container and alternatives.")
    public String selectContainer(
            @ToolParam(description = "Name of the pod") String podName,
            @ToolParam(description = "Kubernetes namespace") String namespace) {

        String ns = namespace != null ? namespace : defaultNamespace;
        logger.info("Selecting container for pod: {}/{}", ns, podName);

        String err = requireClient();
        if (err != null) return err;

        try {
            Pod pod = client.pods().inNamespace(ns).withName(podName).get();
            if (pod == null) {
                return "Pod not found: " + podName + " in namespace " + ns;
            }

            List<ContainerStatus> containers = pod.getStatus().getContainerStatuses();
            if (containers == null || containers.isEmpty()) {
                return "No containers found in pod: " + podName;
            }

            if (containers.size() == 1) {
                String name = containers.get(0).getName();
                return "Single container pod. Container: " + name
                        + "\nUse container='" + name + "' for log tools, targetContainer='" + name + "' for JVM tools.";
            }

            List<String> containerNames = containers.stream()
                    .map(ContainerStatus::getName)
                    .collect(Collectors.toList());

            // Use SmartContainerResolver to pick the best container
            if (smartContainerResolver != null) {
                ContainerResolution cr = smartContainerResolver.resolve(ns, podName, containerNames);
                if (cr.selected() != null) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("Selected container: ").append(cr.selected()).append("\n");
                    sb.append("All containers:\n");
                    for (ContainerStatus cs : containers) {
                        sb.append("  - ").append(cs.getName()).append(": ");
                        sb.append(cs.getReady() != null && cs.getReady() ? "Ready" : "Not Ready");
                        sb.append("\n");
                    }
                    sb.append("\nUse container='").append(cr.selected())
                      .append("' for log tools (getPodLogs, searchPodLogs, etc.).\n");
                    sb.append("Use targetContainer='").append(cr.selected())
                      .append("' for JVM tools (captureThreadDump, analyzeJvm, etc.).\n");
                    sb.append("Other containers: ").append(cr.others()).append("\n");
                    return sb.toString();
                }
            }

            // Fallback: list containers for manual selection
            StringBuilder sb = new StringBuilder();
            sb.append("Could not auto-select a container. Available containers:\n");
            for (ContainerStatus cs : containers) {
                sb.append("  - ").append(cs.getName()).append(": ");
                sb.append(cs.getReady() != null && cs.getReady() ? "Ready" : "Not Ready");
                sb.append("\n");
            }
            return sb.toString();

        } catch (Exception e) {
            logger.error("Failed to select container: {}", e.getMessage(), e);
            return "Error selecting container: " + e.getMessage();
        }
    }

    @Tool(description = """
            Gets Kubernetes events, optionally filtered by resource name, severity, and time window. \
            Groups events by type (Warning first, then Normal), shows summary counts and reason breakdown.""")
    public String getEvents(
            @ToolParam(description = "Kubernetes namespace") String namespace,
            @ToolParam(description = "Filter by resource name (optional)") String resourceName,
            @ToolParam(description = "Filter by severity: 'Normal', 'Warning', or 'all' (default 'all')") String severity,
            @ToolParam(description = "Time window to look back, e.g. '5m', '1h', '24h' (optional, shows all events if not set)") String since) {

        String ns = namespace != null ? namespace : defaultNamespace;
        String severityFilter = (severity != null && !severity.isBlank()) ? severity.trim() : "all";
        logger.info("Getting events in namespace: {}, filter: {}, severity: {}, since: {}",
                ns, resourceName, severityFilter, since);

        String err = requireClient();
        if (err != null) return err;

        try {
            var eventRequest = client.v1().events().inNamespace(ns);

            List<Event> events;
            if (resourceName != null && !resourceName.isBlank()) {
                events = eventRequest.withField("involvedObject.name", resourceName)
                        .list().getItems();
            } else {
                events = eventRequest.list().getItems();
            }

            // Apply time window filter
            if (since != null && !since.isBlank()) {
                int seconds = parseDuration(since);
                if (seconds <= 0) {
                    return "Invalid duration format for 'since'. Use format like '5m', '1h', '24h'";
                }
                Instant cutoff = Instant.now().minusSeconds(seconds);
                events = events.stream().filter(event -> {
                    String ts = event.getLastTimestamp();
                    if (ts == null || ts.isBlank()) {
                        ts = event.getMetadata() != null ? event.getMetadata().getCreationTimestamp() : null;
                    }
                    if (ts == null || ts.isBlank()) return false;
                    try {
                        return Instant.parse(ts).isAfter(cutoff);
                    } catch (Exception e) {
                        return true; // include if timestamp can't be parsed
                    }
                }).collect(Collectors.toList());
            }

            // Apply severity filter
            if (!"all".equalsIgnoreCase(severityFilter)) {
                events = events.stream()
                        .filter(e -> severityFilter.equalsIgnoreCase(e.getType()))
                        .collect(Collectors.toList());
            }

            if (events.isEmpty()) {
                StringBuilder noEvents = new StringBuilder("No events found");
                if (resourceName != null && !resourceName.isBlank()) noEvents.append(" for ").append(resourceName);
                if (!"all".equalsIgnoreCase(severityFilter)) noEvents.append(" with severity '").append(severityFilter).append("'");
                if (since != null && !since.isBlank()) noEvents.append(" in the last ").append(since);
                return noEvents.toString();
            }

            // Count by type
            long warningCount = events.stream().filter(e -> "Warning".equals(e.getType())).count();
            long normalCount = events.stream().filter(e -> "Normal".equals(e.getType())).count();

            // Count by reason
            Map<String, Integer> reasonCounts = new LinkedHashMap<>();
            for (Event event : events) {
                String reason = event.getReason() != null ? event.getReason() : "Unknown";
                reasonCounts.merge(reason, 1, Integer::sum);
            }

            // Build summary header
            StringBuilder sb = new StringBuilder();
            sb.append("Events: ").append(warningCount).append(" Warning, ")
                    .append(normalCount).append(" Normal");
            if (since != null && !since.isBlank()) {
                sb.append(" (last ").append(since).append(")");
            }
            sb.append("\n");

            // Reason summary
            sb.append("Reasons: ");
            List<String> reasonEntries = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : reasonCounts.entrySet()) {
                reasonEntries.add(entry.getKey() + ": " + entry.getValue());
            }
            sb.append(String.join(", ", reasonEntries));
            sb.append("\n");
            sb.append("=".repeat(80)).append("\n\n");

            // Group: Warning events first, then Normal
            List<Event> warningEvents = events.stream()
                    .filter(e -> "Warning".equals(e.getType()))
                    .collect(Collectors.toList());
            List<Event> normalEvents = events.stream()
                    .filter(e -> !"Warning".equals(e.getType()))
                    .collect(Collectors.toList());

            if (!warningEvents.isEmpty()) {
                sb.append("--- Warning Events ---\n");
                for (Event event : warningEvents.stream().limit(30).toList()) {
                    sb.append("\u26A0 ");
                    appendEventLine(sb, event);
                }
                sb.append("\n");
            }

            if (!normalEvents.isEmpty()) {
                sb.append("--- Normal Events ---\n");
                for (Event event : normalEvents.stream().limit(30).toList()) {
                    appendEventLine(sb, event);
                }
            }

            return sb.toString();

        } catch (Exception e) {
            logger.error("Failed to get events: {}", e.getMessage(), e);
            return "Error getting events: " + e.getMessage();
        }
    }

    private void appendEventLine(StringBuilder sb, Event event) {
        sb.append(event.getLastTimestamp() != null ? event.getLastTimestamp() : "").append(" ");
        sb.append(event.getInvolvedObject().getKind()).append("/");
        sb.append(event.getInvolvedObject().getName()).append(": ");
        sb.append(event.getReason()).append(" - ");
        sb.append(event.getMessage()).append("\n");
    }

    @Tool(description = """
            Lists pods filtered by label selector and/or owning deployment.
            Use label selectors in Kubernetes format, e.g. "app=my-service", "tier=backend,env=prod".
            When deploymentName is provided, it looks up the deployment's matchLabels and filters pods by those labels.
            Use namespace='all' to search across all namespaces.
            """)
    public String listPodsByLabel(
            @ToolParam(description = "Kubernetes namespace (defaults to 'default'). Use 'all' or '*' for all namespaces.") String namespace,
            @ToolParam(description = "Label selector in key=value format, e.g. 'app=my-service' or 'tier=backend,env=prod'. Optional if deploymentName is provided.") String labelSelector,
            @ToolParam(description = "Deployment name to filter pods by (looks up the deployment's matchLabels). Optional if labelSelector is provided.") String deploymentName) {

        String requestedNamespace = namespace != null ? namespace.trim() : "";
        boolean allNamespaces = isAllNamespaces(requestedNamespace);
        String ns = allNamespaces ? "<all-namespaces>" : (requestedNamespace.isEmpty() ? defaultNamespace : requestedNamespace);
        logger.info("Listing pods by label in scope: {}, labelSelector: {}, deployment: {}", ns, labelSelector, deploymentName);

        String err = requireClient();
        if (err != null) return err;

        try {
            String effectiveSelector = labelSelector;

            // If deployment name provided, look up its matchLabels
            if ((effectiveSelector == null || effectiveSelector.isBlank()) && deploymentName != null && !deploymentName.isBlank()) {
                if (allNamespaces) {
                    return "Error: deploymentName lookup requires a specific namespace - cannot use 'all'.";
                }
                var deployment = client.apps().deployments().inNamespace(ns).withName(deploymentName).get();
                if (deployment == null) {
                    return "Deployment not found: " + deploymentName + " in namespace " + ns;
                }
                var matchLabels = deployment.getSpec().getSelector().getMatchLabels();
                if (matchLabels == null || matchLabels.isEmpty()) {
                    return "Deployment " + deploymentName + " has no matchLabels defined";
                }
                effectiveSelector = matchLabels.entrySet().stream()
                        .map(e -> e.getKey() + "=" + e.getValue())
                        .collect(java.util.stream.Collectors.joining(","));
                logger.info("Resolved deployment '{}' to label selector: {}", deploymentName, effectiveSelector);
            }

            if (effectiveSelector == null || effectiveSelector.isBlank()) {
                return "Error: provide either labelSelector or deploymentName to filter pods";
            }

            List<Pod> pods = allNamespaces
                    ? client.pods().inAnyNamespace()
                            .withLabelSelector(effectiveSelector)
                            .list().getItems()
                    : client.pods().inNamespace(ns)
                            .withLabelSelector(effectiveSelector)
                            .list().getItems();

            if (pods.isEmpty()) {
                return allNamespaces
                        ? "No pods found matching selector '" + effectiveSelector + "' across all namespaces"
                        : "No pods found matching selector '" + effectiveSelector + "' in namespace " + ns;
            }

            StringBuilder sb = new StringBuilder();
            if (allNamespaces) {
                sb.append("Pods matching '").append(effectiveSelector).append("' across all namespaces:\n\n");
                sb.append(String.format("%-24s %-40s %-12s %-10s %-10s %-15s%n",
                        "NAMESPACE", "NAME", "STATUS", "READY", "RESTARTS", "AGE"));
            } else {
                sb.append("Pods matching '").append(effectiveSelector).append("' in namespace '").append(ns).append("':\n\n");
                sb.append(String.format("%-40s %-12s %-10s %-10s %-15s%n",
                        "NAME", "STATUS", "READY", "RESTARTS", "AGE"));
            }
            sb.append("-".repeat(110)).append("\n");

            for (Pod pod : pods) {
                String name = pod.getMetadata().getName();
                String status = pod.getStatus().getPhase();
                String ready = getReadyCount(pod);
                int restarts = getTotalRestarts(pod);
                String age = getAge(pod.getMetadata().getCreationTimestamp());

                if (allNamespaces) {
                    String podNs = pod.getMetadata().getNamespace();
                    sb.append(String.format("%-24s %-40s %-12s %-10s %-10d %-15s%n",
                            truncate(podNs, 24), truncate(name, 40), status, ready, restarts, age));
                } else {
                    sb.append(String.format("%-40s %-12s %-10s %-10d %-15s%n",
                            truncate(name, 40), status, ready, restarts, age));
                }
            }

            return sb.toString();

        } catch (Exception e) {
            logger.error("Failed to list pods by label: {}", e.getMessage(), e);
            return "Error listing pods by label: " + e.getMessage();
        }
    }


}
