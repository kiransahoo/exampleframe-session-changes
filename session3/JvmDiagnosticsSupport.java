package com.#exampleframe#.kubernetes.tools.jvm;

import com.#exampleframe#.kubernetes.tools.SmartContainerResolver;
import com.#exampleframe#.kubernetes.tools.SmartContainerResolver.ContainerResolution;
import com.#exampleframe#.kubernetes.tools.SmartContainerResolver.Source;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.ExecListener;
import io.fabric8.kubernetes.client.dsl.ExecWatch;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Shared JVM diagnostics support utilities used by tool modules.
 */
abstract class JvmDiagnosticsSupport {

    protected static final int EXEC_TIMEOUT_SECONDS = 120;
    protected static final int HEAP_DUMP_TIMEOUT_SECONDS = 600;
    protected static final String DEBUG_CONTAINER_PREFIX = "jvm-debug-";
    protected static final String DEFAULT_DEBUG_IMAGE = "eclipse-temurin:17-jdk";
    protected static final String DEFAULT_DUMP_PATH = "/tmp";
    protected static final int DEFAULT_MIN_FREE_MB_FOR_HEAP_DUMP = 1024;
    protected static final int DEFAULT_MIN_FREE_MB_FOR_JFR = 256;

    protected static final String ATTACH_REMEDIATION_HINT =
            "\nREMEDIATION: JVM attach is blocked. To enable JDK diagnostics, either:\n"
            + "  1. Add SYS_PTRACE capability to the container securityContext:\n"
            + "       securityContext:\n"
            + "         capabilities:\n"
            + "           add: [\"SYS_PTRACE\"]\n"
            + "  2. Or ensure -XX:+DisableAttachMechanism is NOT set in JVM flags.\n";

    protected static final String EXEC_RBAC_HINT =
            "\nROOT CAUSE: Kubernetes exec failed - the #ExampleFrame# service account does not have "
            + "permission to exec into pods in this namespace.\n\n"
            + "REMEDIATION: Grant the #ExampleFrame# Kubernetes agent's ServiceAccount 'pods/exec' "
            + "permission in the target namespace. Either:\n"
            + "  1. Change the existing RoleBinding to a ClusterRoleBinding (applies to all namespaces):\n"
            + "       kubectl create clusterrolebinding #exampleframe#-exec \\\n"
            + "         --clusterrole=<existing-role> \\\n"
            + "         --serviceaccount=<agent-namespace>:<sa-name>\n"
            + "  2. Or add a RoleBinding in the target namespace:\n"
            + "       kubectl create rolebinding #exampleframe#-exec \\\n"
            + "         --namespace=<target-namespace> \\\n"
            + "         --clusterrole=<existing-role> \\\n"
            + "         --serviceaccount=<agent-namespace>:<sa-name>\n\n"
            + "NOTE: The ServiceAccount needs these permissions: pods/exec (create), pods (get, list).\n";

    protected static final String EXEC_POD_NOT_FOUND_HINT =
            "\nROOT CAUSE: Pod not found. The pod may have been terminated, restarted, or the name is incorrect.\n"
            + "REMEDIATION: Verify the pod exists with: kubectl get pod <pod-name> -n <namespace>\n";

    protected static final String EXEC_CONTAINER_NOT_FOUND_HINT =
            "\nROOT CAUSE: Container not found in this pod.\n"
            + "REMEDIATION: List available containers with: kubectl get pod <pod-name> -n <namespace> -o jsonpath='{.spec.containers[*].name}'\n";

    protected final KubernetesClient client;
    protected final String defaultNamespace;
    protected final String debugImage;
    protected final Logger logger;
    protected final @Nullable SmartContainerResolver smartContainerResolver;

    /**
     * Holds the last error message from resolveDiagnosticsContext when it returns null.
     * Callers should check this for a user-facing message (e.g. multi-container prompt).
     * Thread-local to avoid cross-request contamination.
     */
    private final ThreadLocal<String> lastDiagnosticsError = new ThreadLocal<>();

    protected String getLastDiagnosticsError(String podName, String ns) {
        String error = lastDiagnosticsError.get();
        lastDiagnosticsError.remove();
        return error != null ? error
                : formatError("Could not resolve diagnostics context (pod not found or sidecar deploy failed)", podName, ns);
    }

    protected record PodLookupResult(
            @Nullable Pod pod,
            String resolvedPodName,
            boolean exactMatch,
            List<String> candidates) {
    }

    protected JvmDiagnosticsSupport(
            @Nullable KubernetesClient client,
            String defaultNamespace,
            String debugImage,
            Logger logger,
            @Nullable SmartContainerResolver smartContainerResolver) {
        this.client = client;
        this.defaultNamespace = defaultNamespace;
        this.debugImage = debugImage != null ? debugImage : DEFAULT_DEBUG_IMAGE;
        this.logger = logger;
        this.smartContainerResolver = smartContainerResolver;
    }

    protected String requireClient() {
        if (client == null) {
            return "ERROR: Kubernetes client is not available. "
                    + "The agent failed to connect to the Kubernetes API server. "
                    + "Check ServiceAccount token, RBAC ClusterRoleBinding, and network connectivity.";
        }
        return null;
    }

    protected PodLookupResult lookupPodWithFallback(String namespace, String requestedPodName) {
        if (requestedPodName == null || requestedPodName.isBlank()) {
            return new PodLookupResult(null, "", false, List.of());
        }

        Pod exact = client.pods().inNamespace(namespace).withName(requestedPodName).get();
        if (exact != null) {
            return new PodLookupResult(exact, requestedPodName, true, List.of(requestedPodName));
        }

        List<Pod> pods = client.pods().inNamespace(namespace).list().getItems();
        if (pods == null || pods.isEmpty()) {
            return new PodLookupResult(null, requestedPodName, false, List.of());
        }

        String requestedLower = requestedPodName.toLowerCase(Locale.ROOT);
        String requestedAlphaNum = requestedLower.replaceAll("[^a-z0-9]", "");
        LinkedHashSet<String> matches = new LinkedHashSet<>();

        for (Pod pod : pods) {
            String podName = pod.getMetadata() != null ? pod.getMetadata().getName() : null;
            if (podName == null || podName.isBlank()) {
                continue;
            }
            String podLower = podName.toLowerCase(Locale.ROOT);
            String podAlphaNum = podLower.replaceAll("[^a-z0-9]", "");
            if (podLower.contains(requestedLower)
                    || requestedLower.contains(podLower)
                    || (!requestedAlphaNum.isBlank() && podAlphaNum.contains(requestedAlphaNum))) {
                matches.add(podName);
            }
        }

        if (matches.isEmpty()) {
            for (String token : requestedLower.split("[^a-z0-9]+")) {
                if (token.length() < 4) {
                    continue;
                }
                for (Pod pod : pods) {
                    String podName = pod.getMetadata() != null ? pod.getMetadata().getName() : null;
                    if (podName == null || podName.isBlank()) {
                        continue;
                    }
                    if (podName.toLowerCase(Locale.ROOT).contains(token)) {
                        matches.add(podName);
                    }
                }
                if (matches.size() == 1) {
                    break;
                }
            }
        }

        if (matches.size() == 1) {
            String resolved = matches.iterator().next();
            Pod resolvedPod = client.pods().inNamespace(namespace).withName(resolved).get();
            if (resolvedPod != null) {
                return new PodLookupResult(resolvedPod, resolved, false, List.copyOf(matches));
            }
        }

        return new PodLookupResult(null, requestedPodName, false, List.copyOf(matches));
    }

    protected String podLookupFailureMessage(String namespace, String requestedPodName, PodLookupResult lookup) {
        if (lookup.candidates() == null || lookup.candidates().isEmpty()) {
            return "Pod not found";
        }
        if (lookup.candidates().size() == 1) {
            return "Pod not found. Did you mean '" + lookup.candidates().get(0)
                    + "' in namespace '" + namespace + "'?";
        }
        return "Pod not found. Multiple similar pods in namespace '" + namespace + "': "
                + String.join(", ", lookup.candidates());
    }
    /**
     * Installing an ephemeral debug container is a WRITE to the workload - a new,
     * typically root, process inside the target pod. Two gates, both fail closed:
     * an operator must enable the capability per environment, and the install must
     * come from the explicit {@code deployDiagnosticsSidecar} tool (which requires
     * human approval) - never as a silent fallback of a "read-only" diagnostic.
     */
    @org.springframework.beans.factory.annotation.Value("${#exampleframe#.kubernetes.jvm.allow-ephemeral-containers:false}")
    protected boolean allowEphemeralContainers;

    /** UID for the debug container. Root by default because jattach/jcmd must match or
     *  outrank the target JVM's user - but only reachable behind the two gates above. */
    @org.springframework.beans.factory.annotation.Value("${#exampleframe#.kubernetes.jvm.ephemeral-run-as-user:0}")
    protected long ephemeralRunAsUser;

    /** Legacy entry used by in-tool fallbacks: never allowed to install silently. */
    protected String deployDiagnosticsSidecarInternal(
            String podName,
            String namespace,
            String targetContainer) {
        return deployDiagnosticsSidecarInternal(podName, namespace, targetContainer, false);
    }

    protected String deployDiagnosticsSidecarInternal(
            String podName,
            String namespace,
            String targetContainer,
            boolean explicitlyRequested) {

        if (!allowEphemeralContainers) {
            return "Error: ephemeral diagnostics containers are disabled on this agent "
                    + "(#exampleframe#.kubernetes.jvm.allow-ephemeral-containers=false). An operator "
                    + "must enable them for this environment before a debug sidecar can be installed.";
        }
        if (!explicitlyRequested) {
            return "Error: no diagnostics sidecar is installed in this pod, and installing one is "
                    + "a write action that needs human approval. Call deployDiagnosticsSidecar to "
                    + "request it, then retry this diagnostic.";
        }

        String ns = resolveNamespace(namespace);
        logger.info("Deploying diagnostics sidecar to pod: {}/{}", ns, podName);

        try {
            PodLookupResult podLookup = lookupPodWithFallback(ns, podName);
            Pod pod = podLookup.pod();
            if (pod == null) {
                return formatError(podLookupFailureMessage(ns, podName, podLookup), podName, ns);
            }

            String effectivePodName = podLookup.resolvedPodName();
            if (!podLookup.exactMatch()) {
                logger.info("Resolved pod name '{}' to '{}' in namespace '{}'",
                        podName, effectivePodName, ns);
            }

            if (!"Running".equals(pod.getStatus().getPhase())) {
                return formatError("Pod is not running", effectivePodName, ns);
            }

            ContainerResolution cr = resolveContainer(pod, targetContainer, namespace, effectivePodName);
            if (cr.selected() == null) {
                if (isMultiContainer(pod)) {
                    return formatMultiContainerError(effectivePodName, ns, pod);
                }
                return formatError("Could not determine container", effectivePodName, ns);
            }
            String target = cr.selected();

            // Check for existing sidecar
            String existingSidecar = findExistingDebugContainer(pod);
            if (existingSidecar != null) {
                int javaPid = findJavaPidFromSidecar(ns, effectivePodName, existingSidecar);
                return SmartContainerResolver.appendContainerHint(String.format("""
 [OK] Diagnostics sidecar already exists

                        Pod: %s
                        Namespace: %s
                        Sidecar: %s
                        Java PID: %s

                        Ready for: captureThreadDump, captureHeapDump
                        """, effectivePodName, ns, existingSidecar, javaPid > 0 ? javaPid : "auto-detect"),
                        cr, "targetContainer");
            }

            // Add ephemeral container via Fabric8's HTTP client (bypasses serialization, uses existing auth)
            String sidecarName = DEBUG_CONTAINER_PREFIX + System.currentTimeMillis() % 100000;

            // Build JSON patch for ephemeral container
            // Uses strategic merge patch to ADD to ephemeralContainers array
            String patchJson = String.format("""
                    {
                        "spec": {
                            "ephemeralContainers": [{
                                "name": "%s",
                                "image": "%s",
                                "command": ["/bin/sh", "-c", "sleep 3600"],
                                "targetContainerName": "%s",
                                "stdin": true,
                                "tty": true,
                                "securityContext": {
                                    "runAsUser": %d
                                }
                            }]
                        }
                    }
                    """, sidecarName, debugImage, target, ephemeralRunAsUser);

            // Construct URL for ephemeralcontainers subresource
            // AKS/EKS/GKE all use: /api/v1/namespaces/{ns}/pods/{name}/ephemeralcontainers
            String masterUrl = client.getConfiguration().getMasterUrl();
            if (masterUrl.endsWith("/")) {
                masterUrl = masterUrl.substring(0, masterUrl.length() - 1);
            }
            String subresourceUrl = masterUrl + "/api/v1/namespaces/" + ns + "/pods/" + effectivePodName + "/ephemeralcontainers";

            logger.info("Deploying ephemeral container to: {}", subresourceUrl);

            try {
                io.fabric8.kubernetes.client.http.HttpRequest httpRequest = client.getHttpClient()
                        .newHttpRequestBuilder()
                        .uri(subresourceUrl)
                        .header("Content-Type", "application/strategic-merge-patch+json")
                        .patch("application/strategic-merge-patch+json", patchJson)
                        .build();

                var responseFuture = client.getHttpClient().sendAsync(httpRequest, String.class);
                var response = responseFuture.get(30, TimeUnit.SECONDS);

                if (!response.isSuccessful()) {
                    String body = response.body() != null ? response.body() : "No response body";
                    logger.error("Failed to patch ephemeral containers: HTTP {} - {}", response.code(), body);

                    // Provide helpful error messages
                    if (response.code() == 404) {
                        return formatError("Ephemeral containers not supported. Requires Kubernetes 1.25+. " +
                                "Check: kubectl version", effectivePodName, ns);
                    } else if (response.code() == 403) {
                        return formatError("Permission denied. ServiceAccount needs 'patch' on 'pods/ephemeralcontainers'. " +
                                "See RBAC requirements.", effectivePodName, ns);
                    } else if (response.code() == 422) {
                        return formatError("Invalid request: " + body.substring(0, Math.min(200, body.length())), effectivePodName, ns);
                    }
                    return formatError("HTTP " + response.code() + ": " + body.substring(0, Math.min(200, body.length())), effectivePodName, ns);
                }

                logger.info("Ephemeral container {} added successfully", sidecarName);

            } catch (java.util.concurrent.TimeoutException e) {
                return formatError("Timeout waiting for ephemeral container deployment", effectivePodName, ns);
            } catch (Exception e) {
                logger.error("HTTP request failed: {}", e.getMessage(), e);
                return formatError("Failed to deploy sidecar: " + e.getMessage(), effectivePodName, ns);
            }

            // Wait for sidecar to be ready (with retry)
            logger.info("Waiting for sidecar {} to start...", sidecarName);
            boolean ready = false;
            int maxAttempts = 15; // 15 * 2s = 30s max wait

            for (int attempt = 1; attempt <= maxAttempts && !ready; attempt++) {
                Thread.sleep(2000);

                Pod updatedPod = client.pods().inNamespace(ns).withName(effectivePodName).get();
                if (updatedPod.getStatus().getEphemeralContainerStatuses() != null) {
                    for (ContainerStatus cs : updatedPod.getStatus().getEphemeralContainerStatuses()) {
                        if (cs.getName().equals(sidecarName)) {
                            if (cs.getState().getRunning() != null) {
                                ready = true;
                                logger.info("Sidecar {} is running (attempt {})", sidecarName, attempt);
                                break;
                            } else if (cs.getState().getWaiting() != null) {
                                String reason = cs.getState().getWaiting().getReason();
                                logger.debug("Sidecar waiting: {} (attempt {})", reason, attempt);
                                if ("ImagePullBackOff".equals(reason) || "ErrImagePull".equals(reason)) {
                                    return formatError("Sidecar image pull failed: " + debugImage + ". Reason: " + reason, effectivePodName, ns);
                                }
                            } else if (cs.getState().getTerminated() != null) {
                                String reason = cs.getState().getTerminated().getReason();
                                return formatError("Sidecar terminated unexpectedly: " + reason, effectivePodName, ns);
                            }
                        }
                    }
                }
            }

            if (!ready) {
                return formatError("Sidecar failed to start within 30s. Check image availability and cluster resources.", effectivePodName, ns);
            }

            // Find Java PID from sidecar (process namespace is shared via targetContainerName)
            int javaPid = findJavaPidFromSidecar(ns, effectivePodName, sidecarName);

            return SmartContainerResolver.appendContainerHint(String.format("""
 [OK] Diagnostics sidecar deployed

                    Pod: %s
                    Namespace: %s
                    Sidecar: %s
                    Target: %s
                    Image: %s
                    Java PID: %s

                    Available tools:
                    - captureThreadDump (jstack)
                    - captureHeapDump (jmap)

                    Auto-terminates in 1 hour.
                    """, effectivePodName, ns, sidecarName, target, debugImage,
                    javaPid > 0 ? javaPid : "not found - specify manually"),
                    cr, "targetContainer");

        } catch (Exception e) {
            logger.error("Failed to deploy sidecar: {}", e.getMessage(), e);
            if (e.getMessage() != null && e.getMessage().contains("ephemeralcontainers")) {
                return """
                        Error: Ephemeral containers not supported

                        Requirements:
                        - Kubernetes 1.23+
                        - EphemeralContainers feature enabled

                        Check: kubectl api-resources | grep ephemeralcontainers
                        """;
            }
            return formatError(e.getMessage(), podName, ns);
        }
    }
    protected String padRight(String s, int n) {
        return String.format("%-" + n + "s", s);
    }
    protected String tryDirectJstack(String namespace, String podName, String container, Integer requestedPid) {
        try {
            logger.info("Attempting direct jstack in container '{}' of pod {}/{}", container, namespace, podName);

            // Find jstack - try multiple strategies:
            // 1. which/command -v (works if jstack is in PATH)
            // 2. $JAVA_HOME/bin/jstack (works if JAVA_HOME is set)
            // 3. Common JDK installation paths
            String jstackPath;
            try {
                jstackPath = findJdkTool(namespace, podName, container, "jstack");
            } catch (RuntimeException execError) {
                // findJdkTool throws RuntimeException when exec itself fails
                // (pod not running, CrashLoopBackOff, RBAC, network, etc.)
                // Return the error directly - don't fall through to sidecar (it will likely fail too)
                logger.error("Cannot exec into container '{}': {}", container, execError.getMessage());
                return formatError(execError.getMessage(), podName, namespace);
            }
            if (jstackPath == null) {
                logger.info("Container {} has no jstack available, will use sidecar", container);
                return null;
            }
            logger.info("Found jstack at: {}", jstackPath);

            // Find Java PID using jps or ps
            int javaPid;
            if (requestedPid != null && requestedPid > 0) {
                javaPid = requestedPid;
                logger.info("Using requested PID: {}", javaPid);
            } else {
                javaPid = findJavaPidDirect(namespace, podName, container);
                logger.info("Auto-detected Java PID: {}", javaPid);
            }

            if (javaPid <= 0) {
                logger.warn("Could not find Java PID in container {}", container);
                return null;
            }

            logger.info("Running {} {} in container {} of pod {}/{}", jstackPath, javaPid, container, namespace, podName);

            // Execute jstack using the resolved path
            // No 2>&1 - let stderr go to separate channel for proper error diagnosis
            String[] jstackCmd = {"/bin/sh", "-c", jstackPath + " -l " + javaPid};
            ExecResult result = executeCommand(namespace, podName, container, jstackCmd, EXEC_TIMEOUT_SECONDS);

            logger.info("jstack result: success={}, stdout.length={}, stderr.length={}",
                    result.success,
                    result.stdout != null ? result.stdout.length() : 0,
                    result.stderr != null ? result.stderr.length() : 0);

            if (!result.stdout.contains("java.lang.Thread.State")) {
                logger.warn("jstack -l failed or incomplete. stderr: {}",
                        result.stderr != null ? result.stderr.substring(0, Math.min(300, result.stderr.length())) : "null");

                // If "No such process" - the PID is stale (LLM passed old PID or pod restarted).
                // Auto-detect fresh PID and retry instead of giving up.
                String stderr = result.stderr != null ? result.stderr : "";
                if (stderr.contains("No such process") && requestedPid != null && requestedPid > 0) {
                    logger.warn("PID {} does not exist - LLM likely passed stale PID. Auto-detecting fresh PID...", javaPid);
                    int freshPid = findJavaPidDirect(namespace, podName, container);
                    if (freshPid > 0 && freshPid != javaPid) {
                        logger.info("Fresh PID detected: {} (was {}). Retrying jstack...", freshPid, javaPid);
                        javaPid = freshPid;
                        jstackCmd = new String[]{"/bin/sh", "-c", jstackPath + " -l " + javaPid};
                        result = executeCommand(namespace, podName, container, jstackCmd, EXEC_TIMEOUT_SECONDS);
                        if (result.stdout.contains("java.lang.Thread.State")) {
                            logger.info("jstack succeeded with fresh PID {}", javaPid);
                            // Fall through to success path below
                        }
                    }
                }

                // Still no valid output - try without -l flag
                if (!result.stdout.contains("java.lang.Thread.State")) {
                    String[] forceCmd = {"/bin/sh", "-c", jstackPath + " " + javaPid};
                    result = executeCommand(namespace, podName, container, forceCmd, EXEC_TIMEOUT_SECONDS);

                    if (result.stdout.isBlank() || !result.stdout.contains("java.lang.Thread.State")) {
                        logger.warn("Direct jstack failed completely. stdout(first 200)={}, stderr={}",
                                result.stdout.substring(0, Math.min(200, result.stdout.length())),
                                result.stderr);
                        return null; // Fall through to sidecar in caller
                    }
                }
            }

            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            StringBuilder sb = new StringBuilder();
            sb.append("=== Thread Dump (jstack - direct exec) ===\n");
            sb.append("Pod: ").append(podName).append("\n");
            sb.append("Namespace: ").append(namespace).append("\n");
            sb.append("Container: ").append(container).append("\n");
            sb.append("Java PID: ").append(javaPid).append("\n");
            sb.append("Timestamp: ").append(timestamp).append("\n");
            sb.append("==========================================\n\n");
            sb.append(result.stdout).append("\n\n");
            sb.append(analyzeThreadDump(result.stdout));

            logger.info("Successfully captured thread dump with {} characters", result.stdout.length());
            return sb.toString();

        } catch (Exception e) {
            logger.error("Direct jstack attempt failed with exception: {}", e.getMessage(), e);
            return null;
        }
    }

    /**
     * Finds a JDK tool (jstack, jmap, jcmd, jps) inside the container by trying
     * multiple strategies. Returns the absolute path if found, null otherwise.
     *
     * <p>Strategies (in order):
     * <ol>
     *   <li>{@code command -v <tool>} - works on all POSIX shells, unlike {@code which}</li>
     *   <li>{@code $JAVA_HOME/bin/<tool>} - works when JAVA_HOME is set but bin isn't in PATH</li>
     *   <li>Common JDK installation paths - covers standard distro and container image layouts</li>
     * </ol>
     */
    /**
     * Finds a JDK tool inside the container. Returns the absolute path if found.
     * Returns null if tool not found. Throws RuntimeException if exec itself fails
     * (RBAC, network) so callers can report the real error instead of "tool not found".
     */
    protected String findJdkTool(String namespace, String podName, String container, String toolName) {
        // No separate connectivity test - Strategy 1 (command -v) serves as both
        // the connectivity check and tool search. A separate "echo ok" adds an extra
        // exec call that can fail intermittently due to WebSocket connection reuse issues.

        try {
            // Strategy 1: command -v (POSIX-compliant, works on Alpine/busybox unlike 'which')
            // This also serves as the connectivity test - if exec is broken, this fails
            // and we throw with the real diagnosis.
            String[] cmdV = {"/bin/sh", "-c", "command -v " + toolName + " 2>/dev/null"};
            ExecResult result = executeCommand(namespace, podName, container, cmdV, 100);
            if (result.success && !result.stdout.isBlank()) {
                String path = result.stdout.trim();
                logger.info("Found {} via command -v: {}", toolName, path);
                return path;
            }

            // If exec itself failed (not just "tool not found"), throw with diagnosis
            if (!result.success && result.stderr != null
                    && (result.stderr.contains("EXEC FAILED") || result.stderr.contains("CAUSE:"))) {
                throw new RuntimeException(result.stderr);
            }

            // Strategy 2: $JAVA_HOME/bin/<tool>
            String[] javaHomeCmd = {"/bin/sh", "-c",
                    "test -x \"$JAVA_HOME/bin/" + toolName + "\" && echo \"$JAVA_HOME/bin/" + toolName + "\""};
            result = executeCommand(namespace, podName, container, javaHomeCmd, 100);
            if (result.success && !result.stdout.isBlank()) {
                String path = result.stdout.trim();
                logger.info("Found {} via JAVA_HOME: {}", toolName, path);
                return path;
            }

            // Strategy 3: Probe common JDK installation paths
            String[] probePaths = {"/bin/sh", "-c",
                    "for p in " +
                            "/usr/lib/jvm/*/bin/" + toolName + " " +
                            "/usr/java/*/bin/" + toolName + " " +
                            "/opt/java/*/bin/" + toolName + " " +
                            "/usr/local/openjdk-*/bin/" + toolName + " " +
                            "/opt/jdk/bin/" + toolName + " " +
                            "/opt/graalvm/bin/" + toolName + "; do " +
                            "  if [ -x \"$p\" ]; then echo \"$p\"; exit 0; fi; " +
                            "done"};
            result = executeCommand(namespace, podName, container, probePaths, 100);
            if (result.success && !result.stdout.isBlank()) {
                String path = result.stdout.trim().split("\n")[0]; // first match
                logger.info("Found {} at common path: {}", toolName, path);
                return path;
            }

            logger.info("JDK tool '{}' not found in container {} via any strategy", toolName, container);
            return null;

        } catch (RuntimeException e) {
            throw e; // re-throw RBAC errors
        } catch (Exception e) {
            logger.warn("Error searching for JDK tool '{}': {}", toolName, e.getMessage());
            return null;
        }
    }

    /**
     * Find Java PID by running jps or ps inside the container.
     */
    protected int findJavaPidDirect(String namespace, String podName, String container) {
        try {
            logger.info("[findJavaPid] Starting PID detection in container '{}' of pod {}/{}", container, namespace, podName);

            // ── Strategy 0: hsperfdata directory scan ──
            // Every running JVM creates /tmp/hsperfdata_<user>/<pid>.
            // This works regardless of which user runs jps or whether ps/pgrep exist.
            // Most reliable strategy - try it first.
            String[] hsperfdataCmd = {"/bin/sh", "-c",
                    "ls /tmp/hsperfdata_*/[0-9]* 2>/dev/null | head -5"};
            ExecResult result = executeCommand(namespace, podName, container, hsperfdataCmd, 100);
            String hsperfdataOut = result.stdout != null ? result.stdout.trim() : "";
            logger.info("[findJavaPid] hsperfdata scan: '{}'", hsperfdataOut);
            if (!hsperfdataOut.isBlank()) {
                for (String line : hsperfdataOut.split("\n")) {
                    line = line.trim();
                    if (line.isEmpty()) continue;
                    // Extract PID from path like /tmp/hsperfdata_appuser/47
                    String filename = line.substring(line.lastIndexOf('/') + 1);
                    try {
                        int pid = Integer.parseInt(filename);
                        if (pid > 0) {
                            logger.info("[findJavaPid] Found Java PID {} via hsperfdata directory", pid);
                            return pid;
                        }
                    } catch (NumberFormatException e) {
                        logger.debug("[findJavaPid] hsperfdata entry not a PID: {}", filename);
                    }
                }
            }

            // ── Strategy 1: PID 1 check ──
            // If the container entrypoint IS java (common in simple containers)
            String[] commCmd = {"/bin/sh", "-c", "cat /proc/1/comm 2>/dev/null"};
            ExecResult commResult = executeCommand(namespace, podName, container, commCmd, 100);
            String comm = commResult.stdout != null ? commResult.stdout.trim() : "";
            logger.info("[findJavaPid] PID 1 comm: '{}'", comm);
            if ("java".equals(comm)) {
                logger.info("[findJavaPid] PID 1 executable is 'java' - using PID 1");
                return 1;
            }
            // Check first token of cmdline (e.g. /opt/java/openjdk/bin/java)
            String[] firstArgCmd = {"/bin/sh", "-c", "cat /proc/1/cmdline 2>/dev/null | tr '\\0' '\\n' | head -1"};
            ExecResult firstArgResult = executeCommand(namespace, podName, container, firstArgCmd, 100);
            String firstArg = firstArgResult.stdout != null ? firstArgResult.stdout.trim() : "";
            logger.info("[findJavaPid] PID 1 first arg: '{}'", firstArg);
            if (firstArg.endsWith("/java") || "java".equals(firstArg)) {
                logger.info("[findJavaPid] PID 1 first cmdline arg is java - using PID 1");
                return 1;
            }

            // ── Strategy 2: jcmd (no args) ──
            // Unlike jps, jcmd can attach to JVMs even when hsperfdata is not
            // visible to the current user (it uses the attach API directly).
            String jcmdPath = findJdkTool(namespace, podName, container, "jcmd");
            if (jcmdPath != null) {
                String[] jcmdCmd = {"/bin/sh", "-c", jcmdPath + " -l 2>/dev/null"};
                result = executeCommand(namespace, podName, container, jcmdCmd, 100);
                String jcmdOut = result.stdout != null ? result.stdout.trim() : "";
                logger.info("[findJavaPid] jcmd output: '{}'", jcmdOut);
                if (!jcmdOut.isBlank()) {
                    for (String line : jcmdOut.split("\n")) {
                        line = line.trim();
                        if (line.isEmpty()) continue;
                        // Skip jcmd itself (sun.tools.jcmd.JCmd)
                        if (line.contains("jcmd") || line.contains("JCmd")) continue;
                        String[] parts = line.split("\\s+");
                        if (parts.length >= 1) {
                            try {
                                int pid = Integer.parseInt(parts[0]);
                                if (pid > 0) {
                                    logger.info("[findJavaPid] Found Java PID {} via jcmd (class: {})", pid,
                                            parts.length > 1 ? parts[1] : "unknown");
                                    return pid;
                                }
                            } catch (NumberFormatException e) {
                                // not a PID line
                            }
                        }
                    }
                }
            }

            // ── Strategy 3: jps ──
            String jpsPath = findJdkTool(namespace, podName, container, "jps");
            if (jpsPath != null) {
                String[] jpsCmd = {"/bin/sh", "-c", jpsPath + " -l 2>/dev/null"};
                result = executeCommand(namespace, podName, container, jpsCmd, 100);
                logger.info("[findJavaPid] jps output: '{}'", result.stdout != null ? result.stdout.trim() : "null");

                if (!result.stdout.isBlank()) {
                    for (String line : result.stdout.split("\n")) {
                        line = line.trim();
                        if (line.isEmpty()) continue;
                        if (line.contains("Jps") || line.contains("jps")) continue;
                        String[] parts = line.split("\\s+");
                        if (parts.length >= 1) {
                            try {
                                int pid = Integer.parseInt(parts[0]);
                                logger.info("[findJavaPid] Found Java PID {} via jps (class: {})", pid,
                                        parts.length > 1 ? parts[1] : "unknown");
                                return pid;
                            } catch (NumberFormatException e) {
                                logger.debug("[findJavaPid] Could not parse PID from jps line: {}", line);
                            }
                        }
                    }
                }
            }

            // ── Strategy 4: /proc scan ──
            // Scan /proc/*/cmdline for any process whose executable or first arg is java.
            // Works on minimal images without ps/pgrep.
            String[] procCmd = {"/bin/sh", "-c",
                    "for pid in /proc/[0-9]*/cmdline; do " +
                            "  p=$(echo $pid | grep -o '[0-9]*'); " +
                            "  first=$(cat \"$pid\" 2>/dev/null | tr '\\0' '\\n' | head -1); " +
                            "  case \"$first\" in *java|*/java) echo $p; exit 0;; esac; " +
                            "done"};
            result = executeCommand(namespace, podName, container, procCmd, 100);
            String procOut = result.stdout != null ? result.stdout.trim() : "";
            logger.info("[findJavaPid] /proc cmdline scan output: '{}'", procOut);
            if (!procOut.isBlank()) {
                try {
                    int pid = Integer.parseInt(procOut.split("\n")[0].trim());
                    if (pid > 0) {
                        logger.info("[findJavaPid] Found Java PID {} via /proc cmdline scan", pid);
                        return pid;
                    }
                } catch (NumberFormatException e) {
                    logger.info("[findJavaPid] /proc cmdline output not a number: {}", procOut);
                }
            }

            // ── Strategy 5: /proc/exe symlink scan ──
            // Check /proc/<pid>/exe symlink - works even when cmdline is unreadable
            // (e.g. in some security-restricted containers).
            String[] exeCmd = {"/bin/sh", "-c",
                    "for d in /proc/[0-9]*; do " +
                            "  p=$(basename $d); " +
                            "  exe=$(readlink $d/exe 2>/dev/null); " +
                            "  case \"$exe\" in *java|*/java) echo $p; exit 0;; esac; " +
                            "done"};
            result = executeCommand(namespace, podName, container, exeCmd, 100);
            String exeOut = result.stdout != null ? result.stdout.trim() : "";
            logger.info("[findJavaPid] /proc exe scan output: '{}'", exeOut);
            if (!exeOut.isBlank()) {
                try {
                    int pid = Integer.parseInt(exeOut.split("\n")[0].trim());
                    if (pid > 0) {
                        logger.info("[findJavaPid] Found Java PID {} via /proc exe symlink", pid);
                        return pid;
                    }
                } catch (NumberFormatException e) {
                    logger.info("[findJavaPid] /proc exe output not a number: {}", exeOut);
                }
            }

            // ── Strategy 6: pgrep ──
            String[] pgrepCmd = {"/bin/sh", "-c", "pgrep -x java 2>/dev/null || pgrep -f 'java ' 2>/dev/null | head -1"};
            result = executeCommand(namespace, podName, container, pgrepCmd, 100);
            String pgrepOut = result.stdout != null ? result.stdout.trim() : "";
            logger.info("[findJavaPid] pgrep output: '{}'", pgrepOut);
            if (!pgrepOut.isBlank()) {
                try {
                    int pid = Integer.parseInt(pgrepOut.split("\n")[0].trim());
                    if (pid > 0) {
                        logger.info("[findJavaPid] Found Java PID {} via pgrep", pid);
                        return pid;
                    }
                } catch (NumberFormatException e) {
                    logger.info("[findJavaPid] Could not parse PID from pgrep output");
                }
            }

            // ── Strategy 7: ps (last resort) ──
            String[] psCmd = {"/bin/sh", "-c", "ps aux 2>/dev/null | awk '$11 ~ /(^|\\/)?java$/ {print $2; exit}'"};
            result = executeCommand(namespace, podName, container, psCmd, 100);
            String psOut = result.stdout != null ? result.stdout.trim() : "";
            logger.info("[findJavaPid] ps output: '{}'", psOut);
            if (!psOut.isBlank()) {
                try {
                    int pid = Integer.parseInt(psOut);
                    if (pid > 0) {
                        logger.info("[findJavaPid] Found Java PID {} via ps", pid);
                        return pid;
                    }
                } catch (NumberFormatException e) {
                    logger.info("[findJavaPid] Could not parse PID from ps output");
                }
            }

            logger.warn("[findJavaPid] Could not find Java PID in container '{}' - all 8 strategies failed " +
                    "(hsperfdata, PID 1, jcmd, jps, /proc cmdline, /proc exe, pgrep, ps)", container);
        } catch (Exception e) {
            logger.error("[findJavaPid] PID detection failed: {}", e.getMessage());
        }
        return -1;
    }

    /**
     * Try to run class histogram directly in the target container (prefer jcmd, fallback to jmap).
     */
    protected String tryDirectHistogram(String namespace, String podName, String container, Integer requestedPid, int limit) {
        try {
            // Always auto-detect PID - ignore LLM-provided PID to prevent stale PID issues
            if (requestedPid != null && requestedPid > 0) {
                logger.info("[tryDirectHistogram] Ignoring LLM-provided PID {} - will auto-detect", requestedPid);
            }
            int javaPid = findJavaPidDirect(namespace, podName, container);

            if (javaPid <= 0) {
                logger.info("[tryDirectHistogram] Could not find Java PID in container '{}' - skipping direct histogram", container);
                return null;
            }

            logger.info("[tryDirectHistogram] Found Java PID {} in container '{}', running class histogram directly", javaPid, container);

            ExecResult result = runClassHistogram(namespace, podName, container, javaPid, limit);

            if (!result.success || result.stdout.isBlank()) {
                logger.info("[tryDirectHistogram] Direct class histogram failed (success={}, stdout.blank={}, stderr={}). Will try sidecar next.",
                        result.success, result.stdout.isBlank(),
                        result.stderr != null ? result.stderr.substring(0, Math.min(300, result.stderr.length())) : "null");
                if (!result.stdout.isBlank()) {
                    logger.info("[tryDirectHistogram] jcmd/jmap stdout (first 500 chars): {}",
                            result.stdout.substring(0, Math.min(500, result.stdout.length())));
                }
                return null; // Fall through to sidecar in caller
            }

            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            StringBuilder sb = new StringBuilder();
            sb.append("=== Memory Histogram (direct exec) ===\n");
            sb.append("Pod: ").append(podName).append("\n");
            sb.append("Namespace: ").append(namespace).append("\n");
            sb.append("Container: ").append(container).append("\n");
            sb.append("Java PID: ").append(javaPid).append("\n");
            sb.append("Timestamp: ").append(timestamp).append("\n");
            sb.append("Top ").append(limit).append(" classes by memory usage\n");
            sb.append("=".repeat(60)).append("\n\n");
            sb.append(result.stdout);
            sb.append("\n").append("=".repeat(60)).append("\n");
            sb.append(analyzeMemoryHistogram(result.stdout));

            return sb.toString();

        } catch (Exception e) {
            logger.debug("Direct histogram attempt failed: {}", e.getMessage());
            return null;
        }
    }

    protected ExecResult runClassHistogram(String namespace, String podName, String container, int javaPid, int limit) {
        // Resolve tool paths using findJdkTool
        String jcmdPath = findJdkTool(namespace, podName, container, "jcmd");
        String jmapPath = findJdkTool(namespace, podName, container, "jmap");

        logger.info("[runClassHistogram] container='{}', pid={}, jcmd={}, jmap={}",
                container, javaPid, jcmdPath != null ? jcmdPath : "NOT FOUND", jmapPath != null ? jmapPath : "NOT FOUND");

        if (jcmdPath != null) {
            // No 2>&1 - let stderr go to separate channel so we can distinguish errors from output
            String[] jcmdCmd = {"/bin/sh", "-c", jcmdPath + " " + javaPid + " GC.class_histogram | head -" + (limit + 10)};
            logger.info("[runClassHistogram] Running: {} {} GC.class_histogram", jcmdPath, javaPid);
            ExecResult jcmdResult = executeCommand(namespace, podName, container, jcmdCmd, 60);

            boolean hasStderr = jcmdResult.stderr != null && !jcmdResult.stderr.isBlank();
            boolean isHistogram = looksLikeHistogram(jcmdResult.stdout);
            logger.info("[runClassHistogram] jcmd result: success={}, looksLikeHistogram={}, stdout.length={}, stderr.length={}, hasStderr={}",
                    jcmdResult.success, isHistogram,
                    jcmdResult.stdout != null ? jcmdResult.stdout.length() : 0,
                    jcmdResult.stderr != null ? jcmdResult.stderr.length() : 0,
                    hasStderr);

            if (isHistogram) {
                logger.info("[runClassHistogram] jcmd GC.class_histogram succeeded - using direct result");
                if (hasStderr) {
                    logger.info("[runClassHistogram] jcmd stderr (ignored, histogram valid): {}",
                            jcmdResult.stderr.substring(0, Math.min(200, jcmdResult.stderr.length())));
                }
                return jcmdResult;
            }

            // Log WHY it was rejected - show both stdout and stderr
            logger.info("[runClassHistogram] jcmd histogram rejected. stdout(first 500): {}",
                    jcmdResult.stdout != null ? jcmdResult.stdout.substring(0, Math.min(500, jcmdResult.stdout.length())) : "null");
            if (hasStderr) {
                logger.info("[runClassHistogram] jcmd stderr(first 500): {}",
                        jcmdResult.stderr.substring(0, Math.min(500, jcmdResult.stderr.length())));
            }

            // Avoid paying a second attach timeout when jcmd already proved attach is blocked.
            if (containsAttachFailureMarkers(jcmdResult.stdout) || containsAttachFailureMarkers(jcmdResult.stderr)) {
                logger.info("[runClassHistogram] jcmd attach failure detected - skipping jmap");
                String stderr = summarizeHistogramCommandFailure("jcmd", jcmdResult)
                        + "; jmap skipped because JVM attach is unavailable";
                String stdout = "jcmd output:\n" + (jcmdResult.stdout == null ? "" : jcmdResult.stdout);
                return new ExecResult(false, stdout, stderr);
            }
        }

        if (jmapPath != null) {
            String[] jmapCmd = {"/bin/sh", "-c", jmapPath + " -histo:live " + javaPid + " | head -" + (limit + 10)};
            logger.info("[runClassHistogram] Trying jmap: {} -histo:live {}", jmapPath, javaPid);
            ExecResult jmapResult = executeCommand(namespace, podName, container, jmapCmd, 60);

            boolean jmapIsHistogram = looksLikeHistogram(jmapResult.stdout);
            boolean jmapHasStderr = jmapResult.stderr != null && !jmapResult.stderr.isBlank();
            logger.info("[runClassHistogram] jmap result: success={}, looksLikeHistogram={}, stdout.length={}, stderr.length={}",
                    jmapResult.success, jmapIsHistogram,
                    jmapResult.stdout != null ? jmapResult.stdout.length() : 0,
                    jmapResult.stderr != null ? jmapResult.stderr.length() : 0);

            if (jmapIsHistogram) {
                logger.info("[runClassHistogram] jmap -histo:live succeeded - using direct result");
                return jmapResult;
            }

            logger.info("[runClassHistogram] jmap output doesn't look like histogram. stdout(first 500): {}",
                    jmapResult.stdout != null ? jmapResult.stdout.substring(0, Math.min(500, jmapResult.stdout.length())) : "null");
            if (jmapHasStderr) {
                logger.info("[runClassHistogram] jmap stderr(first 500): {}",
                        jmapResult.stderr.substring(0, Math.min(500, jmapResult.stderr.length())));
            }
        }

        if (jcmdPath == null && jmapPath == null) {
            logger.info("[runClassHistogram] Neither jcmd nor jmap found in container '{}'", container);
            return new ExecResult(false, "", "Neither jcmd nor jmap found in container");
        }

        String stderr = "Class histogram failed - jcmd" + (jcmdPath != null ? " ran but failed" : " not found")
                + ", jmap" + (jmapPath != null ? " ran but failed" : " not found");
        logger.info("[runClassHistogram] Both tools failed: {}", stderr);
        return new ExecResult(false, "", stderr);
    }

    protected boolean looksLikeHistogram(String output) {
        if (output == null || output.isBlank()) {
            return false;
        }
        String normalized = output.toLowerCase(Locale.ROOT);

        // Negative check: common error messages that are NOT histograms
        if (normalized.contains("attachnotsupportedexception")
                || normalized.contains("no such process")
                || normalized.contains("unable to open socket")
                || normalized.contains("operation not permitted")
                || normalized.contains("not a hotspot vm")
                || normalized.contains("hotspot vm not loaded")
                || normalized.contains("agentloadexception")) {
            logger.debug("[looksLikeHistogram] Detected error marker in output - not a histogram");
            return false;
        }

        // Header markers present in standard OpenJDK/Corretto/Zulu histogram output
        if (normalized.contains("class name")
                || normalized.contains("#instances")
                || normalized.contains("#bytes")
                || normalized.contains("num     #")) {
            return true;
        }

        // Numbered row pattern:  "   1:    12345    12345678  java.lang.String"
        if (output.matches("(?s).*\\n\\s*\\d+:\\s+\\d+\\s+\\d+\\s+.+")) {
            return true;
        }

        // Total line pattern: "Total    12345    12345678"
        if (normalized.contains("\ntotal")) {
            return true;
        }

        // Java class references (strong signal of histogram data)
        if ((normalized.contains("java.lang.") || normalized.contains("[b") || normalized.contains("[c"))
                && output.matches("(?s).*\\d+\\s+\\d+.*")) {
            return true;
        }

        logger.debug("[looksLikeHistogram] No histogram patterns found in output");
        return false;
    }

    protected boolean containsAttachFailureMarkers(String output) {
        if (output == null || output.isBlank()) {
            return false;
        }
        String lower = output.toLowerCase(Locale.ROOT);
        return lower.contains("attachnotsupportedexception")
                || lower.contains("unable to open socket file /tmp/.java_pid")
                || lower.contains("target process")
                || lower.contains("operation not permitted")
                || lower.contains("hotspot vm not loaded");
    }

    protected String buildHistogramFailureDetails(ExecResult jcmdResult, ExecResult jmapResult) {
        String jcmdSummary = summarizeHistogramCommandFailure("jcmd", jcmdResult);
        String jmapSummary = summarizeHistogramCommandFailure("jmap", jmapResult);

        String combined = (jcmdResult.stdout == null ? "" : jcmdResult.stdout)
                + "\n"
                + (jcmdResult.stderr == null ? "" : jcmdResult.stderr)
                + "\n"
                + (jmapResult.stdout == null ? "" : jmapResult.stdout)
                + "\n"
                + (jmapResult.stderr == null ? "" : jmapResult.stderr);
        String lower = combined.toLowerCase(Locale.ROOT);

        String hint = "";
        if (lower.contains("attachnotsupportedexception")
                || lower.contains("unable to open socket file /tmp/.java_pid")) {
            hint = " Hint: JVM attach failed. This is commonly caused by selecting a non-Java PID, "
                    + "or by a JVM/container policy that blocks attach.";
        } else if (lower.contains("doesn't appear to be a hotspot vm")
                || lower.contains("hotspot vm not loaded")) {
            hint = " Hint: target process may not be a compatible HotSpot JVM.";
        }

        return jcmdSummary + "; " + jmapSummary + hint;
    }

    private String summarizeHistogramCommandFailure(String toolName, ExecResult result) {
        String details = firstMeaningfulLine(result.stderr);
        if (details == null) {
            details = firstMeaningfulLine(result.stdout);
        }
        if (details == null) {
            details = "no diagnostic output captured";
        }
        return toolName + " failed: " + details;
    }

    private String firstMeaningfulLine(String text) {
        if (text == null || text.isBlank()) {
            return null;
        }
        for (String line : text.split("\\R")) {
            String trimmed = line.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            if (trimmed.matches("^\\d+:$")) {
                continue;
            }
            return trimmed.length() > 240 ? trimmed.substring(0, 240) : trimmed;
        }
        return null;
    }

    protected int getFreeSpaceMb(String namespace, String podName, String container, String path) {
        try {
            String[] dfCmd = {"/bin/sh", "-c", "df -Pm " + path + " | tail -1 | awk '{print $4}'"};
            ExecResult dfResult = executeCommand(namespace, podName, container, dfCmd, 100);
            if (!dfResult.success || dfResult.stdout.isBlank()) {
                return -1;
            }
            return Integer.parseInt(dfResult.stdout.trim());
        } catch (Exception e) {
            logger.debug("Could not determine free space for path {}: {}", path, e.getMessage());
            return -1;
        }
    }

    protected int clampMinFreeMb(Integer requested, int defaultValue) {
        if (requested == null) {
            return defaultValue;
        }
        return Math.max(128, Math.min(requested, 102400));
    }

    protected boolean isSafePath(String path) {
        return path != null
                && path.startsWith("/")
                && !path.contains("..")
                && path.matches("^[a-zA-Z0-9_./-]+$");
    }

    protected String resolveNamespace(String namespace) {
        return (namespace != null && !namespace.isBlank()) ? namespace : defaultNamespace;
    }

    protected ContainerResolution resolveContainer(Pod pod, String requested) {
        return resolveContainer(pod, requested, null, null);
    }

    protected ContainerResolution resolveContainer(Pod pod, String requested, String namespace, String podName) {
        List<ContainerStatus> containers = pod.getStatus().getContainerStatuses();
        if (containers == null || containers.isEmpty()) {
            return new ContainerResolution(null, List.of(), false, Source.UNRESOLVED);
        }

        // Get list of valid container names (preserving order)
        List<String> validNames = containers.stream()
                .map(ContainerStatus::getName)
                .collect(java.util.stream.Collectors.toList());

        // If requested container specified and valid, accept it directly
        if (requested != null && !requested.isBlank()) {
            if (validNames.contains(requested)) {
                List<String> others = validNames.stream()
                        .filter(n -> !n.equals(requested))
                        .collect(java.util.stream.Collectors.toList());
                return new ContainerResolution(requested, others, false, Source.EXPLICIT);
            }
            // Requested container doesn't exist - log warning and try to auto-select
            logger.warn("Requested container '{}' not found. Valid containers: {}", requested, validNames);
        }

        // Auto-select if only one container
        if (containers.size() == 1) {
            String autoSelected = containers.get(0).getName();
            logger.info("Auto-selected container: {}", autoSelected);
            return new ContainerResolution(autoSelected, List.of(), false, Source.SINGLE);
        }

        // Multiple containers - try LLM resolver
        String ns = resolveNamespace(namespace);
        String effectivePodName = podName != null ? podName : "unknown";
        if (smartContainerResolver != null) {
            ContainerResolution llmResult = smartContainerResolver.resolve(ns, effectivePodName, validNames);
            if (llmResult.selected() != null) {
                return llmResult; // source is LLM
            }
        }

        // LLM failed or unavailable - return UNRESOLVED, caller handles via formatMultiContainerError
        logger.warn("Pod has {} containers: {}. Smart resolver could not pick one.", containers.size(), validNames);
        return new ContainerResolution(null, validNames, false, Source.UNRESOLVED);
    }

    /**
     * Formats an error message listing all containers in a multi-container pod,
     * prompting the user to specify which container to run diagnostics on.
     */
    protected String formatMultiContainerError(String podName, String ns, Pod pod) {
        List<ContainerStatus> containers = pod.getStatus().getContainerStatuses();
        if (containers == null || containers.isEmpty()) {
            return formatError("No containers found", podName, ns);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("=== Multiple Containers Detected ===\n");
        sb.append("Pod: ").append(podName).append("\n");
        sb.append("Namespace: ").append(ns).append("\n\n");
        sb.append("This pod has ").append(containers.size()).append(" containers:\n\n");

        for (int i = 0; i < containers.size(); i++) {
            ContainerStatus cs = containers.get(i);
            String state = cs.getReady() != null && cs.getReady() ? "Running" : "Not Ready";
            sb.append("  ").append(i + 1).append(") ").append(cs.getName())
              .append(" (").append(state).append(")\n");
        }

        sb.append("\nPlease specify the targetContainer parameter with one of the container names above.\n");
        sb.append("Example: targetContainer=\"").append(containers.get(0).getName()).append("\"\n");

        return sb.toString();
    }

    protected boolean isMultiContainer(Pod pod) {
        List<ContainerStatus> containers = pod.getStatus().getContainerStatuses();
        return containers != null && containers.size() > 1;
    }

    /**
     * Builds a container info string for tool output.
     * For auto-selected: "java-primary (auto-selected from: java-primary, java-secondary)"
     * For explicit/single: "java-primary"
     */
    protected static String containerInfoLine(ContainerResolution cr) {
        if (cr == null || cr.selected() == null) return "unknown";
        if (cr.wasAutoSelected() && cr.others() != null && !cr.others().isEmpty()) {
            return cr.selected() + " (auto-selected, alternatives: "
                    + String.join(", ", cr.others()) + ")";
        }
        return cr.selected();
    }

    protected String getContainerNames(Pod pod) {
        if (pod.getStatus().getContainerStatuses() == null) return "none";
        return pod.getStatus().getContainerStatuses().stream()
                .map(ContainerStatus::getName)
                .reduce((a, b) -> a + ", " + b)
                .orElse("none");
    }

    protected String findExistingDebugContainer(Pod pod) {
        if (pod.getStatus().getEphemeralContainerStatuses() == null) return null;
        for (ContainerStatus cs : pod.getStatus().getEphemeralContainerStatuses()) {
            if (cs.getName().startsWith(DEBUG_CONTAINER_PREFIX) &&
                    cs.getState() != null && cs.getState().getRunning() != null) {
                return cs.getName();
            }
        }
        return null;
    }

    protected int findJavaPidFromSidecar(String namespace, String podName, String sidecar) {
        try {
            // Strategy 1: Use jps (most reliable for HotSpot JVMs)
            String[] jpsCmd = {"/bin/sh", "-c", "jps -l 2>/dev/null | grep -v Jps | head -1 | awk '{print $1}'"};
            ExecResult result = executeCommand(namespace, podName, sidecar, jpsCmd, 100);
            if (result.success && !result.stdout.isBlank()) {
                try {
                    int pid = Integer.parseInt(result.stdout.trim());
                    if (pid > 0) {
                        logger.info("Found Java PID {} via jps", pid);
                        return pid;
                    }
                } catch (NumberFormatException e) {
                    logger.debug("jps output not a number: {}", result.stdout);
                }
            }

            // Strategy 2: Look for java process via /proc
            // This works even if jps can't attach to the JVM and avoids false positives from shell scripts.
            String[] procCmd = {"/bin/sh", "-c",
                    "for pid in /proc/[0-9]*; do " +
                            "  if [ -f \"$pid/comm\" ] && [ \"$(cat \"$pid/comm\" 2>/dev/null)\" = \"java\" ]; then " +
                            "    echo ${pid##*/}; break; " +
                            "  fi; " +
                            "done"};
            result = executeCommand(namespace, podName, sidecar, procCmd, 100);
            if (result.success && !result.stdout.isBlank()) {
                try {
                    int pid = Integer.parseInt(result.stdout.trim());
                    if (pid > 0) {
                        logger.info("Found Java PID {} via /proc scan", pid);
                        return pid;
                    }
                } catch (NumberFormatException e) {
                    logger.debug("/proc scan output not a number: {}", result.stdout);
                }
            }

            // Strategy 3: ps command fallback
            String[] psCmd = {"/bin/sh", "-c", "ps aux 2>/dev/null | awk '$11 ~ /(^|\\/)?java$/ {print $2; exit}'"};
            result = executeCommand(namespace, podName, sidecar, psCmd, 100);
            if (result.success && !result.stdout.isBlank()) {
                try {
                    int pid = Integer.parseInt(result.stdout.trim());
                    if (pid > 0) {
                        logger.info("Found Java PID {} via ps", pid);
                        return pid;
                    }
                } catch (NumberFormatException e) {
                    logger.debug("ps output not a number: {}", result.stdout);
                }
            }

            logger.warn("Could not find Java process in pod {}/{} from sidecar {}", namespace, podName, sidecar);
        } catch (Exception e) {
            logger.error("PID detection failed: {}", e.getMessage());
        }
        return -1;
    }

    protected String analyzeThreadDump(String dump) {
        int runnable = 0, waiting = 0, timedWaiting = 0, blocked = 0;
        for (String line : dump.split("\n")) {
            if (line.contains("java.lang.Thread.State:")) {
                if (line.contains("RUNNABLE")) runnable++;
                else if (line.contains("TIMED_WAITING")) timedWaiting++;
                else if (line.contains("WAITING")) waiting++;
                else if (line.contains("BLOCKED")) blocked++;
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("ANALYSIS:\n");
        sb.append("  RUNNABLE: ").append(runnable).append("\n");
        sb.append("  WAITING: ").append(waiting).append("\n");
        sb.append("  TIMED_WAITING: ").append(timedWaiting).append("\n");
        sb.append("  BLOCKED: ").append(blocked).append("\n");
        sb.append("  Total: ").append(runnable + waiting + timedWaiting + blocked).append("\n\n");

        if (dump.toLowerCase().contains("deadlock")) {
 sb.append("[WARN] DEADLOCK DETECTED!\n");
        } else {
 sb.append("[OK] No deadlocks\n");
        }

        if (blocked > 5) {
 sb.append("[WARN] High blocked count - possible lock contention\n");
        }

        return sb.toString();
    }

    protected String analyzeMemoryHistogram(String histo) {
        try {
            return analyzeMemoryHistogramInternal(histo);
        } catch (Exception e) {
            // If parsing fails, return minimal analysis so raw output is still visible
            return "MEMORY ANALYSIS:\n\n(Auto-analysis skipped - could not parse histogram output)\n";
        }
    }

    private String analyzeMemoryHistogramInternal(String histo) {
        StringBuilder analysis = new StringBuilder();
        analysis.append("=== Memory Analysis ===\n\n");

        String[] lines = histo.split("\n");
        long totalInstances = 0;
        long totalBytes = 0;

        // Parsed entries: each is {instances, bytes, className}
        List<long[]> entryCounts = new ArrayList<>();
        List<String> entryNames = new ArrayList<>();

        // Suspicious-pattern accumulators
        long stringByteCharBytes = 0;
        long stringByteCharInstances = 0;
        String topCustomClass = null;
        long topCustomClassInstances = 0;

        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("num") || line.startsWith("-") || line.startsWith("Total")) continue;

            String[] parts = line.split("\\s+");
            if (parts.length >= 4) {
                try {
                    long instances = Long.parseLong(parts[1]);
                    long bytes = Long.parseLong(parts[2]);
                    String className = parts[3];

                    totalInstances += instances;
                    totalBytes += bytes;

                    entryCounts.add(new long[]{instances, bytes});
                    entryNames.add(className);

                    // Track byte[]/char[]/String accumulation
                    if (className.equals("[B") || className.equals("[C")
                            || className.equals("java.lang.String")) {
                        stringByteCharBytes += bytes;
                        stringByteCharInstances += instances;
                    }

                    // Track custom classes (not java.*, jdk.*, [array types, sun.*, etc.)
                    if (!className.startsWith("java.") && !className.startsWith("jdk.")
                            && !className.startsWith("[") && !className.startsWith("sun.")
                            && !className.startsWith("javax.") && !className.startsWith("com.sun.")
                            && instances > topCustomClassInstances) {
                        topCustomClass = className;
                        topCustomClassInstances = instances;
                    }
                } catch (NumberFormatException ignored) {}
            }
        }

        if (entryCounts.isEmpty()) {
            return "MEMORY ANALYSIS:\n\n(No parseable histogram entries found)\n";
        }

        // --- Top 10 classes by bytes with percentage ---
        analysis.append("Top 10 Classes by Memory:\n");
        analysis.append(String.format("  %-6s %-14s %-14s %s%n", "#", "Bytes", "Instances", "Class"));
        analysis.append("  ").append("-".repeat(70)).append("\n");

        // entries are already sorted by bytes in jmap -histo output (descending)
        int top = Math.min(10, entryCounts.size());
        for (int i = 0; i < top; i++) {
            long bytes = entryCounts.get(i)[1];
            long instances = entryCounts.get(i)[0];
            String className = entryNames.get(i);
            double pct = totalBytes > 0 ? (bytes * 100.0 / totalBytes) : 0;
            analysis.append(String.format("  %-6d %-14s %-14s %s (%.1f%%)%n",
                    i + 1, formatBytes(bytes), String.format("%,d", instances), className, pct));
        }

        // --- Suspicious Patterns ---
        analysis.append("\nSuspicious Patterns:\n");
        List<String> warnings = new ArrayList<>();

        // byte[]/char[]/String accumulation
        if (stringByteCharBytes > 0 && totalBytes > 0) {
            double pct = stringByteCharBytes * 100.0 / totalBytes;
            if (pct > 40) {
                warnings.add(String.format(
                        "Possible string/buffer accumulation: byte[]/char[]/String consume %.1f%% of heap (%s, %,d instances)",
                        pct, formatBytes(stringByteCharBytes), stringByteCharInstances));
            }
        }

        // Custom class with very high instance count
        if (topCustomClass != null && topCustomClassInstances > 100000) {
            warnings.add(String.format("Possible object leak: %,d instances of %s",
                    topCustomClassInstances, topCustomClass));
        }

        // Very high total object count
        if (totalInstances > 10_000_000) {
            warnings.add(String.format("Very high object count (%,d total) - GC pressure likely",
                    totalInstances));
        }

        // Additional checks from original analysis
        for (int i = 0; i < entryNames.size(); i++) {
            String className = entryNames.get(i);
            long instances = entryCounts.get(i)[0];
            long bytes = entryCounts.get(i)[1];

            if (className.contains("HashMap") && instances > 100000) {
                warnings.add("High HashMap count (" + String.format("%,d", instances) + ") - check for unbounded caches");
            }
            if (className.contains("ArrayList") && instances > 100000) {
                warnings.add("High ArrayList count (" + String.format("%,d", instances) + ") - check for growing lists");
            }
            if (className.contains("Connection") && instances > 1000) {
                warnings.add("High connection count (" + String.format("%,d", instances) + ") - check for connection leaks");
            }
            if (className.contains("Thread") && instances > 500) {
                warnings.add("High thread count (" + String.format("%,d", instances) + ") - check for thread leaks");
            }
            if (className.contains("$") && className.contains("Lambda") && instances > 50000) {
                warnings.add("Many lambda instances (" + String.format("%,d", instances) + ") - possible lambda leak in streams");
            }
        }

        if (warnings.isEmpty()) {
            analysis.append("  No suspicious patterns detected.\n");
        } else {
            for (String w : warnings) {
                analysis.append("  WARNING: ").append(w).append("\n");
            }
        }

        // --- Summary ---
        String topConsumer = !entryNames.isEmpty() ? entryNames.get(0) : "N/A";
        long topConsumerBytes = !entryCounts.isEmpty() ? entryCounts.get(0)[1] : 0;
        double topPct = totalBytes > 0 ? (topConsumerBytes * 100.0 / totalBytes) : 0;

        analysis.append("\nSummary:\n");
        analysis.append("  Total instances: ").append(String.format("%,d", totalInstances)).append("\n");
        analysis.append("  Total bytes:     ").append(formatBytes(totalBytes)).append("\n");
        analysis.append(String.format("  Top consumer:    %s (%s, %.1f%% of total)%n",
                topConsumer, formatBytes(topConsumerBytes), topPct));

        analysis.append("\nRecommendations:\n");
        analysis.append("  * Compare with previous histogram to identify growing classes\n");
        analysis.append("  * Look for your application classes in top 20 (com.yourcompany.*)\n");
        analysis.append("  * High [B (byte[]) or [C (char[]) often point to String/IO issues\n");
        if (!warnings.isEmpty()) {
            analysis.append("  * Capture a heap dump for detailed leak analysis\n");
        }

        return analysis.toString();
    }

    protected String explainJstatGC(String jstatOutput) {
        StringBuilder sb = new StringBuilder();
        sb.append("\nColumn meanings:\n");
        sb.append("  S0C/S1C: Survivor space 0/1 capacity (KB)\n");
        sb.append("  S0U/S1U: Survivor space 0/1 used (KB)\n");
        sb.append("  EC/EU: Eden space capacity/used (KB)\n");
        sb.append("  OC/OU: Old gen capacity/used (KB)\n");
        sb.append("  MC/MU: Metaspace capacity/used (KB)\n");
        sb.append("  YGC/YGCT: Young GC count/time (seconds)\n");
        sb.append("  FGC/FGCT: Full GC count/time (seconds)\n");
        return sb.toString();
    }

    protected String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
    }

    protected ExecResult executeCommand(String namespace, String podName, String container,
                                      String[] command, int timeoutSeconds) {
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        ByteArrayOutputStream stderr = new ByteArrayOutputStream();
        CompletableFuture<Boolean> future = new CompletableFuture<>();

        String cmdStr = String.join(" ", command);
        logger.debug("Executing in {}/{}/{}: {}", namespace, podName, container, cmdStr);

        try {
            ExecWatch exec = client.pods()
                    .inNamespace(namespace)
                    .withName(podName)
                    .inContainer(container)
                    .writingOutput(stdout)
                    .writingError(stderr)
                    .usingListener(new ExecListener() {
                        @Override public void onOpen() {
                            logger.debug("Exec connection opened for {}/{}", podName, container);
                        }
                        @Override public void onFailure(Throwable t, ExecListener.Response r) {
                            String responseBody = r != null ? String.valueOf(r.code()) : "null";
                            logger.warn("Exec failed for {}/{} in namespace {}: {} (response: {})",
                                    podName, container, namespace, t.getMessage(), responseBody);
                            future.complete(false);
                        }
                        @Override public void onClose(int code, String reason) {
                            logger.debug("Exec closed for {}/{}: code={}, reason={}", podName, container, code, reason);
                            future.complete(code == 0 || code == 1000);
                        }
                    })
                    .exec(command);

            boolean success = future.get(timeoutSeconds, TimeUnit.SECONDS);
            exec.close();

            String stdoutStr = stdout.toString(StandardCharsets.UTF_8);
            String stderrStr = stderr.toString(StandardCharsets.UTF_8);

            logger.info("Exec result: success={}, stdout.len={}, stderr.len={}",
                    success, stdoutStr.length(), stderrStr.length());
            if (!stderrStr.isBlank()) {
                logger.info("Exec stderr(first 300): {}",
                        stderrStr.substring(0, Math.min(300, stderrStr.length())));
            }

            return new ExecResult(success, stdoutStr, stderrStr);
        } catch (java.util.concurrent.TimeoutException e) {
            logger.warn("Exec timed out after {}s for {}/{}: {}", timeoutSeconds, podName, container, cmdStr);
            return new ExecResult(false, stdout.toString(StandardCharsets.UTF_8), "Timeout after " + timeoutSeconds + "s");
        } catch (Exception e) {
            String errorMsg = e.getMessage() != null ? e.getMessage() : "unknown error";
            String diagnosis = diagnoseExecFailure(errorMsg, namespace, podName, container);
            logger.error("Exec exception for {}/{} in namespace {}: {} - DIAGNOSIS: {}",
                    podName, container, namespace, errorMsg, diagnosis);
            return new ExecResult(false, "", diagnosis);
        }
    }

    /**
     * Diagnoses why a kubectl exec failed. Checks actual pod status first to give
     * accurate context rather than guessing (e.g., CrashLoopBackOff vs RBAC).
     */
    protected String diagnoseExecFailure(String errorMsg, String namespace, String podName, String container) {
        String lower = errorMsg.toLowerCase();
        StringBuilder diagnosis = new StringBuilder();
        diagnosis.append("EXEC FAILED: Could not exec into container '").append(container)
                .append("' in pod '").append(podName).append("' (namespace: ").append(namespace).append(")\n");
        diagnosis.append("Error: ").append(errorMsg).append("\n\n");

        // Explicit RBAC / Forbidden - only when K8s API explicitly says so
        if (lower.contains("forbidden") || lower.contains("403")
                || lower.contains("unauthorized") || lower.contains("401")
                || lower.contains("cannot exec") || lower.contains("not allowed")) {
            diagnosis.append("CAUSE: Permission denied by Kubernetes API.\n");
            diagnosis.append(EXEC_RBAC_HINT);
            return diagnosis.toString();
        }

        // Pod not found
        if (lower.contains("not found") || lower.contains("404")) {
            diagnosis.append("CAUSE: Pod or container not found.\n");
            diagnosis.append(EXEC_POD_NOT_FOUND_HINT);
            return diagnosis.toString();
        }

        // Connection refused / network
        if (lower.contains("connection refused") || lower.contains("connection reset")
                || lower.contains("unreachable")) {
            diagnosis.append("CAUSE: Network error - cannot reach Kubernetes API server or target pod.\n");
            diagnosis.append("REMEDIATION: Check API server connectivity, pod network, and network policies.\n");
            return diagnosis.toString();
        }

        // For generic errors ("An error has occurred", null, etc.) - check pod status
        // to determine the actual cause instead of guessing RBAC
        String podStatus = checkPodStatus(namespace, podName, container);
        if (podStatus != null) {
            diagnosis.append(podStatus);
        } else {
            diagnosis.append("CAUSE: Unable to determine - the Kubernetes API returned a generic error.\n\n");
            diagnosis.append("Possible causes (check in order):\n");
            diagnosis.append("  1. Pod is not running (CrashLoopBackOff, Pending, Terminating, etc.)\n");
            diagnosis.append("     -> Check: kubectl get pod ").append(podName).append(" -n ").append(namespace).append("\n");
            diagnosis.append("  2. Container '").append(container).append("' does not exist in this pod\n");
            diagnosis.append("     -> Check: kubectl get pod ").append(podName).append(" -n ").append(namespace)
                    .append(" -o jsonpath='{.spec.containers[*].name}'\n");
            diagnosis.append("  3. ServiceAccount lacks 'pods/exec' permission in namespace '")
                    .append(namespace).append("'\n");
            diagnosis.append("     -> Check: kubectl auth can-i create pods/exec -n ").append(namespace)
                    .append(" --as system:serviceaccount:<agent-ns>:<sa-name>\n");
            diagnosis.append("  4. Pod security policy or admission controller blocking exec\n");
        }

        return diagnosis.toString();
    }

    /**
     * Checks the actual pod status to provide accurate exec failure diagnosis.
     * Returns a diagnosis string, or null if pod status couldn't be retrieved.
     */
    private String checkPodStatus(String namespace, String podName, String container) {
        try {
            if (client == null) return null;

            var pod = client.pods().inNamespace(namespace).withName(podName).get();
            if (pod == null) {
                return "CAUSE: Pod '" + podName + "' does not exist in namespace '" + namespace + "'.\n"
                        + "REMEDIATION: Verify pod name and namespace. The pod may have been terminated or restarted "
                        + "with a different name.\n"
                        + "  -> kubectl get pods -n " + namespace + " | grep <partial-name>\n";
            }

            // Check pod phase
            String phase = pod.getStatus() != null ? pod.getStatus().getPhase() : "Unknown";

            // Check container statuses
            if (pod.getStatus() != null && pod.getStatus().getContainerStatuses() != null) {
                for (var cs : pod.getStatus().getContainerStatuses()) {
                    if (container != null && !container.equals(cs.getName())) continue;

                    // CrashLoopBackOff or other waiting states
                    if (cs.getState() != null && cs.getState().getWaiting() != null) {
                        String reason = cs.getState().getWaiting().getReason();
                        String message = cs.getState().getWaiting().getMessage();
                        int restarts = cs.getRestartCount() != null ? cs.getRestartCount() : 0;

                        StringBuilder sb = new StringBuilder();
                        sb.append("CAUSE: Container '").append(cs.getName())
                                .append("' is not running - state: ").append(reason).append("\n");
                        if (message != null) sb.append("  Message: ").append(message).append("\n");
                        sb.append("  Restarts: ").append(restarts).append("\n");
                        sb.append("  Pod phase: ").append(phase).append("\n\n");

                        if ("CrashLoopBackOff".equals(reason)) {
                            sb.append("The container keeps crashing and restarting. Cannot exec into a crashed container.\n");
                            sb.append("REMEDIATION:\n");
                            sb.append("  1. Check previous logs: kubectl logs ").append(podName)
                                    .append(" -n ").append(namespace).append(" -c ").append(cs.getName())
                                    .append(" --previous\n");
                            sb.append("  2. Check events: kubectl describe pod ").append(podName)
                                    .append(" -n ").append(namespace).append("\n");
                            sb.append("  3. Fix the crash cause, then retry JVM diagnostics after the pod stabilizes.\n");
                        } else if ("ImagePullBackOff".equals(reason) || "ErrImagePull".equals(reason)) {
                            sb.append("The container image cannot be pulled.\n");
                            sb.append("REMEDIATION: Check image name, tag, and registry credentials.\n");
                        } else {
                            sb.append("The container is in a '").append(reason).append("' state and cannot accept exec.\n");
                            sb.append("REMEDIATION: Wait for the container to reach Running state, then retry.\n");
                        }
                        return sb.toString();
                    }

                    // Terminated
                    if (cs.getState() != null && cs.getState().getTerminated() != null) {
                        String reason = cs.getState().getTerminated().getReason();
                        Integer exitCode = cs.getState().getTerminated().getExitCode();
                        return "CAUSE: Container '" + cs.getName() + "' is terminated (reason: " + reason
                                + ", exitCode: " + exitCode + ").\n"
                                + "Cannot exec into a terminated container.\n"
                                + "REMEDIATION: Check logs with: kubectl logs " + podName + " -n " + namespace
                                + " -c " + cs.getName() + " --previous\n";
                    }
                }
            }

            // Check if container name is valid
            if (container != null && pod.getSpec() != null && pod.getSpec().getContainers() != null) {
                boolean found = pod.getSpec().getContainers().stream()
                        .anyMatch(c -> container.equals(c.getName()));
                if (!found) {
                    var validNames = pod.getSpec().getContainers().stream()
                            .map(c -> c.getName()).toList();
                    return "CAUSE: Container '" + container + "' does not exist in pod '" + podName + "'.\n"
                            + "Available containers: " + validNames + "\n"
                            + "REMEDIATION: Specify a valid container name from the list above.\n";
                }
            }

            // Pod is Running but exec still failed - could be RBAC or PSP
            if ("Running".equals(phase)) {
                return "CAUSE: Pod is Running but exec failed. This is likely a permissions issue.\n"
                        + "The #ExampleFrame# ServiceAccount may not have 'pods/exec' permission in namespace '"
                        + namespace + "'.\n" + EXEC_RBAC_HINT;
            }

            return "CAUSE: Pod phase is '" + phase + "'. Exec requires the pod to be Running.\n"
                    + "REMEDIATION: Wait for pod to reach Running state.\n"
                    + "  -> kubectl get pod " + podName + " -n " + namespace + " -w\n";

        } catch (Exception e) {
            logger.debug("Could not check pod status for diagnosis: {}", e.getMessage());
            return null; // Fall back to generic message
        }
    }

    protected String formatError(String msg, String pod, String ns) {
        return String.format("Error: %s\n\nPod: %s\nNamespace: %s", msg, pod, ns);
    }

    /**
     * Combines stdout and stderr from an ExecResult for pattern matching.
     */
    protected static String combineStdoutStderr(ExecResult result) {
        String out = result.stdout == null ? "" : result.stdout;
        String err = result.stderr == null ? "" : result.stderr;
        return out + "\n" + err;
    }

    /**
     * Fallback thread dump capture using kill -3 (SIGQUIT).
     * Does NOT require JVM attach - the JVM handles SIGQUIT internally and prints
     * the thread dump to its stdout, which goes to Kubernetes container logs.
     *
     * @return formatted thread dump string, or null if fallback failed
     */
    protected String captureThreadDumpViaSigquit(String namespace, String podName, String container, int javaPid) {
        try {
            logger.info("[SIGQUIT] Sending kill -3 to PID {} in container '{}' of pod {}/{}",
                    javaPid, container, namespace, podName);

            // Send SIGQUIT
            String[] killCmd = {"/bin/sh", "-c", "kill -3 " + javaPid};
            ExecResult killResult = executeCommand(namespace, podName, container, killCmd, 100);
            logger.info("[SIGQUIT] kill -3 result: success={}, stderr='{}'",
                    killResult.success, killResult.stderr != null ? killResult.stderr.trim() : "");

            if (!killResult.success && killResult.stderr != null
                    && killResult.stderr.contains("Operation not permitted")) {
                logger.warn("[SIGQUIT] kill -3 not permitted - signal blocked");
                return null;
            }

            // Poll container logs with retries - JVM may take a moment to write the dump,
            // and chatty apps can push it out of a small tail window quickly.
            // Strategy: short initial wait, then retry with increasing tail windows and delays.
            logger.info("[SIGQUIT] Polling container logs for thread dump...");
            String threadDump = null;
            int[][] attempts = {
                    // {sleepMs, tailLines}
                    {1500, 2000},   // fast first check
                    {2000, 5000},   // JVM might still be writing
                    {3000, 10000},  // chatty app pushed it further back
            };
            for (int[] attempt : attempts) {
                int sleepMs = attempt[0];
                int tailLines = attempt[1];

                Thread.sleep(sleepMs);

                String logs = client.pods()
                        .inNamespace(namespace)
                        .withName(podName)
                        .inContainer(container)
                        .tailingLines(tailLines)
                        .getLog();

                if (logs == null || logs.isBlank()) {
                    logger.warn("[SIGQUIT] No logs returned from container (tailLines={})", tailLines);
                    continue;
                }

                threadDump = extractThreadDumpFromLogs(logs);
                if (threadDump != null) {
                    logger.info("[SIGQUIT] Found thread dump in container logs (tailLines={}, logs.length={})",
                            tailLines, logs.length());
                    break;
                }
                logger.info("[SIGQUIT] Thread dump not found in last {} lines (logs.length={}), retrying with more...",
                        tailLines, logs.length());
            }

            if (threadDump == null) {
                logger.warn("[SIGQUIT] Could not find thread dump in container logs after multiple attempts");
                return null;
            }

            logger.info("[SIGQUIT] Successfully captured thread dump via SIGQUIT ({} chars)", threadDump.length());

            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            StringBuilder sb = new StringBuilder();
            sb.append("=== Thread Dump (kill -3 SIGQUIT - no attach required) ===\n");
            sb.append("Pod: ").append(podName).append("\n");
            sb.append("Namespace: ").append(namespace).append("\n");
            sb.append("Container: ").append(container).append("\n");
            sb.append("Java PID: ").append(javaPid).append("\n");
            sb.append("Timestamp: ").append(timestamp).append("\n");
            sb.append("NOTE: Captured from container logs after SIGQUIT signal.\n");
            sb.append("      Lock info (-l flag) not available via this method.\n");
            sb.append("=".repeat(60)).append("\n\n");
            sb.append(threadDump);
            sb.append("\n").append("=".repeat(60)).append("\n");
            sb.append(analyzeThreadDump(threadDump));

            return sb.toString();

        } catch (Exception e) {
            logger.warn("[SIGQUIT] Fallback failed: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Extracts thread dump section from container log output.
     * Thread dumps from SIGQUIT typically start with a timestamp line or
     * "Full thread dump" and end with "JNI global refs" or "Heap".
     */
    private String extractThreadDumpFromLogs(String logs) {
        // Look for "Full thread dump" marker
        int start = logs.lastIndexOf("Full thread dump");
        if (start < 0) {
            // Some JVMs use different formats
            start = logs.lastIndexOf("java.lang.Thread.State");
            if (start >= 0) {
                // Back up to find the thread name line (starts with ")
                int lineStart = logs.lastIndexOf("\n\"", start);
                if (lineStart >= 0) {
                    start = lineStart + 1;
                }
            }
        }

        if (start < 0) {
            return null;
        }

        // Find end markers
        String section = logs.substring(start);
        int end = section.length();

        // Common end markers for thread dumps
        for (String marker : new String[]{"JNI global refs:", "Heap\n", "\nEnd of Thread Dump"}) {
            int idx = section.indexOf(marker);
            if (idx > 0) {
                // Include the marker line
                int lineEnd = section.indexOf('\n', idx + marker.length());
                end = Math.min(end, lineEnd > 0 ? lineEnd : idx + marker.length());
                break;
            }
        }

        String extracted = section.substring(0, end).trim();
        return extracted.contains("java.lang.Thread.State") ? extracted : null;
    }

    /**
     * Fallback memory info using /proc when JVM attach is blocked.
     * Provides process-level memory stats (RSS, VmSize, etc.) and cgroup limits.
     * Less detailed than jcmd/jmap histogram but requires no attach.
     */
    protected String captureProcMemoryInfo(String namespace, String podName, String container, int javaPid) {
        try {
            logger.info("[procMemory] Capturing /proc-based memory info for PID {} in {}/{}", javaPid, namespace, podName);

            StringBuilder sb = new StringBuilder();
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            sb.append("=== Memory Info (/proc - attach unavailable) ===\n");
            sb.append("Pod: ").append(podName).append("\n");
            sb.append("Namespace: ").append(namespace).append("\n");
            sb.append("Container: ").append(container).append("\n");
            sb.append("Java PID: ").append(javaPid).append("\n");
            sb.append("Timestamp: ").append(timestamp).append("\n");
            sb.append("NOTE: JVM attach is blocked. Showing OS-level memory stats.\n");
            sb.append("      For class-level histogram, ").append(ATTACH_REMEDIATION_HINT);
            sb.append("=".repeat(60)).append("\n\n");

            // /proc/PID/status - VmRSS, VmSize, Threads, etc.
            String[] statusCmd = {"/bin/sh", "-c",
                    "cat /proc/" + javaPid + "/status 2>/dev/null | grep -E '^(VmSize|VmRSS|VmHWM|VmData|VmStk|VmSwap|Threads|RssAnon|RssFile|RssShmem):'"};
            ExecResult statusResult = executeCommand(namespace, podName, container, statusCmd, 100);
            if (!statusResult.stdout.isBlank()) {
                sb.append("--- Process Memory (/proc/").append(javaPid).append("/status) ---\n");
                sb.append(statusResult.stdout).append("\n");
            }

            // /proc/PID/smaps_rollup - aggregated memory map stats
            String[] smapsCmd = {"/bin/sh", "-c",
                    "cat /proc/" + javaPid + "/smaps_rollup 2>/dev/null"};
            ExecResult smapsResult = executeCommand(namespace, podName, container, smapsCmd, 100);
            if (!smapsResult.stdout.isBlank()) {
                sb.append("--- Memory Map Summary (/proc/").append(javaPid).append("/smaps_rollup) ---\n");
                sb.append(smapsResult.stdout).append("\n");
            }

            // Cgroup memory limits (cgroup v2 first, then v1)
            String[] cgroupCmd = {"/bin/sh", "-c",
                    "cat /sys/fs/cgroup/memory.max 2>/dev/null || cat /sys/fs/cgroup/memory/memory.limit_in_bytes 2>/dev/null"};
            ExecResult cgroupLimit = executeCommand(namespace, podName, container, cgroupCmd, 100);
            String[] cgroupUsageCmd = {"/bin/sh", "-c",
                    "cat /sys/fs/cgroup/memory.current 2>/dev/null || cat /sys/fs/cgroup/memory/memory.usage_in_bytes 2>/dev/null"};
            ExecResult cgroupUsage = executeCommand(namespace, podName, container, cgroupUsageCmd, 100);
            if (!cgroupLimit.stdout.isBlank() || !cgroupUsage.stdout.isBlank()) {
                sb.append("--- Container Memory Limits (cgroup) ---\n");
                if (!cgroupLimit.stdout.isBlank()) {
                    sb.append("Limit: ").append(formatBytes(cgroupLimit.stdout.trim())).append("\n");
                }
                if (!cgroupUsage.stdout.isBlank()) {
                    sb.append("Usage: ").append(formatBytes(cgroupUsage.stdout.trim())).append("\n");
                }
                sb.append("\n");
            }

            // JVM flags from /proc/PID/cmdline (shows -Xmx, -Xms, GC settings)
            String[] cmdlineCmd = {"/bin/sh", "-c",
                    "cat /proc/" + javaPid + "/cmdline 2>/dev/null | tr '\\0' '\\n' | grep -E '^-X|-XX:|^-D' | head -20"};
            ExecResult cmdlineResult = executeCommand(namespace, podName, container, cmdlineCmd, 100);
            if (!cmdlineResult.stdout.isBlank()) {
                sb.append("--- JVM Flags (from cmdline) ---\n");
                sb.append(cmdlineResult.stdout).append("\n");
            }

            sb.append("=".repeat(60)).append("\n");

            // Only return if we got some useful data
            if (statusResult.stdout.isBlank() && smapsResult.stdout.isBlank()) {
                logger.warn("[procMemory] No /proc memory data available");
                return null;
            }

            return sb.toString();

        } catch (Exception e) {
            logger.warn("[procMemory] Failed: {}", e.getMessage());
            return null;
        }
    }

    private String formatBytes(String bytesStr) {
        try {
            if ("max".equals(bytesStr)) return "unlimited";
            long bytes = Long.parseLong(bytesStr);
            if (bytes >= 1024L * 1024 * 1024) {
                return String.format("%.1f GB (%s bytes)", bytes / (1024.0 * 1024 * 1024), bytesStr);
            } else if (bytes >= 1024L * 1024) {
                return String.format("%.1f MB (%s bytes)", bytes / (1024.0 * 1024), bytesStr);
            }
            return bytesStr + " bytes";
        } catch (NumberFormatException e) {
            return bytesStr;
        }
    }

    /**
     * Context resolved by {@link #resolveDiagnosticsContext} - captures which container
     * to run diagnostics in, the Java PID, and whether direct tools are available.
     */
    protected record DiagnosticsContext(
            Pod pod,
            String effectivePodName,
            String diagnosticsContainer,
            int javaPid,
            boolean usingSidecar,
            ContainerResolution containerResolution
    ) {}

    /**
     * Resolves the best container + PID for running JDK diagnostics.
     *
     * <p>Strategy:
     * <ol>
     *   <li>Try the <b>application container</b> first - check if the needed JDK tool
     *       exists there (via {@link #findJdkTool}). If yes, use it directly (no sidecar).</li>
     *   <li>If the tool is not available in the app container, find or deploy a sidecar
     *       and use that instead.</li>
     * </ol>
     *
     * @param requiredTool the JDK tool name needed (e.g. "jstack", "jstat", "jcmd", "jmap")
     * @return context, or null if resolution failed (caller should return the error string)
     */
    protected DiagnosticsContext resolveDiagnosticsContext(
            String podName, String namespace, String targetContainer,
            Integer requestedPid, String requiredTool) {

        String ns = resolveNamespace(namespace);

        PodLookupResult podLookup = lookupPodWithFallback(ns, podName);
        Pod pod = podLookup.pod();
        if (pod == null) {
            return null; // caller checks and calls formatError
        }
        String effectivePodName = podLookup.resolvedPodName();
        if (!podLookup.exactMatch()) {
            logger.info("Resolved pod name '{}' -> '{}' in namespace '{}'", podName, effectivePodName, ns);
        }

        ContainerResolution cr = resolveContainer(pod, targetContainer, namespace, effectivePodName);
        String appContainer = cr.selected();

        // --- Step 0: Multi-container pod without targetContainer specified ---
        if (appContainer == null && isMultiContainer(pod)) {
            logger.warn("[DiagCtx] Multi-container pod detected without targetContainer. "
                    + "User must specify which container to diagnose.");
            lastDiagnosticsError.set(formatMultiContainerError(effectivePodName, ns, pod));
            return null;
        }

        // --- Step 1: Try direct in app container ---
        if (appContainer != null) {
            logger.info("[DiagCtx] Checking if '{}' is available in app container '{}'", requiredTool, appContainer);
            String toolPath = findJdkTool(ns, effectivePodName, appContainer, requiredTool);
            if (toolPath != null) {
                logger.info("[DiagCtx] Found {} at '{}' in app container '{}' - using DIRECT mode (no sidecar)",
                        requiredTool, toolPath, appContainer);

                // Always auto-detect PID - ignore LLM-provided PID to prevent stale PID issues
                if (requestedPid != null && requestedPid > 0) {
                    logger.info("[DiagCtx] Ignoring LLM-provided PID {} - will auto-detect", requestedPid);
                }
                int javaPid = findJavaPidDirect(ns, effectivePodName, appContainer);
                if (javaPid > 0) {
                    return new DiagnosticsContext(pod, effectivePodName, appContainer, javaPid, false, cr);
                }
                logger.warn("[DiagCtx] Tool found but no Java PID in container '{}' - falling through to sidecar",
                        appContainer);
            } else {
                logger.info("[DiagCtx] '{}' not found in app container '{}' - will try sidecar",
                        requiredTool, appContainer);
            }
        }

        // --- Step 2: Use sidecar ---
        logger.info("[DiagCtx] Attempting sidecar path for '{}'", requiredTool);
        String sidecarName = findExistingDebugContainer(pod);
        if (sidecarName != null) {
            logger.info("[DiagCtx] Existing sidecar found: '{}'", sidecarName);
        } else {
            logger.info("[DiagCtx] No existing sidecar - deploying new sidecar (image: {})", debugImage);
            String deployResult = deployDiagnosticsSidecarInternal(effectivePodName, namespace, targetContainer);
            if (deployResult.contains("Error")) {
                logger.error("[DiagCtx] Sidecar deployment failed: {}", deployResult);
                return null;
            }
            pod = client.pods().inNamespace(ns).withName(effectivePodName).get();
            sidecarName = findExistingDebugContainer(pod);
        }

        if (sidecarName == null) {
            logger.error("[DiagCtx] Could not find/deploy sidecar for pod {}/{}", ns, effectivePodName);
            return null;
        }

        // Always auto-detect PID from sidecar
        int javaPid = findJavaPidFromSidecar(ns, effectivePodName, sidecarName);

        return new DiagnosticsContext(pod, effectivePodName, sidecarName, javaPid, true, cr);
    }

    /**
     * Fallback: when direct mode returned by {@link #resolveDiagnosticsContext} fails
     * (e.g. attach blocked), retry with a sidecar. Skips if the original context was
     * already using a sidecar.
     *
     * @return sidecar context, or null if sidecar deploy failed
     */
    protected DiagnosticsContext resolveSidecarFallback(
            DiagnosticsContext directCtx, String namespace, String targetContainer, Integer requestedPid) {

        if (directCtx.usingSidecar()) {
            logger.info("[DiagCtx] Already using sidecar - no fallback available");
            return null;
        }

        String ns = resolveNamespace(namespace);
        Pod pod = directCtx.pod();
        String effectivePodName = directCtx.effectivePodName();

        logger.info("[DiagCtx] Direct mode failed - falling back to sidecar for pod {}/{}", ns, effectivePodName);

        String sidecarName = findExistingDebugContainer(pod);
        if (sidecarName == null) {
            logger.info("[DiagCtx] No existing sidecar - deploying new sidecar (image: {})", debugImage);
            String deployResult = deployDiagnosticsSidecarInternal(effectivePodName, namespace, targetContainer);
            if (deployResult.contains("Error")) {
                logger.error("[DiagCtx] Sidecar deployment failed: {}", deployResult);
                return null;
            }
            pod = client.pods().inNamespace(ns).withName(effectivePodName).get();
            sidecarName = findExistingDebugContainer(pod);
        } else {
            logger.info("[DiagCtx] Existing sidecar found: '{}'", sidecarName);
        }

        if (sidecarName == null) {
            return null;
        }

        // Always auto-detect PID from sidecar
        int javaPid = findJavaPidFromSidecar(ns, effectivePodName, sidecarName);

        return new DiagnosticsContext(pod, effectivePodName, sidecarName, javaPid, true, directCtx.containerResolution());
    }

    /**
     * Returns the resolved path for a JDK tool in the given diagnostics context.
     * If the context is using the app container (direct mode), re-resolves to confirm.
     * If using sidecar, the sidecar has full JDK, so the bare tool name works.
     */
    protected String resolveToolPath(DiagnosticsContext ctx, String namespace, String toolName) {
        if (!ctx.usingSidecar()) {
            // App container - must resolve actual path
            String path = findJdkTool(resolveNamespace(namespace), ctx.effectivePodName(), ctx.diagnosticsContainer(), toolName);
            if (path != null) {
                return path;
            }
            logger.warn("[DiagCtx] Tool '{}' no longer found in container '{}' - using bare name as fallback",
                    toolName, ctx.diagnosticsContainer());
        }
        // Sidecar has full JDK in PATH
        return toolName;
    }


    protected record ExecResult(boolean success, String stdout, String stderr) {}
}
