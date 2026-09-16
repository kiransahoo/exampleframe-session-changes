package com.#exampleframe#.kubernetes.tools.kubernetes;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class KubernetesWorkloadWriteTools extends KubernetesToolSupport {

    private static final Logger logger = LoggerFactory.getLogger(KubernetesWorkloadWriteTools.class);
    private final boolean destructiveOperationsUnlocked;
    private final boolean allowProtectedNamespaceWrites;
    private final Set<String> protectedNamespaces;

    public KubernetesWorkloadWriteTools(
            @Nullable KubernetesClient client,
            @Value("${#exampleframe#.kubernetes.client.default-namespace:default}") String defaultNamespace,
            @Value("${#exampleframe#.kubernetes.agent.enable-write-operations:false}") boolean writeOperationsEnabled,
            @Value("${#exampleframe#.kubernetes.agent.enable-human-approval:true}") boolean humanApprovalEnabled,
            @Value("${#exampleframe#.kubernetes.agent.destructive-operations-unlocked:false}") boolean destructiveOperationsUnlocked,
            @Value("${#exampleframe#.kubernetes.agent.allow-protected-namespace-writes:false}") boolean allowProtectedNamespaceWrites,
            @Value("${#exampleframe#.kubernetes.agent.protected-namespaces:kube-system,kube-public,kube-node-lease}") String protectedNamespacesCsv) {
        super(client, defaultNamespace, writeOperationsEnabled, humanApprovalEnabled, null);
        this.destructiveOperationsUnlocked = destructiveOperationsUnlocked;
        this.allowProtectedNamespaceWrites = allowProtectedNamespaceWrites;
        this.protectedNamespaces = Arrays.stream(protectedNamespacesCsv.split(","))
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .map(s -> s.toLowerCase(Locale.ROOT))
                .collect(Collectors.toUnmodifiableSet());
    }

    @Tool(description = "DESTRUCTIVE: Deletes a pod. The pod will be terminated immediately. If managed by a controller (Deployment, ReplicaSet), a new pod will be created.")
    public String deletePod(
            @ToolParam(description = "Name of the pod to delete") String podName,
            @ToolParam(description = "Kubernetes namespace") String namespace) {

        String ns = namespace != null ? namespace : defaultNamespace;
        logger.warn("DELETE POD blocked by admin policy: {}/{}", ns, podName);
 return "Pod deletion is currently disabled by the #ExampleFrame# administrators. "
                + "To delete it, run `kubectl delete pod " + podName + " -n " + ns + "` directly, "
                + "or go through your team's change-approval process.";

        // --- Original implementation, disabled by admin policy. To re-enable, remove the
        //     early return above and uncomment this block. ---
        // if (!writeOperationsEnabled) {
        //     return "Write operations are disabled. Enable with #exampleframe#.kubernetes.agent.enable-write-operations=true";
        // }
        // String safetyMessage = validateWriteSafety(ns, "delete pod", podName);
        // if (safetyMessage != null) {
        //     return safetyMessage;
        // }
        // String err = requireClient();
        // if (err != null) return err;
        // try {
        //     io.fabric8.kubernetes.api.model.Pod pod = client.pods().inNamespace(ns).withName(podName).get();
        //     if (pod == null) {
        //         return "Pod not found: " + podName + " in namespace " + ns;
        //     }
        //     client.pods().inNamespace(ns).withName(podName).delete();
        //     logger.warn("Pod deleted: {}/{}", ns, podName);
        //     return String.format("Successfully deleted pod '%s' in namespace '%s'. " +
        //             "If managed by a controller, a replacement pod will be created.", podName, ns);
        // } catch (Exception e) {
        //     logger.error("Failed to delete pod: {}", e.getMessage(), e);
        //     return "Error deleting pod: " + e.getMessage();
        // }
    }

    @Tool(description = "Scales a deployment to the specified number of replicas. This will increase or decrease the number of running pods.")
    public String scaleDeployment(
            @ToolParam(description = "Name of the deployment") String deploymentName,
            @ToolParam(description = "Kubernetes namespace") String namespace,
            @ToolParam(description = "Desired number of replicas (0-100)") Integer replicas) {

        String ns = namespace != null ? namespace : defaultNamespace;
        logger.warn("SCALE DEPLOYMENT requested: {}/{} to {} replicas", ns, deploymentName, replicas);

        if (!writeOperationsEnabled) {
            return "Write operations are disabled. Enable with #exampleframe#.kubernetes.agent.enable-write-operations=true";
        }
        String safetyMessage = validateWriteSafety(ns, "scale deployment", deploymentName);
        if (safetyMessage != null) {
            return safetyMessage;
        }

        if (replicas == null || replicas < 0 || replicas > 100) {
            return "Invalid replica count. Must be between 0 and 100.";
        }

        String err = requireClient();
        if (err != null) return err;

        try {
            Deployment deployment = client.apps().deployments()
                    .inNamespace(ns).withName(deploymentName).get();

            if (deployment == null) {
                return "Deployment not found: " + deploymentName + " in namespace " + ns;
            }

            int previousReplicas = deployment.getSpec().getReplicas() != null
                    ? deployment.getSpec().getReplicas() : 0;

            client.apps().deployments()
                    .inNamespace(ns)
                    .withName(deploymentName)
                    .scale(replicas);

            logger.warn("Deployment scaled: {}/{} from {} to {} replicas",
                    ns, deploymentName, previousReplicas, replicas);

            return String.format("Successfully scaled deployment '%s' in namespace '%s' from %d to %d replicas.",
                    deploymentName, ns, previousReplicas, replicas);

        } catch (Exception e) {
            logger.error("Failed to scale deployment: {}", e.getMessage(), e);
            return "Error scaling deployment: " + e.getMessage();
        }
    }

    @Tool(description = "Triggers a rolling restart of a deployment. All pods will be restarted one by one to avoid downtime.")
    public String restartDeployment(
            @ToolParam(description = "Name of the deployment") String deploymentName,
            @ToolParam(description = "Kubernetes namespace") String namespace) {

        String ns = namespace != null ? namespace : defaultNamespace;
        logger.warn("RESTART DEPLOYMENT requested: {}/{}", ns, deploymentName);

        if (!writeOperationsEnabled) {
            return "Write operations are disabled. Enable with #exampleframe#.kubernetes.agent.enable-write-operations=true";
        }
        String safetyMessage = validateWriteSafety(ns, "restart deployment", deploymentName);
        if (safetyMessage != null) {
            return safetyMessage;
        }

        String err = requireClient();
        if (err != null) return err;

        try {
            Deployment deployment = client.apps().deployments()
                    .inNamespace(ns).withName(deploymentName).get();

            if (deployment == null) {
                return "Deployment not found: " + deploymentName + " in namespace " + ns;
            }

            // Trigger a rolling restart the way kubectl does: patch ONLY the restartedAt
            // annotation, as a raw strategic-merge patch string. The previous edit(d -> ...)
            // mutated the fetched object in place and round-tripped the FULL model through
            // the client's serializer, which fails against some server versions with a JSON
            // mapping error (and NPEs when template.metadata is absent). A minimal patch has
            // nothing to round-trip and nothing to conflict with.
            String restartTime = Instant.now().toString();
            String restartPatch = String.format(
                    "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":"
                    + "{\"kubectl.kubernetes.io/restartedAt\":\"%s\"}}}}}", restartTime);
            client.apps().deployments()
                    .inNamespace(ns)
                    .withName(deploymentName)
                    .patch(io.fabric8.kubernetes.client.dsl.base.PatchContext.of(
                            io.fabric8.kubernetes.client.dsl.base.PatchType.STRATEGIC_MERGE), restartPatch);

            logger.warn("Deployment restart triggered: {}/{}", ns, deploymentName);

            return String.format("Successfully triggered rolling restart for deployment '%s' in namespace '%s'. " +
                    "Pods will be restarted gradually.", deploymentName, ns);

        } catch (Exception e) {
            logger.error("Failed to restart deployment: {}", e.getMessage(), e);
            return "Error restarting deployment: " + e.getMessage();
        }
    }

    @Tool(description = "DESTRUCTIVE: Permanently deletes a deployment and all its pods. This action cannot be undone.")
    public String deleteDeployment(
            @ToolParam(description = "Name of the deployment to delete") String deploymentName,
            @ToolParam(description = "Kubernetes namespace") String namespace) {

        String ns = namespace != null ? namespace : defaultNamespace;
        logger.warn("DELETE DEPLOYMENT blocked by admin policy: {}/{}", ns, deploymentName);
 return "Deployment deletion is currently disabled by the #ExampleFrame# administrators. "
                + "To delete it, run `kubectl delete deployment " + deploymentName + " -n " + ns + "` directly, "
                + "or go through your team's change-approval process.";

        // --- Original implementation, disabled by admin policy. To re-enable, remove the
        //     early return above and uncomment this block. ---
        // if (!writeOperationsEnabled) {
        //     return "Write operations are disabled. Enable with #exampleframe#.kubernetes.agent.enable-write-operations=true";
        // }
        // String safetyMessage = validateWriteSafety(ns, "delete deployment", deploymentName);
        // if (safetyMessage != null) {
        //     return safetyMessage;
        // }
        // String err = requireClient();
        // if (err != null) return err;
        // try {
        //     Deployment deployment = client.apps().deployments()
        //             .inNamespace(ns).withName(deploymentName).get();
        //     if (deployment == null) {
        //         return "Deployment not found: " + deploymentName + " in namespace " + ns;
        //     }
        //     int replicas = deployment.getSpec().getReplicas() != null ? deployment.getSpec().getReplicas() : 0;
        //     client.apps().deployments().inNamespace(ns).withName(deploymentName).delete();
        //     logger.warn("Deployment deleted: {}/{} ({} replicas)", ns, deploymentName, replicas);
        //     return String.format("Successfully deleted deployment '%s' in namespace '%s'. " +
        //             "%d pod(s) will be terminated.", deploymentName, ns, replicas);
        // } catch (Exception e) {
        //     logger.error("Failed to delete deployment: {}", e.getMessage(), e);
        //     return "Error deleting deployment: " + e.getMessage();
        // }
    }

    @Nullable
    private String validateWriteSafety(String namespace, String action, String resourceName) {
        if (!destructiveOperationsUnlocked) {
            return "Destructive Kubernetes operations are locked. " +
                    "Set #exampleframe#.kubernetes.agent.destructive-operations-unlocked=true " +
                    "(or K8S_DESTRUCTIVE_UNLOCKED=true) to enable write tools.";
        }

        String ns = namespace == null ? "" : namespace.trim().toLowerCase(Locale.ROOT);
        if (!allowProtectedNamespaceWrites && protectedNamespaces.contains(ns)) {
            return String.format(
                    "Blocked %s for %s/%s: namespace '%s' is protected. " +
                            "Set #exampleframe#.kubernetes.agent.allow-protected-namespace-writes=true to override.",
                    action,
                    namespace,
                    resourceName,
                    namespace
            );
        }
        return null;
    }

}
