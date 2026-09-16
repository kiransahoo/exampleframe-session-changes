package com.#exampleframe#.orchestrator.service;

import com.#exampleframe#.orchestrator.config.AgentConfigResolver;
import com.#exampleframe#.orchestrator.config.AppContextResolver;
import com.#exampleframe#.orchestrator.config.PlaybookProperties;
import com.#exampleframe#.orchestrator.config.PlaybookProperties.ParamDefinition;
import com.#exampleframe#.orchestrator.config.PlaybookProperties.ParamRef;
import com.#exampleframe#.orchestrator.config.PlaybookProperties.PlaybookDefinition;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Shared deterministic resolve step for the playbook engine's three doors:
 * {@link PlaybookGateway} (interactive UI pre-route), {@code PlaybookTools}
 * (LLM-invoked tool), and the A2A ingress (delegated path).
 * <p>
 * Resolving = choose a playbook (explicit id/name reference first, scored
 * matching second), extract parameters from the task text, overlay any
 * caller-supplied structured parameters (normalized, restricted to the
 * playbook's declared params), fill infrastructure defaults from app-context,
 * and report which required parameters are still missing. What each door DOES
 * with a missing parameter differs (prompt the user / return prompt text /
 * fall back to ReAct) and stays in the door.
 *
 * @author kiransahoo
 */
@Service
public class PlaybookResolver {

    private static final Logger logger = LoggerFactory.getLogger(PlaybookResolver.class);

    private final PlaybookRouter router;
    private final PlaybookProperties playbookProperties;
    private final @Nullable AppContextResolver appContextResolver;
    private final @Nullable AgentConfigResolver agentConfigResolver;

    public PlaybookResolver(
            PlaybookRouter router,
            PlaybookProperties playbookProperties,
            @Nullable AppContextResolver appContextResolver,
            @Nullable AgentConfigResolver agentConfigResolver) {
        this.router = router;
        this.playbookProperties = playbookProperties;
        this.appContextResolver = appContextResolver;
        this.agentConfigResolver = agentConfigResolver;
    }

    /** A parameter the playbook still needs, with its user-facing prompt. */
    public record MissingParam(String key, String prompt, boolean optional) {}

    /** A resolved playbook: definition, working params, and what is still missing. */
    public record Resolution(
            String playbookId,
            PlaybookDefinition definition,
            boolean softMatch,
            Map<String, String> params,
            List<MissingParam> missingParams) {

        /** Keys of required params with no value and no default - the playbook cannot run without them. */
        public List<String> missingRequiredKeys() {
            return missingParams.stream().filter(m -> !m.optional()).map(MissingParam::key).toList();
        }
    }

    /**
     * Full non-interactive resolve for the delegated and tool paths.
     * Explicit playbook references win over scored matching; caller-supplied
     * params win over text extraction; app-context fills what neither provided.
     *
     * @param taskText       the bare task text (not the labeled delegation envelope)
     * @param providedParams structured params from the caller (may be null/empty);
     *                       restricted to the playbook's declared params and normalized
     */
    public Optional<Resolution> resolve(String taskText, @Nullable Map<String, String> providedParams) {
        String playbookId;
        PlaybookDefinition definition;
        boolean softMatch = false;

        Map.Entry<String, PlaybookDefinition> explicit = resolveByExplicitId(taskText).orElse(null);
        if (explicit != null) {
            playbookId = explicit.getKey();
            definition = explicit.getValue();
            logger.info("Playbook resolved by explicit reference: {}", playbookId);
        } else {
            PlaybookRouter.PlaybookMatch match = router.match(taskText).orElse(null);
            if (match == null) {
                return Optional.empty();
            }
            playbookId = match.playbookId();
            definition = match.definition();
            softMatch = match.softMatch();
        }

        Map<String, String> params = router.extractParameters(
                taskText, definition.params(), playbookProperties.paramDefinitions());
        overlayProvidedParams(params, providedParams, definition);
        applyAppContext(params, taskText);

        List<MissingParam> missing = findMissingParams(
                definition.params(), params, playbookProperties.paramDefinitions());
        return Optional.of(new Resolution(playbookId, definition, softMatch, params, missing));
    }

    /**
     * Non-interactive resolve for a caller that names the playbook by ID - the meta's
     * deterministic forward does, via {@code taskParams.playbookId}. The ID, not text
     * matching, selects the playbook, so a re-phrased task can neither pick a different
     * playbook nor fail to find the one the caller already resolved. Params flow exactly
     * like {@link #resolve}. Empty when the ID is unknown here or its steps need agents
     * this deployment lacks - the caller falls back to ReAct.
     */
    public Optional<Resolution> resolveById(@Nullable String playbookId, String taskText,
                                            @Nullable Map<String, String> providedParams) {
        if (playbookId == null || playbookId.isBlank() || playbookProperties.playbooks() == null) {
            return Optional.empty();
        }
        String id = playbookId.trim();
        PlaybookDefinition definition = playbookProperties.playbooks().get(id);
        if (definition == null || !agentsAvailable(definition)) {
            return Optional.empty();
        }
        logger.info("Playbook resolved by caller-supplied id: {}", id);

        Map<String, String> params = router.extractParameters(
                taskText, definition.params(), playbookProperties.paramDefinitions());
        overlayProvidedParams(params, providedParams, definition);
        applyAppContext(params, taskText);

        List<MissingParam> missing = findMissingParams(
                definition.params(), params, playbookProperties.paramDefinitions());
        return Optional.of(new Resolution(id, definition, false, params, missing));
    }

    /**
     * Tries to resolve a playbook by explicit ID/name reference in the query.
     * Handles patterns like "run service-down playbook", "execute oracle-slow",
     * "service-down playbook for checkout-service".
     * <p>
     * Uses word-boundary matching to avoid substring false positives
     * (e.g., "service-down" should not match inside "my-service-downtime-analysis").
     * The hyphenated id matches anywhere; the space-normalized forms ("service down",
     * or the playbook's display name) count ONLY when the query actually says
     * "playbook" - otherwise every ordinary sentence containing "... service slow ..."
     * would silently bypass scored matching on the gateway and delegated doors.
     */
    public Optional<Map.Entry<String, PlaybookDefinition>> resolveByExplicitId(@Nullable String query) {
        return resolveByExplicitId(query, true);
    }

    /**
     * Same explicit-reference matching, but without the agents-available filter: names the
     * playbook the user asked for even where this deployment cannot run it itself. A meta uses
     * it to recognise "run the X playbook" and hand the request to the owning domain cell
     * (which does have the agents) instead of letting the LLM paraphrase the request away.
     */
    public Optional<Map.Entry<String, PlaybookDefinition>> resolveByExplicitIdAnyAgents(@Nullable String query) {
        return resolveByExplicitId(query, false);
    }

    private Optional<Map.Entry<String, PlaybookDefinition>> resolveByExplicitId(@Nullable String query,
                                                                                 boolean requireAgents) {
        if (query == null || playbookProperties.playbooks() == null) {
            return Optional.empty();
        }
        String lowerQuery = query.toLowerCase();
        String normalizedQuery = lowerQuery.replace('-', ' ');
        boolean mentionsPlaybook = lowerQuery.contains("playbook");

        for (Map.Entry<String, PlaybookDefinition> entry : playbookProperties.playbooks().entrySet()) {
            // A playbook whose steps need agents this deployment doesn't have (a meta only
            // has cells) is not resolvable here even by name - it would delegate into thin
            // air. Same rule the router applies to scored matches; without it an explicit
            // "run the X playbook" at the meta would run a playbook of guaranteed failures
            // instead of falling through to ReAct and delegating to the owning cell.
            if (requireAgents && !agentsAvailable(entry.getValue())) {
                continue;
            }
            // Exact hyphenated id ("service-slow") is unambiguous wherever it appears
            if (matchesWithWordBoundary(lowerQuery, entry.getKey().toLowerCase())) {
                return Optional.of(entry);
            }
            if (!mentionsPlaybook) {
                continue;
            }
            String normalizedId = entry.getKey().toLowerCase().replace('-', ' ');
            if (matchesWithWordBoundary(normalizedQuery, normalizedId)) {
                return Optional.of(entry);
            }
            if (entry.getValue().name() != null) {
                String normalizedName = entry.getValue().name().toLowerCase().replace('-', ' ');
                if (matchesWithWordBoundary(normalizedQuery, normalizedName)) {
                    return Optional.of(entry);
                }
            }
        }
        return Optional.empty();
    }

    /**
     * True when every agent the playbook's unconditional steps delegate to exists in this
     * deployment - mirrors the router's scored-match filter. Absent/empty configs mean
     * "cannot judge": no filtering.
     */
    public boolean agentsAvailable(@Nullable PlaybookDefinition definition) {
        return PlaybookAgentAvailability.available(definition, agentConfigResolver,
                definition != null ? definition.name() : "<none>");
    }

    /**
     * Checks if {@code needle} appears in {@code haystack} at word boundaries.
     * Both inputs should already be lowercased and hyphen-normalized.
     */
    private boolean matchesWithWordBoundary(String haystack, String needle) {
        // A playbook id inside a LONGER token is not a reference to the playbook: the
        // service named "oracle-slow-app" must not resolve the "oracle-slow" playbook
        // (with \W boundaries it did - a hyphen is \W). So the id may not be preceded by
        // a word char, hyphen, or dot, and may not be followed by a word char, hyphen,
        // or a dot that continues a token ("oracle-slow." at sentence end still counts,
        // "oracle-slow.app" does not).
        Pattern pattern = Pattern.compile(
                "(?<![\\w.-])" + Pattern.quote(needle) + "(?![\\w-])(?!\\.\\w)",
                Pattern.CASE_INSENSITIVE);
        return pattern.matcher(haystack).find();
    }

    /**
     * Overlays caller-supplied structured params onto the extracted ones.
     * Only params the playbook declares are accepted; values run through the
     * same normalizers as text extraction. Caller values win over extraction
     * (they are the user's explicit inputs carried on the wire).
     */
    void overlayProvidedParams(
            Map<String, String> params,
            @Nullable Map<String, String> provided,
            PlaybookDefinition definition) {
        if (provided == null || provided.isEmpty() || definition.params() == null) {
            return;
        }
        Set<String> declared = new LinkedHashSet<>();
        for (ParamRef ref : definition.params()) {
            declared.add(ref.ref());
        }
        for (Map.Entry<String, String> e : provided.entrySet()) {
            if (!declared.contains(e.getKey())) {
                continue;
            }
            String value = normalize(e.getKey(), e.getValue());
            if (value != null && !value.isBlank()) {
                params.put(e.getKey(), value);
                logger.debug("Provided param '{}' = '{}' overlaid onto playbook params", e.getKey(), value);
            }
        }
    }

    /** Normalizes one param value with the same rules text extraction uses. */
    static @Nullable String normalize(String key, @Nullable String value) {
        if (value == null) {
            return null;
        }
        return switch (key) {
            case "timeRange" -> PlaybookRouter.normalizeTimeRange(value);
            case "service" -> PlaybookRouter.normalizeService(value);
            case "namespace" -> PlaybookRouter.normalizeNamespace(value);
            case "schema" -> PlaybookRouter.normalizeSchema(value);
            case "tables" -> PlaybookRouter.normalizeTables(value);
            case "workload" -> PlaybookRouter.normalizeWorkload(value);
            default -> value;
        };
    }

    /**
     * Playbook params {@link #applyAppContext} can fill from a service's infrastructure mapping.
     * A meta prompts the user only for keys NOT in this set (e.g. timeRange): infrastructure
     * scope is the owning cell's to resolve from ITS mapping - the meta's mapping may hold only
     * service-to-domain ownership. Keep in sync with applyAppContext below.
     */
    public static final java.util.Set<String> APP_CONTEXT_PARAM_KEYS =
            java.util.Set.of("namespace", "workload", "schema", "tables", "packageName");

    /**
     * App-context: auto-fill playbook params from per-service infrastructure mappings.
     * Runs BEFORE the missing-param check so callers aren't asked for values we already know.
     * Priority: caller/LLM-extracted > app-context (never overwrites an existing value).
     */
    public void applyAppContext(Map<String, String> params, String originalQuery) {
        if (appContextResolver == null) {
            return;
        }
        // Resolve service from either the extracted param or the original query text
        String serviceName = params.get("service");
        AppContextResolver.TextAnalysis analysis = appContextResolver.analyzeText(
                serviceName != null ? serviceName : originalQuery);

        if (analysis.knownServiceName() == null) {
            return;
        }
        String resolved = analysis.knownServiceName();
        logger.info("App-context: resolved known service '{}' - injecting infrastructure defaults", resolved);

        // Map agent-specific context to flat playbook params (don't overwrite existing values)
        Map<String, String> k8s = appContextResolver.getAgentContext(resolved, "kubernetes");
        Map<String, String> oracle = appContextResolver.getAgentContext(resolved, "oracle");
        Map<String, String> azmon = appContextResolver.getAgentContext(resolved, "azure-monitor");

        // kubernetes.namespace -> namespace
        if (!params.containsKey("namespace") && k8s.containsKey("namespace")) {
            params.put("namespace", k8s.get("namespace"));
            logger.info("App-context: filled namespace='{}' from app-context", k8s.get("namespace"));
        }
        // kubernetes.workload -> workload (available for step templates)
        if (!params.containsKey("workload") && k8s.containsKey("workload")) {
            params.put("workload", k8s.get("workload"));
            logger.info("App-context: filled workload='{}' from app-context", k8s.get("workload"));
        }
        // oracle.schema -> schema
        if (!params.containsKey("schema") && oracle.containsKey("schema")) {
            params.put("schema", oracle.get("schema"));
            logger.info("App-context: filled schema='{}' from app-context", oracle.get("schema"));
        }
        // oracle.tables -> tables (available for step templates)
        if (!params.containsKey("tables") && oracle.containsKey("tables")) {
            params.put("tables", oracle.get("tables"));
        }
        // azure-monitor.packageName -> packageName (available for step templates)
        if (!params.containsKey("packageName") && azmon.containsKey("packageName")) {
            params.put("packageName", azmon.get("packageName"));
        }
    }

    /**
     * Finds params that were not extracted AND have no usable default.
     * <p>
     * Required params (required=true) are always included - the playbook cannot run without them.
     * Optional params (required=false) are also included so an interactive caller can offer them
     * upfront; they carry {@code optional=true} so the UI can label them.
     * <p>
     * Only reports anything if at least one REQUIRED param is missing - if all required params
     * are present, optional gaps are silently accepted (agents handle their absence gracefully).
     */
    public List<MissingParam> findMissingParams(
            @Nullable List<ParamRef> paramRefs,
            Map<String, String> extracted,
            @Nullable Map<String, ParamDefinition> paramDefs) {

        if (paramRefs == null || paramDefs == null) return List.of();

        // Collect all unfilled params in playbook-defined order (e.g., namespace, workload, schema, tables).
        List<MissingParam> all = new ArrayList<>();
        boolean hasRequiredMissing = false;

        for (ParamRef ref : paramRefs) {
            String key = ref.ref();
            ParamDefinition def = paramDefs.get(key);
            if (def == null) continue;

            String value = extracted.get(key);
            boolean hasValue = value != null && !value.isBlank();

            if (!hasValue && (def.defaultValue() == null || def.defaultValue().isBlank())) {
                String prompt = def.prompt() != null ? def.prompt() : "Please provide: " + key;
                boolean isRequired = (ref.required() != null) ? ref.required() : def.required();
                boolean optional = !isRequired;
                all.add(new MissingParam(key, prompt, optional));
                if (!optional) hasRequiredMissing = true;
            }
        }

        return hasRequiredMissing ? all : List.of();
    }
}
