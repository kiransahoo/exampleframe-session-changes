package com.#exampleframe#.orchestrator.service;

import com.#exampleframe#.orchestrator.config.PlaybookProperties.ParamDefinition;
import com.#exampleframe#.orchestrator.config.PlaybookProperties.ParamRef;
import com.#exampleframe#.orchestrator.config.PlaybookProperties.PlaybookDefinition;
import com.#exampleframe#.orchestrator.config.PlaybookRoutingProperties;
import com.#exampleframe#.orchestrator.config.PlaybookRoutingProperties.RoutingMode;
import com.#exampleframe#.orchestrator.service.PlaybookMatcher.PlaybookCandidate;
import io.micrometer.core.instrument.Counter;
import com.#exampleframe#.orchestrator.config.AgentConfigResolver;
import org.jspecify.annotations.Nullable;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.regex.Pattern;

import static com.#exampleframe#.orchestrator.tools.ToolFormatUtils.truncate;

/**
 * Routes user queries to playbooks via the active {@link PlaybookMatcher}.
 * Applies score threshold policy and extracts parameters from queries using LLM.
 *
 * <p>All parameter extraction is LLM-based - no brittle regex heuristics.
 * The LLM understands natural language variations like "checkout service" vs
 * "checkout-service", "in production" vs "namespace staging", typos, etc.
 */
@Service
public class PlaybookRouter {

    private static final Logger logger = LoggerFactory.getLogger(PlaybookRouter.class);

    /** Matches a normalized time range - number followed by h/d/m. */
    private static final Pattern VALID_TIME_RANGE = Pattern.compile("^\\d+[hdm]$");

    private static final Pattern SURROUNDING_QUOTES = Pattern.compile("^(?:\"([^\"]*)\"|'([^']*)')$");

    /**
     * Normalizes natural-language time expressions into the compact format (e.g. "6h").
     * Handles: "1h", "1 h", "1 hour", "1 hours", "2 days", "30 mins", "hour", "day", etc.
     * Returns null if the value cannot be normalized.
     */
    static String normalizeTimeRange(String raw) {
        if (raw == null) return null;
        String v = raw.strip().toLowerCase()
                // Strip LLM explanatory suffixes like "1h (from the query)" or "1h - user specified"
                .replaceAll("\\s*[(-].*$", "")
                .replaceAll("^(last|past|in the last|in the past|previous|within)\\s+", "")
                .strip();

        // Already in compact form (e.g. "6h", "24h", "7d")
        if (VALID_TIME_RANGE.matcher(v).matches()) return v;

        // Bare unit with no number -> treat as 1 (e.g. "hour" -> "1h", "day" -> "1d")
        if (v.matches("^(hour|hr|hours|hrs|h)$")) return "1h";
        if (v.matches("^(day|days|d)$")) return "1d";
        if (v.matches("^(min|mins|minute|minutes|m)$")) return "1m";

        // Number + unit with optional whitespace (e.g. "1 hour", "36 hrs", "2 days")
        var m = Pattern.compile("^(\\d+)\\s*(hours?|hrs?|h)$").matcher(v);
        if (m.matches()) return m.group(1) + "h";

        m = Pattern.compile("^(\\d+)\\s*(days?|d)$").matcher(v);
        if (m.matches()) return m.group(1) + "d";

        m = Pattern.compile("^(\\d+)\\s*(mins?|minutes?|m)$").matcher(v);
        if (m.matches()) return m.group(1) + "m";

        return null; // unrecognizable
    }

    static String normalizeService(String raw) {
        if (raw == null) return null;
        String v = stripSurroundingQuotes(raw).trim();
        if (v.isEmpty()) return "";
        String lower = v.toLowerCase();
        // "all" / "all services" -> scan default package (empty = azure-monitor uses its configured default)
        if (lower.matches("^(all|all services|every service)$")) {
            return "";
        }
        // "none", "skip", "n/a" -> user explicitly declined; null = not provided -> skipped
        if (lower.matches("^(none|skip|n/a)$")) {
            return null;
        }
        return v.replaceAll("\\s+", "-").toLowerCase();
    }

    static String normalizeNamespace(String raw) {
        if (raw == null) return null;
        String v = stripSurroundingQuotes(raw).trim().toLowerCase();
        if (v.isEmpty()) return "";
        if (v.matches("^(all|all namespaces|every namespace|across all|across all namespaces|\\*)$")) {
            return "all";
        }
        return v;
    }

    static String normalizeSchema(String raw) {
        if (raw == null) return null;
        String v = stripSurroundingQuotes(raw).trim();
        if (v.isEmpty()) return "";
        String lower = v.toLowerCase();
        if (lower.matches("^(all|all schemas|all of them|every schema)$")) {
            return "all";
        }
        return v.toUpperCase();
    }

    static String normalizeTables(String raw) {
        if (raw == null) return null;
        String v = stripSurroundingQuotes(raw).trim();
        if (v.isEmpty()) return "";
        String lower = v.toLowerCase();
        if (lower.matches("^(all|all tables|everything)$")) {
            return "all";
        }
        String[] parts = v.split(",");
        List<String> cleaned = new ArrayList<>();
        for (String part : parts) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                cleaned.add(trimmed);
            }
        }
        return String.join(",", cleaned);
    }

    static String normalizeWorkload(String raw) {
        if (raw == null) return null;
        String v = stripSurroundingQuotes(raw).trim().toLowerCase();
        if (v.isEmpty()) return "";
        if (v.matches("^(all|all pods|everything)$")) {
            return "all";
        }
        return v;
    }

    private static String stripSurroundingQuotes(String raw) {
        String stripped = raw == null ? null : raw.strip();
        if (stripped == null || stripped.isEmpty()) return stripped;
        var matcher = SURROUNDING_QUOTES.matcher(stripped);
        if (matcher.matches()) {
            return matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
        }
        return stripped;
    }

    private final PlaybookMatcher activeMatcher;
    private final PlaybookRoutingProperties routingProperties;
    private final ChatModel chatModel;
    private final Counter matchCounter;
    private final Counter noMatchCounter;

    private final @Nullable AgentConfigResolver agentConfigResolver;

    public PlaybookRouter(
            List<PlaybookMatcher> matchers,
            PlaybookRoutingProperties routingProperties,
            MeterRegistry meterRegistry,
            ChatModel chatModel,
            @org.springframework.beans.factory.annotation.Autowired(required = false)
            @Nullable AgentConfigResolver agentConfigResolver) {
        this.routingProperties = routingProperties;
        this.chatModel = chatModel;
        this.agentConfigResolver = agentConfigResolver;

        // Select matcher by configured mode - fail-closed, no silent fallback
        RoutingMode mode = routingProperties.mode();
        this.activeMatcher = matchers.stream()
                .filter(m -> m.supports(mode))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "No PlaybookMatcher found for mode '" + mode + "'. " +
                        "Available matchers: " + matchers.stream()
                                .map(m -> m.getClass().getSimpleName())
                                .toList()));

        logger.info("PlaybookRouter initialized with mode={}, activeMatcher={}",
                mode, activeMatcher.getClass().getSimpleName());

        // Micrometer counters for match telemetry
        this.matchCounter = Counter.builder("playbook.match")
                .tag("result", "hit")
                .description("Playbook match hits")
                .register(meterRegistry);
        this.noMatchCounter = Counter.builder("playbook.match")
                .tag("result", "miss")
                .description("Playbook match misses")
                .register(meterRegistry);
    }

    /**
     * Matches a user query to a playbook.
     *
     * <p>Uses a two-tier threshold:
     * <ul>
     *   <li><b>Strong match</b> (≥ minScore): runs playbook directly</li>
     *   <li><b>Soft match</b> (≥ softMatchScore but &lt; minScore): returns match flagged as
     *       {@code softMatch=true}. The caller (PlaybookGateway) should only accept this
     *       if required params are missing - i.e. the query is vague enough that prompting
     *       the user for details is better than letting ReAct run blind.</li>
     *   <li><b>No match</b> (&lt; softMatchScore): falls through to ReAct</li>
     * </ul>
     *
     * @return matched playbook or empty if no match meets even the soft threshold
     */
    /** True when every agent the playbook's steps delegate to exists in this deployment. */
    private boolean agentsAvailable(PlaybookCandidate candidate) {
        return PlaybookAgentAvailability.available(
                candidate.definition(), agentConfigResolver, candidate.playbookId());
    }

    public Optional<PlaybookMatch> match(String userQuery) {
        return match(userQuery, true);
    }

    /**
     * @param requireAgentsAvailable when true (every non-meta caller), candidates whose steps
     *        need an agent this deployment lacks are dropped - running one would delegate into
     *        thin air. A meta that FORWARDS matches passes false: it never runs the playbook
     *        itself, it only needs to recognise the question as playbook-shaped so the gateway
     *        can hand it to the owning cell instead of a lossy ReAct paraphrase.
     */
    public Optional<PlaybookMatch> match(String userQuery, boolean requireAgentsAvailable) {
        List<PlaybookCandidate> candidates = activeMatcher.match(
                userQuery, routingProperties.topK());

        // A playbook drives agents directly, bypassing the ReAct loop: one whose steps need an
        // agent this deployment does not have (a meta routes to cells; a cell may run only some
        // agent types) would delegate into thin air, so it is not a candidate here.
        if (requireAgentsAvailable) {
            candidates = candidates.stream().filter(this::agentsAvailable).toList();
        }

        if (candidates.isEmpty()) {
            noMatchCounter.increment();
            logger.info("No playbook match for query: {}", truncate(userQuery, 80));
            return Optional.empty();
        }

        PlaybookCandidate top = candidates.get(0);

        // Strong match - high confidence, run directly
        if (top.score() >= routingProperties.minScore()) {
            matchCounter.increment();
            logger.info("Playbook matched: {} (score={}, source={})",
                    top.playbookId(), top.score(), top.matchSource());
            return Optional.of(new PlaybookMatch(top.playbookId(), top.definition(), false));
        }

        // Soft match - moderate confidence, route to playbook for prompting only
        if (top.score() >= routingProperties.softMatchScore()) {
            matchCounter.increment();
            logger.info("Playbook soft-matched: {} (score={}, below strong threshold {} but above soft {})",
                    top.playbookId(), top.score(), routingProperties.minScore(), routingProperties.softMatchScore());
            return Optional.of(new PlaybookMatch(top.playbookId(), top.definition(), true));
        }

        // Below both thresholds - no match
        noMatchCounter.increment();
        logger.info("Playbook candidate '{}' score {} below soft threshold {}",
                top.playbookId(), top.score(), routingProperties.softMatchScore());
        return Optional.empty();
    }

    /**
     * Extracts parameters from user query using LLM-based natural language understanding.
     * Replaces brittle regex heuristics with a single LLM call that understands
     * natural language variations, typos, and implicit references.
     *
     * <p>Falls back to param-definitions defaults for anything not extracted.
     */
    public Map<String, String> extractParameters(
            String userQuery,
            List<ParamRef> params,
            Map<String, ParamDefinition> paramDefs) {

        Map<String, String> result = new HashMap<>();
        if (params == null || paramDefs == null) return result;

        // Collect the keys we need to extract
        List<String> paramKeys = params.stream().map(ParamRef::ref).toList();
        if (paramKeys.isEmpty()) return result;

        try {
            // Build param descriptions for the LLM
            StringBuilder paramDescriptions = new StringBuilder();
            for (String key : paramKeys) {
                ParamDefinition def = paramDefs.get(key);
                String desc = def != null && def.prompt() != null ? def.prompt() : key;
                paramDescriptions.append("- ").append(key).append(": ").append(desc).append("\n");
            }

            String prompt = """
                    Extract investigation parameters from this user query.

                    Query: "%s"

                    Parameters to extract:
                    %s
                    Rules:
                    - service: Extract the APPLICATION service name being investigated. Common formats \
                    include hyphenated names like "checkout-service", "claims-api", "esign". \
                    Normalize spaces to hyphens (e.g., "my service" -> "my-service"). \
                    Do NOT extract infrastructure or technology names as the service - \
                    "oracle database", "the database", "the DB", "kubernetes cluster", \
                    "the cluster", "k8s" are NOT service names -> UNKNOWN. \
                    Only extract actual application/microservice names. Handle typos gracefully.
                    - namespace: Extract the Kubernetes namespace if explicitly mentioned \
                    (e.g., "in <namespace>", "namespace <name>"). \
                    Do NOT guess or infer a namespace - only extract if the user clearly states one.
                    - timeRange: Extract ONLY if the user explicitly mentions a time period. \
                    "last 24h" -> "24h", "past 2 hours" -> "2h", "in the last 6 hours" -> "6h", \
                    "yesterday" -> "48h", "last week" -> "7d", "since this morning" -> estimate in hours. \
                    Format MUST be a number followed by h, d, or m (e.g., 6h, 24h, 7d, 30m). \
                    Do NOT infer a time range from verb tense (was, were, is, are). \
                    "why was X slow" has NO explicit time -> UNKNOWN. \
                    "why is X slow" has NO explicit time -> UNKNOWN.
                    - schema: Extract the Oracle/database schema name if mentioned \
                    (e.g., "schema <NAME>", "in the <NAME> schema"). \
                    Handle typos like "schem" or "schma" gracefully.
                    - For any parameter not mentioned or not clear from the query -> value is "UNKNOWN"

                    Respond in EXACTLY this format (one line per parameter, no extra text):
                    paramKey=value

                    Example for "why was my-service slow in the last 6 hours in ns-prod":
                    service=my-service
                    namespace=ns-prod
                    timeRange=6h
                    schema=UNKNOWN
                    """.formatted(userQuery, paramDescriptions);

            String response = chatModel.call(new Prompt(prompt))
                    .getResult().getOutput().getText().trim();

            logger.debug("LLM param extraction raw response: '{}'", response);

            // Robust parsing - handles markdown fences, = and : separators, quotes, bullets
            Map<String, String> parsed = PlaybookGateway.parseLlmKeyValueResponse(response);
            for (String paramKey : paramKeys) {
                String value = parsed.get(paramKey);
                if (value != null && !value.isEmpty() && !"UNKNOWN".equalsIgnoreCase(value)) {
                    value = switch (paramKey) {
                        case "timeRange" -> normalizeTimeRange(value);
                        case "service" -> normalizeService(value);
                        case "namespace" -> normalizeNamespace(value);
                        case "schema" -> normalizeSchema(value);
                        case "tables" -> normalizeTables(value);
                        case "workload" -> normalizeWorkload(value);
                        default -> value;
                    };
                    if (value == null) {
                        if ("timeRange".equals(paramKey)) {
                            logger.warn("LLM returned unrecognizable timeRange '{}' - treating as not extracted", parsed.get(paramKey));
                        }
                        continue;
                    }
                    result.put(paramKey, value);
                    logger.info("LLM extracted param '{}' = '{}' from query", paramKey, value);
                }
            }
        } catch (Exception e) {
            logger.warn("LLM param extraction failed: {} - will rely on defaults/prompting", e.getMessage());
        }

        // Apply defaults for anything not extracted
        for (ParamRef paramRef : params) {
            String key = paramRef.ref();
            if (!result.containsKey(key)) {
                ParamDefinition def = paramDefs.get(key);
                if (def != null && def.defaultValue() != null && !def.defaultValue().isBlank()) {
                    result.put(key, def.defaultValue());
                }
            }
        }

        return result;
    }

    /**
     * @param softMatch true if this is a soft match (score between soft and strong thresholds).
     *                  Soft matches should only be accepted if required params are missing -
     *                  otherwise the query is specific enough that ReAct can handle it.
     */
    public record PlaybookMatch(String playbookId, PlaybookDefinition definition, boolean softMatch) {}
}
