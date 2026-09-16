package com.#exampleframe#.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.#exampleframe#.orchestrator.config.AgentConfigResolver;
import com.#exampleframe#.orchestrator.config.AppContextResolver;
import com.#exampleframe#.orchestrator.config.PlaybookProperties;
import com.#exampleframe#.orchestrator.config.PlaybookProperties.ParamDefinition;
import com.#exampleframe#.orchestrator.exception.ChildAgentHitlException;
import com.#exampleframe#.orchestrator.federation.DomainCellRouter;
import com.#exampleframe#.orchestrator.federation.FederationProperties;
import com.#exampleframe#.orchestrator.federation.MetaPlaybookRouting;
import com.#exampleframe#.orchestrator.service.PlaybookResolver.MissingParam;
import com.#exampleframe#.orchestrator.service.PlaybookRouter.PlaybookMatch;
import com.#exampleframe#.orchestrator.thread.history.model.Message;
import com.#exampleframe#.orchestrator.thread.history.model.ThreadKey;
import com.#exampleframe#.orchestrator.thread.history.store.ThreadStore;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Pre-routing interceptor. Called from {@code OrchestratorController.runAgent()}
 * BEFORE {@code executeAgent()}.
 *
 * <p>If a playbook matches the user message, it executes the playbook deterministically
 * and returns the SSE flux directly - bypassing the ReAct loop entirely.
 */
@Service
public class PlaybookGateway {

    private static final Logger logger = LoggerFactory.getLogger(PlaybookGateway.class);

    /** Maximum age of a pending playbook entry before it's considered stale and evicted. */
    private static final Duration PENDING_TTL = Duration.ofMinutes(10);

    /**
     * A forwarded cell turn is ONE blocking A2A call: it emits nothing until it finishes, which
     * for a real investigation is minutes. Both the browser (idle-cancel) and the ingress
     * (proxy-read-timeout) cut a silent stream at 300s, so the request died before the answer
     * arrived. A comment frame carries no data - the UI drops every line that is not "data:"
     * before parsing - but it is bytes on the socket, which is all either timer needs.
     */
    private static final Duration CELL_KEEPALIVE = Duration.ofSeconds(15);

    /**
     * Hard bound on one forwarded cell turn. Until now the browser's 300s cut WAS the bound;
     * once keepalives defeat that, an unbounded wait would leave the user on a spinner for the
     * A2A client's own budget (20 minutes). Applied to the cell call, never to the merged
     * stream - Reactor's timeout is inter-element, so on the merged stream the 15s keepalives
     * would reset it forever and silently remove the bound. Kept strictly under the A2A client's
     * own 1200s budget, so the meta gives up before the transport beneath it does.
     */
    private static final Duration CELL_TURN_TIMEOUT = Duration.ofMinutes(15);

    /**
     * Stores pending playbook context when the gateway prompts the user for missing params.
     * Keyed by threadId. When the user's next message arrives, the gateway checks here first
     * and tries to fill in the missing params from the new message.
     *
     * <p>Entries expire after {@link #PENDING_TTL} - evicted on access and by scheduled cleanup.
     */
    private final ConcurrentHashMap<String, PendingPlaybook> pendingPlaybooks = new ConcurrentHashMap<>();

    private static final String DEFAULT_APP_NAME = "orchestrator_agent";

    private final PlaybookRouter router;
    private final PlaybookExecutor executor;
    private final PlaybookProperties playbookProperties;
    private final PlaybookResolver resolver;
    private final InvestigationStateService investigationStateService;
    private final AgentResponseSinkRegistry agentResponseSinkRegistry;
    private final ThreadStore threadStore;
    private final ChatModel chatModel;
    private final ObjectMapper objectMapper;

    // Meta-only collaborators: a domain cell runs with federation disabled and never uses them.
    private final @Nullable FederationProperties federationProperties;
    private final @Nullable DomainCellRouter domainCellRouter;
    private final @Nullable DelegationExecutionService delegationExecutionService;
    private final @Nullable AppContextResolver appContextResolver;
    private final @Nullable AgentConfigResolver agentConfigResolver;
    private final @Nullable ChildHitlEventBuilder childHitlEventBuilder;

    public PlaybookGateway(
            PlaybookRouter router,
            PlaybookExecutor executor,
            PlaybookProperties playbookProperties,
            PlaybookResolver resolver,
            InvestigationStateService investigationStateService,
            AgentResponseSinkRegistry agentResponseSinkRegistry,
            ThreadStore threadStore,
            ChatModel chatModel,
            ObjectMapper objectMapper,
            @Nullable FederationProperties federationProperties,
            @Nullable DomainCellRouter domainCellRouter,
            @Nullable DelegationExecutionService delegationExecutionService,
            @Nullable AppContextResolver appContextResolver,
            @Nullable AgentConfigResolver agentConfigResolver,
            @Nullable ChildHitlEventBuilder childHitlEventBuilder) {
        this.router = router;
        this.executor = executor;
        this.playbookProperties = playbookProperties;
        this.resolver = resolver;
        this.investigationStateService = investigationStateService;
        this.agentResponseSinkRegistry = agentResponseSinkRegistry;
        this.threadStore = threadStore;
        this.chatModel = chatModel;
        this.objectMapper = objectMapper;
        this.federationProperties = federationProperties;
        this.domainCellRouter = domainCellRouter;
        this.delegationExecutionService = delegationExecutionService;
        this.appContextResolver = appContextResolver;
        this.agentConfigResolver = agentConfigResolver;
        this.childHitlEventBuilder = childHitlEventBuilder;
    }

    /**
     * Tries to match user message to a playbook.
     * If matched: registers sink, executes playbook, returns SSE flux directly.
     * If not matched: returns empty - caller falls through to ReAct.
     *
     * <p>Namespace is primarily extracted from the user's natural language query by the LLM.
     * However, if the API request includes an explicit namespace (non-null, non-blank),
     * it is injected into params so the playbook uses it - this ensures API-level
     * namespace is not silently dropped.
     *
     * @param userMessage       the user's query
     * @param threadId          thread ID for this conversation
     * @param userId            user ID
     * @param requestNamespace  namespace from the API request (may be null)
     * @return SSE flux if playbook matched, empty otherwise
     */
    public Optional<Flux<ServerSentEvent<String>>> tryExecute(
            String userMessage,
            String threadId,
            String userId,
            String requestNamespace) {

        // Meta forwarding continuation: an answer to "which domain should run it?" or to the
        // param prompt of a playbook the meta is about to hand to a cell. Checked before the
        // generic pending machinery - the meta door keeps its own state so a domain answer
        // can never be mistaken for a service value (or vice versa). The pending must not
        // hijack the thread, though: an explicit cancel drops it with an acknowledgement, and
        // a message that is its own request (a new playbook, a full question) falls through
        // to normal routing instead of being force-fed into the stale forward.
        if (metaForwardingEnabled()) {
            PendingMetaForward metaPending = removePendingMetaForward(threadId);
            if (metaPending != null) {
                if (isMetaForwardCancellation(userMessage)) {
                    logger.info("[meta-playbook] pending '{}' cancelled by the user", metaPending.playbookId());
                    return Optional.of(Flux.just(buildAssistantEvent(
                            "Okay - I've dropped the pending **" + metaPending.playbookId()
                                    + "** playbook request. What would you like to do instead?")));
                }
                if (isMetaForwardPivot(userMessage, metaPending)) {
                    logger.info("[meta-playbook] pending '{}' dropped - the new message is its own request",
                            metaPending.playbookId());
                    // fall through: the message is processed fresh below
                } else {
                    return Optional.of(resumeMetaForward(metaPending, userMessage, threadId, userId));
                }
            }
        }

        // Step 1: Check if there's a pending playbook waiting for params from this thread
        PendingPlaybook pending = removePending(threadId);
        PlaybookMatch resolvedMatch;
        Map<String, String> params;
        String originalQuery;  // the ORIGINAL problem statement, not the follow-up

        if (pending != null) {
            logger.info("Resuming pending playbook '{}' - merging user response into params", pending.playbookId());
            resolvedMatch = new PlaybookMatch(pending.playbookId(), pending.definition(), false);
            params = new HashMap<>(pending.extractedParams());
            originalQuery = pending.originalUserMessage();  // preserve original context

            // Use LLM to parse the user's natural language response and extract param values.
            // Include both required missing keys AND optional params that are still empty,
            // so the LLM can capture things like "java-healthy pod" -> workload, "orders table" -> tables.
            List<String> extractionKeys = new ArrayList<>(pending.missingKeys());
            if (resolvedMatch.definition().params() != null) {
                for (var ref : resolvedMatch.definition().params()) {
                    String key = ref.ref();
                    if (!extractionKeys.contains(key)
                            && (params.get(key) == null || params.get(key).isBlank())) {
                        extractionKeys.add(key);
                    }
                }
            }
            Map<String, String> llmExtracted = extractParamsWithLLM(
                    userMessage, extractionKeys, playbookProperties.paramDefinitions());
            for (Map.Entry<String, String> entry : llmExtracted.entrySet()) {
                if (entry.getValue() != null && !entry.getValue().isBlank()) {
                    params.put(entry.getKey(), entry.getValue());
                    logger.info("LLM extracted param '{}' = '{}' from user response",
                            entry.getKey(), entry.getValue());
                }
            }
        } else {
            // Step 2: explicit playbook reference ("run the service-slow playbook") wins
            // over scored matching - same resolution order as the tool and delegated doors.
            // A meta resolves the reference even when only a domain cell can run the playbook,
            // so the request reaches that cell as the playbook it named, not as an LLM paraphrase.
            var explicit = (metaForwardingEnabled()
                    ? resolver.resolveByExplicitIdAnyAgents(userMessage)
                    : resolver.resolveByExplicitId(userMessage)).orElse(null);
            if (explicit != null) {
                resolvedMatch = new PlaybookMatch(explicit.getKey(), explicit.getValue(), false);
            } else {
                // Step 3: Try fresh playbook match
                Optional<PlaybookMatch> match = router.match(userMessage);
                if (match.isEmpty()) return Optional.empty();
                resolvedMatch = match.get();
            }
            originalQuery = userMessage;  // this IS the original query

            params = router.extractParameters(
                    userMessage,
                    resolvedMatch.definition().params(),
                    playbookProperties.paramDefinitions());
        }

        // Soft-match gate: if the match was soft (low confidence) AND the user's query
        // already provided all required params, reject the match - the query is specific
        // enough that ReAct can handle it properly. Soft matches are only useful when
        // the query is vague and the user needs to be prompted for missing details.
        if (resolvedMatch.softMatch()) {
            List<MissingParam> preCheckMissing = resolver.findMissingParams(
                    resolvedMatch.definition().params(), params, playbookProperties.paramDefinitions());
            if (preCheckMissing.isEmpty()) {
                logger.info("Soft-match '{}' rejected - all required params present, deferring to ReAct",
                        resolvedMatch.playbookId());
                return Optional.empty();
            }
            logger.info("Soft-match '{}' accepted - {} required param(s) missing, will prompt user",
                    resolvedMatch.playbookId(), preCheckMissing.size());
        }

        logger.info("Playbook matched: {} - bypassing ReAct", resolvedMatch.playbookId());

        // Inject request-level namespace if explicitly provided by the API caller.
        // LLM extraction takes priority (it already ran above), but if the LLM didn't
        // extract a namespace AND the API request provided one, use the request value.
        // Filter out "default" - that's the frontend's generic fallback, not an explicit
        // user choice. Letting it through would prevent the playbook from prompting for
        // the actual namespace and cause kubernetes to search only the default namespace.
        if (requestNamespace != null && !requestNamespace.isBlank()) {
            String normalizedNs = PlaybookRouter.normalizeNamespace(requestNamespace);
            if (normalizedNs != null
                    && !normalizedNs.isBlank()
                    && !"default".equalsIgnoreCase(normalizedNs)
                    && !params.containsKey("namespace")) {
                params.put("namespace", normalizedNs);
                logger.info("Injected request-level namespace '{}' into playbook params", normalizedNs);
            }
        }

        // App-context: auto-fill playbook params from per-service infrastructure mappings.
        // Runs BEFORE missing-param check so the user isn't prompted for values we already know.
        // Priority: LLM-extracted > app-context > paramDefaults > prompt user
        // Provenance snapshot for the meta door: keys present BEFORE applyAppContext came from
        // the user's own words or the API request; keys appContext adds are mapping-derived and
        // must not override the owning cell's mapping when forwarded.
        Set<String> callerProvidedKeys = new LinkedHashSet<>(params.keySet());

        resolver.applyAppContext(params, originalQuery);

        // Meta: a playbook this deployment cannot run itself belongs to the owning domain cell.
        if (metaForwardingEnabled() && !resolver.agentsAvailable(resolvedMatch.definition())) {
            return Optional.of(forwardToOwningCell(
                    resolvedMatch, params, null, callerProvidedKeys,
                    originalQuery, threadId, userId));
        }

        // Check for missing params that have prompts defined - ask user instead of running blind
        List<MissingParam> missingParams = resolver.findMissingParams(
                resolvedMatch.definition().params(), params, playbookProperties.paramDefinitions());
        if (!missingParams.isEmpty()) {
            logger.info("Playbook '{}' has {} missing param(s) - prompting user: {}",
                    resolvedMatch.playbookId(), missingParams.size(),
                    missingParams.stream().map(MissingParam::key).toList());

            // Store context so the next message can continue where we left off.
            // Always preserves the ORIGINAL user query for synthesis/evidence context.
            pendingPlaybooks.put(threadId, new PendingPlaybook(
                    resolvedMatch.playbookId(),
                    resolvedMatch.definition(),
                    params,
                    missingParams.stream().map(MissingParam::key).toList(),
                    originalQuery,
                    Instant.now()));

            return Optional.of(Flux.just(buildParamPromptEvent(
                    resolvedMatch.playbookId(), resolvedMatch.definition().name(), missingParams)));
        }

        // Record ORIGINAL user message to investigation state (not the follow-up param response)
        investigationStateService.recordUserMessage(threadId, originalQuery);

        // Register sink for SSE events (step progress, HITL events). A sink already active
        // for this threadId means another request is streaming the same conversation -
        // reject this one instead of stealing or orphaning the first stream.
        if (!agentResponseSinkRegistry.register(threadId)) {
            return Optional.of(Flux.just(ServerSentEvent.<String>builder()
                    .event("error")
                    .data("Another request is already streaming for this conversation. "
                            + "Wait for it to finish, or start a new thread.")
                    .build()));
        }

        // Thread binding parity with executeAgent path
        InvestigationStateService.CURRENT_THREAD_ID.set(threadId);
        investigationStateService.bindCurrentThread(threadId);

        // Persist user message to thread history (creates thread doc if needed)
        final ThreadKey threadKey = ThreadKey.of(DEFAULT_APP_NAME, userId, threadId);
        persistMessageAsync(threadKey, originalQuery, Message.MessageRole.USER);

        try {
            // Execute playbook with the ORIGINAL query for synthesis context
            Flux<ServerSentEvent<String>> playbookFlux =
                    executor.execute(resolvedMatch.definition(), params, threadId, originalQuery);

            // Merge with agent-response sink (same pattern as executeAgent)
            Flux<ServerSentEvent<String>> agentResponseFlux =
                    agentResponseSinkRegistry.getFlux(threadId);

            // Capture final report text for message persistence
            final String finalThreadId = threadId;
            final String finalUserId = userId;

            Flux<ServerSentEvent<String>> terminatingPlaybook = playbookFlux
                    .doOnNext(event -> {
                        // Capture the synthesized report from StreamResponse events
                        if (event.data() != null) {
                            try {
                                var parsed = objectMapper.readTree(event.data());
                                if (parsed.has("content") && parsed.has("node")) {
                                    String content = parsed.get("content").asText("");
                                    if (!content.isBlank()) {
                                        // Persist assistant response
                                        persistMessageAsync(threadKey, content,
                                                Message.MessageRole.ASSISTANT);
                                    }
                                }
                            } catch (Exception ignored) {
                                // Not a StreamResponse JSON - skip (step-progress, errors, etc.)
                            }
                        }
                    })
                    .doFinally(signal -> agentResponseSinkRegistry.unregister(threadId));

            return Optional.of(
                    Flux.merge(terminatingPlaybook, agentResponseFlux)
                            .doFinally(signal -> {
                                agentResponseSinkRegistry.unregister(finalThreadId); // defensive
                                agentResponseSinkRegistry.persistCostSnapshot(finalThreadId);
                                InvestigationStateService.CURRENT_THREAD_ID.remove();
                                investigationStateService.unbindCurrentThread(finalThreadId);
                            })
            );
        } catch (Exception e) {
            // Cleanup sink if setup fails before flux is returned
            agentResponseSinkRegistry.unregister(threadId);
            InvestigationStateService.CURRENT_THREAD_ID.remove();
            throw e;
        }
    }


    private boolean metaForwardingEnabled() {
        return federationProperties != null && federationProperties.enabled()
                && domainCellRouter != null && delegationExecutionService != null;
    }

    /**
     * META door for an explicit playbook request. The meta has no infrastructure agents, so it
     * never runs the playbook itself: it names the owning cell from the service mapping (or the
     * user's own domain choice from a previous ask) and hands the user's words over verbatim -
     * the cell's delegated door resolves the same reference and runs that playbook - or asks
     * which domain when the request pins none. Only params the user alone can supply (timeRange,
     * the service itself) are collected here; infrastructure scope (namespace, schema, ...) is
     * the owning cell's to fill from ITS mapping - the meta's may hold only domain ownership.
     */
    private Flux<ServerSentEvent<String>> forwardToOwningCell(
            PlaybookMatch match,
            Map<String, String> params,
            @Nullable String chosenDomain,
            Set<String> callerProvidedKeys,
            String originalQuery,
            String threadId,
            String userId) {
        Map<String, FederationProperties.DomainCell> cells = federationProperties.domainsOrEmpty();

        // The original text is the authority on which service the user named - an extracted
        // value (which may have passed through an LLM on a resume turn) must not reroute the
        // playbook to a domain the user never asked for. The extracted value counts only when
        // the text pins no known service (the bare-domain flow, where the user supplies the
        // service in a later turn), and an alias is canonicalized ("payments" -> payments-api).
        // An unmapped name (ledger-sync-svc) stays as given for the chosen domain's cell.
        String service = blankToNull(params.get("service"));
        // "run the service-slow playbook" (no service named) sometimes extracts the PLAYBOOK ID
        // into the service slot. Forwarded, it resolves to no mapping, so the cell finds no
        // namespace/schema and abandons the playbook for an open-ended ReAct run. A service is
        // never a playbook id - the mirror of the guard that stops a service name being read as
        // a playbook reference.
        if (service != null && playbookProperties.playbooks() != null
                && (service.equalsIgnoreCase(match.playbookId())
                    || playbookProperties.playbooks().containsKey(service.toLowerCase(Locale.ROOT)))) {
            logger.info("[meta-playbook] ignoring '{}' as a service - it is a playbook id", service);
            service = null;
            params.remove("service");
        }
        if (appContextResolver != null) {
            String fromText = appContextResolver.analyzeText(originalQuery).knownServiceName();
            if (fromText != null) {
                service = fromText;
            } else if (service != null) {
                String known = appContextResolver.analyzeText(service).knownServiceName();
                if (known != null) service = known;
            }
        }

        // owningDomain is a pure mapping lookup (no access check, no exception); per-domain
        // ACCESS is enforced by delegateToAgent's checkDomainAccess against the caller bound
        // to this thread, and a denial comes back as the response text below.
        Optional<String> owningDomain =
                service != null ? domainCellRouter.owningDomain(service) : Optional.empty();

        MetaPlaybookRouting.Decision decision = MetaPlaybookRouting.decide(
                Map.entry(match.playbookId(), match.definition()), service, owningDomain, chosenDomain, cells);

        if (decision.kind() == MetaPlaybookRouting.Kind.ASK_DOMAIN) {
            logger.info("[meta-playbook] '{}' pins no domain (service='{}') - asking which domain should run it",
                    match.playbookId(), service);
            pendingMetaForwards.put(threadId, new PendingMetaForward(
                    match.playbookId(), match.definition(), params, null, callerProvidedKeys,
                    List.of(AWAITING_DOMAIN), originalQuery, Instant.now()));
            return Flux.just(buildAssistantEvent(decision.message()));
        }

        if (decision.service() != null) {
            params.put("service", decision.service());
        }

        // Collect only what the user alone can supply - timeRange, and the service itself after
        // a bare domain choice. Infrastructure params stay unfilled for the cell to resolve.
        List<MissingParam> userOnly = resolver.findMissingParams(
                        match.definition().params(), params, playbookProperties.paramDefinitions())
                .stream()
                .filter(m -> !m.optional())
                .filter(m -> !PlaybookResolver.APP_CONTEXT_PARAM_KEYS.contains(m.key()))
                .toList();
        if (!userOnly.isEmpty()) {
            logger.info("[meta-playbook] '{}' -> {} cell needs {} user-only param(s) first: {}",
                    match.playbookId(), decision.domain(), userOnly.size(),
                    userOnly.stream().map(MissingParam::key).toList());
            pendingMetaForwards.put(threadId, new PendingMetaForward(
                    match.playbookId(), match.definition(), params, decision.domain(), callerProvidedKeys,
                    userOnly.stream().map(MissingParam::key).toList(), originalQuery, Instant.now()));
            return Flux.just(buildParamPromptEvent(
                    match.playbookId(), match.definition().name(), userOnly));
        }

        final String cellKey = decision.cellAgentKey();
        final String toolName = toolNameFor(cellKey);
        // Structured taskParams OVERRIDE the cell's own mapping, so infrastructure values the
        // meta merely derived from ITS mapping must not travel - the cell is the authority on
        // its namespaces and schemas. Values the user or the API caller stated themselves
        // (callerProvidedKeys, including reply-volunteered ones) do travel: user provenance
        // outranks either mapping.
        final Map<String, String> extra = new LinkedHashMap<>();
        params.forEach((k, v) -> {
            if (v != null && !v.isBlank() && !"UNKNOWN".equalsIgnoreCase(v)
                    && (!PlaybookResolver.APP_CONTEXT_PARAM_KEYS.contains(k) || callerProvidedKeys.contains(k))) {
                extra.put(k, v);
            }
        });
        extra.put("playbookId", match.playbookId());
        extra.put("domain", decision.domain());

        // Owner of any HITL the cell raises: captured HERE, at the HTTP boundary, because the
        // delegation runs on a worker thread whose security context is empty - stored under
        // "anonymous" the real user's later approval would be refused as cross-user access.
        final String requestOwner = com.#exampleframe#.orchestrator.security.CallerContext.capture().name();

        logger.info("[meta-playbook] '{}' -> {} cell '{}' (service='{}') - forwarding the request verbatim ({} chars)",
                match.playbookId(), decision.domain(), cellKey, decision.service(), originalQuery.length());

        investigationStateService.recordUserMessage(threadId, originalQuery);

        if (!agentResponseSinkRegistry.register(threadId)) {
            // Transient refusal: another stream owns this conversation right now. Keep the
            // fully-resolved forward (awaiting nothing) so the user's next message retries the
            // delegation instead of the request silently dying to context-free ReAct.
            pendingMetaForwards.put(threadId, new PendingMetaForward(
                    match.playbookId(), match.definition(), params, decision.domain(), callerProvidedKeys,
                    List.of(), originalQuery, Instant.now()));
            return Flux.just(ServerSentEvent.<String>builder()
                    .event("error")
                    .data("Another request is already streaming for this conversation. "
                            + "Wait for it to finish, or start a new thread.")
                    .build());
        }

        InvestigationStateService.CURRENT_THREAD_ID.set(threadId);
        investigationStateService.bindCurrentThread(threadId);

        final ThreadKey threadKey = ThreadKey.of(DEFAULT_APP_NAME, userId, threadId);
        persistMessageAsync(threadKey, originalQuery, Message.MessageRole.USER);

        // Guards the delegation's side effects against a cancelled stream: after the client
        // goes away (refresh, timeout) this conversation may register a NEW sink under the
        // same threadId, and a late emit/persist would inject the aborted turn's panel and
        // history into that unrelated stream.
        final java.util.concurrent.atomic.AtomicBoolean streamActive =
                new java.util.concurrent.atomic.AtomicBoolean(true);

        Mono<ServerSentEvent<String>> cellTurn = Mono.fromCallable(() -> {
            InvestigationStateService.CURRENT_THREAD_ID.set(threadId);
            try {
                String response = delegationExecutionService.delegateToAgentPinned(
                        cellKey, originalQuery, threadId, extra, null);
                if (streamActive.get()) {
                    // Same "[X CELL AGENT] Raw Response" panel the ReAct path's hook produces.
                    agentResponseSinkRegistry.emit(threadId, toolName, response);
                    persistMessageAsync(threadKey, response, Message.MessageRole.ASSISTANT);
                } else {
                    logger.warn("[meta-playbook] stream for thread {} ended before the {} cell "
                            + "answered - dropping the stale response ({} chars)",
                            threadId, cellKey, response.length());
                }
                return buildAssistantEvent(response);
            } catch (ChildAgentHitlException e) {
                logger.info("[meta-playbook] cell '{}' raised HITL - {} tool(s) pending approval",
                        cellKey, e.getPendingTools().size());
                // Record the approval so /resume-child finds it - without this the user's
                // approve/deny would come back "No pending approval for this task".
                investigationStateService.storePendingHitlAs(
                        e.getTaskId(), threadId, e.getAgentName(), e.getAgentBaseUrl(), requestOwner);
                if (childHitlEventBuilder != null) {
                    return childHitlEventBuilder.buildEvent(e);
                }
                return buildAssistantEvent("The " + decision.domain()
                        + " cell needs an approval before it can continue.");
            } finally {
                InvestigationStateService.CURRENT_THREAD_ID.remove();
            }
        }).subscribeOn(Schedulers.boundedElastic())
                .timeout(CELL_TURN_TIMEOUT, Mono.fromSupplier(() -> {
                    logger.warn("[meta-playbook] cell '{}' did not answer within {} - giving up",
                            cellKey, CELL_TURN_TIMEOUT);
                    return buildAssistantEvent("The " + decision.domain() + " cell did not answer within "
                            + CELL_TURN_TIMEOUT.toMinutes() + " minutes. Nothing was changed - try again, "
                            + "or ask the cell directly.");
                }));

        Flux<ServerSentEvent<String>> answer = cellTurn
                .onErrorResume(e -> {
                    logger.error("[meta-playbook] delegation to cell '{}' failed: {}", cellKey, e.getMessage(), e);
                    return Mono.just(buildAssistantEvent("The " + decision.domain()
                            + " cell could not complete this request: " + e.getMessage()));
                })
                .flux()
                .doFinally(signal -> agentResponseSinkRegistry.unregister(threadId));

        // Stops the heartbeat the instant the turn ends, however it ends. Without an explicit
        // stop an unbounded interval would keep Flux.merge from ever completing.
        Sinks.Empty<Void> keepAliveStop = Sinks.empty();

        Flux<ServerSentEvent<String>> turn =
                Flux.merge(answer, agentResponseSinkRegistry.getFlux(threadId))
                .doFinally(signal -> {
                    keepAliveStop.tryEmitEmpty();
                    streamActive.set(false);
                    agentResponseSinkRegistry.unregister(threadId); // defensive
                    agentResponseSinkRegistry.persistCostSnapshot(threadId);
                    InvestigationStateService.CURRENT_THREAD_ID.remove();
                    investigationStateService.unbindCurrentThread(threadId);
                });

        Flux<ServerSentEvent<String>> keepAlive = Flux.interval(CELL_KEEPALIVE, CELL_KEEPALIVE)
                .onBackpressureDrop()
                .map(tick -> ServerSentEvent.<String>builder().comment("keepalive").build())
                .takeUntilOther(keepAliveStop.asMono());

        return Flux.merge(turn, keepAlive);
    }

    /** Sentinel awaiting-key: the meta asked "which domain should run it?". Never a param name. */
    private static final String AWAITING_DOMAIN = "__domain__";

    /**
     * Meta-door pending state: a "which domain?" question or a user-only param prompt, kept
     * separate from {@link #pendingPlaybooks} so a domain answer is never mistaken for a
     * service value. Keyed by threadId; same TTL as the generic pending map.
     */
    private final ConcurrentHashMap<String, PendingMetaForward> pendingMetaForwards = new ConcurrentHashMap<>();

    private record PendingMetaForward(
            String playbookId,
            PlaybookProperties.PlaybookDefinition definition,
            Map<String, String> params,
            @Nullable String chosenDomain,
            Set<String> callerProvidedKeys,
            List<String> awaiting,
            String originalUserMessage,
            Instant createdAt
    ) {}

    private static final java.util.regex.Pattern CANCEL_RE = java.util.regex.Pattern.compile(
            "\\b(cancel|abort|stop|never\\s?mind|forget\\s+(it|that|the\\s+playbook))\\b",
            java.util.regex.Pattern.CASE_INSENSITIVE);

    /** Tokens that are conversational filler, never a service name or a retry nudge. */
    private static final Set<String> NON_ANSWER_TOKENS = Set.of(
            "yes", "no", "ok", "okay", "sure", "help", "thanks", "thank", "please",
            "hi", "hello", "hey", "go", "run", "do", "it", "what", "why", "how",
            "hmm", "huh", "maybe", "idk", "dunno", "wait");

    private boolean isMetaForwardCancellation(@Nullable String reply) {
        return reply != null && CANCEL_RE.matcher(reply).find();
    }

    /**
     * True when the reply is its own request rather than an answer to the meta door's one
     * question: it names a playbook itself, or (for the short-answer states - the domain
     * question and the busy-retry) it reads like a full question. Param prompts stay greedy:
     * their LLM extraction returns UNKNOWN for off-topic text and simply re-prompts.
     */
    private boolean isMetaForwardPivot(String reply, PendingMetaForward pending) {
        if (resolver.resolveByExplicitIdAnyAgents(reply).isPresent()) {
            return true;
        }
        if (pending.awaiting().contains(AWAITING_DOMAIN) || pending.awaiting().isEmpty()) {
            String trimmed = reply == null ? "" : reply.trim();
            return trimmed.contains("?") || trimmed.split("\\s+").length > 6;
        }
        return false;
    }

    private @Nullable PendingMetaForward removePendingMetaForward(String threadId) {
        PendingMetaForward pending = pendingMetaForwards.remove(threadId);
        if (pending == null) return null;
        Duration age = Duration.between(pending.createdAt(), Instant.now());
        if (age.compareTo(PENDING_TTL) > 0) {
            logger.info("Evicting stale meta forward '{}' for thread {} (age={}s)",
                    pending.playbookId(), threadId, age.toSeconds());
            return null;
        }
        return pending;
    }

    /**
     * The user's answer to the meta door's one question - either the domain choice or the
     * user-only params. Fills in what the reply provides and re-enters the forwarding
     * decision; anything still missing produces the next (single) question.
     */
    private Flux<ServerSentEvent<String>> resumeMetaForward(
            PendingMetaForward pending, String reply, String threadId, String userId) {
        Map<String, String> params = new HashMap<>(pending.params());
        Set<String> callerKeys = new LinkedHashSet<>(pending.callerProvidedKeys());
        String chosenDomain = pending.chosenDomain();
        Map<String, FederationProperties.DomainCell> cells = federationProperties.domainsOrEmpty();

        if (pending.awaiting().isEmpty()) {
            // A fully-resolved forward that was refused because another stream was active;
            // this message is the retry nudge - forward as stored, nothing to parse.
            logger.info("[meta-playbook] retrying the stored '{}' forward", pending.playbookId());
        } else if (pending.awaiting().contains(AWAITING_DOMAIN)) {
            String domain = MetaPlaybookRouting.mentionedDomain(reply, cells.keySet());
            // The reply may carry BOTH ("claims - it's ledger-sync-svc"): capture the service
            // alongside the domain rather than re-asking for what the user just typed.
            String service = appContextResolver != null
                    ? appContextResolver.analyzeText(reply).knownServiceName() : null;
            if (domain != null) {
                chosenDomain = domain;
                if (service != null && blankToNull(params.get("service")) == null) {
                    params.put("service", service);
                    callerKeys.add("service");
                }
            } else {
                // The ask offers a service as the alternative answer. A recognized mapping
                // name wins; failing that, a single bare token is taken as the service -
                // unless it is conversational filler, which just earns the question again.
                if (service == null && reply != null) {
                    String token = reply.trim();
                    if (token.matches("[A-Za-z0-9][A-Za-z0-9._-]{1,63}")
                            && !NON_ANSWER_TOKENS.contains(token.toLowerCase(Locale.ROOT))) {
                        service = token;
                    }
                }
                if (service != null) {
                    params.put("service", service);
                    callerKeys.add("service");
                } else {
                    logger.info("[meta-playbook] '{}': reply names neither a domain nor a service - asking again",
                            pending.playbookId());
                    pendingMetaForwards.put(threadId, pending);
                    return Flux.just(buildAssistantEvent(
                            "I didn't recognize a domain or service in that. Known domains: "
                                    + String.join(", ", new TreeSet<>(cells.keySet()))
                                    + ". Reply with one of them, or the service to investigate."));
                }
            }
        } else {
            // Ask for the awaited keys, but capture volunteered values too ("36h, in namespace
            // staging") - the same courtesy the generic door extends. Reply-extracted values
            // are the user's own words: user provenance.
            List<String> extractionKeys = new ArrayList<>(pending.awaiting());
            if (pending.definition().params() != null) {
                for (var ref : pending.definition().params()) {
                    String key = ref.ref();
                    if (!extractionKeys.contains(key) && blankToNull(params.get(key)) == null) {
                        extractionKeys.add(key);
                    }
                }
            }
            Map<String, String> extracted = extractParamsWithLLM(
                    reply, extractionKeys, playbookProperties.paramDefinitions());
            for (Map.Entry<String, String> entry : extracted.entrySet()) {
                if (entry.getValue() != null && !entry.getValue().isBlank()
                        && !"UNKNOWN".equalsIgnoreCase(entry.getValue())) {
                    params.put(entry.getKey(), entry.getValue());
                    callerKeys.add(entry.getKey());
                }
            }
        }

        PlaybookMatch match = new PlaybookMatch(pending.playbookId(), pending.definition(), false);
        return forwardToOwningCell(match, params, chosenDomain, callerKeys,
                pending.originalUserMessage(), threadId, userId);
    }

    /** The configured delegation tool name for a cell (what the UI labels the panel with). */
    private String toolNameFor(String cellKey) {
        if (agentConfigResolver != null) {
            var config = agentConfigResolver.getAgentConfigs().get(cellKey);
            if (config != null && config.toolName() != null && !config.toolName().isBlank()) {
                return config.toolName();
            }
        }
        return MetaPlaybookRouting.toolNameFor(cellKey);
    }

    private static @Nullable String blankToNull(@Nullable String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }


    private record PendingPlaybook(
            String playbookId,
            PlaybookProperties.PlaybookDefinition definition,
            Map<String, String> extractedParams,
            List<String> missingKeys,
            String originalUserMessage,
            Instant createdAt
    ) {}

    /**
     * Removes and returns a pending playbook for the given thread, if it exists and is not stale.
     * Returns null if no entry, or if the entry has exceeded {@link #PENDING_TTL}.
     */
    private PendingPlaybook removePending(String threadId) {
        PendingPlaybook pending = pendingPlaybooks.remove(threadId);
        if (pending == null) return null;

        Duration age = Duration.between(pending.createdAt(), Instant.now());
        if (age.compareTo(PENDING_TTL) > 0) {
            logger.info("Evicting stale pending playbook '{}' for thread {} (age={}s, ttl={}s)",
                    pending.playbookId(), threadId, age.toSeconds(), PENDING_TTL.toSeconds());
            return null;
        }
        return pending;
    }

    /**
     * Scheduled cleanup of stale pending playbook entries.
     * Runs every 5 minutes to prevent unbounded map growth from abandoned threads.
     */
    @Scheduled(fixedDelay = 300_000)   // 5 minutes
    void evictStalePendingPlaybooks() {
        Instant cutoff = Instant.now().minus(PENDING_TTL);
        pendingMetaForwards.entrySet().removeIf(entry -> entry.getValue().createdAt().isBefore(cutoff));
        if (pendingPlaybooks.isEmpty()) return;

        int before = pendingPlaybooks.size();

        pendingPlaybooks.entrySet().removeIf(entry -> {
            boolean stale = entry.getValue().createdAt().isBefore(cutoff);
            if (stale) {
                logger.debug("Evicting stale pending playbook '{}' for thread {}",
                        entry.getValue().playbookId(), entry.getKey());
            }
            return stale;
        });

        int evicted = before - pendingPlaybooks.size();
        if (evicted > 0) {
            logger.info("Evicted {} stale pending playbook(s), {} remaining", evicted, pendingPlaybooks.size());
        }
    }


    /**
     * Uses the LLM to extract parameter values from the user's natural language response.
     * Called when the user replies to a parameter prompt (e.g., "across all namespaces and 36 hrs").
     *
     * @param userResponse  the user's follow-up message
     * @param missingKeys   the parameter keys we're looking for
     * @param paramDefs     parameter definitions (for context about what each param means)
     * @return map of paramKey -> extracted value (may be partial)
     */
    private Map<String, String> extractParamsWithLLM(
            String userResponse,
            List<String> missingKeys,
            Map<String, ParamDefinition> paramDefs) {

        Map<String, String> result = new HashMap<>();
        if (missingKeys == null || missingKeys.isEmpty()) return result;

        try {
            StringBuilder paramDescriptions = new StringBuilder();
            for (String key : missingKeys) {
                ParamDefinition def = paramDefs.get(key);
                String desc = def != null && def.prompt() != null ? def.prompt() : key;
                paramDescriptions.append("- ").append(key).append(": ").append(desc).append("\n");
            }

            String prompt = """
                    The user was asked to provide these investigation parameters:
                    %s
                    They responded: "%s"

                    Extract the value for EACH parameter from their response.
                    Rules:
                    - namespace: "all", "all namespaces", "across all", "everything" -> value is "all"
                    - schema: "all schemas", "all of them", "all" -> value is "all"
                    - tables: "all tables", "all", "everything" -> value is "all"
                    - workload: "all pods", "all", "everything" -> value is "all"
                    - timeRange: MUST be a time duration. Convert natural language to compact format: \
                    "36 hrs" -> "36h", "2 days" -> "2d", "last week" -> "7d", "24 hours" -> "24h", \
                    "1 hour" -> "1h", "30 minutes" -> "30m", "hour" -> "1h". \
                    "all" is NOT valid for timeRange. If the user says "all", skips, or gives an unclear duration \
                    -> value is "UNKNOWN".
                    - If the user says to skip or leave blank for non-time params -> value is "all"
                    - If you cannot determine a value for a parameter -> value is "UNKNOWN"

                    Respond in EXACTLY this format (one line per parameter, no extra text):
                    paramKey=value

                    Example response:
                    namespace=all
                    timeRange=36h
                    schema=all
                    """.formatted(paramDescriptions, userResponse);

            String response = chatModel.call(new Prompt(prompt))
                    .getResult().getOutput().getText().trim();

            logger.debug("LLM param extraction raw response: '{}'", response);

            Map<String, String> parsed = parseLlmKeyValueResponse(response);
            for (String key : missingKeys) {
                String value = parsed.get(key);
                if (value != null && !value.isEmpty() && !"UNKNOWN".equalsIgnoreCase(value)) {
                    value = switch (key) {
                        case "timeRange" -> {
                            String normalized = PlaybookRouter.normalizeTimeRange(value);
                            if (normalized == null) {
                                logger.warn("Follow-up returned invalid timeRange '{}' - will re-prompt", value);
                            }
                            yield normalized;
                        }
                        case "service" -> PlaybookRouter.normalizeService(value);
                        case "namespace" -> PlaybookRouter.normalizeNamespace(value);
                        case "schema" -> PlaybookRouter.normalizeSchema(value);
                        case "tables" -> PlaybookRouter.normalizeTables(value);
                        case "workload" -> PlaybookRouter.normalizeWorkload(value);
                        default -> value;
                    };
                    if (value == null) {
                        continue;
                    }
                    result.put(key, value);
                }
            }
        } catch (Exception e) {
            logger.warn("LLM param extraction failed: {} - will re-prompt user", e.getMessage());
        }

        return result;
    }


    /**
     * Parses a key=value (or key: value) response from the LLM.
     * Handles common format drift issues:
     * <ul>
     *   <li>Markdown code fences ({@code ```})</li>
     *   <li>Both {@code =} and {@code :} separators</li>
     *   <li>Surrounding quotes on values</li>
     *   <li>Leading bullets/dashes ({@code - key=value})</li>
     *   <li>Comment lines (starting with {@code #} or {@code //})</li>
     *   <li>Extra blank lines and whitespace</li>
     * </ul>
     *
     * @param rawResponse the raw LLM output
     * @return parsed key-value pairs (keys stored in camelCase-normalized form, values as-is)
     */
    static Map<String, String> parseLlmKeyValueResponse(String rawResponse) {
        Map<String, String> result = new LinkedHashMap<>();
        if (rawResponse == null || rawResponse.isBlank()) return result;

        // Strip markdown code fences
        String cleaned = rawResponse
                .replaceAll("```[a-z]*\\n?", "")
                .replaceAll("```", "")
                .trim();

        for (String line : cleaned.split("\n")) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("#") || line.startsWith("//")) continue;

            // Strip leading bullets/dashes
            line = line.replaceAll("^[-*-]\\s*", "");

            // Find the separator: prefer = over : (avoids splitting on "value: something")
            String key = null;
            String value = null;
            int eqIdx = line.indexOf('=');
            int colonIdx = line.indexOf(':');

            if (eqIdx > 0 && (colonIdx < 0 || eqIdx < colonIdx)) {
                key = line.substring(0, eqIdx).trim();
                value = line.substring(eqIdx + 1).trim();
            } else if (colonIdx > 0) {
                key = line.substring(0, colonIdx).trim();
                value = line.substring(colonIdx + 1).trim();
            }

            if (key == null || key.isEmpty() || value == null) continue;

            // Strip surrounding quotes from value
            if ((value.startsWith("\"") && value.endsWith("\""))
                    || (value.startsWith("'") && value.endsWith("'"))) {
                value = value.substring(1, value.length() - 1);
            }

            // Normalize key to camelCase to handle LLM case drift.
            // The LLM may return "Namespace", "TimeRange", "time_range", "SCHEMA" etc.
            // Callers expect exact camelCase keys: "namespace", "timeRange", "service", "schema".
            result.put(normalizeToCamelCase(key), value);
        }

        return result;
    }

    /**
     * Normalizes a key to camelCase. Handles common LLM case-drift patterns:
     * <ul>
     *   <li>"TimeRange", "TIMERANGE" -> "timeRange"</li>
     *   <li>"time_range", "TIME_RANGE" -> "timeRange"</li>
     *   <li>"Namespace", "NAMESPACE" -> "namespace"</li>
     *   <li>"service", "Service" -> "service"</li>
     * </ul>
     */
    /** Known compound param keys - maps lowercase form to expected camelCase. */
    private static final Map<String, String> KNOWN_KEYS = Map.of(
            "timerange", "timeRange",
            "namespace", "namespace",
            "service", "service",
            "schema", "schema",
            "tables", "tables",
            "workload", "workload",
            "defaultvalue", "defaultValue"
    );

    static String normalizeToCamelCase(String key) {
        if (key == null || key.isEmpty()) return key;

        // Handle snake_case: split on underscores, camelCase-join
        if (key.contains("_")) {
            String[] parts = key.toLowerCase().split("_");
            StringBuilder sb = new StringBuilder(parts[0]);
            for (int i = 1; i < parts.length; i++) {
                if (!parts[i].isEmpty()) {
                    sb.append(Character.toUpperCase(parts[i].charAt(0)));
                    sb.append(parts[i].substring(1));
                }
            }
            return sb.toString();
        }

        // Handle PascalCase/ALLCAPS: lowercase first char, preserve internal case boundaries
        // "TimeRange" -> "timeRange", "NAMESPACE" -> "namespace", "service" -> "service"
        if (key.equals(key.toUpperCase()) && key.length() > 1) {
            // ALL CAPS like "NAMESPACE" or "TIMERANGE" -> all lowercase, then lookup
            String lower = key.toLowerCase();
            return KNOWN_KEYS.getOrDefault(lower, lower);
        }

        // PascalCase like "TimeRange" -> "timeRange"
        String result = Character.toLowerCase(key.charAt(0)) + key.substring(1);
        // Final safety net: check known keys in case of unexpected casing
        return KNOWN_KEYS.getOrDefault(result.toLowerCase(), result);
    }


    /**
     * Builds a StreamResponse-compatible SSE event prompting the user for missing params.
     */
    private ServerSentEvent<String> buildParamPromptEvent(
            String playbookId, String playbookName, List<MissingParam> missing) {
        StringBuilder message = new StringBuilder();
        message.append("I can help investigate that!");
        message.append(" Just need a few details before I start:\n\n");

        for (MissingParam param : missing) {
            message.append("- **").append(param.key()).append("**");
            if (param.optional()) message.append(" *(optional)*");
            message.append(": ").append(param.prompt()).append("\n");
        }

        message.append("\nReply with values in order, separated by commas. Use `|` for multiple values in one field. Use `-` to skip optional fields.");

        return buildAssistantEvent(message.toString());
    }

    /** One assistant turn in the StreamResponse shape the ReAct path streams. */
    private ServerSentEvent<String> buildAssistantEvent(String message) {
        try {
            Map<String, Object> responseData = new LinkedHashMap<>();
            responseData.put("node", "agent");
            responseData.put("agentName", "orchestrator_agent");
            responseData.put("messageType", "assistant");
            responseData.put("content", message);
            responseData.put("chunk", message);
            responseData.put("promptTokens", null);
            responseData.put("completionTokens", null);
            String json = objectMapper.writeValueAsString(responseData);
            return ServerSentEvent.<String>builder().data(json).build();
        } catch (Exception e) {
            logger.error("Failed to serialize assistant event", e);
            return ServerSentEvent.<String>builder()
                    .data("{\"messageType\":\"assistant\",\"content\":\"" +
                            message.replace("\"", "\\\"").replace("\n", "\\n") + "\"}")
                    .build();
        }
    }

    /**
     * Persists a message to thread history asynchronously on a virtual thread.
     * Creates the thread document if it doesn't exist (via getOrCreateThread).
     */
    private void persistMessageAsync(ThreadKey threadKey, String content, Message.MessageRole role) {
        Thread.ofVirtual().start(() -> {
            try {
                threadStore.getOrCreateThread(threadKey);
                Message msg = new Message(
                        UUID.randomUUID().toString(),
                        threadKey.appName(),
                        threadKey.threadId(),
                        role,
                        content,
                        Instant.now(),
                        Map.of()
                );
                threadStore.appendMessage(threadKey, msg);
                logger.debug("Playbook persisted {} message for threadId={}", role, threadKey.threadId());
            } catch (Exception e) {
                logger.warn("Failed to persist playbook {} message for threadId={}: {}",
                        role, threadKey.threadId(), e.getMessage());
            }
        });
    }
}
