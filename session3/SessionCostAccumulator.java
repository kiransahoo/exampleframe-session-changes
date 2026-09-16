package com.#exampleframe#.orchestrator.observation.cost;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.anthropic.api.AnthropicApi;
import org.springframework.ai.chat.metadata.Usage;
import org.springframework.ai.openai.api.OpenAiApi;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory per-session (threadId) cost accumulator.
 * <p>
 * Tracks real token counts and estimated costs for each chat session.
 * Data lives only in memory and is lost on restart - this is intentional
 * for lightweight cost visibility without requiring Prometheus or a database.
 */
@Component
public class SessionCostAccumulator {

    private static final Logger logger = LoggerFactory.getLogger(SessionCostAccumulator.class);
    private static final BigDecimal MILLION = new BigDecimal("1000000");

    private final ConcurrentHashMap<String, SessionCost> sessions = new ConcurrentHashMap<>();
    private final ModelPricingProperties pricingProperties;

    public SessionCostAccumulator(@Nullable ModelPricingProperties pricingProperties) {
        this.pricingProperties = pricingProperties;
    }

    /**
     * ThreadLocal to track the current thread/session ID for observation filters.
     * Set by the controller before LLM calls, cleared after.
     */
    private static final ThreadLocal<String> currentThreadId = new ThreadLocal<>();

    /**
     * Set the current thread ID for observation-based cost tracking.
     * Call this before executing agent/LLM calls so the ObservationFilter
     * can attribute costs to the correct session.
     */
    public static void setCurrentThreadId(@Nullable String threadId) {
        if (threadId != null) {
            currentThreadId.set(threadId);
        }
    }

    /** Clear the current thread ID. Call in a finally block. */
    public static void clearCurrentThreadId() {
        currentThreadId.remove();
    }

    /** Get the current thread ID (used by ObservationFilter). */
    public static @Nullable String getCurrentThreadId() {
        return currentThreadId.get();
    }

    /**
     * Runs {@code work} with the ThreadLocal bound to {@code threadId}, restoring whatever
     * this thread carried before - so an enclosing binding (the A2A ingress binds a whole
     * request) survives a nested one, and a pooled thread never keeps a stale id.
     */
    public static <T> T withThreadId(@Nullable String threadId, java.util.function.Supplier<T> work) {
        if (threadId == null) {
            return work.get();     // no identity to bind - inherit the enclosing binding, never blank it
        }
        String previous = currentThreadId.get();
        currentThreadId.set(threadId);
        try {
            return work.get();
        } finally {
            if (previous != null) {
                currentThreadId.set(previous);
            } else {
                currentThreadId.remove();
            }
        }
    }

    /**
     * Record token usage for a session.
     * Skips empty usage (0/0 tokens) which occurs on every streaming chunk.
     *
     * @param threadId   The session/thread ID
     * @param modelName  The model that was used
     * @param usage      Token usage from the model response
     */
    public void recordUsage(@NonNull String threadId, @Nullable String modelName, @NonNull Usage usage) {
        long inputTokens = usage.getPromptTokens() != null ? usage.getPromptTokens().longValue() : 0;
        long outputTokens = usage.getCompletionTokens() != null ? usage.getCompletionTokens().longValue() : 0;

        if (inputTokens == 0 && outputTokens == 0) {
            return; // Skip empty usage from streaming chunks
        }

        SessionCost session = sessions.computeIfAbsent(threadId, k -> new SessionCost());
        session.addUsage(inputTokens, outputTokens);

        // Calculate cost for this call if pricing is available
        // Handles cache tokens the same way CostCalculationService does
        if (pricingProperties != null && modelName != null) {
            ModelPricingProperties.ModelPrice pricing = pricingProperties.getModelPrice(modelName);
            if (pricing != null) {
                // Extract cache tokens from native usage (provider-specific)
                long cacheReadTokens = 0;
                long cacheWriteTokens = 0;
                long regularInputTokens = inputTokens;

                Object nativeUsage = usage.getNativeUsage();
                if (nativeUsage instanceof AnthropicApi.Usage anthropicUsage) {
                    cacheReadTokens = anthropicUsage.cacheReadInputTokens() != null
                            ? anthropicUsage.cacheReadInputTokens() : 0;
                    cacheWriteTokens = anthropicUsage.cacheCreationInputTokens() != null
                            ? anthropicUsage.cacheCreationInputTokens() : 0;
                    regularInputTokens = inputTokens - cacheReadTokens - cacheWriteTokens;
                } else if (nativeUsage instanceof OpenAiApi.Usage openAiUsage) {
                    if (openAiUsage.promptTokensDetails() != null
                            && openAiUsage.promptTokensDetails().cachedTokens() != null) {
                        cacheReadTokens = openAiUsage.promptTokensDetails().cachedTokens();
                        regularInputTokens = inputTokens - cacheReadTokens;
                    }
                }

                // Ensure no negative values from provider quirks
                if (regularInputTokens < 0) regularInputTokens = 0;

                // Cost = regular input + cache read + cache write + output
                BigDecimal inputCost = BigDecimal.valueOf(regularInputTokens)
                        .multiply(pricing.inputPricePerMillion())
                        .divide(MILLION, 6, RoundingMode.HALF_UP);
                BigDecimal outputCost = BigDecimal.valueOf(outputTokens)
                        .multiply(pricing.outputPricePerMillion())
                        .divide(MILLION, 6, RoundingMode.HALF_UP);

                BigDecimal cacheReadCost = BigDecimal.ZERO;
                BigDecimal cacheWriteCost = BigDecimal.ZERO;
                if (pricing.hasCachePricing()) {
                    if (cacheReadTokens > 0 && pricing.cacheReadPricePerMillion() != null) {
                        cacheReadCost = BigDecimal.valueOf(cacheReadTokens)
                                .multiply(pricing.cacheReadPricePerMillion())
                                .divide(MILLION, 6, RoundingMode.HALF_UP);
                    }
                    if (cacheWriteTokens > 0 && pricing.cacheWritePricePerMillion() != null) {
                        cacheWriteCost = BigDecimal.valueOf(cacheWriteTokens)
                                .multiply(pricing.cacheWritePricePerMillion())
                                .divide(MILLION, 6, RoundingMode.HALF_UP);
                    }
                }

                session.addCost(inputCost.add(outputCost).add(cacheReadCost).add(cacheWriteCost));

                if (cacheReadTokens > 0 || cacheWriteTokens > 0) {
                    logger.info("Session {} cost update (cache): tokens={}/{}, cached={}/{}, totalCost={}, llmCalls={}",
                            threadId, session.totalInputTokens.get(), session.totalOutputTokens.get(),
                            cacheReadTokens, cacheWriteTokens, session.totalCost, session.llmCalls.get());
                    return;
                }
            }
        }

        logger.info("Session {} cost update: tokens={}/{}, totalCost={}, llmCalls={}",
                threadId, session.totalInputTokens.get(), session.totalOutputTokens.get(),
                session.totalCost, session.llmCalls.get());
    }

    /**
     * Get the accumulated cost snapshot for a session.
     */
    public @NonNull SessionCostSnapshot getSnapshot(@NonNull String threadId) {
        SessionCost session = sessions.get(threadId);
        if (session == null) {
            return SessionCostSnapshot.EMPTY;
        }
        return new SessionCostSnapshot(
                session.totalInputTokens.get(),
                session.totalOutputTokens.get(),
                session.llmCalls.get(),
                session.totalCost.toPlainString()
        );
    }

    /**
     * Per-child watermarks for the non-isolated (cumulative) mode.
     * Keyed by "orchestratorThreadId::childAgentName".
     * Only used when task isolation is disabled and the child's snapshot is cumulative.
     */
    private final ConcurrentHashMap<String, ChildCostWatermark> childWatermarks = new ConcurrentHashMap<>();

    /**
     * Record cost data from a child agent's A2A response.
     * <p>
     * Two modes based on the child's task isolation setting:
     * <ul>
     *   <li><b>isolated=true</b> (default): Each A2A call gets a fresh per-task threadId,
     *       so the snapshot IS the exact cost for that single task. Add directly.</li>
     *   <li><b>isolated=false</b>: The child reuses the orchestrator threadId, so its
     *       snapshot is cumulative across calls. We compute the delta against a watermark
     *       to avoid double-counting.</li>
     * </ul>
     *
     * @param threadId      The orchestrator session/thread ID
     * @param childAgent    The child agent name (for logging and watermark keying)
     * @param inputTokens   Child's input tokens (per-task if isolated, cumulative if not)
     * @param outputTokens  Child's output tokens
     * @param llmCalls      Child's LLM call count
     * @param totalCost     Child's total cost as a string
     * @param isolated      Whether the child used per-task isolation
     */
    public void recordChildAgentCost(@NonNull String threadId, @NonNull String childAgent,
                                     long inputTokens, long outputTokens,
                                     int llmCalls, @Nullable String totalCost, boolean isolated) {
        BigDecimal costValue = BigDecimal.ZERO;
        if (totalCost != null) {
            try {
                costValue = new BigDecimal(totalCost);
            } catch (NumberFormatException e) {
                logger.warn("Invalid cost value from child agent {}: {}", childAgent, totalCost);
            }
        }

        long deltaInput, deltaOutput;
        int deltaCalls;
        BigDecimal deltaCost;

        if (isolated) {
            // Isolated mode: snapshot is per-task, add directly
            deltaInput = inputTokens;
            deltaOutput = outputTokens;
            deltaCalls = llmCalls;
            deltaCost = costValue;
        } else {
            // Non-isolated mode: snapshot is cumulative, compute delta against watermark
            String watermarkKey = threadId + "::" + childAgent;
            ChildCostWatermark prev = childWatermarks.put(watermarkKey,
                    new ChildCostWatermark(inputTokens, outputTokens, llmCalls, costValue));

            deltaInput = inputTokens - (prev != null ? prev.inputTokens : 0);
            deltaOutput = outputTokens - (prev != null ? prev.outputTokens : 0);
            deltaCalls = llmCalls - (prev != null ? prev.llmCalls : 0);
            deltaCost = costValue.subtract(prev != null ? prev.totalCost : BigDecimal.ZERO);

            if (deltaInput <= 0 && deltaOutput <= 0 && deltaCalls <= 0) {
                logger.debug("Session {} child {} - no new cost delta, skipping", threadId, childAgent);
                return;
            }
        }

        SessionCost session = sessions.computeIfAbsent(threadId, k -> new SessionCost());
        if (deltaInput > 0) session.totalInputTokens.addAndGet(deltaInput);
        if (deltaOutput > 0) session.totalOutputTokens.addAndGet(deltaOutput);
        if (deltaCalls > 0) session.llmCalls.addAndGet(deltaCalls);
        if (deltaCost.compareTo(BigDecimal.ZERO) > 0) session.addCost(deltaCost);

        logger.debug("Session {} child {} cost (isolated={}): delta={}/{}/{}/{}, raw={}/{}/{}/{}",
                threadId, childAgent, isolated,
                deltaInput, deltaOutput, deltaCalls, deltaCost,
                inputTokens, outputTokens, llmCalls, costValue);
    }

    private record ChildCostWatermark(long inputTokens, long outputTokens, int llmCalls, BigDecimal totalCost) {}

    /**
     * Reset a session's cost tracking (e.g., on [RESET] button).
     */
    public void resetSession(@NonNull String threadId) {
        sessions.remove(threadId);
        // Clear watermarks for non-isolated child agents under this session
        childWatermarks.keySet().removeIf(key -> key.startsWith(threadId + "::"));
    }

    /**
     * Clean up old sessions to prevent memory leaks.
     * Called periodically or when sessions exceed a threshold.
     */
    public void evictOldSessions(int maxSessions) {
        if (sessions.size() > maxSessions) {
            // Simple eviction: remove entries beyond the limit
            int toRemove = sessions.size() - maxSessions;
            var iterator = sessions.entrySet().iterator();
            while (iterator.hasNext() && toRemove > 0) {
                iterator.next();
                iterator.remove();
                toRemove--;
            }
        }
    }

    /**
     * Mutable accumulator for a single session.
     */
    private static class SessionCost {
        final AtomicLong totalInputTokens = new AtomicLong(0);
        final AtomicLong totalOutputTokens = new AtomicLong(0);
        final AtomicInteger llmCalls = new AtomicInteger(0);
        volatile BigDecimal totalCost = BigDecimal.ZERO;

        void addUsage(long inputTokens, long outputTokens) {
            totalInputTokens.addAndGet(inputTokens);
            totalOutputTokens.addAndGet(outputTokens);
            llmCalls.incrementAndGet();
        }

        synchronized void addCost(BigDecimal cost) {
            totalCost = totalCost.add(cost);
        }
    }

    /**
     * Immutable snapshot of session cost data, safe to serialize to JSON.
     */
    public record SessionCostSnapshot(
            long inputTokens,
            long outputTokens,
            int llmCalls,
            String totalCost
    ) {
        public static final SessionCostSnapshot EMPTY = new SessionCostSnapshot(0, 0, 0, "0.000000");

        public long totalTokens() {
            return inputTokens + outputTokens;
        }
    }
}
