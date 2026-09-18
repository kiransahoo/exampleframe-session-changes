package com.#exampleframe#.orchestrator.federation;

import com.#exampleframe#.orchestrator.config.PlaybookProperties.PlaybookDefinition;
import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Decides what a META orchestrator does with an explicit playbook request
 * ("run the service-slow playbook for payments-api"). Pure logic, no I/O:
 *
 * <ul>
 *   <li>no explicit playbook reference -> {@link Kind#PASS_THROUGH}: ordinary ReAct routing;</li>
 *   <li>reference + a service whose mapping names a domain with a configured cell ->
 *       {@link Kind#DELEGATE} to that cell (the cell runs the playbook itself);</li>
 *   <li>reference + a user-chosen domain (the answer to a previous ask) ->
 *       {@link Kind#DELEGATE} to that cell, keeping whatever service was named;</li>
 *   <li>otherwise -> {@link Kind#ASK_DOMAIN}: every cell could run the playbook, so the meta
 *       asks instead of guessing or fanning out.</li>
 * </ul>
 * Ownership comes only from the mapping (never a default domain), matching prompt rule 5b.
 */
public final class MetaPlaybookRouting {

    /**
     * taskParams marker for a deliberate meta playbook-door forward. The cell's delegated
     * door must not demote a marked turn to ReAct at its first-turn gate just because the
     * conversation touched the cell before; with no {@code playbookId} alongside it, the
     * cell still routes the verbatim text with its own router - the marker moves only the
     * gate, never the choice of playbook.
     */
    public static final String DETERMINISTIC_FORWARD_PARAM = "deterministicForward";

    /**
     * Capability declaration: the forwarding meta understands a {@code status: input_required}
     * answer (it relays the cell's param question to the user and re-forwards with the reply).
     * A cell must NEVER send input_required without seeing this - an older meta treats the
     * unknown status as completed and renders "Completed but no output extracted" instead of
     * falling back to ReAct, which is strictly worse than the pre-relay behavior. The marker
     * alone is not enough: pin-only third-party callers set it too.
     */
    public static final String ACCEPTS_INPUT_REQUIRED_PARAM = "acceptsInputRequired";

    public enum Kind { PASS_THROUGH, ASK_DOMAIN, DELEGATE }

    public record Decision(Kind kind,
                           @Nullable String playbookId,
                           @Nullable String playbookName,
                           @Nullable String service,
                           @Nullable String domain,
                           @Nullable String cellAgentKey,
                           @Nullable String message) {
    }

    private MetaPlaybookRouting() {}

    public static Decision decide(Map.@Nullable Entry<String, PlaybookDefinition> explicit,
                                  @Nullable String service,
                                  Optional<String> owningDomain,
                                  @Nullable String chosenDomain,
                                  Map<String, FederationProperties.DomainCell> cells) {
        if (explicit == null) {
            return new Decision(Kind.PASS_THROUGH, null, null, service, null, null, null);
        }
        String id = explicit.getKey();
        String name = explicit.getValue() != null && explicit.getValue().name() != null
                ? explicit.getValue().name() : id;
        String svc = service != null && !service.isBlank() ? service.trim() : null;

        if (svc != null && owningDomain.isPresent()) {
            String cellKey = agentKeyOf(cells, owningDomain.get());
            if (cellKey != null) {
                return new Decision(Kind.DELEGATE, id, name, svc, owningDomain.get(), cellKey, null);
            }
        }
        // The user's explicit domain choice covers an unmapped service too - they answered the
        // very question the missing mapping raised. The mapping, when present, wins above.
        if (chosenDomain != null) {
            String cellKey = agentKeyOf(cells, chosenDomain);
            if (cellKey != null) {
                return new Decision(Kind.DELEGATE, id, name, svc, chosenDomain, cellKey, null);
            }
        }
        return new Decision(Kind.ASK_DOMAIN, id, name, svc, null, null, askMessage(name, svc, cells.keySet()));
    }

    /** The configured cell agent key for a domain, or null when the domain has no usable cell. */
    public static @Nullable String agentKeyOf(Map<String, FederationProperties.DomainCell> cells, String domain) {
        FederationProperties.DomainCell cell = cells.get(domain);
        if (cell == null || cell.agentKey() == null || cell.agentKey().isBlank()) {
            return null;
        }
        return cell.agentKey().trim();
    }

    /** The one question the meta asks instead of guessing a domain. */
    static String askMessage(String playbookName, @Nullable String service, Collection<String> domains) {
        String known = domains.isEmpty() ? "none configured" : String.join(", ", new TreeSet<>(domains));
        StringBuilder sb = new StringBuilder();
        sb.append("You asked me to run the **").append(playbookName).append("** playbook, but ");
        if (service == null) {
            sb.append("no service was named");
        } else {
            sb.append("`").append(service).append("` has no owning domain in the service mapping");
        }
        sb.append(". Every domain cell can run it, so I won't guess.\n\n")
          .append("**Which domain should run it?** Known domains: ").append(known).append(".\n\n")
          .append("Reply with the service to investigate (for example `payments-api`) or the domain name.");
        return sb.toString();
    }

    /**
     * The single configured domain named in {@code text} as a standalone word - "payments" in
     * "payments please", not in "payments-api". Null when none or more than one is named.
     */
    public static @Nullable String mentionedDomain(@Nullable String text, Collection<String> domains) {
        if (text == null || text.isBlank()) {
            return null;
        }
        String found = null;
        for (String domain : domains) {
            Pattern p = Pattern.compile("(?<![\\w-])" + Pattern.quote(domain) + "(?![\\w-])",
                    Pattern.CASE_INSENSITIVE);
            if (p.matcher(text).find()) {
                if (found != null) {
                    return null;
                }
                found = domain;   // the key as configured - decide() looks cells up by it
            }
        }
        return found;
    }

    /** Tool name the meta's delegation tools use for a cell key, e.g. {@code claims-cell -> delegateToClaimsCell}. */
    public static String toolNameFor(String cellAgentKey) {
        StringBuilder sb = new StringBuilder("delegateTo");
        for (String part : cellAgentKey.split("[-_ ]+")) {
            if (part.isEmpty()) continue;
            sb.append(Character.toUpperCase(part.charAt(0))).append(part.substring(1));
        }
        return sb.toString();
    }
}
