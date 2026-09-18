package com.#exampleframe#.orchestrator.config;

import io.a2a.spec.AgentCard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/**
 * Rewrites the server self-{@link AgentCard} bean's name to the cell identity
 * ({@link CellAgentNames#cellCardName}) before anything consumes it.
 * <p>
 * {@code spring.ai.alibaba.a2a.server.card.name} is a static literal
 * ({@code orchestrator_agent}), and {@code A2aServerAgentCardAutoConfiguration}
 * builds the card bean from it. That bean has exactly two consumers:
 * {@code A2aServerRegistryAutoConfiguration#agentRegistryService} (registers the
 * card - and therefore its name - into Nacos) and
 * {@code A2aServerHandlerAutoConfiguration#jsonrpcHandler} (serves the card as the
 * server's self-description). Neither routes inbound messages by the card name -
 * the executor binds the {@code Agent} bean directly and our ingress routes on the
 * application name - so renaming the card changes only what the outside world sees,
 * which is the point: the registered name, the JSONRPC self-description, and our
 * {@code /.well-known/agent.json} all say {@code orchestrator_agent__<cell>}, the
 * name the meta's {@code remote-agents.<cell>.name} entry looks up.
 * <p>
 * Rewriting the bean (rather than defining our own card bean over the library's
 * {@code @ConditionalOnMissingBean}) keeps the library's card construction - URL,
 * skills, capabilities, transports - authoritative: we copy every field and change
 * one. A no-op on a meta or single-cell deployment ({@code cell-name} blank) and on
 * peer cards (peers come from {@code AgentCardProvider} lookups, never beans).
 * <p>
 * Reads {@link Environment} directly instead of {@code @Value}: a
 * {@link BeanPostProcessor} is instantiated before value resolution is guaranteed.
 */
@Component
public class CellAgentCardPostProcessor implements BeanPostProcessor {

    private static final Logger logger = LoggerFactory.getLogger(CellAgentCardPostProcessor.class);

    static final String CELL_NAME_PROPERTY = "#exampleframe#.orchestrator.federation.cell-name";

    private final Environment environment;

    public CellAgentCardPostProcessor(Environment environment) {
        this.environment = environment;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        if (!(bean instanceof AgentCard card)) {
            return bean;
        }
        String cellName = environment.getProperty(CELL_NAME_PROPERTY, "");
        String cellCardName = CellAgentNames.cellCardName(card.name(), cellName);
        if (cellCardName == null || cellCardName.equals(card.name())) {
            return bean; // not a named cell (meta / single), or already suffixed
        }
        logger.info("A2A self-card '{}' (bean '{}') renamed to '{}' for cell '{}' - this is the name "
                        + "registered in Nacos and served on the card endpoints",
                card.name(), beanName, cellCardName, cellName.trim());
        return new AgentCard.Builder()
                .name(cellCardName)
                .description(card.description())
                .url(card.url())
                .provider(card.provider())
                .version(card.version())
                .documentationUrl(card.documentationUrl())
                .capabilities(card.capabilities())
                .defaultInputModes(card.defaultInputModes())
                .defaultOutputModes(card.defaultOutputModes())
                .skills(card.skills())
                .supportsAuthenticatedExtendedCard(card.supportsAuthenticatedExtendedCard())
                .securitySchemes(card.securitySchemes())
                .security(card.security())
                .iconUrl(card.iconUrl())
                .additionalInterfaces(card.additionalInterfaces())
                .preferredTransport(card.preferredTransport())
                .protocolVersion(card.protocolVersion())
                .build();
    }
}
