package com.#exampleframe#.orchestrator.config;

import io.a2a.spec.AgentCapabilities;
import io.a2a.spec.AgentCard;
import io.a2a.spec.AgentInterface;
import io.a2a.spec.AgentProvider;
import io.a2a.spec.AgentSkill;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import java.lang.reflect.RecordComponent;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * The registered self-card must carry the cell identity, and the rename must change
 * NOTHING else. The field-preservation test walks {@link AgentCard}'s record
 * components reflectively, so an SDK upgrade that adds a field our copy misses
 * fails here instead of silently registering a card with that field nulled.
 */
class CellAgentCardPostProcessorTest {

    private static AgentCard fullCard() {
        // Every field populated with a distinctive value - a dropped field cannot
        // masquerade as a legitimately-null one.
        return new AgentCard.Builder()
                .name("orchestrator_agent")
                .description("cell under test")
                .url("http://localhost:8081/a2a/message")
                .provider(new AgentProvider("#ExampleFrame#", "http://tb.example"))
                .version("1.0.0")
                .documentationUrl("http://tb.example/docs")
                .capabilities(new AgentCapabilities.Builder().streaming(true).build())
                .defaultInputModes(List.of("text"))
                .defaultOutputModes(List.of("text"))
                .skills(List.of(new AgentSkill.Builder()
                        .id("rca").name("root cause analysis").description("rca")
                        .tags(List.of("rca")).build()))
                .supportsAuthenticatedExtendedCard(true)
                .securitySchemes(Map.of())
                .security(List.of())
                .iconUrl("http://tb.example/icon.png")
                .additionalInterfaces(List.of(new AgentInterface("JSONRPC", "http://localhost:8081/a2a/message")))
                .preferredTransport("JSONRPC")
                .protocolVersion("0.2.5")
                .build();
    }

    private static Object process(String cellName, Object bean) {
        MockEnvironment env = new MockEnvironment();
        if (cellName != null) {
            env.setProperty(CellAgentCardPostProcessor.CELL_NAME_PROPERTY, cellName);
        }
        return new CellAgentCardPostProcessor(env).postProcessAfterInitialization(bean, "agentCard");
    }

    @Test
    void cellDeploymentRegistersSuffixedName() {
        AgentCard renamed = (AgentCard) process("claims", fullCard());
        assertEquals("orchestrator_agent__claims", renamed.name());
    }

    @Test
    void everyFieldExceptNameSurvivesTheRename() throws Exception {
        AgentCard original = fullCard();
        AgentCard renamed = (AgentCard) process("claims", original);
        for (RecordComponent component : AgentCard.class.getRecordComponents()) {
            Object before = component.getAccessor().invoke(original);
            Object after = component.getAccessor().invoke(renamed);
            if ("name".equals(component.getName())) {
                assertNotEquals(before, after, "name must change");
            } else {
                assertEquals(before, after,
                        "field '" + component.getName() + "' must survive the rename - "
                        + "if this fails after an SDK upgrade, CellAgentCardPostProcessor "
                        + "must copy the new field");
            }
        }
    }

    @Test
    void metaOrSingleDeploymentIsUntouched() {
        AgentCard card = fullCard();
        assertSame(card, process(null, card), "blank cell-name must be a no-op");
        assertSame(card, process("  ", card), "whitespace cell-name must be a no-op");
    }

    @Test
    void alreadySuffixedCardIsUntouched() {
        AgentCard card = (AgentCard) process("claims", fullCard());
        assertSame(card, process("claims", card), "second pass must not double-suffix");
    }

    @Test
    void nonCardBeansPassThrough() {
        Object bean = new Object();
        assertSame(bean, process("claims", bean));
    }

    @Test
    void derivationMatchesTheWellKnownEndpointsDerivation() {
        // The registered name and /.well-known/agent.json both use CellAgentNames -
        // this pins the shared derivation itself.
        assertEquals("orchestrator_agent__claims",
                CellAgentNames.cellCardName("orchestrator_agent", "claims"));
        assertEquals("orchestrator_agent",
                CellAgentNames.cellCardName("orchestrator_agent", ""));
        assertEquals("orchestrator_agent",
                CellAgentNames.cellCardName("orchestrator_agent", null));
        assertEquals("orchestrator_agent__claims",
                CellAgentNames.cellCardName("orchestrator_agent", " claims "));
        assertEquals("orchestrator_agent__claims",
                CellAgentNames.cellCardName("orchestrator_agent__claims", "claims"));
    }
}
