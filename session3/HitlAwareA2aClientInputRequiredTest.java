package com.#exampleframe#.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.#exampleframe#.orchestrator.exception.CellInputRequiredException;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Locks the seam the adversarial review caught OPEN: {@code processResponse} threw
 * {@link CellInputRequiredException}, but {@code invokeAgent}'s catch-all wrapped it into a
 * plain RuntimeException, so downstream the relay became an error string and the whole
 * input_required loop was dead on the real wire path - while unit tests that mock the
 * delegation layer to throw directly all passed. This test drives the REAL client
 * (stubbed HTTP exchange only) end to end through invokeAgent.
 */
class HitlAwareA2aClientInputRequiredTest {

    private static HitlAwareA2aClient clientReturning(String body) {
        ExchangeFunction exchange = request -> Mono.just(
                ClientResponse.create(HttpStatus.OK)
                        .header("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                        .body(body)
                        .build());
        HitlAwareA2aClient client = new HitlAwareA2aClient(
                WebClient.builder().exchangeFunction(exchange), new ObjectMapper(), null, null);
        ReflectionTestUtils.setField(client, "timeoutSeconds", 5);
        ReflectionTestUtils.setField(client, "maxMemoryMb", 10);
        return client;
    }

    @Test
    void inputRequiredSurvivesInvokeAgentUnwrapped() {
        String body = """
                {"jsonrpc":"2.0","id":"r1","result":{
                  "status":"input_required","taskId":"a2a-claims-t1",
                  "playbookId":"service-slow","playbookName":"Service Performance Investigation",
                  "missingKeys":["schema"],
                  "missingParams":[{"key":"schema","prompt":"Which Oracle schema?"}],
                  "output":"This playbook needs values for: schema."}}
                """;
        assertThatThrownBy(() -> clientReturning(body)
                .invokeAgent("claims-cell", "http://localhost:8081", "why is esign slow", null))
                .isInstanceOf(CellInputRequiredException.class)
                .satisfies(t -> {
                    CellInputRequiredException e = (CellInputRequiredException) t;
                    assertThat(e.getMissingKeys()).containsExactly("schema");
                    assertThat(e.getMissingPrompts())
                            .containsExactly(Map.entry("schema", "Which Oracle schema?"));
                    assertThat(e.getOptionalKeys()).isEmpty();
                    assertThat(e.getPlaybookId()).isEqualTo("service-slow");
                    assertThat(e.getTaskId()).isEqualTo("a2a-claims-t1");
                });
    }

    /**
     * The cell's door offers still-empty OPTIONAL params in the same question; the relay
     * carries them as optionalKeys plus a per-prompt flag - either alone must suffice, and
     * the prompt order (the cell's playbook order) must survive the wire.
     */
    @Test
    void optionalKeysAreParsedFromTheListOrThePerPromptFlag() {
        String withList = """
                {"jsonrpc":"2.0","id":"r1","result":{
                  "status":"input_required","taskId":"a2a-claims-t2",
                  "playbookId":"service-slow","playbookName":"Service Performance Investigation",
                  "missingKeys":["schema"],"optionalKeys":["tables"],
                  "missingParams":[{"key":"schema","prompt":"Which Oracle schema?","optional":false},
                                   {"key":"tables","prompt":"Which database tables?","optional":true}],
                  "output":"This playbook needs values for: schema (optional: tables)."}}
                """;
        assertThatThrownBy(() -> clientReturning(withList)
                .invokeAgent("claims-cell", "http://localhost:8081", "why is esign slow", null))
                .isInstanceOf(CellInputRequiredException.class)
                .satisfies(t -> {
                    CellInputRequiredException e = (CellInputRequiredException) t;
                    assertThat(e.getMissingKeys()).containsExactly("schema");
                    assertThat(e.getOptionalKeys()).containsExactly("tables");
                    assertThat(e.getMissingPrompts().keySet()).containsExactly("schema", "tables");
                });

        String flagOnly = """
                {"jsonrpc":"2.0","id":"r1","result":{
                  "status":"input_required","taskId":"a2a-claims-t3",
                  "playbookId":"service-slow","missingKeys":["schema"],
                  "missingParams":[{"key":"schema","prompt":"Which Oracle schema?"},
                                   {"key":"tables","prompt":"Which database tables?","optional":"true"}]}}
                """;
        assertThatThrownBy(() -> clientReturning(flagOnly)
                .invokeAgent("claims-cell", "http://localhost:8081", "why is esign slow", null))
                .isInstanceOf(CellInputRequiredException.class)
                .satisfies(t -> assertThat(((CellInputRequiredException) t).getOptionalKeys())
                        .containsExactly("tables"));
    }

    @Test
    void completedResponsesStillExtractNormally() {
        String body = """
                {"jsonrpc":"2.0","id":"r1","result":{
                  "status":"completed","taskId":"t1",
                  "artifacts":[{"parts":[{"type":"text","text":"# RCA report"}]}]}}
                """;
        String out = clientReturning(body)
                .invokeAgent("claims-cell", "http://localhost:8081", "q", null);
        assertThat(out).isEqualTo("# RCA report");
    }

    @Test
    void oldCellsWithoutTheStatusAreUntouched() {
        // An old cell that hits missing params defers to ReAct and answers completed -
        // the new client must not misread anything about that shape.
        String body = """
                {"jsonrpc":"2.0","id":"r1","result":{
                  "status":"completed","taskId":"t1",
                  "artifacts":[{"parts":[{"type":"text","text":"react answer"}]}],
                  "missingKeys":["ignored-on-completed"]}}
                """;
        String out = clientReturning(body)
                .invokeAgent("claims-cell", "http://localhost:8081", "q", null);
        assertThat(out).isEqualTo("react answer");
    }
}
