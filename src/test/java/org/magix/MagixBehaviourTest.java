package org.magix;

import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.net.Socket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Pins down the externally visible behaviour of the broker. Every test gets a fresh application
 * context (and so a fresh broker), because some scenarios break the broker for later tests.
 */
@MicronautTest(rebuildContext = true)
class MagixBehaviourTest {

    private static final long TIMEOUT_MS = 5_000;
    /** Time for a just-opened subscription to be registered; the broker gives no signal for it. */
    private static final long SETTLE_MS = 500;

    @Inject
    EmbeddedServer server;

    private final HttpClient http = HttpClient.newHttpClient();

    private URI uri(String path) {
        return URI.create(server.getURL() + path);
    }

    /** Returns the HTTP status, or -1 if the request failed or timed out. */
    private int broadcast(String channel, String json) throws Exception {
        var path = "/magix/api/broadcast" + (channel == null ? "" : "?channel=" + channel);
        var request = HttpRequest.newBuilder(uri(path))
                .header("Content-Type", "application/json")
                .timeout(java.time.Duration.ofSeconds(5))
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();
        try {
            return http.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
        } catch (java.io.IOException e) {
            return -1;
        }
    }

    private SseTestClient subscribe() throws Exception {
        var client = new SseTestClient(uri("/magix/api/subscribe"));
        Thread.sleep(SETTLE_MS);
        return client;
    }

    @Test
    void subscriberReceivesBroadcast() throws Exception {
        try (var sub = subscribe()) {
            broadcast("greetings", "{\"hello\":\"world\"}");

            var event = sub.next(TIMEOUT_MS);
            assertNotNull(event, "no event received");
            assertEquals("{\"hello\":\"world\"}", event.data());
        }
    }

    @Test
    void eventNameEqualsChannel() throws Exception {
        try (var sub = subscribe()) {
            broadcast("sensors", "{\"n\":1}");
            broadcast(null, "{\"n\":2}");

            var named = sub.next(TIMEOUT_MS);
            assertNotNull(named, "no event received");
            assertEquals("sensors", named.name());

            var defaulted = sub.next(TIMEOUT_MS);
            assertNotNull(defaulted, "no event received for default channel");
            assertEquals("message", defaulted.name());
        }
    }

    @Test
    void twoSubscribersBothReceiveBroadcast() throws Exception {
        try (var first = subscribe(); var second = subscribe()) {
            broadcast("shared", "{\"n\":1}");

            var a = first.next(TIMEOUT_MS);
            var b = second.next(TIMEOUT_MS);
            assertNotNull(a, "first subscriber got nothing");
            assertNotNull(b, "second subscriber got nothing");
            assertEquals(a.data(), b.data());
        }
    }

    /** Issue 1: broadcasts made while nobody listens must not break the stream for later subscribers. */
    @Test
    void broadcastsWithoutSubscribersDoNotBreakLaterSubscribers() throws Exception {
        for (int i = 0; i < 200; i++) {
            broadcast("early", "{\"n\":" + i + "}");
        }

        try (var sub = subscribe()) {
            broadcast("late", "{\"after\":true}");

            var event = sub.next(TIMEOUT_MS);
            assertNotNull(event, "subscriber got nothing after broadcasts with zero subscribers");
            assertEquals("late", event.name());
        }
    }

    /** Issue 2: one client that never reads must not stop delivery to the others. */
    @Test
    void stalledSubscriberDoesNotBlockHealthySubscriber() throws Exception {
        var payload = "{\"blob\":\"" + "x".repeat(32 * 1024) + "\"}";

        try (var stalled = new Socket(server.getHost(), server.getPort())) {
            stalled.setReceiveBufferSize(1024);
            OutputStream out = stalled.getOutputStream();
            out.write(("GET /magix/api/subscribe HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n")
                    .getBytes(StandardCharsets.US_ASCII));
            out.flush();
            // never read from `stalled`

            try (var healthy = subscribe()) {
                for (int i = 0; i < 300; i++) {
                    broadcast("bulk", payload);
                }
                broadcast("marker", "{\"last\":true}");

                SseTestClient.Event event;
                boolean sawMarker = false;
                while ((event = healthy.next(TIMEOUT_MS)) != null) {
                    if ("marker".equals(event.name())) {
                        sawMarker = true;
                        break;
                    }
                }
                assertEquals(true, sawMarker, "healthy subscriber never received the marker event");
            }
        }
    }
}
