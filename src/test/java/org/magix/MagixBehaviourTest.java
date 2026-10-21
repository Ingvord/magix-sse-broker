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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins down the externally visible behaviour of the broker. Every test gets a fresh application
 * context (and so a fresh broker), because some scenarios break the broker for later tests.
 */
@MicronautTest(rebuildContext = true)
class MagixBehaviourTest {

    private static final long TIMEOUT_MS = 5_000;

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
        return subscribe(null);
    }

    private SseTestClient subscribe(String channels) throws Exception {
        return subscribeTo("/magix/api/subscribe" + (channels == null ? "" : "?channel=" + channels));
    }

    private SseTestClient subscribeTo(String path) throws Exception {
        var client = new SseTestClient(uri(path));
        // the first keepalive is sent once the subscription is registered
        assertNotNull(client.nextComment(TIMEOUT_MS), "subscription was not confirmed");
        return client;
    }

    /** Issue 3: idle connections need regular traffic, and clients need a first byte right away. */
    @Test
    void idleSubscriptionReceivesPeriodicKeepalives() throws Exception {
        try (var sub = subscribe()) { // already waited for the initial keepalive
            assertNotNull(sub.nextComment(2_000), "no second keepalive while idle");
            assertNotNull(sub.nextComment(2_000), "no third keepalive while idle");
            assertNull(sub.next(0), "keepalives must not surface as events");
        }
    }

    @Test
    void subscriptionResponseDisablesCachingAndProxyBuffering() throws Exception {
        try (var sub = subscribe()) {
            var headers = sub.headers(TIMEOUT_MS);
            assertNotNull(headers);
            assertEquals("text/event-stream", headers.firstValue("Content-Type").orElse(null));
            assertEquals("no-cache", headers.firstValue("Cache-Control").orElse(null));
            assertEquals("no", headers.firstValue("X-Accel-Buffering").orElse(null));
        }
    }

    @Test
    void channelWithLineBreakIsRejected() throws Exception {
        assertEquals(400, broadcast("a%0Aevent:%20forged", "{}"));
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

    /** Issue 5: subscribers can restrict themselves to some channels; no filter means all. */
    @Test
    void subscriberOnlyReceivesRequestedChannels() throws Exception {
        try (var onlyA = subscribe("a"); var aAndB = subscribe("a,b"); var all = subscribe()) {
            broadcast("c", "{\"n\":0}");
            broadcast("a", "{\"n\":1}");
            broadcast("b", "{\"n\":2}");

            assertEquals("a", onlyA.next(TIMEOUT_MS).name());
            assertNull(onlyA.next(300), "channel b leaked to a subscriber of a");

            assertEquals("a", aAndB.next(TIMEOUT_MS).name());
            assertEquals("b", aAndB.next(TIMEOUT_MS).name());

            assertEquals("c", all.next(TIMEOUT_MS).name());
        }
    }

    /** Issue 7: every message carries an id; the full envelope is available on request. */
    @Test
    void eventsCarryIncreasingIds() throws Exception {
        try (var sub = subscribe()) {
            broadcast("a", "{\"n\":1}");
            broadcast("a", "{\"n\":2}");

            var first = sub.next(TIMEOUT_MS);
            var second = sub.next(TIMEOUT_MS);
            assertNotNull(first);
            assertNotNull(second);
            assertEquals("{\"n\":1}", first.data());
            assertEquals(Long.parseLong(first.id()) + 1, Long.parseLong(second.id()));
        }
    }

    @Test
    void envelopeOptInWrapsPayloadWithIdChannelAndTimestamp() throws Exception {
        try (var sub = subscribeTo("/magix/api/subscribe?envelope=true")) {
            broadcast("sensors", "{\"temp\":21.5}");

            var event = sub.next(TIMEOUT_MS);
            assertNotNull(event);
            assertEquals("sensors", event.name());
            var data = event.data();
            assertTrue(data.contains("\"id\":" + event.id()), data);
            assertTrue(data.contains("\"channel\":\"sensors\""), data);
            assertTrue(data.contains("\"payload\":{\"temp\":21.5}"), data);
            assertTrue(data.matches(".*\"timestamp\":\"\\d{4}-\\d{2}-\\d{2}T[^\"]+Z\".*"), data);
        }
    }

    /** Issue 6: broadcast is fire-and-forget, so it answers 202 with the id it assigned. */
    @Test
    void broadcastAnswersAcceptedWithAssignedId() throws Exception {
        try (var sub = subscribe()) {
            var request = HttpRequest.newBuilder(uri("/magix/api/broadcast?channel=a"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("{\"n\":1}"))
                    .build();
            var response = http.send(request, HttpResponse.BodyHandlers.ofString());

            var event = sub.next(TIMEOUT_MS);
            assertNotNull(event);
            assertEquals(202, response.statusCode());
            assertEquals("{\"id\":" + event.id() + "}", response.body());
        }
    }

    private double metric(String name) throws Exception {
        var response = http.send(HttpRequest.newBuilder(uri("/metrics/" + name)).build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), response.body());
        var m = java.util.regex.Pattern.compile("\"value\":([0-9.eE+-]+)").matcher(response.body());
        assertTrue(m.find(), response.body());
        return Double.parseDouble(m.group(1));
    }

    @Test
    void metricsExposeSubscribersPublishedAndDroppedEvents() throws Exception {
        assertEquals(0, metric("magix.subscribers.active"));
        try (var sub = subscribe()) {
            assertEquals(1, metric("magix.subscribers.active"));
            broadcast("a", "{}");
            broadcast("a", "{}");
            assertNotNull(sub.next(TIMEOUT_MS));
            assertEquals(2, metric("magix.events.published"));
            assertEquals(0, metric("magix.events.dropped"));
        }
    }

    @Test
    void healthEndpointIsUp() throws Exception {
        var response = http.send(HttpRequest.newBuilder(uri("/health")).build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"UP\""), response.body());
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
