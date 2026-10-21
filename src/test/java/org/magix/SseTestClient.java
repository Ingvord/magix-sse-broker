package org.magix;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Minimal SSE consumer built on the JDK only, so tests do not depend on any Micronaut client.
 * Comment lines (starting with ':') are ignored; an event is dispatched on each blank line.
 */
final class SseTestClient implements AutoCloseable {

    record Event(String name, String id, String data) {}

    private final BlockingQueue<Event> events = new LinkedBlockingQueue<>();
    private final HttpClient http = HttpClient.newHttpClient();
    private final URI uri;
    private final Thread reader;
    private volatile InputStream body;

    SseTestClient(URI uri) {
        this.uri = uri;
        reader = new Thread(this::read, "sse-test-client");
        reader.setDaemon(true);
        reader.start();
    }

    private void read() {
        var request = HttpRequest.newBuilder(uri).header("Accept", "text/event-stream").build();
        try {
            var response = http.send(request, HttpResponse.BodyHandlers.ofInputStream());
            body = response.body();
            try (var in = new BufferedReader(new InputStreamReader(body, StandardCharsets.UTF_8))) {
                parse(in);
            }
        } catch (IOException | InterruptedException ignored) {
            // closed by the test or the server
        }
    }

    private void parse(BufferedReader in) throws IOException {
        String name = null, id = null;
        var data = new StringBuilder();
        String line;
        while ((line = in.readLine()) != null) {
            if (line.isEmpty()) {
                if (data.length() > 0 || name != null) {
                    events.add(new Event(name, id, data.toString()));
                }
                name = null;
                id = null;
                data.setLength(0);
            } else if (line.startsWith("event:")) {
                name = line.substring(6).trim();
            } else if (line.startsWith("id:")) {
                id = line.substring(3).trim();
            } else if (line.startsWith("data:")) {
                if (data.length() > 0) data.append('\n');
                data.append(line.substring(5).stripLeading());
            }
        }
    }

    /** Next event, or null if none arrives within the timeout. */
    Event next(long timeoutMillis) throws InterruptedException {
        return events.poll(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        reader.interrupt();
        var in = body;
        if (in != null) {
            try {
                in.close();
            } catch (IOException ignored) {
                // nothing to do
            }
        }
    }
}
