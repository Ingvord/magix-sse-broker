package org.magix;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micronaut.json.tree.JsonNode;
import jakarta.inject.Singleton;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory fan-out. Nothing is retained for late subscribers, and every subscriber has its own
 * bounded buffer that drops its oldest events, so a slow client never delays the others.
 */
@Singleton
public class Broker {

    private final Sinks.Many<Message> sink = Sinks.many().multicast().directBestEffort();
    private final AtomicLong ids = new AtomicLong();
    private final int bufferSize;
    private final Counter published;
    private final Counter dropped;

    public Broker(MagixConfig config, MeterRegistry metrics) {
        this.bufferSize = config.getBufferSize();
        this.published = metrics.counter("magix.events.published");
        this.dropped = metrics.counter("magix.events.dropped");
    }

    public Message publish(String channel, JsonNode payload) {
        var message = new Message(ids.incrementAndGet(), channel, Instant.now(), payload);
        synchronized (sink) { // tryEmitNext must not be called concurrently
            sink.tryEmitNext(message);
        }
        published.increment();
        return message;
    }

    public Flux<Message> subscribe() {
        return sink.asFlux()
                .onBackpressureBuffer(bufferSize, overflow -> dropped.increment(), BufferOverflowStrategy.DROP_OLDEST);
    }
}
