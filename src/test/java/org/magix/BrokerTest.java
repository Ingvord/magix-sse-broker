package org.magix;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micronaut.json.tree.JsonNode;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BrokerTest {

    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();

    private Broker broker(int bufferSize) {
        return new Broker(new MagixConfig() {
            public int getBufferSize() { return bufferSize; }
            public java.time.Duration getHeartbeat() { return java.time.Duration.ofSeconds(15); }
        }, metrics);
    }

    private static Flux<Integer> numbers(Broker broker) {
        return broker.subscribe().map(m -> m.payload().getIntValue());
    }

    private static void publishRange(Broker broker, int fromInclusive, int toExclusive) {
        for (int i = fromInclusive; i < toExclusive; i++) {
            broker.publish("c", JsonNode.createNumberNode(i));
        }
    }

    @Test
    void slowSubscriberKeepsOnlyNewestEventsAndCountsDrops() {
        var broker = broker(4);

        StepVerifier.create(numbers(broker), 0)
                .expectSubscription()
                .then(() -> publishRange(broker, 0, 10))
                .thenRequest(100)
                .expectNext(6, 7, 8, 9)
                .thenCancel()
                .verify(Duration.ofSeconds(5));

        assertEquals(10, metrics.counter("magix.events.published").count());
        assertEquals(6, metrics.counter("magix.events.dropped").count());
    }

    @Test
    void slowSubscriberDoesNotAffectFastSubscriber() throws Exception {
        var broker = broker(4);
        var slow = numbers(broker).subscribe(ignored -> {}, e -> {}, () -> {}, s -> s.request(0));
        var fast = numbers(broker).take(10).collectList().toFuture();

        publishRange(broker, 0, 10);

        assertEquals(IntStream.range(0, 10).boxed().toList(), fast.get(5, TimeUnit.SECONDS));
        slow.dispose();
    }

    @Test
    void eventsPublishedWithoutSubscribersAreNotReplayed() {
        var broker = broker(4);
        publishRange(broker, 0, 500);

        StepVerifier.create(numbers(broker))
                .expectSubscription()
                .then(() -> broker.publish("c", JsonNode.createNumberNode(1000)))
                .expectNext(1000)
                .thenCancel()
                .verify(Duration.ofSeconds(5));

        assertEquals(0, metrics.counter("magix.events.dropped").count());
    }
}
