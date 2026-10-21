package org.magix;

import io.micronaut.http.HttpHeaders;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.*;
import io.micronaut.json.JsonMapper;
import io.micronaut.json.tree.JsonNode;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

@Controller("/magix/api")
public class Magix {

    private final Broker broker;
    private final MagixConfig config;
    private final JsonMapper json;

    public Magix(Broker broker, MagixConfig config, JsonMapper json) {
        this.broker = broker;
        this.config = config;
        this.json = json;
    }

    @Get("/subscribe")
    @Produces(MediaType.TEXT_EVENT_STREAM)
    public HttpResponse<Flux<byte[]>> subscribe(
            @QueryValue(defaultValue = "") String channel,
            @QueryValue(defaultValue = "false") boolean envelope){
        Flux<byte[]> events = broker.subscribe(channels(channel)).map(m -> frame(m, envelope));
        // The first keepalive is emitted after the broker subscription is registered, so clients get a
        // first byte immediately and know that nothing published from now on will be missed.
        Flux<byte[]> keepalive = Flux.interval(config.getHeartbeat())
                .map(tick -> SseFrames.keepalive())
                .startWith(SseFrames.keepalive());
        return HttpResponse.ok(Flux.merge(events, keepalive))
                .header(HttpHeaders.CACHE_CONTROL, "no-cache")
                .header("X-Accel-Buffering", "no");
    }

    @Post("/broadcast")
    public HttpResponse<?> broadcast(@Body JsonNode message, @QueryValue(defaultValue = "message") String channel){
        if (!SseFrames.isValidName(channel)) {
            return HttpResponse.badRequest("channel must not contain line breaks");
        }
        var published = broker.publish(channel, message);
        return HttpResponse.accepted().body(new Receipt(published.id()));
    }

    /** Parses "a,b" into a set; blank means no filtering. */
    private static Set<String> channels(String csv) {
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(name -> !name.isEmpty())
                .collect(Collectors.toUnmodifiableSet());
    }

    /** The data field is the raw payload, or the whole envelope when the client asked for it. */
    private byte[] frame(Message message, boolean envelope) {
        try {
            var data = envelope ? json.writeValueAsBytes(message) : json.writeValueAsBytes(message.payload());
            return SseFrames.event(message.channel(), message.id(), data);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
