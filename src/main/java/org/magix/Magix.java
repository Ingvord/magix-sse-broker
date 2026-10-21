package org.magix;


import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.*;
import io.micronaut.http.sse.Event;
import io.micronaut.json.tree.JsonNode;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

@Controller("/magix/api")
public class Magix {

    private final Sinks.Many<Event<JsonNode>> sink = Sinks.many().multicast().onBackpressureBuffer();

    @Get("/subscribe")
    @Produces(MediaType.TEXT_EVENT_STREAM)
    public Flux<Event<JsonNode>> subscribe(){
        return sink.asFlux();
    }


    @Post("/broadcast")
    public void broadcast(@Body JsonNode message, @QueryValue(defaultValue = "message") String channel){
        sink.tryEmitNext(
                Event.of(message)
                        .name(channel)
        );
    }
}
