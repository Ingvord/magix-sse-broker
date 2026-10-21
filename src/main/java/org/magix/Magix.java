package org.magix;

import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.*;
import io.micronaut.http.sse.Event;
import io.micronaut.json.tree.JsonNode;
import reactor.core.publisher.Flux;

@Controller("/magix/api")
public class Magix {

    private final Broker broker;

    public Magix(Broker broker) {
        this.broker = broker;
    }

    @Get("/subscribe")
    @Produces(MediaType.TEXT_EVENT_STREAM)
    public Flux<Event<JsonNode>> subscribe(){
        return broker.subscribe().map(m -> Event.of(m.payload()).name(m.channel()));
    }

    @Post("/broadcast")
    public void broadcast(@Body JsonNode message, @QueryValue(defaultValue = "message") String channel){
        broker.publish(channel, message);
    }
}
