package org.magix;


import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.*;
import io.micronaut.http.sse.Event;
import io.micronaut.json.tree.JsonNode;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.processors.MulticastProcessor;

@Controller("/magix/api")
public class Magix {

    private final MulticastProcessor<Event<JsonNode>> eventFlowableProcessor = MulticastProcessor.create();

    {
        eventFlowableProcessor.start();
    }

    @Get("/subscribe")
    @Produces(MediaType.TEXT_EVENT_STREAM)
    public Flowable<Event<JsonNode>> subscribe(){
        return eventFlowableProcessor;
    }


    @Post("/broadcast")
    public void broadcast(@Body JsonNode message, @QueryValue(defaultValue = "message") String channel){
        eventFlowableProcessor.onNext(
                Event.of(message)
                        .name(channel)
        );
    }
}
