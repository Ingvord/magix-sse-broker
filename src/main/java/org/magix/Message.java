package org.magix;

import io.micronaut.json.tree.JsonNode;
import io.micronaut.serde.annotation.Serdeable;

import java.time.Instant;

@Serdeable
public record Message(long id, String channel, Instant timestamp, JsonNode payload) {
}
