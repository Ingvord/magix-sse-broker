package org.magix;

import io.micronaut.serde.annotation.Serdeable;

/** Response to a broadcast: the id the message was given. */
@Serdeable
public record Receipt(long id) {
}
