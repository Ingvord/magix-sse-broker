package org.magix;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.core.bind.annotation.Bindable;

@ConfigurationProperties("magix")
public interface MagixConfig {

    /** Events buffered per subscriber before the oldest are dropped. */
    @Bindable(defaultValue = "256")
    int getBufferSize();
}
