package com.github.maujza.exceptions;

import org.jetbrains.annotations.Nullable;

/** A class for all config exceptions */
public final class ConfigException extends RabbitMQSparkException {

    private static final long serialVersionUID = 1L;

    public ConfigException(final String name, final Object value, @Nullable final String message) {
        this("Invalid value "
                + value
                + " for configuration "
                + name
                + (message == null ? "" : ": " + message));
    }

    public ConfigException(final String message) {
        super(message);
    }

    public ConfigException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public ConfigException(final Throwable cause) {
        super(cause);
    }
}
