package com.github.maujza.exceptions;

public class RabbitMQSparkException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public RabbitMQSparkException(final String message) {
        super(message);
    }

    public RabbitMQSparkException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public RabbitMQSparkException(final Throwable cause) {
        super(cause);
    }
}
