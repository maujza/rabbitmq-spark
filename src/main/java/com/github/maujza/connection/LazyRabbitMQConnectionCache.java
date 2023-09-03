package com.github.maujza.connection;

import com.rabbitmq.client.Connection;

public final class LazyRabbitMQConnectionCache {

    private static final RabbitMQConnectionCache CLIENT_CACHE;

    private static final String SYSTEM_RABBITMQ_CACHE_KEEP_ALIVE_MS_PROPERTY = "keep_alive_ms";

    static {
        int keepAliveMS = 5000;
        try {
            keepAliveMS =
                    Integer.parseInt(System.getProperty(SYSTEM_RABBITMQ_CACHE_KEEP_ALIVE_MS_PROPERTY, "5000"));
        } catch (NumberFormatException e) {
            // ignore and use default
        }

        CLIENT_CACHE = new RabbitMQConnectionCache(keepAliveMS);
    }

    public static Connection getRabbitMQConnection(final RabbitMQConnectionFactory rabbitMQConnectionFactory) {
        return CLIENT_CACHE.acquire(rabbitMQConnectionFactory);
    }

    private LazyRabbitMQConnectionCache() {}
}