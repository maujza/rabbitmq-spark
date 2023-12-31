package com.github.maujza.connection;

import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LazyRabbitMQConnectionCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(LazyRabbitMQConnectionCache.class);

    private static final RabbitMQConnectionCache CLIENT_CACHE;

    private static final String SYSTEM_RABBITMQ_CACHE_KEEP_ALIVE_MS_PROPERTY = "keep_alive_ms";

    static {
        int keepAliveMS = 5000;
        try {
            keepAliveMS = Integer.parseInt(System.getProperty(SYSTEM_RABBITMQ_CACHE_KEEP_ALIVE_MS_PROPERTY, "5000"));
            LOGGER.info("Using keepAliveMS value from system properties: {}", keepAliveMS);
        } catch (NumberFormatException e) {
            LOGGER.warn("Invalid format for keepAliveMS in system properties, using default value: {}. Exception: {}", keepAliveMS, e.getMessage());
        }

        CLIENT_CACHE = new RabbitMQConnectionCache(keepAliveMS);
        LOGGER.info("Initialized RabbitMQConnectionCache with keepAliveMS: {}", keepAliveMS);
    }

    public static Connection getRabbitMQConnection(final RabbitMQConnectionFactory rabbitMQConnectionFactory) {
        LOGGER.info("Attempting to acquire a RabbitMQ connection using factory: {}", rabbitMQConnectionFactory);

        Connection connection = CLIENT_CACHE.acquire(rabbitMQConnectionFactory);

        if(connection != null) {
            LOGGER.info("Successfully acquired a RabbitMQ connection: {}", connection);
        } else {
            LOGGER.warn("Failed to acquire a RabbitMQ connection");
        }

        return connection;
    }

    private LazyRabbitMQConnectionCache() {
        // Added a private constructor log for good measure.
        LOGGER.info("LazyRabbitMQConnectionCache class initialized");
    }
}
