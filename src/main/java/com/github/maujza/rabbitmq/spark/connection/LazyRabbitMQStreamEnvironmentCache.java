package com.github.maujza.rabbitmq.spark.connection;

import com.rabbitmq.stream.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.Map;

public class LazyRabbitMQStreamEnvironmentCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(LazyRabbitMQStreamEnvironmentCache.class);
    private static final RabbitMQStreamEnvironmentCache CLIENT_CACHE;

    private static final String SYSTEM_RABBITMQ_STREAM_CACHE_KEEP_ALIVE_MS_PROPERTY =
            "spark.rabbitmq_stream.keep_alive_ms";

    static {
        int keepAliveMS = 5000;
        try {
            keepAliveMS =
                    Integer.parseInt(System.getProperty(SYSTEM_RABBITMQ_STREAM_CACHE_KEEP_ALIVE_MS_PROPERTY, "5000"));
            LOGGER.debug("Keep alive time set to {} ms", keepAliveMS);
        } catch (NumberFormatException e) {
            // ignore and use default
            LOGGER.error("Invalid format for keep alive time, using default value: {} ms", keepAliveMS, e);

        }

        CLIENT_CACHE = new RabbitMQStreamEnvironmentCache(keepAliveMS);
        LOGGER.info("Initialized RabbitMQStreamEnvironmentCache with keep alive: {} ms", keepAliveMS);

    }

    public static Environment getEnvironment(final Map<String, String> config) throws UnsupportedEncodingException {
        Environment env = CLIENT_CACHE.acquire(config);
        LOGGER.info("Environment acquired for URI: {}", config);
        return env;
    }

    private LazyRabbitMQStreamEnvironmentCache() {}
}
