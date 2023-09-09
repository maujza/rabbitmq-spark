package com.github.maujza.config;

import static java.util.Collections.unmodifiableMap;

import java.util.Map;
import java.util.Objects;

import com.github.maujza.connection.DefaultRabbitMQConnectionFactory;
import com.github.maujza.connection.LazyRabbitMQConnectionCache;
import com.github.maujza.connection.RabbitMQConnectionFactory;
import com.rabbitmq.client.Connection;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractRabbitMQConfig implements RabbitMQConfig {
    private final Map<String, String> options;

    private transient CaseInsensitiveStringMap caseInsensitiveOptions;

    private transient RabbitMQConnectionFactory rabbitMQConnectionFactory;

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRabbitMQConfig.class);

    AbstractRabbitMQConfig(final Map<String, String> originals) {
        this.options = unmodifiableMap(originals);
        LOGGER.info("Initialized with options: {}", originals);
    }

    @Override
    public Map<String, String> getOptions() {
        if (caseInsensitiveOptions == null) {
            caseInsensitiveOptions = new CaseInsensitiveStringMap(options);
            LOGGER.info("Case insensitive options created: {}", caseInsensitiveOptions);
        }
        return caseInsensitiveOptions;
    }

    @Override
    public Map<String, String> getOriginals() {
        return options;
    }

    public Connection getRabbitMQConnection() {
        try {
            Connection connection = LazyRabbitMQConnectionCache.getRabbitMQConnection(getRabbitMQConnectionFactory());
            LOGGER.info("Obtained RabbitMQ connection: {}", connection);
            return connection;
        } catch (Exception e) {
            LOGGER.error("Failed to obtain RabbitMQ connection", e);
            return null; // or throw a custom exception
        }
    }

    private RabbitMQConnectionFactory getRabbitMQConnectionFactory() {
        if (rabbitMQConnectionFactory == null) {
            rabbitMQConnectionFactory = new DefaultRabbitMQConnectionFactory(this);
            LOGGER.info("RabbitMQ Connection Factory created: {}", rabbitMQConnectionFactory);
        }
        return rabbitMQConnectionFactory;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AbstractRabbitMQConfig that = (AbstractRabbitMQConfig) o;
        boolean equals = Objects.equals(getOptions(), that.getOptions());
        return equals;
    }

    @Override
    public int hashCode() {
        int hash = Objects.hash(getOptions());
        return hash;
    }
}
