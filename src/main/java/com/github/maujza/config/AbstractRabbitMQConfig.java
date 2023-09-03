package com.github.maujza.config;

import static java.util.Collections.unmodifiableMap;

import java.util.Map;

import com.github.maujza.connection.DefaultRabbitMQConnectionFactory;
import com.github.maujza.connection.LazyRabbitMQConnectionCache;
import com.github.maujza.connection.RabbitMQConnectionFactory;
import com.rabbitmq.client.Connection;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

abstract class AbstractRabbitMQConfig implements RabbitMQConfig {
    private final Map<String, String> options;

    private transient CaseInsensitiveStringMap caseInsensitiveOptions;

    private transient RabbitMQConnectionFactory rabbitMQConnectionFactory;

    AbstractRabbitMQConfig(final Map<String, String> originals) {
        this.options = unmodifiableMap(originals);
    }

    @Override
    public Map<String, String> getOptions() {
        if (caseInsensitiveOptions == null) {
            caseInsensitiveOptions = new CaseInsensitiveStringMap(options);
        }
        return caseInsensitiveOptions;
    }


    @Override
    public Map<String, String> getOriginals() {
        return options;
    }

    public Connection getRabbitMQConnection() {
        return LazyRabbitMQConnectionCache.getRabbitMQConnection(getRabbitMQConnectionFactory());
    }

    private RabbitMQConnectionFactory getRabbitMQConnectionFactory() {
        if (rabbitMQConnectionFactory == null ) {
            rabbitMQConnectionFactory = new DefaultRabbitMQConnectionFactory(this);
        }
        return rabbitMQConnectionFactory;
    }

}
