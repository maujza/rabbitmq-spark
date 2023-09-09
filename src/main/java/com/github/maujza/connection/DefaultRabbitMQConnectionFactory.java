package com.github.maujza.connection;

import com.github.maujza.config.RabbitMQConfig;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

public class DefaultRabbitMQConnectionFactory implements RabbitMQConnectionFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRabbitMQConnectionFactory.class);
    private final RabbitMQConfig config;

    public DefaultRabbitMQConnectionFactory(RabbitMQConfig rabbitMQConfig) {
        this.config = rabbitMQConfig;
        LOGGER.info("DefaultRabbitMQConnectionFactory initialized with configuration: {}", config.getOptions());
    }

    public ConnectionFactory getConnectionFactory() throws URISyntaxException, NoSuchAlgorithmException, KeyManagementException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.getHostname());
        factory.setPort(config.getPort());
        factory.setVirtualHost(config.getVirtualHost());
        factory.setUsername(config.getUsername());
        factory.setPassword(config.getPassword());

        LOGGER.debug("Creating ConnectionFactory with the following settings:");
        LOGGER.debug("Hostname: {}", config.getHostname());
        LOGGER.debug("Port: {}", config.getPort());
        LOGGER.debug("Virtual Host: {}", config.getVirtualHost());
        LOGGER.debug("Username: {}", config.getUsername());
        LOGGER.debug("Password: [hidden for security reasons]");

        return factory;
    }

    @Override
    public Connection create() throws IOException, TimeoutException, URISyntaxException, NoSuchAlgorithmException, KeyManagementException {
        LOGGER.info("Creating a new RabbitMQ Connection");
        ConnectionFactory factory = getConnectionFactory();
        Connection connection = factory.newConnection();
        LOGGER.info("Successfully created a new RabbitMQ Connection: {}", connection);
        return connection;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final DefaultRabbitMQConnectionFactory that = (DefaultRabbitMQConnectionFactory) o;
        return config.equals(that.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(config);
    }
}
