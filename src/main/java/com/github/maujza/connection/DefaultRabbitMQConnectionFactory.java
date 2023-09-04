package com.github.maujza.connection;

import com.github.maujza.config.RabbitMQConfig;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

public class DefaultRabbitMQConnectionFactory implements RabbitMQConnectionFactory{

    private static final Logger LOGGER = Logger.getLogger(DefaultRabbitMQConnectionFactory.class.getName());
    private final RabbitMQConfig config;

    public DefaultRabbitMQConnectionFactory(RabbitMQConfig rabbitMQConfig) {
        this.config = rabbitMQConfig;
    }

    public ConnectionFactory getConnectionFactory() throws URISyntaxException, NoSuchAlgorithmException, KeyManagementException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.getHostname());
        factory.setPort(config.getPort());
        factory.setVirtualHost(config.getVirtualHost());
        factory.setUsername(config.getUsername());
        factory.setPassword(config.getPassword());

        LOGGER.log(Level.DEBUG, "Creating ConnectionFactory with the following settings:");
        LOGGER.log(Level.DEBUG, "Hostname: " + config.getHostname());
        LOGGER.log(Level.DEBUG, "Port: " + config.getPort());
        LOGGER.log(Level.DEBUG, "Virtual Host: " + config.getVirtualHost());
        LOGGER.log(Level.DEBUG, "Username: " + config.getUsername());
        LOGGER.log(Level.DEBUG, "Password: [hidden for security reasons]");

        return factory;
    }

    @Override
    public Connection create() throws IOException, TimeoutException, URISyntaxException, NoSuchAlgorithmException, KeyManagementException {
        ConnectionFactory factory = getConnectionFactory();
        return factory.newConnection();
    }
}
