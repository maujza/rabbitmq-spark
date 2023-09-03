package com.github.maujza.connection;

import com.github.maujza.config.RabbitMQConfig;
import com.github.maujza.config.RabbitMQConnectionConfig;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

public class DefaultRabbitMQConnectionFactory implements RabbitMQConnectionFactory{
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
        return factory;
    }

    @Override
    public Connection create() throws IOException, TimeoutException, URISyntaxException, NoSuchAlgorithmException, KeyManagementException {
        ConnectionFactory factory = getConnectionFactory();
        return factory.newConnection();
    }
}
