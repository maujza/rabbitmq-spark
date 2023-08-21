package com.github.maujza.connector;

import com.github.maujza.config.RabbitMQConnectionConfig;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

public class RabbitMQConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQConnection.class);

    private final RabbitMQConnectionConfig connectionConfig;
    private Connection connection;
    private final String queueName;
    private Channel channel;

    public RabbitMQConnection(RabbitMQConnectionConfig connectionConfig, String queueName) {
        this(connectionConfig, queueName, null);
    }

    public RabbitMQConnection(RabbitMQConnectionConfig connectionConfig, String queueName, Connection connection) {
        this.queueName = queueName;
        this.connectionConfig = connectionConfig;
        this.connection = connection;
        LOGGER.info("Created RabbitMQConnection with queueName: {}, connectionConfig: {}, and connection: {}", queueName, connectionConfig, connection);
    }

    protected ConnectionFactory setupConnectionFactory() throws IOException, TimeoutException, URISyntaxException, NoSuchAlgorithmException, KeyManagementException {
        LOGGER.debug("Setting up ConnectionFactory");
        return connectionConfig.getConnectionFactory();
    }

    protected Connection setupConnection() throws IOException, TimeoutException, URISyntaxException, NoSuchAlgorithmException, KeyManagementException {
        LOGGER.debug("Setting up Connection");
        return setupConnectionFactory().newConnection();
    }

    private Channel setupChannel(Connection connection) throws IOException, TimeoutException {
        LOGGER.debug("Setting up Channel");
        Channel chan = connection.createChannel();
        if (connectionConfig.getPrefetchCount().isPresent()) {
            chan.basicQos(connectionConfig.getPrefetchCount().get(), true);
        } else {
            LOGGER.warn("Prefetch count not configured; proceeding without it.");
        }
        return chan;
    }

    public RabbitMQConsumer getConsumerFromConfiguredChannel() {
        LOGGER.info("Getting configured channel");
        RabbitMQConsumer consumer;
        try {
            connection = setupConnection();
            channel = setupChannel(connection);
            consumer = new RabbitMQConsumer(channel);
            channel.basicConsume(queueName, false, consumer);
        } catch (IOException | TimeoutException | URISyntaxException | NoSuchAlgorithmException | KeyManagementException e) {
            throw new RuntimeException(
                    "Cannot create RMQ connection with "
                            + queueName
                            + " at "
                            + connectionConfig.getHost(),
                    e);
        } finally {
            try {
                closeAll();
            } catch (IOException e) {
                LOGGER.error("Error while closing connections or channels for queue: " + queueName, e);
            }
        }
        return consumer;
    }

    public void closeAll() throws IOException {
        LOGGER.info("Closing all connections and channels");
        try {
            if (channel != null && channel.isOpen()) {
                LOGGER.debug("Closing channel");
                channel.close();
            }
            if (connection != null && connection.isOpen()) {
                LOGGER.debug("Closing connection");
                connection.close();
            }
        } catch (IOException | TimeoutException e) {
            LOGGER.error("Error while closing connections or channels for queue: " + queueName, e);
        }
    }
}
