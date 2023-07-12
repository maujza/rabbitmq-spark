package com.github.maujza.connector;

import com.github.maujza.config.RabbitMQConnectionConfig;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RabbitMQConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQConnection.class);

    private final RabbitMQConnectionConfig connectionConfig;

    private transient Connection connection;

    private final String queueName;
    private transient Channel channel;

    public RabbitMQConnection(RabbitMQConnectionConfig connectionConfig, String queueName) {
        this(connectionConfig, queueName, null);
    }

    public RabbitMQConnection(RabbitMQConnectionConfig connectionConfig, String queueName, Connection connection) {
        this.queueName = queueName;
        this.connectionConfig = connectionConfig;
        this.connection = connection;
        LOGGER.info("Created RabbitMQConnection with queueName: {}, connectionConfig: {}, and connection: {}", queueName, connectionConfig, connection);
    }

    protected ConnectionFactory setupConnectionFactory() throws Exception {
        LOGGER.debug("Setting up ConnectionFactory");
        return connectionConfig.getConnectionFactory();
    }

    protected Connection setupConnection() throws Exception {
        LOGGER.debug("Setting up Connection");
        return setupConnectionFactory().newConnection();
    }

    private Channel setupChannel(Connection connection) throws Exception {
        LOGGER.debug("Setting up Channel");
        Channel chan = connection.createChannel();
        if (connectionConfig.getPrefetchCount().isPresent()) {
            chan.basicQos(connectionConfig.getPrefetchCount().get(), true);
        }
        return chan;
    }

//    public static void declareQueueDefaults(Channel channel, String queueName) throws IOException {
//        LOGGER.debug("Declaring queue defaults for queueName: {}", queueName);
//        channel.queueDeclare(queueName, true, false, false, null);
//    }
//
//    protected void setupQueue() throws IOException {
//        LOGGER.debug("Setting up Queue");
//        declareQueueDefaults(channel, queueName);
//    }

    public Channel getConfiguredChannel() {
        LOGGER.info("Getting configured channel");
        try {
            connection = setupConnection();
            channel = setupChannel(connection);
            if (channel == null) {
                throw new RuntimeException("None of RabbitMQ channels are available");
            }
//            setupQueue();

            RabbitMQConsumer consumer = new RabbitMQConsumer(channel);

            channel.basicConsume(queueName, false, consumer);

        } catch (Exception e) {
            throw new RuntimeException(
                    "Cannot create RMQ connection with "
                            + queueName
                            + " at "
                            + connectionConfig.getHost(),
                    e);
        }
        return channel;
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
        } catch (Exception e) {
            LOGGER.error("Error while closing connections or channels", e);
        }
    }

}
