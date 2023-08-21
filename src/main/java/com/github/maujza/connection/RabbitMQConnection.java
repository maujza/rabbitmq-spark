package com.github.maujza.connection;

import com.github.maujza.config.RabbitMQConnectionConfig;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

public class RabbitMQConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQConnection.class);

    private final RabbitMQConnectionConfig connectionConfig;
    private final GenericObjectPool<Connection> connectionPool;
    private final String queueName;

    private Connection connection; // Connection object instance variable
    private Channel channel; // Channel object instance variable

    private static final Duration IDLE_TIME_MILLIS = Duration.ofSeconds(30); // 30 seconds


    public RabbitMQConnection(RabbitMQConnectionConfig connectionConfig, String queueName) {
        this.queueName = queueName;
        this.connectionConfig = connectionConfig;
        GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMinEvictableIdleTime(IDLE_TIME_MILLIS);
        poolConfig.setTimeBetweenEvictionRuns(Duration.ofSeconds(15));
        this.connectionPool = new GenericObjectPool<>(new RabbitMQConnectionFactory(connectionConfig), poolConfig);
        LOGGER.info("Created RabbitMQConnection with queueName: {}, connectionConfig: {}", queueName, connectionConfig);
    }

    public RabbitMQConsumer getConsumerFromConfiguredChannel() {
        LOGGER.info("Getting configured channel");
        RabbitMQConsumer consumer;

        try {
            connection = connectionPool.borrowObject();
            channel = setupChannel(connection);
            consumer = new RabbitMQConsumer(channel);
            channel.basicConsume(queueName, false, consumer);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Cannot create RMQ connection with "
                            + queueName
                            + " at "
                            + connectionConfig.getHost(),
                    e);
        }
        return consumer;
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

    public void closeAll() throws IOException {
        LOGGER.info("Closing channel and returning connection to pool");
        try {
            if (channel != null && channel.isOpen()) {
                LOGGER.debug("Closing channel");
                channel.close();
            }
            if (connection != null) {
                LOGGER.debug("Returning connection to pool");
                connectionPool.returnObject(connection);
            }
        } catch (Exception e) {
            LOGGER.error("Error while closing channel or returning connection to pool for queue: " + queueName, e);
        }
    }
}
