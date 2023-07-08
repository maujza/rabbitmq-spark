package com.github.maujza.connector;

import com.github.maujza.config.RabbitMQConnectionConfig;
import com.github.maujza.read.Util;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;

public class RabbitMQConnection {
    private final RabbitMQConnectionConfig connectionConfig;

    private final String queueName;
    private transient Channel channel;
    private transient RabbitMQConsumer consumer;

    protected transient boolean autoAck;

    public RabbitMQConnection(RabbitMQConnectionConfig connectionConfig, String queueName) {
        this.queueName = queueName;
        this.connectionConfig = connectionConfig;
    }

    protected ConnectionFactory setupConnectionFactory() throws Exception {
        return connectionConfig.getConnectionFactory();
    }

    protected Connection setupConnection() throws Exception {
        return setupConnectionFactory().newConnection();
    }

    private Channel setupChannel(Connection connection) throws Exception {
        Channel chan = connection.createChannel();
        if (connectionConfig.getPrefetchCount().isPresent()) {
            chan.basicQos(connectionConfig.getPrefetchCount().get(), true);
        }
        return chan;
    }

    protected void setupQueue() throws IOException {
        Util.declareQueueDefaults(channel, queueName);
    }

    public Channel getConfiguredChannel() {
        return null;
    };

}

