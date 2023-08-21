package com.github.maujza.connection;

import com.github.maujza.config.RabbitMQConnectionConfig;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class RabbitMQConnectionFactory extends BasePooledObjectFactory<Connection> {

    private final RabbitMQConnectionConfig connectionConfig;

    public RabbitMQConnectionFactory(RabbitMQConnectionConfig connectionConfig) {
        this.connectionConfig = connectionConfig;
    }

    @Override
    public Connection create() throws Exception {
        ConnectionFactory factory = connectionConfig.getConnectionFactory();
        return factory.newConnection();
    }

    @Override
    public PooledObject<Connection> wrap(Connection connection) {
        return new DefaultPooledObject<>(connection);
    }

    @Override
    public void destroyObject(PooledObject<Connection> p) throws Exception {
        Connection connection = p.getObject();
        if (connection != null && connection.isOpen()) {
            connection.close();
        }
    }

    @Override
    public boolean validateObject(PooledObject<Connection> p) {
        Connection connection = p.getObject();
        return connection != null && connection.isOpen();
    }
}
