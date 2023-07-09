package com.github.maujza.read;

import com.github.maujza.schema.RabbitMQMessageToRowConverter;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReader;
import org.apache.spark.sql.connector.read.streaming.PartitionOffset;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import com.github.maujza.connector.RabbitMQConnection;
import com.github.maujza.config.RabbitMQConnectionConfig;
import com.github.maujza.schema.RabbitMQMessageToRowConverter;
import java.io.IOException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;

public class RabbitMQContinousPartitionReader implements ContinuousPartitionReader<InternalRow> {
    protected transient Channel channel;
    protected transient RabbitMQConnection connection;
    protected final String queueName;
    protected transient boolean autoAck;
    private final RabbitMQMessageToRowConverter rabbitMQMessageToRowConverter;
    private final CaseInsensitiveStringMap options;
    private RabbitMQConnectionConfig connectionConfig;
    public RabbitMQContinousPartitionReader(final RabbitMQMessageToRowConverter rabbitMQMessageToRowConverter, final RabbitMQConnectionConfig connectionConfig,final CaseInsensitiveStringMap options) {
        this.options = options;
        this.rabbitMQMessageToRowConverter = rabbitMQMessageToRowConverter;
        this.connectionConfig = connectionConfig;
        this.queueName = options.get("queueName");
    }
    @Override
    public PartitionOffset getOffset() {
        return null;
    }
    @Override
    public boolean next() throws IOException {
        return false;
    }
    @Override
    public InternalRow get() {
        return null;
    }
    @Override
    public void close() throws IOException {

    }
}
