package com.github.maujza.read;

import com.github.maujza.connector.RabbitMQConsumer;
import com.github.maujza.schema.RabbitMQMessageToRowConverter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReader;
import org.apache.spark.sql.connector.read.streaming.PartitionOffset;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import com.github.maujza.connector.RabbitMQConnection;
import com.github.maujza.config.RabbitMQConnectionConfig;
import com.github.maujza.schema.DeliverySerializer;
import java.io.IOException;
import com.rabbitmq.client.Delivery;

public class RabbitMQContinousPartitionReader implements ContinuousPartitionReader<InternalRow> {

    private Delivery currentDelivery;

    private InternalRow currentRecord;

    private final RabbitMQConsumer consumer;

    private final RabbitMQMessageToRowConverter rabbitMQMessageToRowConverter;

    public RabbitMQContinousPartitionReader(final RabbitMQMessageToRowConverter rabbitMQMessageToRowConverter, final RabbitMQConnectionConfig connectionConfig,final CaseInsensitiveStringMap options) {
        this.rabbitMQMessageToRowConverter = rabbitMQMessageToRowConverter;
        RabbitMQConnection connection = new RabbitMQConnection(connectionConfig, options.get("queueName"));
        this.consumer = new RabbitMQConsumer(connection.getConfiguredChannel());
    }


    @Override
    public PartitionOffset getOffset() {
        return null;
    }
    @Override
    public boolean next() throws IOException {
        try {
            consumer.startTransaction(); // start the transaction
            currentDelivery = consumer.nextDelivery(); // poll for next message
            String deserializedDelivery = DeliverySerializer.deserialize(currentDelivery);
            currentRecord = rabbitMQMessageToRowConverter.convertToInternalRow(deserializedDelivery);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    @Override
    public InternalRow get() {
        try {
            consumer.ack(currentDelivery.getEnvelope().getDeliveryTag()); // manual ack after message is processed
            consumer.commitTransaction(); // commit the transaction
        } catch (IOException e) {
            try {
                consumer.rollbackTransaction(); // rollback the transaction in case of an error
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            e.printStackTrace();
        }
        return currentRecord;
    }
    @Override
    public void close() throws IOException {

    }
}
