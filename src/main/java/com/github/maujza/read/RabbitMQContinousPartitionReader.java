package com.github.maujza.read;

import com.github.maujza.config.RabbitMQConnectionConfig;
import com.github.maujza.connector.RabbitMQConnection;
import com.github.maujza.connector.RabbitMQConsumer;
import com.github.maujza.schema.DeliveryDeserializer;
import com.github.maujza.schema.RabbitMQMessageToRowConverter;
import com.github.maujza.schema.SerializableCaseInsensitiveStringMap;
import com.rabbitmq.client.Delivery;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReader;
import org.apache.spark.sql.connector.read.streaming.PartitionOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RabbitMQContinousPartitionReader implements ContinuousPartitionReader<InternalRow> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQContinousPartitionReader.class);

    private final RabbitMQConnection connection;

    private Delivery currentDelivery;

    private InternalRow currentRecord;

    private final RabbitMQConsumer consumer;

    private final RabbitMQMessageToRowConverter rabbitMQMessageToRowConverter;

    public RabbitMQContinousPartitionReader(final RabbitMQMessageToRowConverter rabbitMQMessageToRowConverter, final RabbitMQConnectionConfig connectionConfig, final SerializableCaseInsensitiveStringMap options) {
        this.rabbitMQMessageToRowConverter = rabbitMQMessageToRowConverter;
        this.connection = new RabbitMQConnection(connectionConfig, options.get("queue_name"));
        this.consumer = new RabbitMQConsumer(connection.getConfiguredChannel());
    }

    @Override
    public PartitionOffset getOffset() {
        return null;
    }

    @Override
    public boolean next() throws IOException {
        try {
            LOGGER.debug("Starting transaction and polling for next message");
            consumer.startTransaction(); // start the transaction
            currentDelivery = consumer.nextDelivery(); // poll for next message
            String deserializedDelivery = DeliveryDeserializer.deserialize(currentDelivery);
            currentRecord = rabbitMQMessageToRowConverter.convertToInternalRow(deserializedDelivery);
            return true;
        } catch (Exception e) {
            LOGGER.error("Error occurred while fetching next message", e);
            return false;
        }
    }

    @Override
    public InternalRow get() {
        try {
            LOGGER.debug("Acknowledging and committing transaction");
            consumer.ack(currentDelivery.getEnvelope().getDeliveryTag()); // manual ack after message is processed
            consumer.commitTransaction(); // commit the transaction
        } catch (IOException e) {
            LOGGER.error("Error occurred while acknowledging or committing transaction, attempting rollback", e);
            try {
                consumer.rollbackTransaction(); // rollback the transaction in case of an error
            } catch (IOException ex) {
                LOGGER.error("Error occurred while rolling back transaction", ex);
            }
        }
        return currentRecord;
    }

    @Override
    public void close() throws IOException {
        connection.closeAll();
    }
}
