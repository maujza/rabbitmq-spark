package com.github.maujza.read;

import com.github.maujza.config.RabbitMQConnectionConfig;
import com.github.maujza.connector.RabbitMQConnection;
import com.github.maujza.connector.RabbitMQConsumer;
import com.github.maujza.schema.DeliveryDeserializer;
import com.github.maujza.schema.RabbitMQMessageToRowConverter;
import com.github.maujza.schema.SerializableCaseInsensitiveStringMap;
import com.rabbitmq.client.Delivery;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RabbitMQMicroBatchPartitionReader implements PartitionReader<InternalRow> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQMicroBatchPartitionReader.class);

    private final RabbitMQConnection connection;

    private Delivery currentDelivery;

    private InternalRow currentRecord;

    private final RabbitMQConsumer consumer;

    private final RabbitMQMessageToRowConverter rabbitMQMessageToRowConverter;

    public RabbitMQMicroBatchPartitionReader(final RabbitMQMessageToRowConverter rabbitMQMessageToRowConverter, final RabbitMQConnectionConfig connectionConfig, final SerializableCaseInsensitiveStringMap options) {
        this.rabbitMQMessageToRowConverter = rabbitMQMessageToRowConverter;
        this.connection = new RabbitMQConnection(connectionConfig, options.get("queue_name"));
        this.consumer = new RabbitMQConsumer(connection.getConfiguredChannel());
    }

    @Override
    public boolean next() throws IOException {
        try {
            LOGGER.info("Starting transaction and polling for next message");
            currentDelivery = consumer.nextDelivery(30000); // poll for next message
            LOGGER.info("current delivery ", currentDelivery);
            if (currentDelivery.getEnvelope().getDeliveryTag() > 1000) {
                return false; // End of the offset range.
            }

            String deserializedDelivery = DeliveryDeserializer.deserialize(currentDelivery);
            currentRecord = rabbitMQMessageToRowConverter.convertToInternalRow(deserializedDelivery);
            LOGGER.info("returning true");
            return true;
        } catch (Exception e) {
            LOGGER.error("Error occurred while fetching next message", e);
            return false;
        }
    }

    @Override
    public InternalRow get() {
        try {
            LOGGER.info("Acknowledging and committing transaction");
            consumer.ack(currentDelivery.getEnvelope().getDeliveryTag()); // manual ack after message is processed
        } catch (IOException e) {
            LOGGER.error("Error occurred while acknowledging or committing transaction, attempting rollback", e);
        }
        return currentRecord;
    }

    @Override
    public void close() throws IOException {
        connection.closeAll();
    }
}
