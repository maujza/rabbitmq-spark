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
import org.apache.spark.sql.execution.streaming.LongOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class RabbitMQContinousPartitionReader implements ContinuousPartitionReader<InternalRow> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQContinousPartitionReader.class);

    private final RabbitMQConnection connection;
    private final AtomicLong offset;

    private Delivery currentDelivery;

    private InternalRow currentRecord;

    private final RabbitMQConsumer consumer;

    private final RabbitMQMessageToRowConverter rabbitMQMessageToRowConverter;

    private String currentMessage;

    public RabbitMQContinousPartitionReader(final RabbitMQMessageToRowConverter rabbitMQMessageToRowConverter, final RabbitMQConnectionConfig connectionConfig, final SerializableCaseInsensitiveStringMap options) {
        this.rabbitMQMessageToRowConverter = rabbitMQMessageToRowConverter;
        this.connection = new RabbitMQConnection(connectionConfig, options.get("queue_name"));
        this.consumer = connection.getConsumerFromConfiguredChannel();
        this.offset = new AtomicLong(0);
    }

    @Override
    public PartitionOffset getOffset() {
        return (PartitionOffset) new LongOffset(offset.getAndAdd(1000));
    }

    @Override
    public boolean next() throws IOException {
        try {
            currentDelivery = consumer.nextDelivery(); // poll for next message
            currentMessage = new String(currentDelivery.getBody(), "UTF-8");
            if (currentDelivery.getEnvelope().getDeliveryTag() > 1000) {
                return false; // End of the offset range.
            }
            currentRecord = rabbitMQMessageToRowConverter.convertToInternalRow(currentMessage);
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
//            consumer.commitTransaction(); // commit the transaction
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
