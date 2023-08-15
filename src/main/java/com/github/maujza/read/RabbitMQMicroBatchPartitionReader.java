package com.github.maujza.read;

import com.github.maujza.config.RabbitMQConnectionConfig;
import com.github.maujza.connector.RabbitMQConnection;
import com.github.maujza.connector.RabbitMQConsumer;
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
    private static final int MAX_MESSAGE_COUNT = 1000;

    private final RabbitMQConnection connection;
    private final long timeLimit;
    private final long startTime;
    private final long max_messages_per_partition;

    private Delivery currentDelivery;
    private InternalRow currentRecord;
    private final RabbitMQConsumer consumer;
    private final RabbitMQMessageToRowConverter rabbitMQMessageToRowConverter;
    private String currentMessage;

    public RabbitMQMicroBatchPartitionReader(final RabbitMQMessageToRowConverter rabbitMQMessageToRowConverter, final RabbitMQConnectionConfig connectionConfig, final SerializableCaseInsensitiveStringMap options) {
        this.rabbitMQMessageToRowConverter = rabbitMQMessageToRowConverter;
        this.connection = new RabbitMQConnection(connectionConfig, options.get("queue_name"));
        this.consumer = connection.getConsumerFromConfiguredChannel();
        this.timeLimit = Long.parseLong(options.get("time_limit"));
        this.max_messages_per_partition = options.containsKey("max_messages_per_partition") ?
                Long.parseLong(options.get("max_messages_per_partition")) : MAX_MESSAGE_COUNT;
        this.startTime = System.currentTimeMillis();
    }

    private boolean shouldTerminate(long deliveryTag) {
        return (System.currentTimeMillis() - startTime) >= timeLimit || deliveryTag > this.max_messages_per_partition;
    }

    @Override
    public boolean next() throws IOException {
        try {
            currentDelivery = consumer.nextDelivery();
            currentMessage = new String(currentDelivery.getBody(), "UTF-8");
            if (shouldTerminate(currentDelivery.getEnvelope().getDeliveryTag())) {
                return false;
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
            LOGGER.info("Acknowledging and committing transaction");
            consumer.ack(currentDelivery.getEnvelope().getDeliveryTag());
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
