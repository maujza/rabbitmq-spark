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
import java.nio.charset.StandardCharsets;

public class RabbitMQMicroBatchPartitionReader implements PartitionReader<InternalRow> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQMicroBatchPartitionReader.class);
    private static final int MAX_MESSAGE_COUNT = 1000;
    private static final String QUEUE_NAME_OPTION = "queue_name";
    private static final String TIME_LIMIT_OPTION = "time_limit";
    private static final String MAX_MESSAGES_PER_PARTITION_OPTION = "max_messages_per_partition";

    private final RabbitMQConnection connection;
    private final long timeLimit;
    private final long startTime;
    private final long max_messages_per_partition;

    private Delivery currentDelivery;
    private InternalRow currentRecord;
    private final RabbitMQConsumer consumer;
    private final RabbitMQMessageToRowConverter rabbitMQMessageToRowConverter;

    public RabbitMQMicroBatchPartitionReader(final RabbitMQMessageToRowConverter rabbitMQMessageToRowConverter, final RabbitMQConnectionConfig connectionConfig, final SerializableCaseInsensitiveStringMap options) {
        this.rabbitMQMessageToRowConverter = rabbitMQMessageToRowConverter;
        this.connection = new RabbitMQConnection(connectionConfig, options.get(QUEUE_NAME_OPTION));
        this.consumer = connection.getConsumerFromConfiguredChannel();
        this.timeLimit = Long.parseLong(options.get(TIME_LIMIT_OPTION));
        this.max_messages_per_partition = options.containsKey(MAX_MESSAGES_PER_PARTITION_OPTION) ?
                Long.parseLong(options.get(MAX_MESSAGES_PER_PARTITION_OPTION)) : MAX_MESSAGE_COUNT;
        this.startTime = System.currentTimeMillis();
    }

    private boolean shouldTerminate(long deliveryTag) {
        return (System.currentTimeMillis() - startTime) >= timeLimit || deliveryTag > this.max_messages_per_partition;
    }

    @Override
    public boolean next() throws IOException {
        try {
            currentDelivery = consumer.nextDelivery();
            if (shouldTerminate(currentDelivery.getEnvelope().getDeliveryTag())) {
                return false;
            }
            currentRecord = rabbitMQMessageToRowConverter.convertToInternalRow(new String(currentDelivery.getBody(), StandardCharsets.UTF_8));
            return true;
        } catch (Exception e) {
            LOGGER.error("Error occurred while fetching next message. Delivery: " + currentDelivery, e);
            return false;
        }
    }

    @Override
    public InternalRow get() {
        try {
            LOGGER.info("Acknowledging and committing transaction");
            consumer.ack(currentDelivery.getEnvelope().getDeliveryTag());
        } catch (IOException e) {
            LOGGER.error("Error occurred while acknowledging or committing transaction for delivery: " + currentDelivery, e);
        }
        return currentRecord;
    }

    @Override
    public void close() throws IOException {
        connection.closeAll();
    }
}
