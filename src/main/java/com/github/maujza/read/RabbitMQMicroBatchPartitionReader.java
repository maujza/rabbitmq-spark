package com.github.maujza.read;

import com.github.maujza.config.ConsumerConfig;
import com.github.maujza.connection.RabbitMQConsumer;
import com.github.maujza.schema.RabbitMQMessageToRowConverter;
import com.rabbitmq.client.Connection;
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
    private static final String TIME_LIMIT_OPTION = "time_limit";
    private static final long TIME_LIMIT_MAX = 1000L;
    private static final String MAX_MESSAGES_PER_PARTITION_OPTION = "max_messages_per_partition";

    private final Connection connection;
    private final long timeLimit;
    private final long startTime;
    private final long max_messages_per_partition;

    private Delivery currentDelivery;
    private InternalRow currentRecord;
    private final RabbitMQConsumer consumer;
    private final RabbitMQMessageToRowConverter rabbitMQMessageToRowConverter;

    public RabbitMQMicroBatchPartitionReader(final RabbitMQMessageToRowConverter rabbitMQMessageToRowConverter, final ConsumerConfig consumerConfig) throws Exception {
        this.rabbitMQMessageToRowConverter = rabbitMQMessageToRowConverter;
        this.connection = consumerConfig.getRabbitMQConnection();
        this.consumer = consumerConfig.createConsumer(connection);
        this.max_messages_per_partition = consumerConfig.containsKey(MAX_MESSAGES_PER_PARTITION_OPTION) ?
                Long.parseLong(consumerConfig.get(MAX_MESSAGES_PER_PARTITION_OPTION)) : MAX_MESSAGE_COUNT;
        this.timeLimit = consumerConfig.containsKey(TIME_LIMIT_OPTION) ?
                Long.parseLong(consumerConfig.get(TIME_LIMIT_OPTION)) : TIME_LIMIT_MAX;
        this.startTime = System.currentTimeMillis();
    }

    private boolean shouldTerminate(long deliveryTag) {
        boolean timeExceeded = (System.currentTimeMillis() - startTime) >= timeLimit;
        boolean maxMessageExceeded = deliveryTag > this.max_messages_per_partition;

        if (timeExceeded) {
            LOGGER.info("Termination Reason: Time limit exceeded.");
        }

        if (maxMessageExceeded) {
            LOGGER.info("Termination Reason: Max messages per partition limit exceeded.");
        }

        return timeExceeded || maxMessageExceeded;
    }

    @Override
    public boolean next() throws IOException {
        try {
            currentDelivery = consumer.nextDelivery(timeLimit);

            if (currentDelivery == null) {
                LOGGER.warn("Received a null delivery. Skipping...");
                return false;
            }

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
            consumer.ack(currentDelivery.getEnvelope().getDeliveryTag());
        } catch (IOException e) {
            LOGGER.error("Error occurred while acknowledging or committing transaction for delivery: " + currentDelivery, e);
        }
        return currentRecord;
    }

    @Override
    public void close() throws IOException {
        consumer.closeAll();
        connection.close();
    }
}
