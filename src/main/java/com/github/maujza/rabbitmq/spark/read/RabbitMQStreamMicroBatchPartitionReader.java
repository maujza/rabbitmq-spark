package com.github.maujza.rabbitmq.spark.read;

import com.rabbitmq.stream.*;
import com.github.maujza.rabbitmq.spark.connection.LazyRabbitMQStreamEnvironmentCache;
import com.github.maujza.rabbitmq.spark.offset.RabbitMQStreamMessageWithOffset;
import com.github.maujza.rabbitmq.spark.schema.RabbitMQMessageToRowConverter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class RabbitMQStreamMicroBatchPartitionReader implements PartitionReader<InternalRow> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQStreamMicroBatchPartitionReader.class);
    private final RabbitMQMessageToRowConverter rabbitMQMessageToRowConverter;
    private final Map<String, String> config;
    private final BlockingQueue<RabbitMQStreamMessageWithOffset> messageQueue;
    private final Environment environment;
    private final Consumer consumer;
    private final RabbitMQStreamInputPartition partition;
    private final StructType schema;
    private InternalRow currentRecord;
    private static final int DEFAULT_QUEUE_CAPACITY = 10000;

    public RabbitMQStreamMicroBatchPartitionReader(RabbitMQStreamInputPartition partition, StructType schema, Map<String, String> consumerConfig) throws UnsupportedEncodingException {
        if (partition == null || consumerConfig == null || consumerConfig.isEmpty()) {
            throw new IllegalArgumentException("Invalid or null parameters provided to the constructor.");
        }

        this.partition = partition;
        this.schema = schema;
        this.rabbitMQMessageToRowConverter = new RabbitMQMessageToRowConverter(schema);
        this.config = consumerConfig;
        int queueCapacity = Integer.parseInt(consumerConfig.getOrDefault("queueCapacity", String.valueOf(DEFAULT_QUEUE_CAPACITY)));
        this.messageQueue = new LinkedBlockingQueue<>(queueCapacity);
        this.environment = LazyRabbitMQStreamEnvironmentCache.getEnvironment(config);
        this.consumer = initializeConsumer(partition);
    }



    private Consumer initializeConsumer(RabbitMQStreamInputPartition partition) {
        OffsetSpecification offsetSpec = OffsetSpecification.timestamp(partition.getStartOffset().getTimestamp());
        return environment.consumerBuilder()
                .stream(config.get("stream"))
                .flow()
                .strategy(ConsumerFlowStrategy.creditWhenHalfMessagesProcessed())
                .builder()
                .offset(offsetSpec)
                .messageHandler((context, message) -> {
                    try {
                        messageQueue.put(new RabbitMQStreamMessageWithOffset(message, context.timestamp()));
                        context.processed();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        LOGGER.error("Failed to enqueue message with timestamp: {}", context.timestamp(), e);
                    }
                })
                .build();
    }

    @Override
    public boolean next() {
        try {
            RabbitMQStreamMessageWithOffset messageWithOffset = messageQueue.poll(100, TimeUnit.MILLISECONDS);
            if (messageWithOffset == null) {
                LOGGER.debug("No message received within the specified timeout period.");
                return false;
            }

            if (messageWithOffset.getOffset() > partition.getEndOffset().getTimestamp()) {
                LOGGER.debug("Received message offset {} is beyond the end offset {}.", messageWithOffset.getOffset(), partition.getEndOffset().getTimestamp());
                return false;
            }

            String messageContent = new String(messageWithOffset.getMessage().getBodyAsBinary(), StandardCharsets.UTF_8);

            boolean isInferred = false;
            if (schema.getFieldIndex("_raw").isDefined()) {
                isInferred = schema.apply("_raw").metadata().getBoolean("raw");
            }

            if (isInferred) {
                UTF8String utf8MessageContent = UTF8String.fromString(messageContent);

                // Construct an InternalRow directly with the offset timestamp and the message body as string
                currentRecord = new GenericInternalRow(new Object[]{
                        messageWithOffset.getOffset(),
                        utf8MessageContent,
                        true
                });
                LOGGER.debug("Schema is inferred, processing message with timestamp {} and payload as {}.", messageWithOffset.getOffset(), messageContent);
            } else {
                currentRecord = rabbitMQMessageToRowConverter.convertToInternalRow(messageContent);
                LOGGER.debug("Schema is not inferred, processing message with timestamp {} and converted row.", messageWithOffset.getOffset());
            }

            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("Interrupted while waiting for a message", e);
            return false;
        } catch (Exception e) {
            LOGGER.error("Exception occurred while processing the message", e);
            return false;
        }
    }

    @Override
    public InternalRow get() {
        return currentRecord;
    }

    @Override
    public void close() {
        try {
            if (consumer != null) {
                consumer.close();
            }
        } finally {
            if (environment != null) {
                environment.close();
            }
        }
    }
}
