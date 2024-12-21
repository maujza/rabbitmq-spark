package com.github.maujza.rabbitmq.spark.read;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

final class RabbitMQStreamMicroBatchPartitionReaderFactory implements PartitionReaderFactory, Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQStreamMicroBatchPartitionReaderFactory.class);
    private static final long serialVersionUID = 1L;

    private final Map<String, String> consumerConfig;
    private final StructType schema;


    RabbitMQStreamMicroBatchPartitionReaderFactory(final StructType schema, final Map<String, String> options) {
        this.consumerConfig = options;
        this.schema = schema;
    }
    @Override
    public PartitionReader<InternalRow> createReader(final InputPartition partition) {
        LOGGER.info("Creating RabbitMQPartitionReader for {}", partition);
        try {
            return new RabbitMQStreamMicroBatchPartitionReader((RabbitMQStreamInputPartition) partition, schema, consumerConfig);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}