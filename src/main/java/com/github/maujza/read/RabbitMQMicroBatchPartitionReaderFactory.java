package com.github.maujza.read;

import com.github.maujza.config.ConsumerConfig;
import com.github.maujza.schema.RabbitMQMessageToRowConverter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

final class RabbitMQMicroBatchPartitionReaderFactory implements PartitionReaderFactory, Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQMicroBatchPartitionReaderFactory.class);
    private static final long serialVersionUID = 1L;
    private final RabbitMQMessageToRowConverter rabbitMQMessageToRowConverter;

    private final ConsumerConfig consumerConfig;


    RabbitMQMicroBatchPartitionReaderFactory(final StructType schema, final ConsumerConfig options) {
        this.rabbitMQMessageToRowConverter = new RabbitMQMessageToRowConverter(schema);
        this.consumerConfig = options;
    }
    @Override
    public PartitionReader<InternalRow> createReader(final InputPartition partition) {
        LOGGER.info("Creating RabbitMQPartitionReader for {}", partition);
        try {
            return new RabbitMQMicroBatchPartitionReader(rabbitMQMessageToRowConverter, consumerConfig);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
