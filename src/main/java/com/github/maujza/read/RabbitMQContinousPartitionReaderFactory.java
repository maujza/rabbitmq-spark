package com.github.maujza.read;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReader;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.maujza.schema.RabbitMQMessageToRowConverter;
import com.github.maujza.read.RabbitMQInputPartition;

final class RabbitMQContinousPartitionReaderFactory implements ContinuousPartitionReaderFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQContinousPartitionReaderFactory.class);
    private static final long serialVersionUID = 1L;
    private final RabbitMQMessageToRowConverter rabbitMQMessageToRowConverter;

    RabbitMQContinousPartitionReaderFactory(final RabbitMQMessageToRowConverter rabbitMQMessageToRowConverter) {
        this.rabbitMQMessageToRowConverter = rabbitMQMessageToRowConverter;
    }

    @Override
    public ContinuousPartitionReader<InternalRow> createReader(final InputPartition partition) {
        LOGGER.debug("Creating RabbitMQStreamPartitionReader for {}", partition);
        return new RabbitMQContinousPartitionReader();
    }
}
