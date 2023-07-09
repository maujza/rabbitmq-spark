package com.github.maujza.read;

import com.github.maujza.config.RabbitMQConnectionConfig;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReader;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.maujza.schema.RabbitMQMessageToRowConverter;

final class RabbitMQContinousPartitionReaderFactory implements ContinuousPartitionReaderFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQContinousPartitionReaderFactory.class);
    private static final long serialVersionUID = 1L;
    private final RabbitMQMessageToRowConverter rabbitMQMessageToRowConverter;
    private final CaseInsensitiveStringMap options;

    private final RabbitMQConnectionConfig connectionConfig;

    RabbitMQContinousPartitionReaderFactory(final StructType schema, final CaseInsensitiveStringMap options) {
        this.options = options;
        this.rabbitMQMessageToRowConverter = new RabbitMQMessageToRowConverter(schema);
        this.connectionConfig = new RabbitMQConnectionConfig(options);
    }
    @Override
    public ContinuousPartitionReader<InternalRow> createReader(final InputPartition partition) {
        LOGGER.debug("Creating RabbitMQStreamPartitionReader for {}", partition);
        return new RabbitMQContinousPartitionReader(rabbitMQMessageToRowConverter, connectionConfig, options);
    }
}
