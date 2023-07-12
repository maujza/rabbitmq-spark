package com.github.maujza.read;

import com.github.maujza.config.RabbitMQConnectionConfig;
import com.github.maujza.schema.RabbitMQMessageToRowConverter;
import com.github.maujza.schema.SerializableCaseInsensitiveStringMap;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

final class RabbitMQMicroBatchPartitionReaderFactory implements PartitionReaderFactory, Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQMicroBatchPartitionReaderFactory.class);
    private static final long serialVersionUID = 1L;
    private final RabbitMQMessageToRowConverter rabbitMQMessageToRowConverter;

    private final RabbitMQConnectionConfig connectionConfig;

    private SerializableCaseInsensitiveStringMap options;

    RabbitMQMicroBatchPartitionReaderFactory(final StructType schema, final CaseInsensitiveStringMap options) {
        this.options = new SerializableCaseInsensitiveStringMap(options);
        this.rabbitMQMessageToRowConverter = new RabbitMQMessageToRowConverter(schema);
        this.connectionConfig = new RabbitMQConnectionConfig(options);
    }
    @Override
    public PartitionReader<InternalRow> createReader(final InputPartition partition) {
        LOGGER.debug("Creating RabbitMQPartitionReader for {}", partition);
        return new RabbitMQMicroBatchPartitionReader(rabbitMQMessageToRowConverter, connectionConfig, options);
    }
}
