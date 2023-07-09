package com.github.maujza.read;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.ContinuousStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.PartitionOffset;
import org.apache.spark.sql.types.StructType;
import com.github.maujza.schema.RabbitMQMessageToRowConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class RabbitMQContinousStream implements ContinuousStream {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQContinousStream.class);
    private final StructType schema;
    private final CaseInsensitiveStringMap options;

    public RabbitMQContinousStream(StructType schema, CaseInsensitiveStringMap options) {
        this.schema = schema;
        this.options = options;
    }

    @Override
    public InputPartition[] planInputPartitions(Offset offset) {
        return new InputPartition[] {
                new RabbitMQInputPartition(0)
        };
    }

    @Override
    public ContinuousPartitionReaderFactory createContinuousReaderFactory() {
        return new RabbitMQContinousPartitionReaderFactory(schema, options);
    }

    @Override
    public Offset mergeOffsets(PartitionOffset[] partitionOffsets) {
        return null;
    }

    @Override
    public Offset initialOffset() {
        return null;
    }

    @Override
    public Offset deserializeOffset(String s) {
        return null;
    }

    @Override
    public void commit(Offset offset) {
        LOGGER.info("RabbitMQ has no concept of offsets");
    }

    @Override
    public void stop() {
        LOGGER.info("RabbitMQ continuos stream has stopped");
    }
}
