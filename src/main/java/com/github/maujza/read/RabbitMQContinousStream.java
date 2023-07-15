package com.github.maujza.read;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.ContinuousStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.PartitionOffset;
import org.apache.spark.sql.execution.streaming.LongOffset;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class RabbitMQContinousStream implements ContinuousStream {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQContinousStream.class);
    private final StructType schema;
    private final CaseInsensitiveStringMap options;
    private final AtomicLong offset;

    public RabbitMQContinousStream(StructType schema, CaseInsensitiveStringMap options) {
        this.schema = schema;
        this.options = options;
        this.offset = new AtomicLong(0);

    }

    @Override
    public InputPartition[] planInputPartitions(Offset offset) {
        LOGGER.info("Planning input partitions with offset: {}", offset);
        return new InputPartition[] {
                new RabbitMQInputPartition(0)
        };
    }

    @Override
    public ContinuousPartitionReaderFactory createContinuousReaderFactory() {
        LOGGER.info("Creating continuous reader factory");
        return new RabbitMQContinousPartitionReaderFactory(schema, options);
    }

    @Override
    public Offset mergeOffsets(PartitionOffset[] partitionOffsets) {
        return new LongOffset(offset.getAndAdd(1000));
    }

    @Override
    public Offset initialOffset() {
        LOGGER.info("RabbitMQ has no concept of offsets");
        return new LongOffset(offset.get());
    }

    @Override
    public Offset deserializeOffset(String s) {
        return null;
    }

    @Override
    public void commit(Offset offset) {
        LOGGER.info("RabbitMQ has no concept of offsets, attempted to commit: {}", offset);
    }

    @Override
    public void stop() {
        LOGGER.info("RabbitMQ continuous stream has stopped");
    }
}
