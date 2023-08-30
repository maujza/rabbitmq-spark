package com.github.maujza.read;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.execution.streaming.LongOffset;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class RabbitMQMicroBatchStream implements MicroBatchStream {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQMicroBatchStream.class);
    private final StructType schema;
    private final CaseInsensitiveStringMap options;

    private AtomicLong offset;


    public RabbitMQMicroBatchStream(StructType schema, CaseInsensitiveStringMap options) {
        this.schema = schema;
        this.options = options;
        this.offset = new AtomicLong(0);

    }

    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
        LOGGER.info("Planning input partitions with no offsets");
        return new InputPartition[] {
                new RabbitMQInputPartition(0)
        };
    }

    @Override
    public Offset latestOffset() {
        return new LongOffset(offset.getAndAdd(1000));
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        LOGGER.info("Creating micro batch reader factory");
        return new RabbitMQMicroBatchPartitionReaderFactory(schema, options);
    }

    @Override
    public Offset initialOffset() {
        LOGGER.info("RabbitMQ has no concept of offsets, initial offset set to 0");
        return new LongOffset(offset.get());
    }

    @Override
    public Offset deserializeOffset(String s) {
        LOGGER.info("RabbitMQ has no concept of offsets, attempted to deserialize: {}", s);
        return null;
    }

    @Override
    public void commit(Offset offset) {
        LOGGER.info("RabbitMQ has no concept of offsets, attempted to commit: {}", offset);
    }

    @Override
    public void stop() {
        LOGGER.info("RabbitMQ micro batch stream has stopped");
    }
}

