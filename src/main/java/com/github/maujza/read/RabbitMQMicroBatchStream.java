package com.github.maujza.read;

import com.github.maujza.config.ConsumerConfig;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.execution.streaming.LongOffset;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class RabbitMQMicroBatchStream implements MicroBatchStream {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQMicroBatchStream.class);
    private final StructType schema;
    private final ConsumerConfig options;
    private AtomicLong offset;

    public RabbitMQMicroBatchStream(StructType schema, ConsumerConfig options) {
        this.schema = schema;
        this.options = options;
        this.offset = new AtomicLong(0);
        LOGGER.info("Initialized RabbitMQMicroBatchStream with given schema and options");
    }

    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
        LOGGER.info("Planning input partitions from {} to {}", start, end);
        return new InputPartition[] {
                new RabbitMQInputPartition(0)
        };
    }

    @Override
    public Offset latestOffset() {
        Offset newOffset = new LongOffset(offset.getAndAdd(1000));
        LOGGER.info("Latest offset set to {}", newOffset);
        return newOffset;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        LOGGER.info("Creating micro batch reader factory");
        return new RabbitMQMicroBatchPartitionReaderFactory(schema, options);
    }

    @Override
    public Offset initialOffset() {
        Offset initial = new LongOffset(offset.get());
        LOGGER.info("Initial dummy offset set to {}", initial);
        return initial;
    }

    @Override
    public Offset deserializeOffset(String s) {
        LOGGER.warn("RabbitMQ has no concept of offsets, attempted to deserialize: {}", s);
        return null;
    }

    @Override
    public void commit(Offset offset) {
        LOGGER.warn("RabbitMQ has no concept of offsets, attempted to commit: {}", offset);
    }

    @Override
    public void stop() {
        LOGGER.info("RabbitMQ micro batch stream has stopped");
    }
}


