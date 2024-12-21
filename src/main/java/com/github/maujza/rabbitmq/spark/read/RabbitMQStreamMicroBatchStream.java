package com.github.maujza.rabbitmq.spark.read;

import com.github.maujza.rabbitmq.spark.offset.RabbitMQStreamOffsetStore;
import com.github.maujza.rabbitmq.spark.offset.RabbitMQStreamTimestampOffset;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;

public class RabbitMQStreamMicroBatchStream implements MicroBatchStream {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQStreamMicroBatchStream.class);
    private final StructType schema;
    private final Map<String, String> options;
    private final RabbitMQStreamOffsetStore offsetStore;
    private volatile long lastTimestamp = Instant.now().toEpochMilli();


    public RabbitMQStreamMicroBatchStream(StructType schema, final String checkpointLocation, Map<String, String> options) {
        this.schema = schema;
        this.options = options;
        this.offsetStore = new RabbitMQStreamOffsetStore(
                SparkContext.getOrCreate().hadoopConfiguration(),
                checkpointLocation,
                new RabbitMQStreamTimestampOffset(lastTimestamp)
        );
    }

    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
        LOGGER.info("Planning input partitions from {} to {}", start, end);
        return new InputPartition[] {
                new RabbitMQStreamInputPartition(0, (RabbitMQStreamTimestampOffset) start, (RabbitMQStreamTimestampOffset) end)
        };
    }

    @Override
    public Offset latestOffset() {
        long now = Instant.now().toEpochMilli();
        LOGGER.info("Fetching latest offset at timestamp: {}", now);
        if (lastTimestamp < now) {
            lastTimestamp = now;
        }
        return new RabbitMQStreamTimestampOffset(lastTimestamp);
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        LOGGER.info("Creating micro batch reader factory");
        return new RabbitMQStreamMicroBatchPartitionReaderFactory(schema, options);
    }

    @Override
    public Offset initialOffset() {
        Offset initialOffset = offsetStore.initialOffset();
        LOGGER.info("Fetched initial offset: {}", initialOffset);
        return initialOffset;
    }

    @Override
    public Offset deserializeOffset(String json) {
        Offset deserializedOffset = RabbitMQStreamTimestampOffset.fromJson(json);
        LOGGER.info("Deserialized offset: {} from JSON: {}", deserializedOffset, json);
        return deserializedOffset;
    }

    @Override
    public void commit(Offset end) {
        LOGGER.info("Committing offset: {}", end.json());
        offsetStore.updateOffset((RabbitMQStreamTimestampOffset) end);
    }

    @Override
    public void stop() {
        LOGGER.info("RabbitMQ micro batch stream has stopped");
    }
}
