package com.github.maujza.rabbitmq.spark.read;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

final class RabbitMQStreamScan implements Scan {
    private final StructType schema;
    private final Map<String, String> options;
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQStreamScan.class);

    public RabbitMQStreamScan(StructType schema, Map<String, String> options) {
        this.schema = schema;
        this.options = options;
        LOGGER.info("Initialized with schema: {} and options: {}", schema, options);
    }

    @Override
    public StructType readSchema() {
        LOGGER.info("Reading schema: {}", schema);
        return schema;
    }

    @Override
    public MicroBatchStream toMicroBatchStream(final String checkpointLocation) {
        LOGGER.info("Creating a MicroBatchStream with checkpointLocation: {}", checkpointLocation);
        return new RabbitMQStreamMicroBatchStream(schema, checkpointLocation, options);
    }
}