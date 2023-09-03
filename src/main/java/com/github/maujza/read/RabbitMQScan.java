package com.github.maujza.read;

import com.github.maujza.config.ConsumerConfig;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class RabbitMQScan implements Scan {
    private final StructType schema;
    private final ConsumerConfig options;
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQScan.class);

    public RabbitMQScan(StructType schema, ConsumerConfig options) {
        this.schema = schema;
        this.options = options;
        logger.info("Initialized with schema: {} and options: {}", schema, options);
    }

    @Override
    public StructType readSchema() {
        logger.info("Reading schema: {}", schema);
        return schema;
    }

    @Override
    public MicroBatchStream toMicroBatchStream(final String checkpointLocation) {
        logger.info("Creating a MicroBatchStream with no checkpointLocation");
        return new RabbitMQMicroBatchStream(schema, options);
    }
}
