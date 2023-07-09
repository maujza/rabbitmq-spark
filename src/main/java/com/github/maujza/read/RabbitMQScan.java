package com.github.maujza.read;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.ContinuousStream;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class RabbitMQScan implements Scan {
    private final StructType schema;
    private final CaseInsensitiveStringMap options;
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQScan.class);

    public RabbitMQScan(StructType schema, CaseInsensitiveStringMap options) {
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
    public ContinuousStream toContinuousStream(final String checkpointLocation) {
        logger.info("Creating a ContinuousStream with no checkpointLocation");
        return new RabbitMQContinousStream(schema, options);
    }
}
