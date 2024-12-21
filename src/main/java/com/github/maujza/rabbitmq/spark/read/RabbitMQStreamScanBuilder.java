package com.github.maujza.rabbitmq.spark.read;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class RabbitMQStreamScanBuilder implements ScanBuilder {
    private final StructType schema;
    private final Map<String, String> options;
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQStreamScanBuilder.class);

    public RabbitMQStreamScanBuilder(StructType schema, Map<String, String> options) {
        this.schema = schema;
        this.options = options;
    }

    @Override
    public Scan build() {
        LOGGER.info("Building RabbitMQScan with schema: {} and options: {}", schema, options);
        return new RabbitMQStreamScan(schema, options);
    }
}