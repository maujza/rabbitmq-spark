package com.github.maujza.read;

import com.github.maujza.config.ConsumerConfig;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RabbitMQScanBuilder implements ScanBuilder {
    private final StructType schema;
    private final ConsumerConfig options;
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQScanBuilder.class);

    public RabbitMQScanBuilder(StructType schema, ConsumerConfig options) {
        this.schema = schema;
        this.options = options;
    }

    @Override
    public Scan build() {
        logger.info("Building RabbitMQScan with schema: {} and options: {}", schema, options);
        return new RabbitMQScan(schema, options);
    }
}
