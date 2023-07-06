package com.github.maujza.read;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;

public class RabbitMQScanBuilder implements ScanBuilder {

    private final StructType schema;

    public RabbitMQScanBuilder(StructType schema) {
        this.schema = schema;
    }
    @Override
    public Scan build() {
        return new RabbitMQScan(schema);
    }
}