package com.github.maujza.read;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;

public class RabbitMQScanBuilder implements ScanBuilder {
    @Override
    public Scan build() {
        return new RabbitMQScan();
    }
}