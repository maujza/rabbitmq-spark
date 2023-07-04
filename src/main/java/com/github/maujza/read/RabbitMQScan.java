package com.github.maujza.read;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;

final class RabbitMQScan implements Scan {

    @Override
    public StructType readSchema() {
        return null;
    }
}