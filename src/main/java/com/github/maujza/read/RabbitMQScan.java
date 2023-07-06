package com.github.maujza.read;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.ContinuousStream;
import org.apache.spark.sql.types.StructType;

final class RabbitMQScan implements Scan {

    private final StructType schema;

    public RabbitMQScan(StructType schema) {
        this.schema = schema;
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public ContinuousStream toContinuousStream(final String checkpointLocation) {
        return new RabbitMQContinousStream(schema);
    }
}