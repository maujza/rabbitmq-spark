package com.github.maujza.read;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.ContinuousStream;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

final class RabbitMQScan implements Scan {

    private final StructType schema;
    private final CaseInsensitiveStringMap options;

    public RabbitMQScan(StructType schema, CaseInsensitiveStringMap options) {

        this.schema = schema;
        this.options = options;
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public ContinuousStream toContinuousStream(final String checkpointLocation) {
        return new RabbitMQContinousStream(schema, options);
    }
}