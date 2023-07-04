package com.github.maujza.read;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReader;
import org.apache.spark.sql.connector.read.streaming.PartitionOffset;

import java.io.IOException;

public class RabbitMQContinousPartitionReader implements ContinuousPartitionReader<InternalRow> {
    @Override
    public PartitionOffset getOffset() {
        return null;
    }

    @Override
    public boolean next() throws IOException {
        return false;
    }

    @Override
    public InternalRow get() {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
