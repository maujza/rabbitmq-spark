package com.github.maujza.read;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.ContinuousStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.PartitionOffset;

public class RabbitMQContinousStream implements ContinuousStream {
    @Override
    public InputPartition[] planInputPartitions(Offset offset) {
        return new InputPartition[0];
    }

    @Override
    public ContinuousPartitionReaderFactory createContinuousReaderFactory() {
        return new RabbitMQContinousPartitionReaderFactory();
    }

    @Override
    public Offset mergeOffsets(PartitionOffset[] partitionOffsets) {
        return null;
    }

    @Override
    public Offset initialOffset() {
        return null;
    }

    @Override
    public Offset deserializeOffset(String s) {
        return null;
    }

    @Override
    public void commit(Offset offset) {

    }

    @Override
    public void stop() {

    }
}
