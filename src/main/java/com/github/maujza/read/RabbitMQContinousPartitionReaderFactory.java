package com.github.maujza.read;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReader;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReaderFactory;

public class RabbitMQContinousPartitionReaderFactory implements ContinuousPartitionReaderFactory {
    @Override
    public ContinuousPartitionReader<InternalRow> createReader(InputPartition inputPartition) {
        return new RabbitMQContinousPartitionReader();
    }
}
