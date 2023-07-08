package com.github.maujza.read;

import com.github.maujza.schema.RabbitMQMessageToRowConverter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReader;
import org.apache.spark.sql.connector.read.streaming.PartitionOffset;
import com.github.maujza.schema.RabbitMQMessageToRowConverter;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import java.io.IOException;

public class RabbitMQContinousPartitionReader implements ContinuousPartitionReader<InternalRow> {

    private final RabbitMQMessageToRowConverter rabbitMQMessageToRowConverter;
    private final CaseInsensitiveStringMap options;

    public RabbitMQContinousPartitionReader(final RabbitMQMessageToRowConverter rabbitMQMessageToRowConverter, final CaseInsensitiveStringMap options) {
        this.options = options;
        this.rabbitMQMessageToRowConverter = rabbitMQMessageToRowConverter;
    }
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
