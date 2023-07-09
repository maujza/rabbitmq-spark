package com.github.maujza.read;

import org.apache.spark.sql.connector.read.InputPartition;
import java.util.Objects;

public class RabbitMQInputPartition implements InputPartition {
    private static final long serialVersionUID = 1L;
    private final int partitionId;
    public RabbitMQInputPartition(final int partitionId) {
        this.partitionId = partitionId;
    }
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        };
        final RabbitMQInputPartition that = (RabbitMQInputPartition) o;
        return partitionId == that.partitionId;
    }
    @Override
    public int hashCode() {
        return Objects.hash(partitionId);
    }
}
