package com.github.maujza.read;

import org.apache.spark.sql.connector.read.InputPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class RabbitMQInputPartition implements InputPartition {
    private static final long serialVersionUID = 1L;
    private final int partitionId;
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQInputPartition.class);

    public RabbitMQInputPartition(final int partitionId) {
        this.partitionId = partitionId;
        logger.info("Created partition with ID: {}", partitionId);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final RabbitMQInputPartition that = (RabbitMQInputPartition) o;
        boolean equals = partitionId == that.partitionId;
        logger.debug("Comparing partitions: {} and {}. Equal: {}", this, that, equals);
        return equals;
    }

    @Override
    public int hashCode() {
        int hashCode = Objects.hash(partitionId);
        logger.debug("Generated hash code for partition {}: {}", partitionId, hashCode);
        return hashCode;
    }
}
