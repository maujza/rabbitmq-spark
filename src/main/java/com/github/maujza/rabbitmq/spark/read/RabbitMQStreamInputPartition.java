package com.github.maujza.rabbitmq.spark.read;

import com.github.maujza.rabbitmq.spark.offset.RabbitMQStreamTimestampOffset;
import org.apache.spark.sql.connector.read.InputPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class RabbitMQStreamInputPartition implements InputPartition {
    private static final long serialVersionUID = 1L;
    private final int partitionId;
    private final RabbitMQStreamTimestampOffset startOffset;  // Starting offset for this partition
    private final RabbitMQStreamTimestampOffset endOffset;    // Ending offset for this partition
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQStreamInputPartition.class);

    public RabbitMQStreamInputPartition(final int partitionId, RabbitMQStreamTimestampOffset startOffset, RabbitMQStreamTimestampOffset endOffset) {
        this.partitionId = partitionId;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        logger.info("Created partition with ID: {} and Offset range: {} to {}", partitionId, startOffset.json(), endOffset.json());
    }

    public RabbitMQStreamTimestampOffset getStartOffset() {
        return startOffset;
    }

    public RabbitMQStreamTimestampOffset getEndOffset() {
        return endOffset;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final RabbitMQStreamInputPartition that = (RabbitMQStreamInputPartition) o;
        return partitionId == that.partitionId && Objects.equals(startOffset, that.startOffset) && Objects.equals(endOffset, that.endOffset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionId, startOffset, endOffset);
    }

    @Override
    public String toString() {
        return "RabbitMQStreamInputPartition{" +
                "partitionId=" + partitionId +
                ", startOffset=" + startOffset.json() +
                ", endOffset=" + endOffset.json() +
                '}';
    }
}
