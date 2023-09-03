package com.github.maujza;

import com.github.maujza.config.RabbitMQConfig;
import com.github.maujza.read.RabbitMQScanBuilder;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RabbitMQTable implements Table, SupportsRead {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQTable.class);
    private static final Set<TableCapability> TABLE_CAPABILITY_SET = new HashSet<>();

    static {
        TABLE_CAPABILITY_SET.add(TableCapability.CONTINUOUS_READ);
        TABLE_CAPABILITY_SET.add(TableCapability.MICRO_BATCH_READ);
    }

    private final StructType schema;
    private final Transform[] partitioning;
    private final RabbitMQConfig rabbitMQConfig;

    RabbitMQTable(
            final StructType schema, final Transform[] partitioning, final RabbitMQConfig rabbitMQConfig) {
        LOGGER.info("Creating RabbitMQTable");
        this.schema = schema;
        this.partitioning = partitioning;
        this.rabbitMQConfig = rabbitMQConfig;
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public Transform[] partitioning() {
        return partitioning;
    }

    @Override
    public Map<String, String> properties() {
        return rabbitMQConfig.getOriginals();
    }

    @Override
    public StructType schema() {
        return schema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return TABLE_CAPABILITY_SET;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new RabbitMQScanBuilder(schema, rabbitMQConfig.toConsumerConfig());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final RabbitMQTable that = (RabbitMQTable) o;
        return Objects.equals(schema, that.schema)
                && Arrays.equals(partitioning, that.partitioning)
                && Objects.equals(rabbitMQConfig, that.rabbitMQConfig);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(schema, rabbitMQConfig);
        result = 31 * result + Arrays.hashCode(partitioning);
        return result;
    }
}
