package com.github.maujza.rabbitmq.spark;


import com.github.maujza.rabbitmq.spark.read.RabbitMQStreamScanBuilder;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RabbitMQTable implements Table, SupportsRead {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQTable.class);
    private static final Set<TableCapability> TABLE_CAPABILITY_SET = new HashSet<>();

    static {
        TABLE_CAPABILITY_SET.add(TableCapability.MICRO_BATCH_READ);
    }

    private final StructType schema;
    private final Transform[] partitioning;
    private final Map<String, String> properties;

    RabbitMQTable(
            final StructType schema, final Transform[] partitioning, final Map<String, String> properties) {
        LOGGER.info("Initializing RabbitMQTable with given schema, partitioning, and RabbitMQ configuration.");
        this.schema = schema;
        this.partitioning = partitioning;
        this.properties = properties;
    }

    @Override
    public String name() {
        LOGGER.debug("Retrieving table name, which is null in this case.");
        if (properties.containsKey("stream")) {
            return "RabbitMQStreamTable("+properties.get("stream")+")";
        }
        return "RabbitMQStreamTable()";
    }

    @Override
    public Transform[] partitioning() {
        LOGGER.debug("Retrieving partitioning information.");
        return partitioning;
    }

    @Override
    public Map<String, String> properties() {
        LOGGER.debug("Retrieving RabbitMQ table properties.");
        return properties;
    }

    @Override
    public StructType schema() {
        LOGGER.debug("Retrieving schema information for the RabbitMQ table.");
        return schema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        LOGGER.debug("Retrieving table capabilities.");
        return TABLE_CAPABILITY_SET;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        LOGGER.info("Creating new RabbitMQScanBuilder with given options.");
        return new RabbitMQStreamScanBuilder(schema, properties);
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
                && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(schema, properties);
        result = 31 * result + Arrays.hashCode(partitioning);
        return result;
    }
}
