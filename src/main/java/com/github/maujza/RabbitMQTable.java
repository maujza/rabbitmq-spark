package com.github.maujza;

import static java.util.Arrays.asList;
import com.github.maujza.read.RabbitMQScanBuilder;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class RabbitMQTable implements Table, SupportsRead {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQTable.class);
    private static final Set<TableCapability> TABLE_CAPABILITY_SET = new HashSet<>(asList(TableCapability.CONTINUOUS_READ));
    @Override
    public String name() {
        return null;
    }

    @Override
    public StructType schema() {
        return null;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return TABLE_CAPABILITY_SET;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new RabbitMQScanBuilder();
    }
}
