package com.github.maujza;

import com.github.maujza.read.RabbitMQScanBuilder;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.connector.read.ScanBuilder;
import java.util.Set;

public class RabbitMQTable implements Table, SupportsRead {
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
        return null;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new RabbitMQScanBuilder();
    }
}
