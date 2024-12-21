package com.github.maujza.rabbitmq.spark;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public final class RabbitMQTableProvider implements TableProvider, DataSourceRegister {

    public RabbitMQTableProvider() {};

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap caseInsensitiveStringMap) {
        StructField[] fields = new StructField[]{
                new StructField("timestamp", DataTypes.LongType, false, new MetadataBuilder().putBoolean("raw", true).build()),
                new StructField("payload", DataTypes.StringType, true, new MetadataBuilder().putBoolean("raw", true).build()),
                new StructField("_raw", DataTypes.BooleanType, true, new MetadataBuilder().putBoolean("raw", true).build()),
        };

        return new StructType(fields);
    }

    @Override
    public Table getTable(StructType schema, Transform[] transforms, Map<String, String> properties) {
        return new RabbitMQTable(schema, transforms, properties);
    }

    @Override
    public String shortName() {
        return "rabbitmq";
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }
}