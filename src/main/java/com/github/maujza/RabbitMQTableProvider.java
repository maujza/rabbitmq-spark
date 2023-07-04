package com.github.maujza;

import java.util.Map;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public final class RabbitMQTableProvider implements TableProvider, DataSourceRegister {
    @Override
    public StructType inferSchema(CaseInsensitiveStringMap caseInsensitiveStringMap) {
        return null;
    }

    @Override
    public Table getTable(StructType structType, Transform[] transforms, Map<String, String> map) {
        return new RabbitMQTable();
    }

    @Override
    public String shortName() {
        return "rabbitmq";
    }
}
