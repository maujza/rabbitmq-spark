package com.github.maujza;

import java.util.Map;

import com.github.maujza.config.RabbitMQConfig;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public final class RabbitMQTableProvider implements TableProvider, DataSourceRegister {

    public RabbitMQTableProvider() {};

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap caseInsensitiveStringMap) {
        throw new UnsupportedOperationException("Schema inference is not supported.");
    }

    @Override
    public Table getTable(StructType schema, Transform[] transforms, Map<String, String> properties) {
        return new RabbitMQTable(schema, transforms, RabbitMQConfig.createConfig(properties));
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
