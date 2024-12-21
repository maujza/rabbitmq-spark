package com.github.maujza.rabbitmq.spark;

import com.github.maujza.rabbitmq.spark.RabbitMQTable;
import com.github.maujza.rabbitmq.spark.RabbitMQTableProvider;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class RabbitMQTableProviderTest {

    @Test
    void testRabbitMQTableProvider() {
        RabbitMQTableProvider tableProvider = new RabbitMQTableProvider();
        assertEquals("rabbitmq", tableProvider.shortName());
        assertTrue(tableProvider.supportsExternalMetadata());
    }

    @Test
    void testInferSchema() {
        RabbitMQTableProvider tableProvider = new RabbitMQTableProvider();
        StructType schema = tableProvider.inferSchema(new CaseInsensitiveStringMap(new HashMap<>()));

        assertEquals(3, schema.fields().length);
        assertEquals("timestamp", schema.fields()[0].name());
        assertEquals(DataTypes.LongType, schema.fields()[0].dataType());
        assertEquals("payload", schema.fields()[1].name());
        assertEquals(DataTypes.StringType, schema.fields()[1].dataType());
        assertEquals("_raw", schema.fields()[2].name());
        assertEquals(DataTypes.BooleanType, schema.fields()[2].dataType());
    }

    @Test
    void testGetTable() {
        RabbitMQTableProvider tableProvider = new RabbitMQTableProvider();
        StructType schema = new StructType(new StructField[]{
                new StructField("test", DataTypes.StringType, true, null)
        });
        Transform[] partitioning = new Transform[0];
        Map<String, String> properties = new HashMap<>();

        Table table = tableProvider.getTable(schema, partitioning, properties);

        assertTrue(table instanceof RabbitMQTable);
        assertEquals(schema, table.schema());
        assertArrayEquals(partitioning, table.partitioning());
        assertEquals(properties, table.properties());
    }
}