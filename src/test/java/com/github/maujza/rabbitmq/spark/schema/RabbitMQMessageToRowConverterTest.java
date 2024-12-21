package com.github.maujza.rabbitmq.spark.schema;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class RabbitMQMessageToRowConverterTest {

    private RabbitMQMessageToRowConverter converter;
    private StructType schema;

    @BeforeEach
    void setUp() {
        schema = new StructType()
                .add("string_field", DataTypes.StringType)
                .add("int_field", DataTypes.IntegerType)
                .add("double_field", DataTypes.DoubleType)
                .add("boolean_field", DataTypes.BooleanType)
                .add("timestamp_field", DataTypes.TimestampType)
                .add("array_field", DataTypes.createArrayType(DataTypes.StringType))
                .add("map_field", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType))
                .add("struct_field", new StructType()
                        .add("nested_string", DataTypes.StringType)
                        .add("nested_int", DataTypes.IntegerType));

        converter = new RabbitMQMessageToRowConverter(schema);
    }

    @Test
    @DisplayName("Test conversion of all supported types")
    void testConvertAllTypes() throws Exception {
        String message = "{" +
                "\"string_field\":\"test\"," +
                "\"int_field\":42," +
                "\"double_field\":3.14," +
                "\"boolean_field\":true," +
                "\"timestamp_field\":\"2023-05-01 12:34:56.789\"," +
                "\"array_field\":[\"a\",\"b\",\"c\"]," +
                "\"map_field\":{\"key1\":1,\"key2\":2}," +
                "\"struct_field\":{\"nested_string\":\"nested\",\"nested_int\":10}" +
                "}";

        InternalRow row = converter.convertToInternalRow(message);

        assertEquals(UTF8String.fromString("test"), row.get(0, schema.fields()[0].dataType()));
        assertEquals(42, row.getInt(1));
        assertEquals(3.14, row.getDouble(2), 0.001);
        assertTrue(row.getBoolean(3));
        assertEquals(Timestamp.valueOf("2023-05-01 12:34:56.789"), row.get(4, schema.fields()[4].dataType()));

        Object[] arrayField = (Object[]) row.get(5, schema.fields()[5].dataType());
        assertArrayEquals(new UTF8String[]{UTF8String.fromString("a"), UTF8String.fromString("b"), UTF8String.fromString("c")},
                arrayField);

        Map<?, ?> actualMap = (Map<?, ?>) row.get(6, schema.fields()[6].dataType());
        assertEquals(2, actualMap.size());
        assertEquals(1, actualMap.get(UTF8String.fromString("key1")));
        assertEquals(2, actualMap.get(UTF8String.fromString("key2")));

        InternalRow nestedRow = (InternalRow) row.get(7, schema.fields()[7].dataType());
        assertEquals(UTF8String.fromString("nested"), nestedRow.get(0, DataTypes.StringType));
        assertEquals(10, nestedRow.getInt(1));
    }

    @Test
    @DisplayName("Test handling of null values")
    void testNullValues() throws Exception {
        String message = "{" +
                "\"string_field\":null," +
                "\"int_field\":null," +
                "\"double_field\":null," +
                "\"boolean_field\":null," +
                "\"timestamp_field\":null," +
                "\"array_field\":null," +
                "\"map_field\":null," +
                "\"struct_field\":null" +
                "}";

        InternalRow row = converter.convertToInternalRow(message);

        for (int i = 0; i < schema.fields().length; i++) {
            assertNull(row.get(i, schema.fields()[i].dataType()));
        }
    }

    @Test
    @DisplayName("Test handling of missing fields")
    void testMissingFields() throws Exception {
        String message = "{}";

        InternalRow row = converter.convertToInternalRow(message);

        for (int i = 0; i < schema.fields().length; i++) {
            assertNull(row.get(i, schema.fields()[i].dataType()));
        }
    }

    @Test
    @DisplayName("Test handling of invalid data types")
    void testInvalidDataTypes() throws Exception {
        String message = "{" +
                "\"string_field\":42," +
                "\"int_field\":\"not an int\"," +
                "\"double_field\":\"not a double\"," +
                "\"boolean_field\":\"not a boolean\"," +
                "\"timestamp_field\":\"invalid timestamp\"" +
                "}";

        InternalRow row = converter.convertToInternalRow(message);

        assertEquals(UTF8String.fromString("42"), row.get(0, schema.fields()[0].dataType()));
        assertNull(row.get(1, schema.fields()[1].dataType())); // null for invalid int
        assertTrue(Double.isNaN(row.getDouble(2))); // NaN for invalid double
        assertFalse(row.getBoolean(3)); // default value for boolean
        assertNull(row.get(4, schema.fields()[4].dataType())); // null for invalid timestamp
    }
}