package com.github.maujza.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.sql.Row;
import java.util.function.Function;

import java.io.IOException;
import java.io.Serializable;

public class RabbitMQMessageToRowConverter implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQMessageToRowConverter.class);

    private final StructType schema;
    private final StructField[] fields;
    private final ObjectMapper objectMapper;
    private final Function<Row, InternalRow> rowToInternalRowFunction;

    public RabbitMQMessageToRowConverter(StructType schema) {
        this.schema = schema;
        this.fields = schema.fields();
        this.objectMapper = new ObjectMapper();
        this.rowToInternalRowFunction = new RowToInternalRowFunction(schema);
        LOGGER.info("Initialized RabbitMQMessageToRowConverter with given schema");
    }

    public InternalRow convertToInternalRow(String message) {
        try {
            JsonNode rootNode = objectMapper.readTree(message);
            return rowToInternalRowFunction.apply(convertToInternalRow(rootNode, this.schema));
        } catch (IOException e) {
            LOGGER.error("Failed to convert message to JsonNode: {}", e.getMessage());
            return null;
        }
    }

    private GenericRowWithSchema convertToInternalRow(JsonNode node, StructType schema) {
        Object[] values = new Object[fields.length];
        populateValues(node, values);
        return new GenericRowWithSchema(values, schema);
    }

    private void populateValues(JsonNode node, Object[] values) {
        for (int i = 0; i < fields.length; i++) {
            StructField field = fields[i];
            JsonNode fieldNode = node.get(field.name());
            values[i] = getValue(fieldNode, field.dataType());
        }
    }

    private Object getValue(JsonNode node, DataType dataType) {
        if (node == null || node.isNull()) {
            return null;
        }

        switch (dataType.typeName()) {
            case "string":
                return UTF8String.fromString(node.asText());
            case "integer":
                return node.asInt();
            case "long":
                return node.asLong();
            case "double":
                return node.asDouble();
            case "boolean":
                return node.asBoolean();
            case "struct":
                return convertToInternalRow(node, (StructType) dataType);
            case "array":
                return extractList(node, ((ArrayType) dataType).elementType());
            default:
                LOGGER.warn("Unsupported data type: {}", dataType);
                throw new UnsupportedOperationException("Unsupported data type: " + dataType);
        }
    }

    private Object[] extractList(JsonNode arrayNode, DataType elementType) {
        Object[] array = new Object[arrayNode.size()];
        for (int i = 0; i < arrayNode.size(); i++) {
            array[i] = getValue(arrayNode.get(i), elementType);
        }
        return array;
    }
}
