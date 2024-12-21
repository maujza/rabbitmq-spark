package com.github.maujza.rabbitmq.spark.schema;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RabbitMQMessageToRowConverter implements Serializable {
    private final StructType schema;
    private final ObjectMapper objectMapper;
    private final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public RabbitMQMessageToRowConverter(StructType schema) {
        this.schema = schema;
        this.objectMapper = new ObjectMapper();
    }

    private Object convertField(Object jsonValue, DataType dataType) {
        if (jsonValue == null) {
            return null;
        }

        switch (dataType.typeName()) {
            case "string":
                return UTF8String.fromString(jsonValue.toString());
            case "integer":
                try {
                    return Integer.parseInt(jsonValue.toString());
                } catch (NumberFormatException e) {
                    return null; // or a default value like 0
                }
            case "double":
                try {
                    return Double.parseDouble(jsonValue.toString());
                } catch (NumberFormatException e) {
                    return Double.NaN; // or null
                }
            case "boolean":
                return Boolean.parseBoolean(jsonValue.toString());
            case "timestamp":
                try {
                    return new Timestamp(timestampFormat.parse(jsonValue.toString()).getTime());
                } catch (Exception e) {
                    return null; // handle parse exceptions
                }
            case "array":
                List<?> jsonList = (List<?>) jsonValue;
                ArrayType arrayType = (ArrayType) dataType;
                Object[] convertedArray = new Object[jsonList.size()];
                for (int i = 0; i < jsonList.size(); i++) {
                    convertedArray[i] = convertField(jsonList.get(i), arrayType.elementType());
                }
                return convertedArray;
            case "map":
                Map<?, ?> jsonMap = (Map<?, ?>) jsonValue;
                MapType mapType = (MapType) dataType;
                Map<Object, Object> convertedMap = new HashMap<>();
                for (Map.Entry<?, ?> entry : jsonMap.entrySet()) {
                    convertedMap.put(
                            convertField(entry.getKey(), mapType.keyType()),
                            convertField(entry.getValue(), mapType.valueType())
                    );
                }
                return convertedMap;
            case "struct":
                if (jsonValue instanceof Map) {
                    @SuppressWarnings("unchecked") // Safely cast after instanceof check
                    Map<String, Object> nestedJsonMap = (Map<String, Object>) jsonValue;
                    return convertToInternalRow(nestedJsonMap, (StructType) dataType);
                }
                return null;
            default:
                return null; // default to null for unsupported data types
        }
    }

    public InternalRow convertToInternalRow(Map<String, Object> jsonMap, StructType schema) {
        Object[] values = new Object[schema.size()];

        for (int i = 0; i < schema.size(); i++) {
            StructField field = schema.fields()[i];
            Object value = jsonMap.get(field.name());
            values[i] = convertField(value, field.dataType());
        }

        return new GenericInternalRow(values);
    }

    public InternalRow convertToInternalRow(String message) throws Exception {
        Map<String, Object> deserializedMessage = objectMapper.readValue(message, new TypeReference<Map<String, Object>>() {});
        return convertToInternalRow(deserializedMessage, schema);
    }
}