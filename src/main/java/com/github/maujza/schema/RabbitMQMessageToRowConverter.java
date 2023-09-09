package com.github.maujza.schema;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class RabbitMQMessageToRowConverter implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQMessageToRowConverter.class);


    private final StructType schema;
    private final StructField[] fields;

    public RabbitMQMessageToRowConverter(StructType schema) {
        this.schema = schema;
        this.fields = schema.fields();
        LOGGER.info("Initialized RabbitMQMessageToRowConverter with given schema");

    }

    public InternalRow convertToInternalRow(String message) {
        LOGGER.debug("Converting message to InternalRow: {}", message);

        return convertToInternalRow(new JSONObject(message));
    }

    private InternalRow convertToInternalRow(JSONObject jsonObject) {
        LOGGER.debug("Converting JSONObject to InternalRow: {}", jsonObject.toString());

        Object[] values = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
            StructField field = fields[i];
            values[i] = getValue(jsonObject, field.name(), field.dataType());
        }
        return new GenericInternalRow(values);
    }

    private Object getValue(JSONObject jsonObject, String name, DataType dataType) {
        LOGGER.debug("Extracting value for field: {} with type: {}", name, dataType);

        if (jsonObject.isNull(name)) {
            return null;
        }

        if (dataType instanceof StringType) return UTF8String.fromString(jsonObject.getString(name));
        if (dataType instanceof IntegerType) return jsonObject.getInt(name);
        if (dataType instanceof LongType) return jsonObject.getLong(name);
        if (dataType instanceof DoubleType) return jsonObject.getDouble(name);
        if (dataType instanceof BooleanType) return jsonObject.getBoolean(name);
        if (dataType instanceof StructType) return convertToInternalRow(jsonObject.getJSONObject(name));
        if (dataType instanceof ArrayType) return extractList(jsonObject.getJSONArray(name), ((ArrayType) dataType).elementType());

        LOGGER.warn("Unsupported data type: {}", dataType);
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }

    private Object[] extractList(JSONArray jsonArray, DataType elementType) {
        LOGGER.debug("Extracting list with element type: {}", elementType);

        Object[] array = new Object[jsonArray.length()];
        for (int i = 0; i < jsonArray.length(); i++) {
            if (elementType instanceof StructType) {
                array[i] = convertToInternalRow(jsonArray.getJSONObject(i));
            } else if (elementType instanceof ArrayType) {
                array[i] = extractList(jsonArray.getJSONArray(i), ((ArrayType) elementType).elementType());
            } else {
                array[i] = getValue(jsonArray, i, elementType);
            }
        }
        return array;
    }

    private Object getValue(JSONArray jsonArray, int index, DataType dataType) {
        LOGGER.debug("Extracting value from array at index: {} with type: {}", index, dataType);

        if (dataType instanceof StringType) return UTF8String.fromString(jsonArray.getString(index));
        if (dataType instanceof IntegerType) return jsonArray.getInt(index);
        if (dataType instanceof LongType) return jsonArray.getLong(index);
        if (dataType instanceof DoubleType) return jsonArray.getDouble(index);
        if (dataType instanceof BooleanType) return jsonArray.getBoolean(index);

        LOGGER.warn("Unsupported data type in array: {}", dataType);
        throw new UnsupportedOperationException("Unsupported data type in array: " + dataType);
    }
}
