package com.github.maujza.schema;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class RabbitMQMessageToRowConverter implements Serializable {

    private final StructType schema;

    public RabbitMQMessageToRowConverter(StructType schema) {
        this.schema = schema;
    }

    public InternalRow convertToInternalRow(String message) {
        JSONObject jsonObject = new JSONObject(message);
        Object[] values = new Object[schema.length()];
        for (int i = 0; i < schema.length(); i++) {
            StructField field = schema.fields()[i];
            values[i] = getValue(jsonObject, field.name(), field.dataType());
        }
        return new GenericInternalRow(values);
    }

    private Object getValue(JSONObject jsonObject, String name, DataType dataType) {
        if (jsonObject.isNull(name)) {
            return null;
        } else if (dataType instanceof StringType) {
            return jsonObject.getString(name);
        } else if (dataType instanceof IntegerType) {
            return jsonObject.getInt(name);
        } else if (dataType instanceof LongType) {
            return jsonObject.getLong(name);
        } else if (dataType instanceof DoubleType) {
            return jsonObject.getDouble(name);
        } else if (dataType instanceof BooleanType) {
            return jsonObject.getBoolean(name);
        } else if (dataType instanceof StructType) {
            return convertToInternalRow(jsonObject.getJSONObject(name).toString());
        } else if (dataType instanceof ArrayType) {
            return extractList(jsonObject.getJSONArray(name), ((ArrayType) dataType).elementType());
        } else {
            throw new UnsupportedOperationException("Unsupported data type: " + dataType);
        }
    }

    private Object[] extractList(JSONArray jsonArray, DataType elementType) {
        List<Object> list = new ArrayList<>();
        for (int i = 0; i < jsonArray.length(); i++) {
            if (elementType instanceof StructType) {
                list.add(convertToInternalRow(jsonArray.getJSONObject(i).toString()));
            } else if (elementType instanceof ArrayType) {
                list.add(extractList(jsonArray.getJSONArray(i), ((ArrayType) elementType).elementType()));
            } else {
                JSONObject tempObject = jsonArray.getJSONObject(i);
                list.add(getValue(tempObject, tempObject.keys().next(), elementType));
            }
        }
        return list.toArray();
    }
}
