package com.github.maujza.rabbitmq.spark.offset;


import org.apache.spark.sql.connector.read.streaming.Offset;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.Serializable;

import static java.lang.String.format;

public class RabbitMQStreamTimestampOffset extends Offset implements Serializable {
    private static final String JSON_TEMPLATE = "{\"timestamp\": %d}";
    private final long timestamp;

    public RabbitMQStreamTimestampOffset(long timestamp) {
        this.timestamp = timestamp;
    }

    public static RabbitMQStreamTimestampOffset fromJson(String json) {
        try {
            JSONObject offsetDocument = new JSONObject(json);
            long timestamp = offsetDocument.getLong("timestamp");
            return new RabbitMQStreamTimestampOffset(timestamp);
        } catch (JSONException e) {
            throw new IllegalArgumentException("Invalid JSON format for offset", e);
        }
    }

    @Override
    public String json() {
        return format(JSON_TEMPLATE, timestamp);
    }

    public long getTimestamp() {
        return timestamp;
    }
}
