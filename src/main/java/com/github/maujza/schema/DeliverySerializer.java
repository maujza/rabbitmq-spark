package com.github.maujza.utils;

import com.github.maujza.schema.UTF8Deserializer;
import com.rabbitmq.client.Delivery;

public class DeliverySerializer {
    private static UTF8Deserializer deserializer = new UTF8Deserializer();

    public static String deserialize(Delivery delivery) {
        return deserializer.deserialize(delivery.getBody());
    }

    public static byte[] serialize(String message) {
        return deserializer.serialize(message);
    }
}
