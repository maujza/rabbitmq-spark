package com.github.maujza.schema;

import com.rabbitmq.client.Delivery;

public class DeliveryDeserializer {
    private static Deserializer deserializer;

    public DeliveryDeserializer(Deserializer deserializer) {
        this.deserializer = deserializer;
    }

    public static String deserialize(Delivery delivery) {
        return deserializer.deserialize(delivery.getBody());
    }
}
