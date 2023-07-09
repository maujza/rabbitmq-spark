package com.github.maujza.schema;

import com.rabbitmq.client.Delivery;

public class DeliverySerializer {
    private static Deserializer deserializer;

    public DeliverySerializer(Deserializer deserializer) {
        this.deserializer = deserializer;
    }

    public static String deserialize(Delivery delivery) {
        return deserializer.deserialize(delivery.getBody());
    }
}
