package com.github.maujza.schema;

import com.rabbitmq.client.Delivery;

public class DeliverySerializer {
    private static final UTF8Deserializer deserializer = new UTF8Deserializer();

    public static String deserialize(Delivery delivery) {
        return deserializer.deserialize(delivery.getBody());
    }
}
