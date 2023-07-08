package com.github.maujza.read;


import com.rabbitmq.client.Channel;

import java.io.IOException;

public class Util {
    public static void declareQueueDefaults(Channel channel, String queueName) throws IOException {
        channel.queueDeclare(queueName, true, false, false, null);
    }
}
