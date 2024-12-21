package com.github.maujza.rabbitmq.spark.offset;

import com.rabbitmq.stream.Message;

public class RabbitMQStreamMessageWithOffset {

    private final Message message;
    private final long offset; // Storing offset as long

    public RabbitMQStreamMessageWithOffset(Message message, long offset) {
        this.message = message;
        this.offset = offset;
    }

    public Message getMessage() {
        return message;
    }

    public long getOffset() {
        return offset;
    }
}
