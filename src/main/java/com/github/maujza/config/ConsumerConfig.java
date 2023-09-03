package com.github.maujza.config;

import com.github.maujza.connection.RabbitMQConsumer;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public final class ConsumerConfig extends AbstractRabbitMQConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(Connection.class);

    public ConsumerConfig(Map<String, String> options) {
        super(options);
    }

    @Override
    public RabbitMQConfig withOption(String key, String value) {
        return null;
    }

    @Override
    public RabbitMQConfig withOptions(Map<String, String> options) {
        return null;
    }

    private Channel setupChannel(Connection connection) throws IOException, TimeoutException, IOException {
        LOGGER.debug("Setting up Channel");
        Channel chan = connection.createChannel();
        if (isPrefetchPresent()) {
            chan.basicQos(getPrefetch(), true);
        } else {
            LOGGER.warn("Prefetch count not configured; proceeding without it.");
        }
        return chan;
    }

    public RabbitMQConsumer createConsumer(Connection connection) throws Exception {

        // Create a new channel from the connection
        Channel channel = setupChannel(connection);

        // Return a new RabbitMQConsumer with the created channel
        return new RabbitMQConsumer(channel);
    }
}
