package com.github.maujza.config;

import com.github.maujza.connection.RabbitMQConsumer;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public final class ConsumerConfig extends AbstractRabbitMQConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(Connection.class);

    public ConsumerConfig(Map<String, String> options) {
        super(options);
    }

    @Override
    public RabbitMQConfig withOption(String key, String value) {
        // Create a mutable copy of the existing options
        Map<String, String> extendedOptions = new HashMap<>(this.getOriginals());

        // Update the single key-value pair
        extendedOptions.put(key, value);

        // Create a new ConsumerConfig with the updated options
        return new ConsumerConfig(extendedOptions);
    }

    @Override
    public RabbitMQConfig withOptions(Map<String, String> newOptions) {
        // Create a mutable copy of the existing options
        Map<String, String> extendedOptions = new HashMap<>(this.getOriginals());

        // Merge the new options
        extendedOptions.putAll(newOptions);

        // Create a new ConsumerConfig with the merged options
        return new ConsumerConfig(extendedOptions);
    }


    private Channel setupChannel(Connection connection) throws IOException, TimeoutException, IOException {
        LOGGER.info("Setting up Channel");
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

        RabbitMQConsumer consumer  = new RabbitMQConsumer(channel);

        channel.basicConsume(getQueueName(), false, consumer);

        // Return a new RabbitMQConsumer with the created channel
        return consumer;
    }
}
