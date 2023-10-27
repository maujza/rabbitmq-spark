package com.github.maujza.config;

import com.github.maujza.connection.RabbitMQConsumer;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public final class ConsumerConfig extends AbstractRabbitMQConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerConfig.class); // Fixed the class reference

    public ConsumerConfig(Map<String, String> options) {
        super(options);
        LOGGER.debug("ConsumerConfig initialized with options: {}", options);
    }

    @Override
    public RabbitMQConfig withOption(String key, String value) {
        LOGGER.info("Updating single option: Key = {}, Value = {}", key, value);

        // Create a mutable copy of the existing options
        Map<String, String> extendedOptions = new HashMap<>(this.getOriginals());

        // Update the single key-value pair
        extendedOptions.put(key, value);

        // Create a new ConsumerConfig with the updated options
        return new ConsumerConfig(extendedOptions);
    }

    @Override
    public ConsumerConfig withOptions(Map<String, String> newOptions) {
        LOGGER.info("Updating multiple options: {}", newOptions);

        // Create a mutable copy of the existing options
        Map<String, String> extendedOptions = new HashMap<>(this.getOriginals());

        // Merge the new options
        extendedOptions.putAll(newOptions);

        // Create a new ConsumerConfig with the merged options
        return new ConsumerConfig(extendedOptions);
    }

    private Channel setupChannel(Connection connection) throws IOException, TimeoutException {
        LOGGER.info("Setting up Channel for connection: {}", connection);

        Channel chan = connection.createChannel();
        if (isPrefetchPresent()) {
            LOGGER.info("Prefetch present, setting prefetch count: {}", getPrefetch());
            chan.basicQos(getPrefetch(), true);
        } else {
            LOGGER.warn("Prefetch count not configured; proceeding without it.");
        }
        return chan;
    }

    public RabbitMQConsumer createConsumer(Connection connection) throws Exception {
        LOGGER.info("Creating consumer with connection: {}", connection);

        try {
            // Create a new channel from the connection
            Channel channel = setupChannel(connection);

            RabbitMQConsumer consumer = new RabbitMQConsumer(channel);

            LOGGER.info("Consumer created, subscribing to queue: {}", getQueueName());
            channel.basicConsume(getQueueName(), false, consumer);

            // Return a new RabbitMQConsumer with the created channel
            return consumer;
        } catch (Exception e) {
            LOGGER.error("Error while creating consumer: ", e);
            throw e;
        }
    }
}
