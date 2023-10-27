package com.github.maujza.config;

import java.io.Serializable;
import java.util.Map;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The RabbitMQConfig interface.
 *
 * <p>Provides RabbitMQ specific configuration.
 */
public interface RabbitMQConfig extends Serializable {

    Logger LOGGER = LoggerFactory.getLogger(RabbitMQConfig.class);

    static RabbitMQConfig createConfig(final Map<String, String> options) {
        LOGGER.debug("Creating a new general RabbitMQConfig with options: {}", options);
        return new SimpleRabbitMQConfig(options);
    }

    static ConsumerConfig consumerConfig(final Map<String, String> options) {
        LOGGER.debug("Creating a new ConsumerConfig with options: {}", options);
        return new ConsumerConfig(options);
    }

    static ProducerConfig producerConfig(final Map<String, String> options) {
        LOGGER.debug("Creating a new ProducerConfig with options: {}", options);
        return new ProducerConfig(options);
    }

    String HOSTNAME_CONFIG = "hostname";

    String PORT_CONFIG = "port";

    String VIRTUAL_HOST_CONFIG = "virtual_host";

    String USERNAME_CONFIG = "username";

    String PASSWORD_CONFIG = "password";

    String QUEUE_NAME_CONFIG = "queue";

    String EXCHANGE_NAME_CONFIG = "exchange";

    String PREFETCH_CONFIG = "prefetch";

    Map<String, String> getOptions();

    RabbitMQConfig withOption(String key, String value);

    RabbitMQConfig withOptions(Map<String, String> options);

    Map<String, String> getOriginals();

    default String getHostname() {
        return getOrDefault(HOSTNAME_CONFIG, "localhost");
    }

    default int getPort() {
        return getInt(PORT_CONFIG, 5672);
    }

    default String getVirtualHost() {
        return getOrDefault(VIRTUAL_HOST_CONFIG, "/");
    }

    default String getUsername() {
        return getOrDefault(USERNAME_CONFIG, "guest");
    }

    default String getPassword() {
        return getOrDefault(PASSWORD_CONFIG, "guest");
    }

    default String getQueueName() {
        return getOrDefault(QUEUE_NAME_CONFIG, "queue_name");
    }

    default int getPrefetch() { // Add this method to get the prefetch value
        return getInt(PREFETCH_CONFIG, 1); // Default prefetch value is 1
    }

    default boolean isPrefetchPresent() { // New method to check if the prefetch key is present
        return containsKey(PREFETCH_CONFIG);
    }

    default String getExchangeName() {
        return getOrDefault(EXCHANGE_NAME_CONFIG, "");
    }

    default ConsumerConfig toConsumerConfig() {
        if (this instanceof ConsumerConfig) {
            return (ConsumerConfig) this;
        }
        return consumerConfig(getOriginals());
    }

    default ProducerConfig toProducerConfig() {
        if (this instanceof ProducerConfig) {
            return (ProducerConfig) this;
        }
        return producerConfig(getOriginals());
    };

    default boolean containsKey(final String key) {
        return getOptions().containsKey(key);
    }

    default String get(final String key) {
        return getOptions().get(key.toLowerCase(Locale.ROOT));
    }

    default String getOrDefault(final String key, final String defaultValue) {
        return getOptions().getOrDefault(key.toLowerCase(Locale.ROOT), defaultValue);
    }

    default int getInt(final String key, final int defaultValue) {
        String value = get(key);
        if (value == null) {
            LOGGER.warn("Configuration key {} is not set. Using the default value: {}", key, defaultValue);
            return defaultValue;
        } else {
            try {
                int intValue = Integer.parseInt(value);
                LOGGER.info("Parsed integer value for key {}: {}", key, intValue);
                return intValue;
            } catch (NumberFormatException e) {
                LOGGER.error("Configuration key {} did not contain a valid int, got: {}. Falling back to default value: {}", key, value, defaultValue);
                throw new IllegalArgumentException(key + " did not contain a valid int, got: " + value);
            }
        }
    }


}
