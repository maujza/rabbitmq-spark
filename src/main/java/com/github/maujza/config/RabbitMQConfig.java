package com.github.maujza.config;

import java.io.Serializable;
import java.util.Map;
import java.util.Locale;

/**
 * The RabbitMQConfig interface.
 *
 * <p>Provides RabbitMQ specific configuration.
 */
public interface RabbitMQConfig extends Serializable {

    static RabbitMQConfig createConfig(final Map<String, String> options) {
        return new SimpleRabbitMQConfig(options);
    }

    static ConsumerConfig consumerConfig(final Map<String, String> options) {
        return new ConsumerConfig(options);
    }

    static ProducerConfig producerConfig(final Map<String, String> options) {
        return new ProducerConfig(options);
    }

    String HOSTNAME_CONFIG = "hostname";

    String PORT_CONFIG = "port";

    String VIRTUAL_HOST_CONFIG = "virtualHost";

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
        return getOrDefault(QUEUE_NAME_CONFIG, "");
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
            return defaultValue;
        } else {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(key + " did not contain a valid int, got: " + value);
            }
        }
    }


}
