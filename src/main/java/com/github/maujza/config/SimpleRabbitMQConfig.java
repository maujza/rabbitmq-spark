package com.github.maujza.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class SimpleRabbitMQConfig implements RabbitMQConfig {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleRabbitMQConfig.class);
    private final Map<String, String> options;

    SimpleRabbitMQConfig(final Map<String, String> options) {
        this.options = options;
        LOGGER.debug("SimpleRabbitMQConfig initialized with options: {}", options);
    }

    @Override
    public Map<String, String> getOptions() {
        LOGGER.debug("Retrieving options: {}", options);
        return options;
    }

    @Override
    public RabbitMQConfig withOption(final String key, final String value) {
        HashMap<String, String> mergedOptions = new HashMap<>(getOptions());
        mergedOptions.put(key, value);
        LOGGER.info("Adding new option: Key = {}, Value = {}", key, value);
        return new SimpleRabbitMQConfig(mergedOptions);
    }

    @Override
    public RabbitMQConfig withOptions(final Map<String, String> newOptions) {
        HashMap<String, String> mergedOptions = new HashMap<>(getOptions());
        mergedOptions.putAll(newOptions);
        LOGGER.info("Adding new options: {}", newOptions);
        return new SimpleRabbitMQConfig(mergedOptions);
    }

    @Override
    public Map<String, String> getOriginals() {
        LOGGER.debug("Retrieving original options: {}", options);
        return options;
    }
}
