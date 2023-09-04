package com.github.maujza.config;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class SimpleRabbitMQConfig implements RabbitMQConfig {
    private static final long serialVersionUID = 1L;
    private final Map<String, String> options;

    SimpleRabbitMQConfig(final Map<String, String> options) {
        this.options = options;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public RabbitMQConfig withOption(final String key, final String value) {
        HashMap<String, String> mergedOptions = new HashMap<>(getOptions());
        mergedOptions.put(key, value);
        return new SimpleRabbitMQConfig(mergedOptions);
    }

    @Override
    public RabbitMQConfig withOptions(final Map<String, String> options) {
        HashMap<String, String> mergedOptions = new HashMap<>(getOptions());
        mergedOptions.putAll(options);
        return new SimpleRabbitMQConfig(mergedOptions);
    }

    @Override
    public Map<String, String> getOriginals() {
        return options;
    }

}
