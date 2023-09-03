package com.github.maujza.config;

import java.util.Map;

public final class ProducerConfig extends AbstractRabbitMQConfig {
    public ProducerConfig(Map<String, String> options) {
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
}
