package com.github.maujza.config;

import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class ConsumerConfigTest {

    private Map<String, String> testOptions;
    private ConsumerConfig consumerConfig;

    @BeforeEach
    public void setUp() {
        testOptions = new HashMap<>();
        testOptions.put("key1", "value1");
        testOptions.put("key2", "value2");
        consumerConfig = new ConsumerConfig(testOptions);
    }

    @Test
    public void testGetRabbitMQConnectionFactory() throws NoSuchMethodException, SecurityException {
        // This part uses reflection to access the private method getRabbitMQConnectionFactory for testing.
        Method method = ConsumerConfig.class.getSuperclass().getDeclaredMethod("getRabbitMQConnectionFactory");
        method.setAccessible(true);

        try {
            // First call to getRabbitMQConnectionFactory
            Object firstFactory = method.invoke(consumerConfig);

            // Second call to getRabbitMQConnectionFactory
            Object secondFactory = method.invoke(consumerConfig);

            // Verify that both calls return the same factory instance
            assertSame(firstFactory, secondFactory, "The returned factories should be the same instance.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


