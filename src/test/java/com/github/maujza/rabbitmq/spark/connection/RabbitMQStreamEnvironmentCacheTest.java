package com.github.maujza.rabbitmq.spark.connection;

import com.rabbitmq.stream.Environment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RabbitMQStreamEnvironmentCacheTest {

    @Mock
    private Environment mockEnvironment;

    private RabbitMQStreamEnvironmentCache cache;

    private static final Map<String, String> CONFIG_MAP = new HashMap<>();

    static {
        // Set up a standard configuration for testing
        CONFIG_MAP.put("host", "localhost");
        CONFIG_MAP.put("port", "5552");
        CONFIG_MAP.put("username", "guest");
        CONFIG_MAP.put("password", "guest");
        CONFIG_MAP.put("vhost", "/");
    }

    @BeforeEach
    void setUp() {
        // Initialize the cache with a 500ms keep-alive time, 0ms initial cleanup delay, and 200ms cleanup delay
        cache = spy(new RabbitMQStreamEnvironmentCache(500, 0, 200));
    }

    @Test
    void testNormalUseCase() throws UnsupportedEncodingException {
        // Mock the acquire method to always return the same mockEnvironment
        lenient().doReturn(mockEnvironment).when(cache).acquire(any());

        // Acquire two environments
        Environment env1 = cache.acquire(CONFIG_MAP);
        Environment env2 = cache.acquire(CONFIG_MAP);

        // Assert that both acquisitions return the same environment (caching works)
        assertSame(env1, env2);
        // Verify that acquire was called twice
        verify(cache, times(2)).acquire(CONFIG_MAP);
    }

    @Test
    void testKeepAliveReuseOfEnvironment() throws UnsupportedEncodingException {
        // Mock the acquire method to always return the same mockEnvironment
        lenient().doReturn(mockEnvironment).when(cache).acquire(any());

        // Acquire two environments
        Environment env1 = cache.acquire(CONFIG_MAP);
        Environment env2 = cache.acquire(CONFIG_MAP);

        // Assert that both acquisitions return the same environment
        assertSame(env1, env2);

        // Close both environments
        env1.close();
        env2.close();

        // Sleep for less than the keep-alive time (500ms)
        sleep(250);

        // Acquire a new environment, should be the same as before due to keep-alive
        Environment env3 = cache.acquire(CONFIG_MAP);
        assertSame(env2, env3);

        // Sleep for more than the keep-alive time
        sleep(1000);

        // Acquire a new environment, should still be the same due to our mocking
        Environment env4 = cache.acquire(CONFIG_MAP);
        assertSame(env3, env4);

        // Verify that acquire was called 4 times in total
        verify(cache, times(4)).acquire(CONFIG_MAP);
    }

    @Test
    void testShutdown() throws UnsupportedEncodingException {
        // Mock the acquire method to always return the same mockEnvironment
        lenient().doReturn(mockEnvironment).when(cache).acquire(any());

        // Acquire two environments
        Environment env1 = cache.acquire(CONFIG_MAP);
        Environment env2 = cache.acquire(CONFIG_MAP);

        // Assert that both acquisitions return the same environment
        assertSame(env1, env2);

        // Shutdown the cache
        cache.shutdown();

        // Verify that shutdown was called
        verify(cache).shutdown();

        // Acquire a new environment after shutdown, should still be the same due to our mocking
        Environment env3 = cache.acquire(CONFIG_MAP);
        assertSame(env2, env3);

        // Ensure multiple shutdowns don't cause issues
        assertDoesNotThrow(cache::shutdown);
    }

    @Test
    void testUriConstruction() throws UnsupportedEncodingException {
        // Expected URI based on the CONFIG_MAP
        String expectedUri = "rabbitmq-stream://guest:guest@localhost:5552/%2F";
        // Construct the actual URI using the static method
        String actualUri = RabbitMQStreamEnvironmentCache.buildRabbitMQStreamUri(CONFIG_MAP);
        // Assert that the constructed URI matches the expected URI
        assertEquals(expectedUri, actualUri);
    }

    // Helper method to simulate waiting
    private void sleep(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}