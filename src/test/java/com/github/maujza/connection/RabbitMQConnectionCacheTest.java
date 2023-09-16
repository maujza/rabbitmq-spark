package com.github.maujza.connection;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.*;

import com.rabbitmq.client.Connection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

@ExtendWith(MockitoExtension.class)
public class RabbitMQConnectionCacheTest {

    @Mock
    private RabbitMQConnectionFactory rabbitMQConnectionFactory;

    @Mock
    private Connection connection;

    @Test
    void testNormalUsecase() throws IOException, URISyntaxException, NoSuchAlgorithmException, KeyManagementException, TimeoutException {
        RabbitMQConnectionCache cache = new RabbitMQConnectionCache(0, 0, 100);
        when(rabbitMQConnectionFactory.create()).thenReturn(connection);

        // Acquire connections
        Connection conn1 = cache.acquire(rabbitMQConnectionFactory);
        Connection conn2 = cache.acquire(rabbitMQConnectionFactory);

        // Ensure only a single connection was created
        verify(rabbitMQConnectionFactory, times(1)).create();

        // Close connections and check if underlying connection is closed
        conn1.close();
        conn2.close();
        sleep(200);
        verify(connection, times(1)).close();
    }

    @Test
    void testKeepAliveReuseOfConnection() throws IOException, URISyntaxException, NoSuchAlgorithmException, KeyManagementException, TimeoutException {
        RabbitMQConnectionCache cache = new RabbitMQConnectionCache(500, 0, 200);
        when(rabbitMQConnectionFactory.create()).thenReturn(connection);

        // Acquire connections
        Connection conn1 = cache.acquire(rabbitMQConnectionFactory);
        Connection conn2 = cache.acquire(rabbitMQConnectionFactory);

        // Ensure only a single connection was created
        verify(rabbitMQConnectionFactory, times(1)).create();

        conn1.close();
        conn2.close();
        sleep(250);
        verify(connection, times(0)).close();

        // Acquire a new Connection within the keep-alive time
        Connection conn3 = cache.acquire(rabbitMQConnectionFactory);
        verify(rabbitMQConnectionFactory, times(1)).create();
        verify(connection, times(0)).close();

        conn3.close();
        sleep(1000);
        verify(connection, times(1)).close();
    }

    @Test
    void testShutdown() throws IOException, URISyntaxException, NoSuchAlgorithmException, KeyManagementException, TimeoutException {
        RabbitMQConnectionCache cache = new RabbitMQConnectionCache(500, 0, 200);
        when(rabbitMQConnectionFactory.create()).thenReturn(connection);

        // Acquire connections
        Connection conn1 = cache.acquire(rabbitMQConnectionFactory);
        Connection conn2 = cache.acquire(rabbitMQConnectionFactory);

        // Shutdown
        cache.shutdown();
        verify(connection, times(1)).close();

        // Verify behavior after shutdown
        assertDoesNotThrow(() -> cache.acquire(rabbitMQConnectionFactory));
        verify(rabbitMQConnectionFactory, times(2)).create();
        assertDoesNotThrow(cache::shutdown);
        verify(connection, times(2)).close();
    }

    private void sleep(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
