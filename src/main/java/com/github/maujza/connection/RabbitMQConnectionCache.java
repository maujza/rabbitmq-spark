package com.github.maujza.connection;

import com.github.maujza.checks.Checks;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


final class RabbitMQConnectionCache {
    private final HashMap<RabbitMQConnectionFactory, CachedRabbitMQConnection> cache = new HashMap<>();
    private final long keepAliveMS;
    private final long initialCleanUpDelayMS;
    private final long cleanUpDelayMS;
    private ScheduledExecutorService scheduler;

    static final int INITIAL_CLEANUP_DELAY_MS = 1000;
    static final int CLEANUP_DELAY_MS = 200;


    RabbitMQConnectionCache(final long keepAliveMS) {
        this(keepAliveMS, INITIAL_CLEANUP_DELAY_MS, CLEANUP_DELAY_MS);
    }

    RabbitMQConnectionCache(
            final long keepAliveMS, final long initialCleanUpDelayMS, final long cleanUpDelayMS) {
        this.keepAliveMS = keepAliveMS;
        this.initialCleanUpDelayMS = initialCleanUpDelayMS;
        this.cleanUpDelayMS = cleanUpDelayMS;
    }

    synchronized Connection acquire(final RabbitMQConnectionFactory rabbitMQConnectionFactory) {
        ensureScheduler();
        return cache
                .computeIfAbsent(
                        rabbitMQConnectionFactory,
                        (factory) -> {
                            try {
                                return new CachedRabbitMQConnection(this, factory.create(), keepAliveMS);
                            } catch (IOException | TimeoutException | URISyntaxException | NoSuchAlgorithmException |
                                     KeyManagementException e) {
                                throw new RuntimeException(e);
                            }
                        })
                .acquire();
    }

    synchronized void shutdown() throws RuntimeException {
        if (scheduler != null) {
            scheduler.shutdownNow();
            cache.values().forEach(connection -> {
                try {
                    connection.shutdownClose();
                } catch (IOException e) {
                    // Log and rethrow as RuntimeException
                    throw new RuntimeException("Failed to close connection", e);
                }
            });
            cache.clear();
            scheduler = null;
        }
    }

    private synchronized void checkClientCache() {
        long currentTimeMillis = System.currentTimeMillis();
        cache.entrySet().removeIf(e -> e.getValue().shouldBeRemoved(currentTimeMillis));
        if (cache.entrySet().isEmpty()) {
            shutdown();
        }
    }

    private synchronized void ensureScheduler() {
        if (scheduler == null) {
            scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleWithFixedDelay(
                    this::checkClientCache, initialCleanUpDelayMS, cleanUpDelayMS, TimeUnit.MILLISECONDS);
        }
    }

    private static final class CachedRabbitMQConnection implements Connection {
        private final RabbitMQConnectionCache cache;
        private final Connection wrapped;
        private final long keepAliveMS;
        private long releasedMillis;
        private int referenceCount;

        private CachedRabbitMQConnection(
                final RabbitMQConnectionCache cache, final Connection wrapped, final long keepAliveMS) {
            super();
            this.cache = cache;
            this.wrapped = wrapped;
            this.keepAliveMS = keepAliveMS;
            this.releasedMillis = System.currentTimeMillis();
            this.referenceCount = 0;
        }

        private CachedRabbitMQConnection acquire() {
            referenceCount += 1;
            return this;
        }

        private void shutdownClose() throws IOException {
            referenceCount = 0;
            wrapped.close();
        }

        private boolean shouldBeRemoved(final long currentMillis) {
            if (referenceCount == 0 && currentMillis - releasedMillis > keepAliveMS) {
                try {
                    wrapped.close((int) currentMillis);
                } catch (RuntimeException | IOException e) {
                    // ignore
                }
                return true;
            }
            return false;
        }

        @Override
        public void close() {
            synchronized (cache) {
                cache.ensureScheduler();
                Checks.ensureState(
                        () -> referenceCount > 0, () -> "Connection reference count cannot be below zero");
                releasedMillis = System.currentTimeMillis();
                referenceCount -= 1;
            }
        }

        @Override
        public InetAddress getAddress() {
            return wrapped.getAddress();
        }

        @Override
        public int getPort() {
            return wrapped.getPort();
        }

        @Override
        public int getChannelMax() {
            return wrapped.getChannelMax();
        }

        @Override
        public int getFrameMax() {
            return wrapped.getFrameMax();
        }

        @Override
        public int getHeartbeat() {
            return wrapped.getHeartbeat();
        }

        @Override
        public Map<String, Object> getClientProperties() {
            return wrapped.getClientProperties();
        }

        @Override
        public String getClientProvidedName() {
            return wrapped.getClientProvidedName();
        }

        @Override
        public Map<String, Object> getServerProperties() {
            return wrapped.getServerProperties();
        }

        @Override
        public Channel createChannel() throws IOException {
            return wrapped.createChannel();
        }

        @Override
        public Channel createChannel(int channelNumber) throws IOException {
            return wrapped.createChannel();
        }

        @Override
        public void close(int closeCode, String closeMessage) throws IOException {
            wrapped.close(closeCode, closeMessage);
        }

        @Override
        public void close(int timeout) throws IOException {
            wrapped.close(timeout);
        }

        @Override
        public void close(int closeCode, String closeMessage, int timeout) throws IOException {
            wrapped.close(closeCode, closeMessage, timeout);
        }

        @Override
        public void abort() {
            wrapped.abort();
        }

        @Override
        public void abort(int closeCode, String closeMessage) {
            wrapped.abort(closeCode, closeMessage);
        }

        @Override
        public void abort(int timeout) {
            wrapped.abort(timeout);
        }

        @Override
        public void abort(int closeCode, String closeMessage, int timeout) {
            wrapped.abort(closeCode, closeMessage, timeout);
        }

        @Override
        public void addBlockedListener(BlockedListener listener) {
            wrapped.addBlockedListener(listener);
        }

        @Override
        public BlockedListener addBlockedListener(BlockedCallback blockedCallback, UnblockedCallback unblockedCallback) {
            return wrapped.addBlockedListener(blockedCallback, unblockedCallback);
        }

        @Override
        public boolean removeBlockedListener(BlockedListener listener) {
            return wrapped.removeBlockedListener(listener);
        }

        @Override
        public void clearBlockedListeners() {
            wrapped.clearBlockedListeners();
        }

        @Override
        public ExceptionHandler getExceptionHandler() {
            return wrapped.getExceptionHandler();
        }

        @Override
        public String getId() {
            return wrapped.getId();
        }

        @Override
        public void setId(String id) {
            wrapped.setId(id);
        }

        @Override
        public void addShutdownListener(ShutdownListener listener) {
            wrapped.addShutdownListener(listener);
        }

        @Override
        public void removeShutdownListener(ShutdownListener listener) {
            wrapped.removeShutdownListener(listener);
        }

        @Override
        public ShutdownSignalException getCloseReason() {
            return wrapped.getCloseReason();
        }

        @Override
        public void notifyListeners() {
            wrapped.notifyListeners();
        }

        @Override
        public boolean isOpen() {
            return wrapped.isOpen();
        }
    }
}
