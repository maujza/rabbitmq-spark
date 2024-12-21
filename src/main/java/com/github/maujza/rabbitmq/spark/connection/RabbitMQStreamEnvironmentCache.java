package com.github.maujza.rabbitmq.spark.connection;

import com.rabbitmq.stream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RabbitMQStreamEnvironmentCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQStreamEnvironmentCache.class);
    private final HashMap<String, CachedEnvironment> CACHE = new HashMap<>();
    private final long keepAliveNanos;
    private final long initialCleanUpDelayMS;
    private final long cleanUpDelayMS;
    private ScheduledExecutorService scheduler;

    static final int INITIAL_CLEANUP_DELAY_MS = 1000;
    static final int CLEANUP_DELAY_MS = 200;

    private String uri;

    public RabbitMQStreamEnvironmentCache(final long keepAliveMS) {
        this(keepAliveMS, INITIAL_CLEANUP_DELAY_MS, CLEANUP_DELAY_MS);
    }

    public RabbitMQStreamEnvironmentCache(
            final long keepAliveMS, final long initialCleanUpDelayMS, final long cleanUpDelayMS) {
        this.keepAliveNanos = TimeUnit.NANOSECONDS.convert(keepAliveMS, TimeUnit.MILLISECONDS);
        this.initialCleanUpDelayMS = initialCleanUpDelayMS;
        this.cleanUpDelayMS = cleanUpDelayMS;
        LOGGER.info("Cache initialized with keepAlive={}ms, initialCleanupDelay={}ms, cleanupDelay={}ms",
                keepAliveMS, initialCleanUpDelayMS, cleanUpDelayMS);
    }

    public static String buildRabbitMQStreamUri(Map<String, String> config) throws UnsupportedEncodingException {
        if (config.containsKey("uri")) {
            return config.get("uri");
        }
        String username = URLEncoder.encode(config.getOrDefault("username", "guest"), String.valueOf(StandardCharsets.UTF_8));
        String password = URLEncoder.encode(config.getOrDefault("password", "guest"), String.valueOf(StandardCharsets.UTF_8));
        String host = config.getOrDefault("host", "localhost");
        String port = config.getOrDefault("port", "5552");
        String encodedVhost = URLEncoder.encode(config.getOrDefault("vhost", "/"), String.valueOf(StandardCharsets.UTF_8));

        return String.format("rabbitmq-stream://%s:%s@%s:%s/%s", username, password, host, port, encodedVhost);
    }

    synchronized Environment acquire(final Map<String, String> config) throws UnsupportedEncodingException {
        String uri = buildRabbitMQStreamUri(config);
        LOGGER.debug("Acquiring environment for URI: {}", uri);
        ensureScheduler();
        Environment env = CACHE.computeIfAbsent(
                uri,
                u -> {
                    LOGGER.info("Creating new environment for URI: {}", uri);
                    try {
                        return new CachedEnvironment(this, createEnvironment(config), keepAliveNanos);
                    } catch (UnsupportedEncodingException e) {
                        throw new RuntimeException(e);
                    }
                }).acquire();
        return env;
    }

    private Environment createEnvironment(final Map<String, String> config) throws UnsupportedEncodingException {

        Address entryPoint = new Address(config.getOrDefault("host", "localhost"), Integer.parseInt(config.getOrDefault("port", "5552")));

        return Environment.builder()
                .host(entryPoint.host())
                .port(entryPoint.port())
                .username(URLEncoder.encode(config.getOrDefault("username", "guest"), String.valueOf(StandardCharsets.UTF_8)))
                .password(URLEncoder.encode(config.getOrDefault("password", "guest"), String.valueOf(StandardCharsets.UTF_8)))
                .virtualHost(URLEncoder.encode(config.getOrDefault("vhost", "/"), String.valueOf(StandardCharsets.UTF_8)))
                .addressResolver(address -> entryPoint)
                .build();
    }

    synchronized void shutdown() {
        LOGGER.info("Shutting down cache and all environments");
        if (scheduler != null) {
            scheduler.shutdownNow();
            CACHE.values().forEach(CachedEnvironment::shutdownClose);
            CACHE.clear();
            scheduler = null;
        }
    }

    private synchronized void checkClientCache() {
        LOGGER.debug("Checking and cleaning cache");
        long currentNanos = System.nanoTime();
        CACHE.entrySet().removeIf(e -> {
            boolean removed = e.getValue().shouldBeRemoved(currentNanos);
            if (removed) {
                LOGGER.info("Environment for URI: {} has been removed from cache", e.getKey());
            }
            return removed;
        });
        if (CACHE.isEmpty()) {
            LOGGER.info("Cache is empty, shutting down scheduler");
            shutdown();
        }
    }


    private synchronized void ensureScheduler() {
        if (scheduler == null) {
            LOGGER.debug("Starting scheduler for cache cleanup");
            scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleWithFixedDelay(
                    this::checkClientCache, initialCleanUpDelayMS, cleanUpDelayMS, TimeUnit.MILLISECONDS);
        }
    }

    private static final class CachedEnvironment implements Environment {

        private final RabbitMQStreamEnvironmentCache cache;
        private final Environment wrapped;
        private final long keepAliveNanos;
        private long releasedNanos;
        private int referenceCount;

        private CachedEnvironment(
                final RabbitMQStreamEnvironmentCache cache, final Environment wrapped, final long keepAliveNanos) {
            this.cache = cache;
            this.wrapped = wrapped;
            this.keepAliveNanos = keepAliveNanos;
            this.releasedNanos = System.nanoTime();
            this.referenceCount = 0;
        }

        private CachedEnvironment acquire() {
            referenceCount += 1;
            return this;
        }

        private void shutdownClose() {
            LOGGER.info("Closing environment, URI shutdown initiated");
            referenceCount = 0;
            wrapped.close();
        }

        @Override
        public void close() {
            synchronized (cache) {
                cache.ensureScheduler();
                releasedNanos = System.nanoTime();
                referenceCount -= 1;
            }
        }

        private boolean shouldBeRemoved(final long currentNanos) {
            if (referenceCount == 0 && currentNanos - releasedNanos > keepAliveNanos) {
                try {
                    wrapped.close();
                } catch (RuntimeException e) {
                    // ignore
                }
                return true;
            }
            return false;
        }

        @Override
        public StreamCreator streamCreator() {
            return wrapped.streamCreator();
        }

        @Override
        public void deleteStream(String stream) {
            wrapped.deleteStream(stream);
        }

        @Override
        public void deleteSuperStream(String superStream) {
            wrapped.deleteSuperStream(superStream);
        }

        @Override
        public StreamStats queryStreamStats(String stream) {
            return wrapped.queryStreamStats(stream);
        }

        @Override
        public boolean streamExists(String stream) {
            return wrapped.streamExists(stream);
        }

        @Override
        public ProducerBuilder producerBuilder() {
            return wrapped.producerBuilder();
        }

        @Override
        public ConsumerBuilder consumerBuilder() {
            return wrapped.consumerBuilder();
        }


    }

}



