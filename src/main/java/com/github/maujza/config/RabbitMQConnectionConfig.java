package com.github.maujza.config;

import com.rabbitmq.client.ConnectionFactory;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class RabbitMQConnectionConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQConnectionConfig.class);

    private static final long DEFAULT_DELIVERY_TIMEOUT = 30000;

    private String host;
    private Integer port;
    private String virtualHost;
    private String username;
    private String password;
    private String uri;

    private Integer networkRecoveryInterval;
    private Boolean automaticRecovery;
    private Boolean topologyRecovery;

    private Integer connectionTimeout;
    private Integer requestedChannelMax;
    private Integer requestedFrameMax;
    private Integer requestedHeartbeat;

    private Integer prefetchCount;
    private final long deliveryTimeout;

    public RabbitMQConnectionConfig(CaseInsensitiveStringMap options) {
        this.host = options.get("host");
        this.port = options.containsKey("port") ? Integer.parseInt(options.get("port")) : null;
        this.virtualHost = options.get("virtualHost");
        this.username = options.get("username");
        this.password = options.get("password");
        this.uri = options.get("uri");

        this.networkRecoveryInterval = options.containsKey("networkRecoveryInterval") ? Integer.parseInt(options.get("networkRecoveryInterval")) : null;
        this.automaticRecovery = options.containsKey("automaticRecovery") ? Boolean.parseBoolean(options.get("automaticRecovery")) : null;
        this.topologyRecovery = options.containsKey("topologyRecovery") ? Boolean.parseBoolean(options.get("topologyRecovery")) : null;

        this.connectionTimeout = options.containsKey("connectionTimeout") ? Integer.parseInt(options.get("connectionTimeout")) : null;
        this.requestedChannelMax = options.containsKey("requestedChannelMax") ? Integer.parseInt(options.get("requestedChannelMax")) : null;
        this.requestedFrameMax = options.containsKey("requestedFrameMax") ? Integer.parseInt(options.get("requestedFrameMax")) : null;
        this.requestedHeartbeat = options.containsKey("requestedHeartbeat") ? Integer.parseInt(options.get("requestedHeartbeat")) : null;

        this.prefetchCount = options.containsKey("prefetchCount") ? Integer.parseInt(options.get("prefetchCount")) : null;
        this.deliveryTimeout = options.containsKey("deliveryTimeout") ? Long.parseLong(options.get("deliveryTimeout")) : DEFAULT_DELIVERY_TIMEOUT;
    }

    private RabbitMQConnectionConfig(
            String host,
            Integer port,
            String virtualHost,
            String username,
            String password,
            Integer networkRecoveryInterval,
            Boolean automaticRecovery,
            Boolean topologyRecovery,
            Integer connectionTimeout,
            Integer requestedChannelMax,
            Integer requestedFrameMax,
            Integer requestedHeartbeat,
            Integer prefetchCount,
            Long deliveryTimeout) {
        this.host = host;
        this.port = port;
        this.virtualHost = virtualHost;
        this.username = username;
        this.password = password;

        this.networkRecoveryInterval = networkRecoveryInterval;
        this.automaticRecovery = automaticRecovery;
        this.topologyRecovery = topologyRecovery;
        this.connectionTimeout = connectionTimeout;
        this.requestedChannelMax = requestedChannelMax;
        this.requestedFrameMax = requestedFrameMax;
        this.requestedHeartbeat = requestedHeartbeat;
        this.prefetchCount = prefetchCount;
        this.deliveryTimeout =
                Optional.ofNullable(deliveryTimeout).orElse(DEFAULT_DELIVERY_TIMEOUT);
    }

    private RabbitMQConnectionConfig(
            String uri,
            Integer networkRecoveryInterval,
            Boolean automaticRecovery,
            Boolean topologyRecovery,
            Integer connectionTimeout,
            Integer requestedChannelMax,
            Integer requestedFrameMax,
            Integer requestedHeartbeat,
            Integer prefetchCount,
            Long deliveryTimeout) {
        this.uri = uri;

        this.networkRecoveryInterval = networkRecoveryInterval;
        this.automaticRecovery = automaticRecovery;
        this.topologyRecovery = topologyRecovery;
        this.connectionTimeout = connectionTimeout;
        this.requestedChannelMax = requestedChannelMax;
        this.requestedFrameMax = requestedFrameMax;
        this.requestedHeartbeat = requestedHeartbeat;
        this.prefetchCount = prefetchCount;
        this.deliveryTimeout =
                Optional.ofNullable(deliveryTimeout).orElse(DEFAULT_DELIVERY_TIMEOUT);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getVirtualHost() {
        return virtualHost;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getUri() {
        return uri;
    }

    public Integer getNetworkRecoveryInterval() {
        return networkRecoveryInterval;
    }

    public Boolean isAutomaticRecovery() {
        return automaticRecovery;
    }

    public Boolean isTopologyRecovery() {
        return topologyRecovery;
    }

    public Integer getConnectionTimeout() {
        return connectionTimeout;
    }

    public Integer getRequestedChannelMax() {
        return requestedChannelMax;
    }


    public Integer getRequestedFrameMax() {
        return requestedFrameMax;
    }

    public Integer getRequestedHeartbeat() {
        return requestedHeartbeat;
    }

    public Optional<Integer> getPrefetchCount() {
        return Optional.ofNullable(prefetchCount);
    }

    public long getDeliveryTimeout() {
        return deliveryTimeout;
    }

    public ConnectionFactory getConnectionFactory()
            throws URISyntaxException, NoSuchAlgorithmException, KeyManagementException {
        ConnectionFactory factory = new ConnectionFactory();
        if (this.uri != null && !this.uri.isEmpty()) {
            try {
                factory.setUri(this.uri);
            } catch (URISyntaxException e) {
                LOG.error("Failed to parse uri", e);
                throw e;
            } catch (KeyManagementException e) {
                // this should never happen
                LOG.error("Failed to initialize ssl context.", e);
                throw e;
            } catch (NoSuchAlgorithmException e) {
                // this should never happen
                LOG.error("Failed to setup ssl factory.", e);
                throw e;
            }
        } else {
            factory.setHost(this.host);
            factory.setPort(this.port);
            factory.setVirtualHost(this.virtualHost);
            factory.setUsername(this.username);
            factory.setPassword(this.password);
        }

        if (this.automaticRecovery != null) {
            factory.setAutomaticRecoveryEnabled(this.automaticRecovery);
        }
        if (this.connectionTimeout != null) {
            factory.setConnectionTimeout(this.connectionTimeout);
        }
        if (this.networkRecoveryInterval != null) {
            factory.setNetworkRecoveryInterval(this.networkRecoveryInterval);
        }
        if (this.requestedHeartbeat != null) {
            factory.setRequestedHeartbeat(this.requestedHeartbeat);
        }
        if (this.topologyRecovery != null) {
            factory.setTopologyRecoveryEnabled(this.topologyRecovery);
        }
        if (this.requestedChannelMax != null) {
            factory.setRequestedChannelMax(this.requestedChannelMax);
        }
        if (this.requestedFrameMax != null) {
            factory.setRequestedFrameMax(this.requestedFrameMax);
        }

        return factory;
    }

    public static class Builder {

        private String host;
        private Integer port;
        private String virtualHost;
        private String username;
        private String password;

        private Integer networkRecoveryInterval;
        private Boolean automaticRecovery;
        private Boolean topologyRecovery;

        private Integer connectionTimeout;
        private Integer requestedChannelMax;
        private Integer requestedFrameMax;
        private Integer requestedHeartbeat;

        // basicQos options for consumers
        private Integer prefetchCount;

        private Long deliveryTimeout;

        private String uri;

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        public Builder setVirtualHost(String virtualHost) {
            this.virtualHost = virtualHost;
            return this;
        }

        public Builder setUserName(String username) {
            this.username = username;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setUri(String uri) {
            this.uri = uri;
            return this;
        }

        public Builder setTopologyRecoveryEnabled(boolean topologyRecovery) {
            this.topologyRecovery = topologyRecovery;
            return this;
        }

        public Builder setRequestedHeartbeat(int requestedHeartbeat) {
            this.requestedHeartbeat = requestedHeartbeat;
            return this;
        }

        public Builder setRequestedFrameMax(int requestedFrameMax) {
            this.requestedFrameMax = requestedFrameMax;
            return this;
        }

        public Builder setRequestedChannelMax(int requestedChannelMax) {
            this.requestedChannelMax = requestedChannelMax;
            return this;
        }

        public Builder setNetworkRecoveryInterval(int networkRecoveryInterval) {
            this.networkRecoveryInterval = networkRecoveryInterval;
            return this;
        }

        public Builder setConnectionTimeout(int connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        public Builder setAutomaticRecovery(boolean automaticRecovery) {
            this.automaticRecovery = automaticRecovery;
            return this;
        }

        public Builder setPrefetchCount(int prefetchCount) {
            this.prefetchCount = prefetchCount;
            return this;
        }

        public Builder setDeliveryTimeout(long deliveryTimeout) {
            this.deliveryTimeout = deliveryTimeout;
            return this;
        }

        public Builder setDeliveryTimeout(long deliveryTimeout, TimeUnit unit) {
            return setDeliveryTimeout(unit.toMillis(deliveryTimeout));
        }

        public RabbitMQConnectionConfig build() {
            if (this.uri != null) {
                return new RabbitMQConnectionConfig(
                        this.uri,
                        this.networkRecoveryInterval,
                        this.automaticRecovery,
                        this.topologyRecovery,
                        this.connectionTimeout,
                        this.requestedChannelMax,
                        this.requestedFrameMax,
                        this.requestedHeartbeat,
                        this.prefetchCount,
                        this.deliveryTimeout);
            } else {
                return new RabbitMQConnectionConfig(
                        this.host,
                        this.port,
                        this.virtualHost,
                        this.username,
                        this.password,
                        this.networkRecoveryInterval,
                        this.automaticRecovery,
                        this.topologyRecovery,
                        this.connectionTimeout,
                        this.requestedChannelMax,
                        this.requestedFrameMax,
                        this.requestedHeartbeat,
                        this.prefetchCount,
                        this.deliveryTimeout);
            }
        }
    }
}
