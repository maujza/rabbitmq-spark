package com.github.maujza.config;

import com.rabbitmq.client.ConnectionFactory;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

public class RabbitMQConnectionConfig {

    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQConnectionConfig.class);
    private static final long DEFAULT_DELIVERY_TIMEOUT = 30000L;

    private final String host, virtualHost, username, password, uri;
    private final Integer port, networkRecoveryInterval, connectionTimeout, requestedChannelMax, requestedFrameMax, requestedHeartbeat, prefetchCount;
    private final Boolean automaticRecovery, topologyRecovery;

    private Long deliveryTimeout;

    public RabbitMQConnectionConfig(CaseInsensitiveStringMap options) {
        host = options.get("host");
        port = parseInteger(options, "port");
        virtualHost = options.get("virtual_host");
        username = options.get("username");
        password = options.get("password");
        uri = options.get("uri");

        networkRecoveryInterval = parseInteger(options, "networkRecoveryInterval");
        automaticRecovery = parseBoolean(options, "automaticRecovery");
        topologyRecovery = parseBoolean(options, "topologyRecovery");

        connectionTimeout = parseInteger(options, "connectionTimeout");
        requestedChannelMax = parseInteger(options, "requestedChannelMax");
        requestedFrameMax = parseInteger(options, "requestedFrameMax");
        requestedHeartbeat = parseInteger(options, "requestedHeartbeat");

        prefetchCount = parseInteger(options, "prefetchCount");
        deliveryTimeout = parseLong(options, "deliveryTimeout", DEFAULT_DELIVERY_TIMEOUT);
    }

    public String getHost() { return host; }
    public Integer getPort() { return port; }
    public String getVirtualHost() { return virtualHost; }
    public String getUsername() { return username; }
    public String getPassword() { return password; }
    public String getUri() { return uri; }
    public Integer getNetworkRecoveryInterval() { return networkRecoveryInterval; }
    public Boolean isAutomaticRecovery() { return automaticRecovery; }
    public Boolean isTopologyRecovery() { return topologyRecovery; }
    public Integer getConnectionTimeout() { return connectionTimeout; }
    public Integer getRequestedChannelMax() { return requestedChannelMax; }
    public Integer getRequestedFrameMax() { return requestedFrameMax; }
    public Integer getRequestedHeartbeat() { return requestedHeartbeat; }
    public Optional<Integer> getPrefetchCount() { return Optional.ofNullable(prefetchCount); }
    public Long getDeliveryTimeout() { return deliveryTimeout; }

    public ConnectionFactory getConnectionFactory()
            throws URISyntaxException, NoSuchAlgorithmException, KeyManagementException {
        ConnectionFactory factory = new ConnectionFactory();
        if (uri != null && !uri.isEmpty()) {
            try {
                factory.setUri(uri);
            } catch (Exception e) {
                LOG.error("Failed to initialize connection.", e);
                throw e;
            }
        } else {
            factory.setHost(host);
            factory.setPort(port);
            factory.setVirtualHost(virtualHost);
            factory.setUsername(username);
            factory.setPassword(password);
        }

        if (automaticRecovery != null) factory.setAutomaticRecoveryEnabled(automaticRecovery);
        if (connectionTimeout != null) factory.setConnectionTimeout(connectionTimeout);
        if (networkRecoveryInterval != null) factory.setNetworkRecoveryInterval(networkRecoveryInterval);
        if (requestedHeartbeat != null) factory.setRequestedHeartbeat(requestedHeartbeat);
        if (topologyRecovery != null) factory.setTopologyRecoveryEnabled(topologyRecovery);
        if (requestedChannelMax != null) factory.setRequestedChannelMax(requestedChannelMax);
        if (requestedFrameMax != null) factory.setRequestedFrameMax(requestedFrameMax);

        return factory;
    }

    private Integer parseInteger(CaseInsensitiveStringMap options, String key) {
        return options.containsKey(key) ? Integer.parseInt(options.get(key)) : null;
    }

    private Boolean parseBoolean(CaseInsensitiveStringMap options, String key) {
        return options.containsKey(key) ? Boolean.parseBoolean(options.get(key)) : null;
    }

    private Long parseLong(CaseInsensitiveStringMap options, String key, long defaultValue) {
        return options.containsKey(key) ? Long.parseLong(options.get(key)) : defaultValue;
    }
}
