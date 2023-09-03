package com.github.maujza.connection;

import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

public interface RabbitMQConnectionFactory {
    Connection create() throws IOException, TimeoutException, URISyntaxException, NoSuchAlgorithmException, KeyManagementException;
}
