package com.github.maujza.connection;

import com.rabbitmq.client.*;
import com.rabbitmq.utility.Utility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class RabbitMQConsumer extends DefaultConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQConsumer.class);

    private final BlockingQueue<Delivery> queue;
    private volatile ShutdownSignalException shutdown;
    private final Channel channel;
    private volatile ConsumerCancelledException cancelled;
    private static final Delivery POISON = new Delivery(null, null, null);

    public RabbitMQConsumer(Channel channel) {
        this(channel, Integer.MAX_VALUE);
    }

    public RabbitMQConsumer(Channel channel, int capacity) {
        super(channel);
        this.channel = channel;
        this.queue = new LinkedBlockingQueue<>(capacity);
        LOGGER.info("RabbitMQConsumer initialized with a channel and capacity: {}", capacity);
    }

    private void checkShutdown() {
        if (shutdown != null) {
            throw Utility.fixStackTrace(shutdown);
        }
    }

    private Delivery handle(Delivery delivery) {
        if (isPoisonOrInvalidDelivery(delivery)) {
            handlePoisonOrInvalid(delivery);
        }
        return delivery;
    }

    private boolean isPoisonOrInvalidDelivery(Delivery delivery) {
        return delivery == POISON || (delivery == null && (shutdown != null || cancelled != null));
    }

    private void handlePoisonOrInvalid(Delivery delivery) {
        if (delivery == POISON) {
            queue.add(POISON);
            checkForBug();
        }
        if (shutdown != null) {
            throw Utility.fixStackTrace(shutdown);
        }
        if (cancelled != null) {
            throw Utility.fixStackTrace(cancelled);
        }
    }

    private void checkForBug() {
        if (shutdown == null && cancelled == null) {
            throw new IllegalStateException("POISON in queue, but null shutdown and null cancelled. This should never happen, please report as a BUG");
        }
    }

    public Delivery nextDelivery()
            throws InterruptedException, UnsupportedEncodingException {
        Delivery delivery = queue.take();
        return handle(delivery);
    }

    public Delivery nextDelivery(long timeout)
            throws InterruptedException {
        return nextDelivery(timeout, TimeUnit.MILLISECONDS);
    }

    public Delivery nextDelivery(long timeout, TimeUnit unit)
            throws InterruptedException {
        return handle(queue.poll(timeout, unit));
    }

    @Override
    public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
        shutdown = sig;
        queue.add(POISON);
    }

    @Override
    public void handleCancel(String consumerTag) throws IOException {
        cancelled = new ConsumerCancelledException();
        queue.add(POISON);
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
            throws IOException {
        checkShutdown();
        this.queue.add(new Delivery(envelope, properties, body));
    }

    public void ack(long deliveryTag) throws IOException {
        this.channel.basicAck(deliveryTag, false);
    }

    public void closeAll() throws IOException {
        LOGGER.info("Closing channel and returning connection to pool");
        try {
            if (this.channel != null && this.channel.isOpen()) {
                LOGGER.debug("Closing channel");
                this.channel.close();
            }
        } catch (Exception e) {
            LOGGER.error("Error while closing channel: ", e);
        }
    }
}
