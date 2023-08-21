package com.github.maujza.connector;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.utility.Utility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class RabbitMQConsumer extends DefaultConsumer {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMQConsumer.class);

    private final BlockingQueue<Delivery> queue;
    private volatile ShutdownSignalException shutdown;
    private final Channel channel;
    private volatile ConsumerCancelledException cancelled;
    private static final Delivery POISON = new Delivery(null, null, null);
    private String taken;
    private Delivery delivery_taken;

    public RabbitMQConsumer(Channel channel) {
        this(channel, Integer.MAX_VALUE);
    }

    public RabbitMQConsumer(Channel channel, int capacity) {
        super(channel);
        this.channel = channel;
        this.queue = new LinkedBlockingQueue<>(capacity);
        logger.info("RabbitMQConsumer initialized with a channel and capacity: {}", capacity);
    }
    private void checkShutdown() {
        if (shutdown != null) {
            throw Utility.fixStackTrace(shutdown);
        }
    }
    private Delivery handle(Delivery delivery) {
        if (delivery == POISON || delivery == null && (shutdown != null || cancelled != null)) {
            if (delivery == POISON) {
                queue.add(POISON);
                if (shutdown == null && cancelled == null) {
                    throw new IllegalStateException(
                            "POISON in queue, but null shutdown and null cancelled. "
                                    + "This should never happen, please report as a BUG");
                }
            }
            if (null != shutdown) {
                throw Utility.fixStackTrace(shutdown);
            }
            if (null != cancelled) {
                throw Utility.fixStackTrace(cancelled);
            }
        }

        return delivery;
    }

    public Delivery nextDelivery()
            throws InterruptedException, ShutdownSignalException, ConsumerCancelledException, UnsupportedEncodingException {
        delivery_taken = queue.take();
        taken = new String(delivery_taken.getBody(), "UTF-8");
        logger.info("Taken message from the queue: ", taken);
        return handle(delivery_taken);
    }

    public Delivery nextDelivery(long timeout)
            throws InterruptedException, ShutdownSignalException, ConsumerCancelledException {
        return nextDelivery(timeout, TimeUnit.MILLISECONDS);
    }

    public Delivery nextDelivery(long timeout, TimeUnit unit)
            throws InterruptedException, ShutdownSignalException, ConsumerCancelledException {
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
    public void handleDelivery(
            String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
            throws IOException {
        checkShutdown();
        this.queue.add(new Delivery(envelope, properties, body));

    }

    public void ack(long deliveryTag) throws IOException {
        this.channel.basicAck(deliveryTag, false);
    }
}
