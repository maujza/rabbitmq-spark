package com.github.maujza.rabbitmq.spark.offset;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;

public final class RabbitMQStreamOffsetStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQStreamOffsetStore.class);
    private final Path checkpointLocation;
    private final FileSystem fs;
    private RabbitMQStreamTimestampOffset offset;


    public RabbitMQStreamOffsetStore(Configuration conf, String checkpointLocation, RabbitMQStreamTimestampOffset offset) {
        try {
            this.fs = FileSystem.get(URI.create(checkpointLocation), conf);
            this.checkpointLocation = new Path(URI.create(checkpointLocation));
            this.offset = offset;
        } catch (IOException e) {
            throw new IllegalStateException("Unable to initialize FileSystem", e);
        }
    }


    public RabbitMQStreamTimestampOffset initialOffset() {
        try {
            if (fs.exists(checkpointLocation)) {
                try (FSDataInputStream in = fs.open(checkpointLocation)) {
                    String json = IOUtils.toString(in, StandardCharsets.UTF_8);
                    offset = RabbitMQStreamTimestampOffset.fromJson(json);
                }
            } else {
                updateOffset(offset);  // Persist initial offset if no existing offset found
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to access or create the offset file", e);
        }
        LOGGER.info("Initial offset loaded: {}", offset.json());
        return offset;
    }


    public void updateOffset(RabbitMQStreamTimestampOffset offset) {
        this.offset = offset;  // Update in-memory offset before writing
        try (FSDataOutputStream out = fs.create(checkpointLocation, true)) {
            out.write(offset.json().getBytes(StandardCharsets.UTF_8));
            out.hflush();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to update offset file", e);
        }
    }
}
