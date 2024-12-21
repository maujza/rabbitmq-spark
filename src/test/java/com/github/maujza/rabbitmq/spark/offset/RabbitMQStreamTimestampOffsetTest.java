package com.github.maujza.rabbitmq.spark.offset;

import org.apache.spark.sql.connector.read.streaming.Offset;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class RabbitMQStreamTimestampOffsetTest {

    private static final String VALID_OFFSET_JSON = "{\"timestamp\": 1234567890}";
    private static final long VALID_TIMESTAMP = 1234567890L;

    @Test
    public void testCreateOffset() {
        RabbitMQStreamTimestampOffset offset = new RabbitMQStreamTimestampOffset(VALID_TIMESTAMP);
        assertEquals(VALID_TIMESTAMP, offset.getTimestamp());
    }

    @Test
    public void testFromValidJson() {
        RabbitMQStreamTimestampOffset offset = RabbitMQStreamTimestampOffset.fromJson(VALID_OFFSET_JSON);
        assertEquals(VALID_TIMESTAMP, offset.getTimestamp());
    }

    @Test
    public void testToJson() {
        RabbitMQStreamTimestampOffset offset = new RabbitMQStreamTimestampOffset(VALID_TIMESTAMP);
        assertEquals(VALID_OFFSET_JSON, offset.json());
    }

    @Test
    public void testFromInvalidJson() {
        String invalidJson = "{\"invalidKey\": 1234567890}";
        assertThrows(IllegalArgumentException.class, () -> RabbitMQStreamTimestampOffset.fromJson(invalidJson));
    }

    @Test
    public void testFromMalformedJson() {
        String malformedJson = "This is not a JSON string";
        assertThrows(IllegalArgumentException.class, () -> RabbitMQStreamTimestampOffset.fromJson(malformedJson));
    }

    @Test
    public void testFromJsonWithInvalidTimestampType() {
        String invalidTypeJson = "{\"timestamp\": \"not a number\"}";
        assertThrows(IllegalArgumentException.class, () -> RabbitMQStreamTimestampOffset.fromJson(invalidTypeJson));
    }

    @Test
    public void testOffsetInheritance() {
        RabbitMQStreamTimestampOffset offset = new RabbitMQStreamTimestampOffset(VALID_TIMESTAMP);
        assertTrue(offset instanceof Offset);
    }
}