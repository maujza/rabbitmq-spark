package com.github.maujza.schema;

public interface Deserializer {
    String deserialize(byte[] message);
}
