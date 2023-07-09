package com.github.maujza.schema;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;


public class UTF8Deserializer implements Deserializer {
    private final Charset charset;

    public UTF8Deserializer() {
        this(StandardCharsets.UTF_8);
    }

    public UTF8Deserializer(Charset charset) {
        this.charset = charset;
    }

    @Override
    public String deserialize(byte[] message) {
        return new String(message, charset);
    }
}