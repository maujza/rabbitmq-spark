package com.github.maujza.schema;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class UTF8Deserializer implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Charset charset;

    public UTF8Deserializer() {
        this(StandardCharsets.UTF_8);
    }

    public UTF8Deserializer(Charset charset) {
        this.charset = charset;
    }

    public String deserialize(byte[] message) {
        return new String(message, charset);
    }


}