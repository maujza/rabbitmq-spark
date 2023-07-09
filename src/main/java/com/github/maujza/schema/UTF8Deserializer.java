package com.github.maujza.schema;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class UTF8Deserializer {

    private static final long serialVersionUID = 1L;

    private transient Charset charset;

    public UTF8Deserializer() {
        this(StandardCharsets.UTF_8);
    }

    public UTF8Deserializer(Charset charset) {
        this.charset = charset;
    }

    public Charset getCharset() {
        return charset;
    }

    public String deserialize(byte[] message) {
        return new String(message, charset);
    }

    public byte[] serialize(String element) {
        return element.getBytes(charset);
    }

}