package com.github.maujza.schema;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Very simple serialization schema for strings.
 *
 * <p>By default, the serializer uses "UTF-8" for string/byte conversion.
 */
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

    public boolean isEndOfStream(String nextElement) {
        return false;
    }

    public byte[] serialize(String element) {
        return element.getBytes(charset);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeUTF(charset.name());
    }

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        String charsetName = in.readUTF();
        this.charset = Charset.forName(charsetName);
    }
}