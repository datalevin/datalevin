package datalevin;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Raw key/value pair supplied to raw KV visitor and filter callbacks.
 *
 * <p>The underlying buffers are cursor-owned. Decode or copy data during the
 * callback; use {@link #keyBytes()} and {@link #valueBytes()} when data must
 * outlive the callback.
 */
public final class RawKV {
    private final Object rawKv;

    RawKV(Object rawKv) {
        this.rawKv = Objects.requireNonNull(rawKv, "rawKv");
    }

    /**
     * Returns the raw key buffer.
     */
    public RawBuffer key() {
        return new RawBuffer((ByteBuffer) ClojureRuntime.invoke("datalevin.lmdb", "k", rawKv));
    }

    /**
     * Returns the raw value buffer.
     */
    public RawBuffer value() {
        return new RawBuffer((ByteBuffer) ClojureRuntime.invoke("datalevin.lmdb", "v", rawKv));
    }

    /**
     * Returns a copy of the raw key bytes.
     */
    public byte[] keyBytes() {
        return key().bytes();
    }

    /**
     * Returns a copy of the raw value bytes.
     */
    public byte[] valueBytes() {
        return value().bytes();
    }

    /**
     * Decodes the key using Datalevin's default {@code :data} type.
     */
    public Object readKey() {
        return key().read();
    }

    /**
     * Decodes the key using a Datalevin KV type such as {@code ":string"}.
     */
    public Object readKey(Object type) {
        return key().read(type);
    }

    /**
     * Decodes the value using Datalevin's default {@code :data} type.
     */
    public Object readValue() {
        return value().read();
    }

    /**
     * Decodes the value using a Datalevin KV type such as {@code ":long"}.
     */
    public Object readValue(Object type) {
        return value().read(type);
    }
}
