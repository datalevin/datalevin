package datalevin;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Read-only view over a raw Datalevin buffer supplied to a raw KV callback.
 *
 * <p>The underlying buffer is cursor-owned. Decode or copy data during the
 * callback; use {@link #bytes()} when data must outlive the callback.
 */
public final class RawBuffer {
    private final ByteBuffer buffer;

    RawBuffer(ByteBuffer buffer) {
        this.buffer = Objects.requireNonNull(buffer, "buffer").asReadOnlyBuffer();
    }

    /**
     * Returns a read-only duplicate positioned at the start of the buffer.
     */
    public ByteBuffer byteBuffer() {
        ByteBuffer duplicate = buffer.asReadOnlyBuffer();
        duplicate.rewind();
        return duplicate;
    }

    /**
     * Returns a copy of this buffer's remaining bytes from the start position.
     */
    public byte[] bytes() {
        ByteBuffer duplicate = byteBuffer();
        byte[] result = new byte[duplicate.remaining()];
        duplicate.get(result);
        return result;
    }

    /**
     * Decodes this buffer using Datalevin's default {@code :data} type.
     */
    public Object read() {
        return read(null);
    }

    /**
     * Decodes this buffer using a Datalevin KV type such as {@code ":long"}.
     */
    public Object read(Object type) {
        Object decoded = type == null
                ? ClojureRuntime.core("read-buffer", byteBuffer())
                : ClojureRuntime.core("read-buffer", byteBuffer(), DatalevinForms.typeInput(type));
        return ClojureCodec.bridgeOutput(decoded);
    }
}
