/* Copyright (c) Huahai Yang. All rights reserved.
 * Distributed under the Eclipse Public License 2.0. */
package datalevin.io;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import java.io.Closeable;
import java.nio.ByteBuffer;

/** Scratch buffers and native contexts leased exclusively by one wire codec.
 * Returned buffers remain valid until the lease is released. */
public final class WireCompression implements Closeable {
    private ZstdCompressCtx compressor;
    private ZstdDecompressCtx decompressor;
    private ByteBuffer raw, packed, input;
    private int level = Integer.MIN_VALUE;
    private boolean closed;

    private static ByteBuffer ensure(ByteBuffer buffer, int size) {
        if (size < 0) throw new IllegalArgumentException("Wire payload too large");
        if (buffer == null || buffer.capacity() < size) {
            long capacity = Math.max(8192L, size);
            if (buffer != null) capacity = Math.max(capacity, 2L * buffer.capacity());
            buffer = ByteBuffer.allocateDirect((int) Math.min(Integer.MAX_VALUE, capacity));
        }
        buffer.clear();
        return buffer;
    }

    private void checkOpen() {
        if (closed) throw new IllegalStateException("Wire compression context is closed");
    }

    /** Reusable space for an encoding that overflowed the socket buffer. */
    public ByteBuffer encodingBuffer(int minimum) {
        checkOpen();
        raw = ensure(raw, minimum);
        return raw;
    }

    private ByteBuffer directInput(ByteBuffer source, int start, int length) {
        input = ensure(input, length);
        input.put(source.duplicate().position(start).limit(start + length)).flip();
        return input;
    }

    /** Compress without changing source positions. Null means no byte saving.
     * The four-byte uncompressed length is part of the existing wire format. */
    public ByteBuffer compress(ByteBuffer source, int start, int length, int requestedLevel) {
        checkOpen();
        long bound = Zstd.compressBound(length) + 4;
        if (bound > Integer.MAX_VALUE) throw new IllegalArgumentException("Wire payload too large");
        packed = ensure(packed, (int) bound);
        if (compressor == null) compressor = new ZstdCompressCtx();
        if (level != requestedLevel) {
            compressor.setLevel(requestedLevel);
            level = requestedLevel;
        }
        if (!source.isDirect()) {
            source = directInput(source, start, length);
            start = 0;
        }
        int size = compressor.compressDirectByteBuffer(
            packed, 4, packed.capacity() - 4, source, start, length);
        if (size + 4 >= length) return null;
        packed.putInt(0, length).limit(size + 4);
        return packed;
    }

    /** Decode directly from the socket buffer into reusable direct storage. */
    public ByteBuffer decompress(ByteBuffer source) {
        checkOpen();
        if (source.remaining() < 4)
            throw new IllegalArgumentException("Wire message missing uncompressed length");
        int expected = source.getInt();
        if (expected < 0)
            throw new IllegalArgumentException("Negative wire uncompressed length");
        int start = source.position(), length = source.remaining();
        ByteBuffer compressed = source;
        if (!source.isDirect()) {
            compressed = directInput(source, start, length);
            start = 0;
        }
        // Reject an inconsistent size prefix before allocating from that prefix.
        long declared = Zstd.getDirectByteBufferFrameContentSize(compressed, start, length);
        if (declared >= 0 && declared != expected)
            throw new IllegalArgumentException("Wire message decompression length mismatch");
        raw = ensure(raw, expected);
        if (decompressor == null) decompressor = new ZstdDecompressCtx();
        int actual = decompressor.decompressDirectByteBuffer(
            raw, 0, expected, compressed, start, length);
        if (actual != expected)
            throw new IllegalArgumentException("Wire message decompression length mismatch");
        source.position(source.limit());
        raw.limit(actual);
        return raw;
    }

    @Override public void close() {
        closed = true;
        try {
            if (compressor != null) compressor.close();
        } finally {
            if (decompressor != null) decompressor.close();
            compressor = null;
            decompressor = null;
            raw = packed = input = null;
        }
    }
}
