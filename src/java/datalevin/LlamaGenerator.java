package datalevin;

import datalevin.dtlvnative.DTLV;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Local llama.cpp-backed text generator backed by {@code dtlvnative}.
 *
 * <p>The constructor expects a GGUF generation model path. Integer tuning
 * arguments default to {@code 0}, which defers to the native implementation's
 * defaults.
 */
public final class LlamaGenerator implements AutoCloseable {

    private final DTLV.dtlv_llama_generator handle;
    private final String modelPath;
    private final int gpuLayers;
    private final int ctxSize;
    private final int threads;
    private final AtomicBoolean closed;

    /**
     * Creates a llama.cpp generator using native defaults for tuning options.
     */
    public LlamaGenerator(String modelPath) {
        this(modelPath, 0, 0, 0);
    }

    /**
     * Creates a llama.cpp generator.
     */
    public LlamaGenerator(String modelPath,
                          int gpuLayers,
                          int ctxSize,
                          int threads) {
        this.modelPath = requireNonBlank(modelPath, "modelPath");
        this.gpuLayers = gpuLayers;
        this.ctxSize = ctxSize;
        this.threads = threads;
        this.closed = new AtomicBoolean(false);
        this.handle = new DTLV.dtlv_llama_generator();

        int rc = DTLV.dtlv_llama_generator_create(handle,
                                                  this.modelPath,
                                                  gpuLayers,
                                                  ctxSize,
                                                  threads);
        checkRc(rc, "initialize llama generator");
    }

    private static String requireNonBlank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    private static void checkRc(int rc, String action) {
        if (rc == 0) {
            return;
        }
        throw new IllegalStateException("Failed to " + action + " (rc=" + rc + ")");
    }

    private static int nextBufferSize(int currentSize, int requiredSize) {
        if (requiredSize < 0) {
            throw new IllegalArgumentException("requiredSize must be non-negative");
        }
        if (currentSize >= Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        int doubled = currentSize > (Integer.MAX_VALUE / 2) ? Integer.MAX_VALUE : currentSize * 2;
        return Math.max(requiredSize, doubled);
    }

    private static int initialTextBufferSize(int maxTokens) {
        long estimatedSize = Math.max(256L, (long) Math.max(1, maxTokens) * 16L);
        return estimatedSize >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) estimatedSize;
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Llama generator is closed");
        }
    }

    private String runTextOperation(String action,
                                    String text,
                                    int maxTokens,
                                    boolean summarize) {
        ensureOpen();
        Objects.requireNonNull(text, "text");
        if (maxTokens < 0) {
            throw new IllegalArgumentException("maxTokens must be non-negative");
        }

        int bufferSize = initialTextBufferSize(maxTokens);
        for (int attempt = 0; attempt < 8; attempt++) {
            byte[] bytes = new byte[bufferSize];
            int actualSize = summarize
                ? DTLV.dtlv_llama_summarize(handle, text, maxTokens, bytes, bufferSize)
                : DTLV.dtlv_llama_generate(handle, text, maxTokens, bytes, bufferSize);
            if (actualSize >= 0 && actualSize <= bufferSize) {
                return new String(bytes, 0, actualSize, StandardCharsets.UTF_8);
            }

            int requiredSize = Math.abs(actualSize);
            if (requiredSize <= bufferSize) {
                throw new IllegalStateException("Failed to " + action + " text (rc=" + actualSize + ")");
            }
            bufferSize = nextBufferSize(bufferSize, requiredSize);
        }

        throw new IllegalStateException("Failed to " + action + " text after resizing output buffer");
    }

    /**
     * Returns the GGUF model path.
     */
    public String modelPath() {
        return modelPath;
    }

    /**
     * Returns the configured GPU layer count.
     */
    public int gpuLayers() {
        return gpuLayers;
    }

    /**
     * Returns the configured context size.
     */
    public int ctxSize() {
        return ctxSize;
    }

    /**
     * Returns the actual context window reported by llama.cpp.
     */
    public synchronized int contextSize() {
        ensureOpen();
        int nCtx = DTLV.dtlv_llama_generator_n_ctx(handle);
        if (nCtx <= 0) {
            throw new IllegalStateException("llama.cpp returned invalid context size: " + nCtx);
        }
        return nCtx;
    }

    /**
     * Returns the configured thread count.
     */
    public int threads() {
        return threads;
    }

    /**
     * Returns whether this generator has already been closed.
     */
    public boolean closed() {
        return closed.get();
    }

    /**
     * Returns the number of tokens the model will use for the given text.
     */
    public synchronized int tokenCount(String text) {
        ensureOpen();
        Objects.requireNonNull(text, "text");
        int count = DTLV.dtlv_llama_generator_token_count(handle, text);
        if (count < 0) {
            throw new IllegalStateException("Failed to count tokens (rc=" + count + ")");
        }
        return count;
    }

    /**
     * Generates completion text for the given prompt.
     */
    public synchronized String generate(String prompt, int maxTokens) {
        Objects.requireNonNull(prompt, "prompt");
        return runTextOperation("generate", prompt, maxTokens, false);
    }

    /**
     * Produces a summary for the given text.
     */
    public synchronized String summarize(String text, int maxTokens) {
        return runTextOperation("summarize", text, maxTokens, true);
    }

    @Override
    public synchronized void close() {
        if (closed.compareAndSet(false, true)) {
            DTLV.dtlv_llama_generator_destroy(handle);
        }
    }
}
