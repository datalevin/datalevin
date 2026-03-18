package datalevin;

import datalevin.dtlvnative.DTLV;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Local llama.cpp-backed text embedder backed by {@code dtlvnative}.
 *
 * <p>The constructor expects a GGUF embedding model path. Integer tuning
 * arguments default to {@code 0}, which defers to the native implementation's
 * defaults.
 */
public final class LlamaEmbedder implements AutoCloseable {

    private final DTLV.dtlv_llama_embedder handle;
    private final String modelPath;
    private final int gpuLayers;
    private final int ctxSize;
    private final int batchSize;
    private final int threads;
    private final int dimensions;
    private final AtomicBoolean closed;

    /**
     * Creates a llama.cpp embedder using native defaults for tuning options.
     */
    public LlamaEmbedder(String modelPath) {
        this(modelPath, 0, 0, 0, 0);
    }

    /**
     * Creates a llama.cpp embedder.
     */
    public LlamaEmbedder(String modelPath,
                         int gpuLayers,
                         int ctxSize,
                         int batchSize,
                         int threads) {
        this.modelPath = requireNonBlank(modelPath, "modelPath");
        this.gpuLayers = gpuLayers;
        this.ctxSize = ctxSize;
        this.batchSize = batchSize;
        this.threads = threads;
        this.closed = new AtomicBoolean(false);
        this.handle = new DTLV.dtlv_llama_embedder();

        int rc = DTLV.dtlv_llama_embedder_create(handle,
                                                 this.modelPath,
                                                 gpuLayers,
                                                 ctxSize,
                                                 batchSize,
                                                 threads);
        checkRc(rc, "initialize llama embedder");

        int dims = DTLV.dtlv_llama_embedder_n_embd(handle);
        if (dims <= 0) {
            DTLV.dtlv_llama_embedder_destroy(handle);
            throw new IllegalStateException("llama.cpp returned invalid embedding dimensions: " + dims);
        }
        this.dimensions = dims;
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

    private static int initialDetokenizeBufferSize(int tokenCount) {
        long estimatedSize = Math.max(32L, (long) tokenCount * 8L);
        return estimatedSize >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) estimatedSize;
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Llama embedder is closed");
        }
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
        int nCtx = DTLV.dtlv_llama_embedder_n_ctx(handle);
        if (nCtx <= 0) {
            throw new IllegalStateException("llama.cpp returned invalid context size: " + nCtx);
        }
        return nCtx;
    }

    /**
     * Returns the configured batch size.
     */
    public int batchSize() {
        return batchSize;
    }

    /**
     * Returns the configured thread count.
     */
    public int threads() {
        return threads;
    }

    /**
     * Returns the embedding dimensions produced by this model.
     */
    public int dimensions() {
        return dimensions;
    }

    /**
     * Returns whether this embedder has already been closed.
     */
    public boolean closed() {
        return closed.get();
    }

    /**
     * Embeds a single input string.
     */
    public synchronized float[] embed(String text) {
        ensureOpen();
        Objects.requireNonNull(text, "text");
        float[] result = new float[dimensions];
        int rc = DTLV.dtlv_llama_embed(handle, text, result, dimensions);
        checkRc(rc, "embed text");
        return result;
    }

    /**
     * Returns the number of tokens the model will use for the given text.
     */
    public synchronized int tokenCount(String text) {
        ensureOpen();
        Objects.requireNonNull(text, "text");
        int count = DTLV.dtlv_llama_token_count(handle, text);
        if (count < 0) {
            throw new IllegalStateException("Failed to count tokens (rc=" + count + ")");
        }
        return count;
    }

    /**
     * Tokenizes the input string using the model tokenizer.
     */
    public synchronized int[] tokenize(String text) {
        ensureOpen();
        Objects.requireNonNull(text, "text");

        int expectedCount = tokenCount(text);
        if (expectedCount == 0) {
            return new int[0];
        }

        int bufferSize = expectedCount;
        for (int attempt = 0; attempt < 8; attempt++) {
            int[] tokens = new int[bufferSize];
            int actualCount = DTLV.dtlv_llama_tokenize(handle, text, tokens, bufferSize);
            if (actualCount >= 0 && actualCount <= bufferSize) {
                return actualCount == bufferSize ? tokens : Arrays.copyOf(tokens, actualCount);
            }

            int requiredSize = Math.abs(actualCount);
            if (requiredSize <= bufferSize) {
                throw new IllegalStateException("Failed to tokenize text (rc=" + actualCount + ")");
            }
            bufferSize = nextBufferSize(bufferSize, requiredSize);
        }

        throw new IllegalStateException("Failed to tokenize text after resizing token buffer");
    }

    /**
     * Detokenizes the given token ids back into text.
     */
    public synchronized String detokenize(int[] tokens) {
        ensureOpen();
        Objects.requireNonNull(tokens, "tokens");
        if (tokens.length == 0) {
            return "";
        }

        int bufferSize = initialDetokenizeBufferSize(tokens.length);
        for (int attempt = 0; attempt < 8; attempt++) {
            byte[] bytes = new byte[bufferSize];
            int actualSize = DTLV.dtlv_llama_detokenize(handle, tokens, tokens.length, bytes, bufferSize);
            if (actualSize >= 0 && actualSize <= bufferSize) {
                return new String(bytes, 0, actualSize, StandardCharsets.UTF_8);
            }

            int requiredSize = Math.abs(actualSize);
            if (requiredSize <= bufferSize) {
                throw new IllegalStateException("Failed to detokenize text (rc=" + actualSize + ")");
            }
            bufferSize = nextBufferSize(bufferSize, requiredSize);
        }

        throw new IllegalStateException("Failed to detokenize text after resizing text buffer");
    }

    /**
     * Truncates text so it fits within the requested token count.
     */
    public synchronized String truncateText(String text, int maxTokens) {
        ensureOpen();
        Objects.requireNonNull(text, "text");
        if (maxTokens < 0) {
            throw new IllegalArgumentException("maxTokens must be non-negative");
        }

        int count = tokenCount(text);
        if (count <= maxTokens) {
            return text;
        }
        if (maxTokens == 0) {
            return "";
        }

        int[] tokens = tokenize(text);
        if (tokens.length <= maxTokens) {
            return text;
        }
        return detokenize(Arrays.copyOf(tokens, maxTokens));
    }

    /**
     * Embeds each input string in order.
     */
    public synchronized List<float[]> embedAll(List<String> texts) {
        ensureOpen();
        Objects.requireNonNull(texts, "texts");
        List<float[]> result = new ArrayList<>(texts.size());
        for (String text : texts) {
            result.add(embed(text));
        }
        return result;
    }

    @Override
    public synchronized void close() {
        if (closed.compareAndSet(false, true)) {
            DTLV.dtlv_llama_embedder_destroy(handle);
        }
    }
}
