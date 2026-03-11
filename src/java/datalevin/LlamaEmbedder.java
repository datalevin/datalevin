package datalevin;

import datalevin.dtlvnative.DTLV;

import java.util.ArrayList;
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
