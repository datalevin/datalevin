package datalevin;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Handle for a KV-backed standalone vector index.
 */
public final class VectorIndex extends HandleResource {

    private Object optsForm;

    VectorIndex(Object index, Object optsForm) {
        super(index,
              resource -> ClojureRuntime.core("close-vector-index", resource),
              "vec",
              "vec");
        this.optsForm = Objects.requireNonNull(optsForm, "optsForm");
    }

    /**
     * Returns whether this wrapper or the underlying index has been closed.
     */
    public boolean closed() {
        return isReleased()
                || ClojureCodec.javaBoolean(ClojureRuntime.invoke("datalevin.interface",
                                                                   "vec-closed?",
                                                                   resource()));
    }

    /**
     * Adds one vector to the index.
     */
    public VectorIndex addVec(Object vecRef, Object vecData) {
        ClojureRuntime.core("add-vec",
                            resource(),
                            ClojureCodec.runtimeInput(vecRef),
                            ClojureCodec.runtimeInput(vecData));
        return this;
    }

    /**
     * Removes all vectors associated with {@code vecRef}.
     */
    public VectorIndex removeVec(Object vecRef) {
        ClojureRuntime.core("remove-vec", resource(), ClojureCodec.runtimeInput(vecRef));
        return this;
    }

    /**
     * Returns whether {@code vecRef} is currently indexed.
     */
    public boolean vecIndexed(Object vecRef) {
        return ClojureRuntime.invoke("datalevin.interface",
                                     "vec-indexed?",
                                     resource(),
                                     ClojureCodec.runtimeInput(vecRef)) != null;
    }

    /**
     * Searches the vector index with default options.
     */
    public List<?> searchVec(Object queryVec) {
        return ResultSupport.sequence(ClojureRuntime.core("search-vec",
                                                         resource(),
                                                         ClojureCodec.runtimeInput(queryVec)));
    }

    /**
     * Searches the vector index with raw options.
     */
    public List<?> searchVec(Object queryVec, Map<?, ?> opts) {
        if (opts == null) {
            return searchVec(queryVec);
        }
        return ResultSupport.sequence(ClojureRuntime.core("search-vec",
                                                         resource(),
                                                         ClojureCodec.runtimeInput(queryVec),
                                                         DatalevinForms.optionsInput(opts)));
    }

    /**
     * Searches the vector index with typed options.
     */
    public List<?> searchVec(Object queryVec, RetrievalOptions opts) {
        if (opts == null) {
            return searchVec(queryVec);
        }
        return ResultSupport.sequence(ClojureRuntime.core("search-vec",
                                                         resource(),
                                                         ClojureCodec.runtimeInput(queryVec),
                                                         opts.buildForm()));
    }

    /**
     * Returns vector index metadata.
     */
    public Map<?, ?> info() {
        return (Map<?, ?>) ClojureRuntime.core("vector-index-info", resource());
    }

    /**
     * Returns checkpoint metadata for this vector index.
     */
    public Map<?, ?> checkpointState() {
        return (Map<?, ?>) ClojureRuntime.core("vector-checkpoint-state", resource());
    }

    /**
     * Forces vector checkpoint persistence to the backing KV store.
     */
    public VectorIndex forceCheckpoint() {
        ClojureRuntime.core("force-vec-checkpoint!", resource());
        return this;
    }

    /**
     * Rebuilds the vector index and returns this wrapper.
     */
    public VectorIndex reIndex() {
        return reIndex((Map<?, ?>) null);
    }

    /**
     * Rebuilds the vector index with raw options and returns this wrapper.
     */
    public VectorIndex reIndex(Map<?, ?> opts) {
        Object nextOpts = opts == null ? optsForm : DatalevinForms.optionsInput(opts);
        Object next = ClojureRuntime.core("re-index", resource(), nextOpts);
        replaceResource(next);
        optsForm = nextOpts;
        return this;
    }

    /**
     * Rebuilds the vector index with typed options and returns this wrapper.
     */
    public VectorIndex reIndex(RetrievalOptions opts) {
        Object nextOpts = opts == null ? optsForm : opts.buildForm();
        Object next = ClojureRuntime.core("re-index", resource(), nextOpts);
        replaceResource(next);
        optsForm = nextOpts;
        return this;
    }

    /**
     * Clears this vector index from memory and disk, then closes this wrapper.
     */
    public VectorIndex clear() {
        ClojureRuntime.core("clear-vector-index", resource());
        close();
        return this;
    }
}
