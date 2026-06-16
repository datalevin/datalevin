package datalevin;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Handle for a KV-backed full-text search engine.
 */
public final class SearchEngine extends HandleResource {

    private Object optsForm;

    SearchEngine(Object engine) {
        this(engine, DatalevinForms.optionsInput(Map.of()));
    }

    SearchEngine(Object engine, Object optsForm) {
        super(engine,
              resource -> {
              },
              "search",
              "search");
        this.optsForm = optsForm == null ? DatalevinForms.optionsInput(Map.of()) : optsForm;
    }

    /**
     * Returns whether this wrapper has been closed.
     */
    public boolean closed() {
        return isReleased();
    }

    /**
     * Adds or updates one document in the search index.
     */
    public SearchEngine addDoc(Object docRef, String docText) {
        Objects.requireNonNull(docText, "docText");
        ClojureRuntime.core("add-doc",
                            resource(),
                            ClojureCodec.runtimeInput(docRef),
                            docText);
        return this;
    }

    /**
     * Adds one document, optionally skipping existence checks for bulk import.
     */
    public SearchEngine addDoc(Object docRef, String docText, boolean checkExist) {
        Objects.requireNonNull(docText, "docText");
        ClojureRuntime.core("add-doc",
                            resource(),
                            ClojureCodec.runtimeInput(docRef),
                            docText,
                            checkExist);
        return this;
    }

    /**
     * Removes one document from the search index.
     */
    public SearchEngine removeDoc(Object docRef) {
        ClojureRuntime.core("remove-doc", resource(), ClojureCodec.runtimeInput(docRef));
        return this;
    }

    /**
     * Clears all indexed documents.
     */
    public SearchEngine clearDocs() {
        ClojureRuntime.core("clear-docs", resource());
        return this;
    }

    /**
     * Returns whether {@code docRef} is currently indexed.
     */
    public boolean docIndexed(Object docRef) {
        return ClojureRuntime.core("doc-indexed?",
                                   resource(),
                                   ClojureCodec.runtimeInput(docRef)) != null;
    }

    /**
     * Returns the number of indexed documents.
     */
    public long docCount() {
        return ClojureCodec.javaLong(ClojureRuntime.core("doc-count", resource()));
    }

    /**
     * Searches indexed documents with default options.
     */
    public List<?> search(String query) {
        return ResultSupport.sequence(ClojureRuntime.core("search", resource(), query));
    }

    /**
     * Searches indexed documents with raw options.
     */
    public List<?> search(String query, Map<?, ?> opts) {
        if (opts == null) {
            return search(query);
        }
        return ResultSupport.sequence(ClojureRuntime.core("search",
                                                         resource(),
                                                         query,
                                                         DatalevinForms.optionsInput(opts)));
    }

    /**
     * Searches indexed documents with typed options.
     */
    public List<?> search(String query, RetrievalOptions opts) {
        if (opts == null) {
            return search(query);
        }
        return ResultSupport.sequence(ClojureRuntime.core("search", resource(), query, opts.buildForm()));
    }

    /**
     * Rebuilds the search index from stored raw text and returns this wrapper.
     */
    public SearchEngine reIndex(Map<?, ?> opts) {
        Object nextOpts = opts == null ? optsForm : DatalevinForms.optionsInput(opts);
        Object next = ClojureRuntime.core("re-index", resource(), nextOpts);
        replaceResource(next);
        optsForm = nextOpts;
        return this;
    }

    /**
     * Rebuilds the search index from stored raw text and returns this wrapper.
     */
    public SearchEngine reIndex(RetrievalOptions opts) {
        Object nextOpts = opts == null ? optsForm : opts.buildForm();
        Object next = ClojureRuntime.core("re-index", resource(), nextOpts);
        replaceResource(next);
        optsForm = nextOpts;
        return this;
    }

    /**
     * Rebuilds the search index with default options and returns this wrapper.
     */
    public SearchEngine reIndex() {
        return reIndex((Map<?, ?>) null);
    }
}
