package datalevin;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Builder for Datalevin runtime UDF descriptor maps.
 */
public final class UdfDescriptor {

    private final LinkedHashMap<Object, Object> values = new LinkedHashMap<>();
    private Object cachedForm;

    private UdfDescriptor() {
    }

    /**
     * Creates a query function descriptor for {@code id}.
     */
    public static UdfDescriptor queryFn(String id) {
        return of("query-fn", id);
    }

    /**
     * Creates a query predicate descriptor for {@code id}.
     */
    public static UdfDescriptor predicate(String id) {
        return of("predicate", id);
    }

    /**
     * Creates a transaction function descriptor for {@code id}.
     */
    public static UdfDescriptor txFn(String id) {
        return of("tx-fn", id);
    }

    /**
     * Creates a full-text analyzer descriptor for {@code id}.
     */
    public static UdfDescriptor analyzer(String id) {
        return of("analyzer", id);
    }

    /**
     * Creates a full-text query analyzer descriptor for {@code id}.
     */
    public static UdfDescriptor queryAnalyzer(String id) {
        return of("query-analyzer", id);
    }

    /**
     * Creates a Java-backed descriptor with the given UDF kind and id.
     */
    public static UdfDescriptor of(String kind, String id) {
        return new UdfDescriptor()
                .lang("java")
                .kind(kind)
                .id(id);
    }

    /**
     * Sets {@code :udf/lang}.
     */
    public UdfDescriptor lang(String lang) {
        return keywordProp("udf/lang", lang);
    }

    /**
     * Sets {@code :udf/kind}.
     */
    public UdfDescriptor kind(String kind) {
        return keywordProp("udf/kind", kind);
    }

    /**
     * Sets {@code :udf/id}.
     */
    public UdfDescriptor id(String id) {
        return keywordProp("udf/id", id);
    }

    /**
     * Sets optional {@code :udf/version}. Pass {@code null} to omit it.
     */
    public UdfDescriptor version(Object version) {
        cachedForm = null;
        Object key = Datalevin.kw("udf/version");
        if (version == null) {
            values.remove(key);
        } else {
            values.put(key, version);
        }
        return this;
    }

    /**
     * Returns a mutable descriptor map suitable for transactions, query inputs,
     * and registry operations.
     */
    public Map<Object, Object> build() {
        return new LinkedHashMap<>(values);
    }

    Object buildForm() {
        if (cachedForm == null) {
            cachedForm = DatalevinForms.udfDescriptorInput(values);
        }
        return cachedForm;
    }

    private UdfDescriptor keywordProp(String key, String value) {
        cachedForm = null;
        values.put(Datalevin.kw(key), Datalevin.kw(value));
        return this;
    }

    @Override
    public String toString() {
        return Edn.render(values);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof UdfDescriptor that)) {
            return false;
        }
        return Objects.equals(buildForm(), that.buildForm());
    }

    @Override
    public int hashCode() {
        return Objects.hash(buildForm());
    }
}
