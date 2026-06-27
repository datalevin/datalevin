package datalevin;

import clojure.lang.PersistentVector;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Builder for search, vector, embedding, and idoc option maps.
 */
public final class RetrievalOptions {

    private final LinkedHashMap<Object, Object> props = new LinkedHashMap<>();

    private RetrievalOptions() {
    }

    /**
     * Starts a full-text query/default option map.
     */
    public static RetrievalOptions search() {
        return new RetrievalOptions();
    }

    /**
     * Starts a full-text domain option map.
     */
    public static RetrievalOptions searchDomain() {
        return new RetrievalOptions();
    }

    /**
     * Starts a vector index/domain option map with required dimensions.
     */
    public static RetrievalOptions vector(long dimensions) {
        return new RetrievalOptions().dimensions(dimensions);
    }

    /**
     * Starts an embedding provider/domain option map.
     */
    public static RetrievalOptions embedding() {
        return new RetrievalOptions();
    }

    /**
     * Starts an idoc match option map.
     */
    public static RetrievalOptions idoc() {
        return new RetrievalOptions();
    }

    /**
     * Sets {@code :top}.
     */
    public RetrievalOptions top(long top) {
        props.put(Datalevin.kw("top"), top);
        return this;
    }

    /**
     * Sets {@code :limit}.
     */
    public RetrievalOptions limit(long limit) {
        props.put(Datalevin.kw("limit"), limit);
        return this;
    }

    /**
     * Sets {@code :offset}.
     */
    public RetrievalOptions offset(long offset) {
        props.put(Datalevin.kw("offset"), offset);
        return this;
    }

    /**
     * Sets {@code :paging-cache-pages}.
     */
    public RetrievalOptions pagingCachePages(long pages) {
        props.put(Datalevin.kw("paging-cache-pages"), pages);
        return this;
    }

    /**
     * Sets {@code :display}.
     */
    public RetrievalOptions display(String display) {
        return keywordProp("display", display);
    }

    /**
     * Sets {@code :domains}.
     */
    public RetrievalOptions domains(String... domains) {
        return stringVectorProp("domains", domains);
    }

    /**
     * Sets {@code :proximity-expansion}.
     */
    public RetrievalOptions proximityExpansion(long expansion) {
        props.put(Datalevin.kw("proximity-expansion"), expansion);
        return this;
    }

    /**
     * Sets {@code :proximity-max-dist}.
     */
    public RetrievalOptions proximityMaxDist(long distance) {
        props.put(Datalevin.kw("proximity-max-dist"), distance);
        return this;
    }

    /**
     * Sets {@code :index-position?}.
     */
    public RetrievalOptions indexPosition(boolean enabled) {
        props.put(Datalevin.kw("index-position?"), enabled);
        return this;
    }

    /**
     * Sets {@code :include-text?}.
     */
    public RetrievalOptions includeText(boolean enabled) {
        props.put(Datalevin.kw("include-text?"), enabled);
        return this;
    }

    /**
     * Sets {@code :indexing-mode}.
     */
    public RetrievalOptions indexingMode(String mode) {
        return keywordProp("indexing-mode", mode);
    }

    /**
     * Sets {@code :dimensions}.
     */
    public RetrievalOptions dimensions(long dimensions) {
        props.put(Datalevin.kw("dimensions"), dimensions);
        return this;
    }

    /**
     * Sets {@code :metric-type}.
     */
    public RetrievalOptions metricType(String metricType) {
        return keywordProp("metric-type", metricType);
    }

    /**
     * Sets {@code :quantization}.
     */
    public RetrievalOptions quantization(String quantization) {
        return keywordProp("quantization", quantization);
    }

    /**
     * Sets {@code :connectivity}.
     */
    public RetrievalOptions connectivity(long connectivity) {
        props.put(Datalevin.kw("connectivity"), connectivity);
        return this;
    }

    /**
     * Sets {@code :expansion-add}.
     */
    public RetrievalOptions expansionAdd(long expansionAdd) {
        props.put(Datalevin.kw("expansion-add"), expansionAdd);
        return this;
    }

    /**
     * Sets {@code :expansion-search}.
     */
    public RetrievalOptions expansionSearch(long expansionSearch) {
        props.put(Datalevin.kw("expansion-search"), expansionSearch);
        return this;
    }

    /**
     * Sets {@code :domain}.
     */
    public RetrievalOptions domain(String domain) {
        props.put(Datalevin.kw("domain"), domain);
        return this;
    }

    /**
     * Sets {@code :provider}.
     */
    public RetrievalOptions provider(String provider) {
        return keywordProp("provider", provider);
    }

    /**
     * Sets {@code :model}.
     */
    public RetrievalOptions model(String model) {
        props.put(Datalevin.kw("model"), model);
        return this;
    }

    /**
     * Sets {@code :base-url}.
     */
    public RetrievalOptions baseUrl(String baseUrl) {
        props.put(Datalevin.kw("base-url"), baseUrl);
        return this;
    }

    /**
     * Sets {@code :api-key-env}.
     */
    public RetrievalOptions apiKeyEnv(String apiKeyEnv) {
        props.put(Datalevin.kw("api-key-env"), apiKeyEnv);
        return this;
    }

    /**
     * Sets {@code :request-dimensions}.
     */
    public RetrievalOptions requestDimensions(long requestDimensions) {
        props.put(Datalevin.kw("request-dimensions"), requestDimensions);
        return this;
    }

    /**
     * Adds an arbitrary option property.
     */
    public RetrievalOptions prop(String key, Object value) {
        props.put(Datalevin.kw(key), value);
        return this;
    }

    /**
     * Returns a mutable option map suitable for connection options or query
     * inputs.
     */
    public Map<Object, Object> build() {
        return new LinkedHashMap<>(props);
    }

    Object buildForm() {
        return DatalevinForms.optionsInput(props);
    }

    @Override
    public String toString() {
        return Edn.render(props);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof RetrievalOptions that)) {
            return false;
        }
        return Objects.equals(buildForm(), that.buildForm());
    }

    @Override
    public int hashCode() {
        return Objects.hash(buildForm());
    }

    private RetrievalOptions keywordProp(String key, String value) {
        props.put(Datalevin.kw(key), Datalevin.kw(value));
        return this;
    }

    private RetrievalOptions stringVectorProp(String key, String... values) {
        ArrayList<Object> items = new ArrayList<>(values.length);
        for (String value : values) {
            items.add(value);
        }
        props.put(Datalevin.kw(key), PersistentVector.create(items));
        return this;
    }
}
