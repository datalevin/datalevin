package datalevin;

import clojure.lang.PersistentHashSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Thin Java wrappers for {@code datalevin.search-utils}.
 *
 * <p>The returned values are Datalevin runtime analyzer/tokenizer/filter
 * functions. Treat them as opaque JVM objects and pass them back in search
 * option maps such as {@code :analyzer}, {@code :query-analyzer},
 * {@code :tokenizer}, and {@code :token-filters}.
 */
public final class SearchUtils {

    private static final String NS = "datalevin.search-utils";

    static {
        ClojureRuntime.requireNamespace(NS);
    }

    private SearchUtils() {
    }

    /**
     * Creates an analyzer from {@code :tokenizer} and {@code :token-filters}
     * options.
     */
    public static Object createAnalyzer(Map<?, ?> opts) {
        return invoke("create-analyzer", DatalevinForms.optionsInput(opts == null ? Map.of() : opts));
    }

    /**
     * Returns the lower-case token filter.
     */
    public static Object lowerCaseTokenFilter() {
        return value("lower-case-token-filter");
    }

    /**
     * Returns the unaccent token filter.
     */
    public static Object unaccentTokenFilter() {
        return value("unaccent-token-filter");
    }

    /**
     * Creates a stop-words token filter from a collection of stop words or a
     * raw Clojure predicate.
     */
    public static Object createStopWordsTokenFilter(Object stopWordsOrPredicate) {
        Object predicate = stopWordsOrPredicate instanceof Collection<?> stopWords
                ? stopWordSet(stopWords)
                : ClojureCodec.runtimeInput(stopWordsOrPredicate);
        return invoke("create-stop-words-token-filter", predicate);
    }

    /**
     * Returns the English stop-words token filter.
     */
    public static Object enStopWordsTokenFilter() {
        return value("en-stop-words-token-filter");
    }

    /**
     * Returns the prefix token filter.
     */
    public static Object prefixTokenFilter() {
        return value("prefix-token-filter");
    }

    /**
     * Creates a fixed-size ngram token filter.
     */
    public static Object createNgramTokenFilter(long gramSize) {
        return invoke("create-ngram-token-filter", gramSize);
    }

    /**
     * Creates a variable-size ngram token filter.
     */
    public static Object createNgramTokenFilter(long minGramSize, long maxGramSize) {
        return invoke("create-ngram-token-filter", minGramSize, maxGramSize);
    }

    /**
     * Creates a minimum-length token filter.
     */
    public static Object createMinLengthTokenFilter(long minLength) {
        return invoke("create-min-length-token-filter", minLength);
    }

    /**
     * Creates a maximum-length token filter.
     */
    public static Object createMaxLengthTokenFilter(long maxLength) {
        return invoke("create-max-length-token-filter", maxLength);
    }

    /**
     * Creates a Snowball stemming token filter for {@code language}.
     */
    public static Object createStemmingTokenFilter(String language) {
        return invoke("create-stemming-token-filter", Objects.requireNonNull(language, "language"));
    }

    /**
     * Creates a regexp tokenizer from a Java regular-expression string.
     */
    public static Object createRegexpTokenizer(String pattern) {
        return createRegexpTokenizer(Pattern.compile(Objects.requireNonNull(pattern, "pattern")));
    }

    /**
     * Creates a regexp tokenizer from a compiled Java pattern.
     */
    public static Object createRegexpTokenizer(Pattern pattern) {
        return invoke("create-regexp-tokenizer", Objects.requireNonNull(pattern, "pattern"));
    }

    private static Object value(String name) {
        return ClojureRuntime.varValue(NS, name);
    }

    private static Object invoke(String name, Object... args) {
        return ClojureRuntime.invoke(NS, name, args);
    }

    private static Object stopWordSet(Collection<?> stopWords) {
        ArrayList<Object> values = new ArrayList<>(stopWords.size());
        for (Object stopWord : stopWords) {
            values.add(ClojureCodec.runtimeInput(stopWord));
        }
        return PersistentHashSet.create(values);
    }
}
