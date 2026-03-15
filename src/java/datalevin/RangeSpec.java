package datalevin;

import clojure.lang.Keyword;
import clojure.lang.PersistentVector;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Typed builder for Datalevin key and value range specs.
 *
 * <p>Range specs map directly to the vector forms accepted by KV and list-DBI
 * range operations, for example {@code [:closed "a" "z"]} or
 * {@code [:all-back]}.
 */
public final class RangeSpec {

    private final Keyword rangeType;
    private final List<Object> bounds;
    private final List<Object> form;

    private RangeSpec(String rangeType, List<?> bounds) {
        this(ClojureCodec.keyword(rangeType), normalizeBounds(bounds));
    }

    private RangeSpec(Keyword rangeType, List<Object> bounds) {
        this.rangeType = rangeType;
        this.bounds = List.copyOf(bounds);
        ArrayList<Object> values = new ArrayList<>(1 + bounds.size());
        values.add(rangeType);
        values.addAll(bounds);
        this.form = persistentVector(values);
    }

    /**
     * Creates {@code [:all]}.
     */
    public static RangeSpec all() {
        return new RangeSpec(":all", List.of());
    }

    /**
     * Creates {@code [:at-least lower]}.
     */
    public static RangeSpec atLeast(Object lower) {
        return new RangeSpec(":at-least", List.of(Objects.requireNonNull(lower, "lower")));
    }

    /**
     * Creates {@code [:at-most upper]}.
     */
    public static RangeSpec atMost(Object upper) {
        return new RangeSpec(":at-most", List.of(Objects.requireNonNull(upper, "upper")));
    }

    /**
     * Creates {@code [:greater-than lower]}.
     */
    public static RangeSpec greaterThan(Object lower) {
        return new RangeSpec(":greater-than", List.of(Objects.requireNonNull(lower, "lower")));
    }

    /**
     * Creates {@code [:less-than upper]}.
     */
    public static RangeSpec lessThan(Object upper) {
        return new RangeSpec(":less-than", List.of(Objects.requireNonNull(upper, "upper")));
    }

    /**
     * Creates {@code [:closed lower upper]}.
     */
    public static RangeSpec closed(Object lower, Object upper) {
        return new RangeSpec(":closed",
                             List.of(Objects.requireNonNull(lower, "lower"),
                                     Objects.requireNonNull(upper, "upper")));
    }

    /**
     * Creates {@code [:closed-open lower upper]}.
     */
    public static RangeSpec closedOpen(Object lower, Object upper) {
        return new RangeSpec(":closed-open",
                             List.of(Objects.requireNonNull(lower, "lower"),
                                     Objects.requireNonNull(upper, "upper")));
    }

    /**
     * Creates {@code [:open lower upper]}.
     */
    public static RangeSpec open(Object lower, Object upper) {
        return new RangeSpec(":open",
                             List.of(Objects.requireNonNull(lower, "lower"),
                                     Objects.requireNonNull(upper, "upper")));
    }

    /**
     * Creates {@code [:open-closed lower upper]}.
     */
    public static RangeSpec openClosed(Object lower, Object upper) {
        return new RangeSpec(":open-closed",
                             List.of(Objects.requireNonNull(lower, "lower"),
                                     Objects.requireNonNull(upper, "upper")));
    }

    /**
     * Returns a backward scan variant such as {@code [:closed-back lower upper]}.
     */
    public RangeSpec backward() {
        if (rangeType.getName().endsWith("-back")) {
            return this;
        }
        return new RangeSpec(Keyword.intern(rangeType.getNamespace(), rangeType.getName() + "-back"), bounds);
    }

    /**
     * Builds the raw vector-like range form expected by Datalevin APIs.
     */
    public List<Object> build() {
        return form;
    }

    Object buildForm() {
        return form;
    }

    @Override
    public String toString() {
        return Edn.render(form);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof RangeSpec that)) {
            return false;
        }
        return Objects.equals(form, that.form);
    }

    @Override
    public int hashCode() {
        return Objects.hash(form);
    }

    private static List<Object> normalizeBounds(List<?> bounds) {
        ArrayList<Object> converted = new ArrayList<>(bounds.size());
        for (Object bound : bounds) {
            converted.add(ClojureCodec.runtimeInput(bound));
        }
        return converted;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> persistentVector(List<Object> values) {
        return (List<Object>) (List<?>) PersistentVector.create(values);
    }
}
