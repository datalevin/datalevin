package datalevin;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builder for pull selectors.
 */
public final class PullSelector {

    private final List<Object> items = new ArrayList<>();

    PullSelector() {
    }

    /**
     * Adds a keyword attribute such as {@code :name}.
     */
    public PullSelector attr(String attr) {
        items.add(Datalevin.keyword(attr));
        return this;
    }

    /**
     * Adds a raw selector item.
     */
    public PullSelector attrRaw(Object attr) {
        items.add(attr);
        return this;
    }

    /**
     * Adds a prebuilt attribute expression.
     */
    public PullSelector attr(Attribute attr) {
        items.add(attr.build());
        return this;
    }

    /**
     * Adds the wildcard selector.
     */
    public PullSelector wildcard() {
        items.add("*");
        return this;
    }

    /**
     * Adds a nested pull expression for the given attribute.
     */
    public PullSelector nested(String attr, PullSelector pattern) {
        return nested(PullSelector.attribute(attr), pattern);
    }

    /**
     * Adds a nested pull expression for the given attribute expression.
     */
    public PullSelector nested(Attribute attr, PullSelector pattern) {
        LinkedHashMap<Object, Object> nested = new LinkedHashMap<>(1);
        nested.put(attr.build(), pattern.build());
        items.add(nested);
        return this;
    }

    /**
     * Adds a raw selector item.
     */
    public PullSelector raw(Object item) {
        items.add(item);
        return this;
    }

    /**
     * Returns a mutable Java value suitable for {@link Connection#pull}.
     */
    public List<Object> build() {
        return new ArrayList<>(items);
    }

    @Override
    public String toString() {
        return Edn.render(items);
    }

    /**
     * Starts an attribute-expression builder.
     */
    public static Attribute attribute(String attr) {
        return new Attribute(attr);
    }

    /**
     * Builder for attribute expressions such as
     * {@code [:friend :limit 1 :as :best-friend]}.
     */
    public static final class Attribute {

        private final List<Object> expr = new ArrayList<>();

        private Attribute(Object attr) {
            expr.add(normalizeAttr(attr));
        }

        /**
         * Adds a {@code :limit} option.
         */
        public Attribute limit(long n) {
            return option(":limit", n);
        }

        /**
         * Adds a {@code :default} option.
         */
        public Attribute defaultValue(Object value) {
            return option(":default", value);
        }

        /**
         * Adds an {@code :as} alias option.
         */
        public Attribute as(String alias) {
            return option(":as", Datalevin.keyword(alias));
        }

        /**
         * Adds an arbitrary option pair.
         */
        public Attribute option(String key, Object value) {
            expr.add(Datalevin.keyword(key));
            expr.add(value);
            return this;
        }

        /**
         * Returns a mutable Java value suitable for selector nesting.
         */
        public List<Object> build() {
            return new ArrayList<>(expr);
        }

        @Override
        public String toString() {
            return Edn.render(expr);
        }

        private static Object normalizeAttr(Object attr) {
            if (attr instanceof String s) {
                return Datalevin.keyword(s);
            }
            return attr;
        }
    }
}
