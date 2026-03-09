package datalevin;

import clojure.lang.IPersistentCollection;
import clojure.lang.IPersistentMap;
import clojure.lang.PersistentArrayMap;
import clojure.lang.PersistentVector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Builder for pull selectors.
 */
public final class PullSelector {

    private final List<Object> items = new ArrayList<>();
    private Object cachedForm;

    PullSelector() {
    }

    /**
     * Adds a keyword attribute such as {@code :name}.
     */
    public PullSelector attr(String attr) {
        invalidateForm();
        items.add(ClojureCodec.keyword(attr));
        return this;
    }

    /**
     * Adds a raw selector item.
     */
    public PullSelector attrRaw(Object attr) {
        invalidateForm();
        items.add(attr);
        return this;
    }

    /**
     * Adds a prebuilt attribute expression.
     */
    public PullSelector attr(Attribute attr) {
        invalidateForm();
        items.add(attr.buildForm());
        return this;
    }

    /**
     * Adds the wildcard selector.
     */
    public PullSelector wildcard() {
        invalidateForm();
        items.add(ClojureCodec.symbol("*"));
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
        invalidateForm();
        items.add(PersistentArrayMap.EMPTY.assoc(attr.buildForm(), pattern.buildForm()));
        return this;
    }

    /**
     * Adds a raw selector item.
     */
    public PullSelector raw(Object item) {
        invalidateForm();
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
        return Edn.render(buildForm());
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
        private Object cachedForm;

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
            return option(":as", ClojureCodec.keyword(alias));
        }

        /**
         * Adds an arbitrary option pair.
         */
        public Attribute option(String key, Object value) {
            cachedForm = null;
            expr.add(ClojureCodec.keyword(key));
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
            return Edn.render(buildForm());
        }

        private static Object normalizeAttr(Object attr) {
            if (attr instanceof String s) {
                return ClojureCodec.keyword(s);
            }
            return attr;
        }

        Object buildForm() {
            if (cachedForm == null) {
                cachedForm = selectorItemForm(expr);
            }
            return cachedForm;
        }
    }

    Object buildForm() {
        if (cachedForm == null) {
            cachedForm = selectorItemForm(items);
        }
        return cachedForm;
    }

    private void invalidateForm() {
        cachedForm = null;
    }

    private static Object selectorItemForm(Object item) {
        if (item instanceof clojure.lang.Keyword || item instanceof clojure.lang.Symbol) {
            return item;
        }
        if (item instanceof EdnLiteral literal) {
            return ClojureRuntime.readEdn(literal.value());
        }
        if (item instanceof String s) {
            return s;
        }
        if (item instanceof Attribute attr) {
            return attr.buildForm();
        }
        if (item instanceof IPersistentMap || item instanceof IPersistentCollection) {
            return item;
        }
        if (item instanceof List<?> list) {
            ArrayList<Object> converted = new ArrayList<>(list.size());
            for (Object value : list) {
                converted.add(selectorItemForm(value));
            }
            return PersistentVector.create(converted);
        }
        if (item instanceof Map<?, ?> map) {
            IPersistentMap converted = PersistentArrayMap.EMPTY;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                converted = converted.assoc(selectorItemForm(entry.getKey()),
                                            selectorItemForm(entry.getValue()));
            }
            return converted;
        }
        if (item instanceof Object[] array) {
            ArrayList<Object> converted = new ArrayList<>(array.length);
            for (Object value : array) {
                converted.add(selectorItemForm(value));
            }
            return PersistentVector.create(converted);
        }
        return ClojureCodec.runtimeInput(item);
    }
}
