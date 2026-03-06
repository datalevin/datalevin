package datalevin;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Builder for Datalevin schema maps.
 */
public final class Schema {

    private final LinkedHashMap<String, Object> attributes = new LinkedHashMap<>();

    Schema() {
    }

    /**
     * Adds an attribute definition built with the typed attribute builder.
     */
    public Schema attr(String attr, Attribute spec) {
        attributes.put(Datalevin.keyword(attr), spec.build());
        return this;
    }

    /**
     * Adds an attribute definition from a raw schema property map.
     */
    public Schema attr(String attr, Map<String, ?> spec) {
        attributes.put(Datalevin.keyword(attr), spec);
        return this;
    }

    /**
     * Returns a mutable schema map suitable for connection creation or update.
     */
    public Map<String, Object> build() {
        return new LinkedHashMap<>(attributes);
    }

    @Override
    public String toString() {
        return Edn.render(attributes);
    }

    /**
     * Starts a typed attribute-spec builder.
     */
    public static Attribute attribute() {
        return new Attribute();
    }

    /**
     * Builder for a single schema attribute definition.
     */
    public static final class Attribute {

        private final LinkedHashMap<String, Object> props = new LinkedHashMap<>();

        /**
         * Sets {@code :db/valueType}.
         */
        public Attribute valueType(String valueType) {
            return keywordProp(":db/valueType", valueType);
        }

        /**
         * Sets {@code :db/cardinality}.
         */
        public Attribute cardinality(String cardinality) {
            return keywordProp(":db/cardinality", cardinality);
        }

        /**
         * Sets {@code :db/unique}.
         */
        public Attribute unique(String unique) {
            return keywordProp(":db/unique", unique);
        }

        /**
         * Sets {@code :db/tupleType}.
         */
        public Attribute tupleType(String tupleType) {
            return keywordProp(":db/tupleType", tupleType);
        }

        /**
         * Sets {@code :db/tupleTypes}.
         */
        public Attribute tupleTypes(String... tupleTypes) {
            ArrayList<Object> values = new ArrayList<>(tupleTypes.length);
            for (String tupleType : tupleTypes) {
                values.add(Datalevin.keyword(tupleType));
            }
            props.put(":db/tupleTypes", values);
            return this;
        }

        /**
         * Sets {@code :db/tupleAttrs}.
         */
        public Attribute tupleAttrs(String... attrs) {
            ArrayList<Object> values = new ArrayList<>(attrs.length);
            for (String attr : attrs) {
                values.add(Datalevin.keyword(attr));
            }
            props.put(":db/tupleAttrs", values);
            return this;
        }

        /**
         * Sets {@code :db/index}.
         */
        public Attribute index(boolean enabled) {
            props.put(":db/index", enabled);
            return this;
        }

        /**
         * Sets {@code :db/fulltext}.
         */
        public Attribute fulltext(boolean enabled) {
            props.put(":db/fulltext", enabled);
            return this;
        }

        /**
         * Sets {@code :db/isComponent}.
         */
        public Attribute isComponent(boolean enabled) {
            props.put(":db/isComponent", enabled);
            return this;
        }

        /**
         * Sets {@code :db/noHistory}.
         */
        public Attribute noHistory(boolean enabled) {
            props.put(":db/noHistory", enabled);
            return this;
        }

        /**
         * Sets {@code :db/doc}.
         */
        public Attribute doc(String doc) {
            props.put(":db/doc", doc);
            return this;
        }

        /**
         * Adds an arbitrary schema property.
         */
        public Attribute prop(String key, Object value) {
            props.put(Datalevin.keyword(key), value);
            return this;
        }

        /**
         * Returns a mutable property map suitable for schema assembly.
         */
        public Map<String, Object> build() {
            return new LinkedHashMap<>(props);
        }

        @Override
        public String toString() {
            return Edn.render(props);
        }

        private Attribute keywordProp(String key, String value) {
            props.put(key, Datalevin.keyword(value));
            return this;
        }
    }
}
