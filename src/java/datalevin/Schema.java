package datalevin;

import clojure.lang.PersistentVector;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Builder for Datalevin schema maps.
 */
public final class Schema {

    private final LinkedHashMap<Object, Object> attributes = new LinkedHashMap<>();
    private Object cachedForm;

    Schema() {
    }

    /**
     * Adds an attribute definition built with the typed attribute builder.
     */
    public Schema attr(String attr, Attribute spec) {
        cachedForm = null;
        attributes.put(Datalevin.kw(attr), spec.buildForm());
        return this;
    }

    /**
     * Adds an attribute definition from a raw schema property map.
     */
    public Schema attr(String attr, Map<?, ?> spec) {
        cachedForm = null;
        attributes.put(Datalevin.kw(attr), spec);
        return this;
    }

    /**
     * Returns a mutable schema map suitable for connection creation or update.
     */
    public Map<Object, Object> build() {
        return new LinkedHashMap<>(attributes);
    }

    Object buildForm() {
        if (cachedForm == null) {
            cachedForm = DatalevinForms.schemaInput(attributes);
        }
        return cachedForm;
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
     * Built-in Datalevin schema value types.
     */
    public enum ValueType {
        KEYWORD("db.type/keyword"),
        SYMBOL("db.type/symbol"),
        STRING("db.type/string"),
        BOOLEAN("db.type/boolean"),
        LONG("db.type/long"),
        DOUBLE("db.type/double"),
        FLOAT("db.type/float"),
        REF("db.type/ref"),
        BIGINT("db.type/bigint"),
        BIGDEC("db.type/bigdec"),
        INSTANT("db.type/instant"),
        UUID("db.type/uuid"),
        BYTES("db.type/bytes"),
        TUPLE("db.type/tuple"),
        VEC("db.type/vec"),
        IDOC("db.type/idoc");

        private final String keyword;

        ValueType(String keyword) {
            this.keyword = keyword;
        }

        String keyword() {
            return keyword;
        }
    }

    /**
     * Built-in Datalevin cardinality values.
     */
    public enum Cardinality {
        ONE("db.cardinality/one"),
        MANY("db.cardinality/many");

        private final String keyword;

        Cardinality(String keyword) {
            this.keyword = keyword;
        }

        String keyword() {
            return keyword;
        }
    }

    /**
     * Built-in Datalevin uniqueness values.
     */
    public enum Unique {
        VALUE("db.unique/value"),
        IDENTITY("db.unique/identity");

        private final String keyword;

        Unique(String keyword) {
            this.keyword = keyword;
        }

        String keyword() {
            return keyword;
        }
    }

    /**
     * Builder for a single schema attribute definition.
     */
    public static final class Attribute {

        private final LinkedHashMap<Object, Object> props = new LinkedHashMap<>();

        /**
         * Sets {@code :db/valueType}.
         */
        public Attribute valueType(String valueType) {
            return keywordProp(":db/valueType", valueType);
        }

        /**
         * Sets {@code :db/valueType}.
         */
        public Attribute valueType(ValueType valueType) {
            return keywordProp(":db/valueType", valueType.keyword());
        }

        /**
         * Sets {@code :db/cardinality}.
         */
        public Attribute cardinality(String cardinality) {
            return keywordProp(":db/cardinality", cardinality);
        }

        /**
         * Sets {@code :db/cardinality}.
         */
        public Attribute cardinality(Cardinality cardinality) {
            return keywordProp(":db/cardinality", cardinality.keyword());
        }

        /**
         * Sets {@code :db/unique}.
         */
        public Attribute unique(String unique) {
            return keywordProp(":db/unique", unique);
        }

        /**
         * Sets {@code :db/unique}.
         */
        public Attribute unique(Unique unique) {
            return keywordProp(":db/unique", unique.keyword());
        }

        /**
         * Sets {@code :db/tupleType}.
         */
        public Attribute tupleType(String tupleType) {
            return keywordProp(":db/tupleType", tupleType);
        }

        /**
         * Sets {@code :db/tupleType}.
         */
        public Attribute tupleType(ValueType tupleType) {
            return keywordProp(":db/tupleType", tupleType.keyword());
        }

        /**
         * Sets {@code :db/tupleTypes}.
         */
        public Attribute tupleTypes(String... tupleTypes) {
            ArrayList<Object> values = new ArrayList<>(tupleTypes.length);
            for (String tupleType : tupleTypes) {
                values.add(Datalevin.kw(tupleType));
            }
            props.put(Datalevin.kw("db/tupleTypes"), PersistentVector.create(values));
            return this;
        }

        /**
         * Sets {@code :db/tupleTypes}.
         */
        public Attribute tupleTypes(ValueType... tupleTypes) {
            ArrayList<Object> values = new ArrayList<>(tupleTypes.length);
            for (ValueType tupleType : tupleTypes) {
                values.add(Datalevin.kw(tupleType.keyword()));
            }
            props.put(Datalevin.kw("db/tupleTypes"), PersistentVector.create(values));
            return this;
        }

        /**
         * Sets {@code :db/tupleAttrs}.
         */
        public Attribute tupleAttrs(String... attrs) {
            ArrayList<Object> values = new ArrayList<>(attrs.length);
            for (String attr : attrs) {
                values.add(Datalevin.kw(attr));
            }
            props.put(Datalevin.kw("db/tupleAttrs"), PersistentVector.create(values));
            return this;
        }

        /**
         * Sets {@code :db/index}.
         */
        public Attribute index(boolean enabled) {
            props.put(Datalevin.kw("db/index"), enabled);
            return this;
        }

        /**
         * Sets {@code :db/fulltext}.
         */
        public Attribute fulltext(boolean enabled) {
            props.put(Datalevin.kw("db/fulltext"), enabled);
            return this;
        }

        /**
         * Sets {@code :db/isComponent}.
         */
        public Attribute isComponent(boolean enabled) {
            props.put(Datalevin.kw("db/isComponent"), enabled);
            return this;
        }

        /**
         * Sets {@code :db/noHistory}.
         */
        public Attribute noHistory(boolean enabled) {
            props.put(Datalevin.kw("db/noHistory"), enabled);
            return this;
        }

        /**
         * Sets {@code :db/doc}.
         */
        public Attribute doc(String doc) {
            props.put(Datalevin.kw("db/doc"), doc);
            return this;
        }

        /**
         * Adds an arbitrary schema property.
         */
        public Attribute prop(String key, Object value) {
            props.put(Datalevin.kw(key), value);
            return this;
        }

        /**
         * Returns a mutable property map suitable for schema assembly.
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

        private Attribute keywordProp(String key, String value) {
            props.put(Datalevin.kw(key), Datalevin.kw(value));
            return this;
        }
    }
}
