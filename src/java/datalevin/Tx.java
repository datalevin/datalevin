package datalevin;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.Objects;

import clojure.lang.PersistentVector;

/**
 * Static helpers for Datalevin transaction forms.
 */
public final class Tx {

    private Tx() {
    }

    /**
     * Starts an entity-map transaction item.
     */
    public static Entity entity() {
        return new Entity();
    }

    /**
     * Starts an entity-map transaction item with an explicit {@code :db/id}.
     */
    public static Entity entity(Object dbId) {
        return new Entity().id(dbId);
    }

    /**
     * Creates a {@code :db/add} transaction form.
     */
    public static List<Object> add(Object entityId, String attr, Object value) {
        return PersistentVector.create(Arrays.asList(
                Datalevin.kw("db/add"),
                DatalevinForms.lookupRefInput(entityId),
                Datalevin.kw(attr),
                ClojureCodec.runtimeInput(value)
        ));
    }

    /**
     * Creates a {@code :db/retract} transaction form.
     */
    public static List<Object> retract(Object entityId, String attr, Object value) {
        return PersistentVector.create(Arrays.asList(
                Datalevin.kw("db/retract"),
                DatalevinForms.lookupRefInput(entityId),
                Datalevin.kw(attr),
                ClojureCodec.runtimeInput(value)
        ));
    }

    /**
     * Creates a {@code :db/retractEntity} transaction form.
     */
    public static List<Object> retractEntity(Object entityId) {
        return PersistentVector.create(Arrays.asList(
                Datalevin.kw("db/retractEntity"),
                DatalevinForms.lookupRefInput(entityId)
        ));
    }

    /**
     * Builder for entity-map transaction items.
     */
    public static final class Entity {

        private final LinkedHashMap<Object, Object> values = new LinkedHashMap<>();

        /**
         * Sets {@code :db/id}.
         */
        public Entity id(Object dbId) {
            values.put(Datalevin.kw("db/id"), dbId);
            return this;
        }

        /**
         * Adds a keyword attribute and value.
         */
        public Entity put(String attr, Object value) {
            values.put(Datalevin.kw(attr), value);
            return this;
        }

        /**
         * Adds a raw key and value.
         */
        public Entity putRaw(Object key, Object value) {
            values.put(key, value);
            return this;
        }

        /**
         * Returns a mutable entity map suitable for transaction assembly.
         */
        public Map<Object, Object> build() {
            return new LinkedHashMap<>(values);
        }

        Object buildForm() {
            return firstTxItem(DatalevinForms.txDataInput(List.of(values)));
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof Entity that)) {
                return false;
            }
            return Objects.equals(buildForm(), that.buildForm());
        }

        @Override
        public int hashCode() {
            return Objects.hash(buildForm());
        }
    }

    private static Object firstTxItem(Object txData) {
        return ((List<?>) txData).get(0);
    }
}
