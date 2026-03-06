package datalevin;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
        return Datalevin.listOf(":db/add", entityId, Datalevin.keyword(attr), value);
    }

    /**
     * Creates a {@code :db/retract} transaction form.
     */
    public static List<Object> retract(Object entityId, String attr, Object value) {
        return Datalevin.listOf(":db/retract", entityId, Datalevin.keyword(attr), value);
    }

    /**
     * Creates a {@code :db/retractEntity} transaction form.
     */
    public static List<Object> retractEntity(Object entityId) {
        return Datalevin.listOf(":db/retractEntity", entityId);
    }

    /**
     * Builder for entity-map transaction items.
     */
    public static final class Entity {

        private final LinkedHashMap<String, Object> values = new LinkedHashMap<>();

        /**
         * Sets {@code :db/id}.
         */
        public Entity id(Object dbId) {
            values.put(":db/id", dbId);
            return this;
        }

        /**
         * Adds a keyword attribute and value.
         */
        public Entity put(String attr, Object value) {
            values.put(Datalevin.keyword(attr), value);
            return this;
        }

        /**
         * Adds a raw key and value.
         */
        public Entity putRaw(String key, Object value) {
            values.put(key, value);
            return this;
        }

        /**
         * Returns a mutable entity map suitable for transaction assembly.
         */
        public Map<String, Object> build() {
            return new LinkedHashMap<>(values);
        }
    }
}
