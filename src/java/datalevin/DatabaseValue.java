package datalevin;

import java.util.List;
import java.util.Objects;

/**
 * Opaque Java handle for a Datalevin database value.
 *
 * <p>This is primarily useful for bridge runtimes that cannot materialize
 * Datalevin's runtime-generated Clojure database class directly.
 */
public final class DatabaseValue {
    private final Object handle;

    DatabaseValue(Object handle) {
        this.handle = Objects.requireNonNull(handle, "handle");
    }

    Object handle() {
        return handle;
    }

    /**
     * Resolves an entity id or lookup ref against this database value.
     */
    public Object entid(Object eid) {
        return DatalevinInterop.databaseEntid(this, eid);
    }

    /**
     * Returns a Java handle for a lazy entity id or lookup ref.
     */
    public LazyEntity entity(Object eid) {
        return DatalevinInterop.databaseEntity(this, eid);
    }

    /**
     * Returns a touched entity map for the given entity id or lookup ref.
     */
    public Object entityMap(Object eid) {
        return DatalevinInterop.databaseEntityMap(this, eid);
    }

    /**
     * Pulls one entity from this database value.
     */
    public Object pull(Object selector, Object eid) {
        return DatalevinInterop.databasePull(this, selector, eid);
    }

    /**
     * Pulls many entities from this database value.
     */
    public Object pullMany(Object selector, List<?> eids) {
        return DatalevinInterop.databasePullMany(this, selector, eids);
    }

    @Override
    public String toString() {
        return "<Database>";
    }
}
