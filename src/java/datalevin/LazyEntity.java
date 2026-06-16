package datalevin;

import java.util.Objects;

/**
 * Opaque Java handle for a Datalevin lazy entity.
 */
public final class LazyEntity {
    private final Object handle;

    LazyEntity(Object handle) {
        this.handle = Objects.requireNonNull(handle, "handle");
    }

    Object handle() {
        return handle;
    }

    /**
     * Returns the Datalevin entity id.
     */
    public Object id() {
        return DatalevinInterop.entityId(this);
    }

    /**
     * Returns one attribute value without touching the whole entity.
     */
    public Object get(Object attr) {
        return DatalevinInterop.entityGet(this, attr);
    }

    /**
     * Returns whether this entity has a value for the supplied attribute.
     */
    public boolean contains(Object attr) {
        return DatalevinInterop.entityContains(this, attr);
    }

    /**
     * Materializes all current entity attributes into bridge-safe data.
     */
    public Object touch() {
        return DatalevinInterop.entityTouch(this);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof LazyEntity entity) {
            return handle.equals(entity.handle);
        }
        return handle.equals(other);
    }

    @Override
    public int hashCode() {
        return handle.hashCode();
    }

    @Override
    public String toString() {
        return "<LazyEntity>";
    }
}
