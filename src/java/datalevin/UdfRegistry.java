package datalevin;

import java.util.Map;
import java.util.Objects;

/**
 * Wrapper around a Datalevin runtime UDF registry.
 */
public final class UdfRegistry {

    private final Object handle;

    UdfRegistry(Object handle) {
        this.handle = Objects.requireNonNull(handle, "handle");
    }

    /**
     * Returns the raw Clojure registry handle.
     */
    public Object rawHandle() {
        return handle;
    }

    /**
     * Registers a UDF implementation.
     */
    public UdfRegistry register(UdfDescriptor descriptor, UdfFunction fn) {
        Objects.requireNonNull(descriptor, "descriptor");
        DatalevinInterop.registerUdf(handle, descriptor.build(), fn);
        return this;
    }

    /**
     * Registers a UDF implementation with a raw descriptor map.
     */
    public UdfRegistry register(Map<?, ?> descriptor, UdfFunction fn) {
        DatalevinInterop.registerUdf(handle, descriptor, fn);
        return this;
    }

    /**
     * Registers a query function implementation.
     */
    public UdfRegistry queryFn(String id, UdfFunction fn) {
        return register(UdfDescriptor.queryFn(id), fn);
    }

    /**
     * Registers a query predicate implementation.
     */
    public UdfRegistry predicate(String id, UdfFunction fn) {
        return register(UdfDescriptor.predicate(id), fn);
    }

    /**
     * Registers a transaction function implementation.
     */
    public UdfRegistry txFn(String id, UdfFunction fn) {
        return register(UdfDescriptor.txFn(id), fn);
    }

    /**
     * Registers a full-text analyzer implementation.
     */
    public UdfRegistry analyzer(String id, UdfFunction fn) {
        return register(UdfDescriptor.analyzer(id), fn);
    }

    /**
     * Registers a full-text query analyzer implementation.
     */
    public UdfRegistry queryAnalyzer(String id, UdfFunction fn) {
        return register(UdfDescriptor.queryAnalyzer(id), fn);
    }

    /**
     * Unregisters a UDF implementation.
     */
    public UdfRegistry unregister(UdfDescriptor descriptor) {
        Objects.requireNonNull(descriptor, "descriptor");
        DatalevinInterop.unregisterUdf(handle, descriptor.build());
        return this;
    }

    /**
     * Unregisters a UDF implementation with a raw descriptor map.
     */
    public UdfRegistry unregister(Map<?, ?> descriptor) {
        DatalevinInterop.unregisterUdf(handle, descriptor);
        return this;
    }

    /**
     * Returns whether a descriptor is registered.
     */
    public boolean registered(UdfDescriptor descriptor) {
        Objects.requireNonNull(descriptor, "descriptor");
        return DatalevinInterop.registeredUdf(handle, descriptor.build());
    }

    /**
     * Returns whether a raw descriptor map is registered.
     */
    public boolean registered(Map<?, ?> descriptor) {
        return DatalevinInterop.registeredUdf(handle, descriptor);
    }
}
