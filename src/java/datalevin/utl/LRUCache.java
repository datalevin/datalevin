package datalevin.utl;

import java.util.*;
import java.util.function.Function;

public class LRUCache {
    final int capacity;
    Map<Object, Object> map;

    long target;

    // Readers capture tokens without the cache monitor. Generation changes
    // and conditional publication remain serialized by that monitor.
    volatile long generation;

    boolean disabled;

    private final Function<Object, ? extends Iterable<?>> dependencies;
    private final Map<Object, List<Object>> keyDependencies;
    private final Map<Object, Set<Object>> dependencyKeys;
    private boolean indexReady;

    public LRUCache(int capacity) {
        this(capacity, 0, null);
    }

    public LRUCache(int capacity, long target) {
        this(capacity, target, null);
    }

    /** Optionally index immutable key dependencies for selective invalidation. */
    public LRUCache(int capacity, long target,
                    Function<Object, ? extends Iterable<?>> dependencies) {
        this.capacity = capacity;
        this.target = target;
        this.dependencies = dependencies;
        keyDependencies = dependencies == null ? null : new HashMap<>();
        dependencyKeys = dependencies == null ? null : new HashMap<>();
        disabled = false;
        map = Collections.synchronizedMap(new LinkedHashMap<Object, Object>(capacity,
                                                                            0.75f,
                                                                            true) {
                protected boolean	removeEldestEntry(Map.Entry<Object, Object> oldest) {
                    if (size() > capacity) {
                        removeDependencies(oldest.getKey());
                        return true;
                    }
                    return false;
                }
            });
    }

    /** Immutable entry limit, available without acquiring the cache monitor. */
    public int capacity() {
        return capacity;
    }

    private void removeDependencies(Object key) {
        if (!indexReady) return;
        List<Object> tokens = keyDependencies.remove(key);
        if (tokens == null) return;
        for (Object token : tokens) {
            Set<Object> keys = dependencyKeys.get(token);
            if (keys != null) {
                keys.remove(key);
                if (keys.isEmpty()) dependencyKeys.remove(token);
            }
        }
    }

    private void addDependencies(Object key) {
        // Classify before publishing either the value or its index entries.
        List<Object> tokens = new ArrayList<>();
        for (Object token : dependencies.apply(key)) tokens.add(token);
        keyDependencies.put(key, tokens);
        for (Object token : tokens) {
            dependencyKeys.computeIfAbsent(token, ignored -> new HashSet<>())
                .add(key);
        }
    }

    private void putEntry(Object key, Object value) {
        if (indexReady && !map.containsKey(key)) {
            addDependencies(key);
        }
        map.put(key, value);
    }

    public synchronized boolean isDisabled() {
        return disabled;
    }

    public synchronized void disable() {
        disabled = true;
    }

    public synchronized void enable() {
        // Invalidation can precede the native commit. Readers starting while
        // disabled may capture that generation but still read the old snapshot.
        // Reject their publications after caching resumes, retaining entries.
        if (disabled) generation++;
        disabled = false;
    }

    public synchronized long target() {
        return target;
    }

    public synchronized void setTarget(long target) {
        this.target = target;
    }

    /** A publication token, not a snapshot of the cache's mutable state. */
    public long generation() {
        return generation;
    }

    public synchronized void beginInvalidation(long target) {
        this.target = target;
        generation++;
    }

    public synchronized Object get(Object key) {
        if (disabled == true) return null;
        return map.get(key);
    }

    public synchronized void put(Object key, Object value) {
        if (disabled == true) return;
        putEntry(key, value);
    }

    public synchronized boolean putIfGeneration(Object key, Object value,
                                                long expectedGeneration) {
        if (disabled == true || generation != expectedGeneration) return false;
        putEntry(key, value);
        return true;
    }

    public synchronized Object remove(Object key) {
        removeDependencies(key);
        return map.remove(key);
    }

    /** Snapshot keys in the requested dependency buckets; does not affect LRU order. */
    public synchronized Set<Object> candidateKeys(Iterable<?> tokens) {
        if (dependencies == null) return keys();
        // Read-only caches need no dependency bookkeeping. Build once, when
        // the first invalidation needs candidates, under the cache monitor.
        if (!indexReady) {
            keyDependencies.clear();
            dependencyKeys.clear();
            for (Object key : map.keySet()) addDependencies(key);
            indexReady = true;
        }
        Set<Object> candidates = new HashSet<>();
        for (Object token : tokens) {
            Set<Object> keys = dependencyKeys.get(token);
            if (keys != null) candidates.addAll(keys);
        }
        return candidates;
    }

    public synchronized Set<Object> keys() {
        return new HashSet<Object>(map.keySet());
    }

    public synchronized boolean isEmpty() {
        return map.isEmpty();
    }

    public synchronized List<Object> orderedKeys() {
        return new ArrayList<Object>(map.keySet());
    }

    public synchronized void clear() {
        map.clear();
        if (dependencies != null) {
            keyDependencies.clear();
            dependencyKeys.clear();
            indexReady = false;
        }
    }
}
