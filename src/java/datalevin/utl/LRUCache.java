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
    private final Map<Object, Map<Object, Integer>> dependencyGroups;

    public LRUCache(int capacity) {
        this(capacity, 0, null);
    }

    public LRUCache(int capacity, long target) {
        this(capacity, target, null);
    }

    /** Index immutable key dependencies as entries are inserted, so the first
     * selective invalidation does not need to classify the retained cache. */
    public LRUCache(int capacity, long target,
                    Function<Object, ? extends Iterable<?>> dependencies) {
        this.capacity = capacity;
        this.target = target;
        this.dependencies = dependencies;
        keyDependencies = dependencies == null ? null : new HashMap<>();
        dependencyKeys = dependencies == null ? null : new HashMap<>();
        dependencyGroups = dependencies == null ? null : new HashMap<>();
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
        if (dependencies == null) return;
        List<Object> tokens = keyDependencies.remove(key);
        if (tokens == null) return;
        for (Object token : tokens) {
            Set<Object> keys = dependencyKeys.get(token);
            if (keys != null) {
                keys.remove(key);
                if (keys.isEmpty()) {
                    dependencyKeys.remove(token);
                    removeDependencyGroup(token);
                }
            }
        }
    }

    // Count populated buckets under the first two parts of list dependencies.
    // Exact candidate selection still uses the original complete tokens. No
    // duplicate key memberships are needed to inspect a dependency prefix.
    private void addDependencyGroup(Object token) {
        if (!(token instanceof List<?>)) return;
        List<?> parts = (List<?>) token;
        if (parts.size() < 3) return;
        dependencyGroups.computeIfAbsent(parts.get(0), ignored -> new HashMap<>())
            .merge(parts.get(1), 1, Integer::sum);
    }

    private void removeDependencyGroup(Object token) {
        if (!(token instanceof List<?>)) return;
        List<?> parts = (List<?>) token;
        if (parts.size() < 3) return;
        Object group = parts.get(0), member = parts.get(1);
        Map<Object, Integer> members = dependencyGroups.get(group);
        int count = members.get(member);
        if (count == 1) {
            members.remove(member);
            if (members.isEmpty()) dependencyGroups.remove(group);
        } else {
            members.put(member, count - 1);
        }
    }

    private void addDependencies(Object key) {
        // Classify before publishing either the value or its index entries.
        List<Object> tokens = new ArrayList<>();
        for (Object token : dependencies.apply(key)) tokens.add(token);
        keyDependencies.put(key, tokens);
        for (Object token : tokens) {
            Set<Object> keys = dependencyKeys.computeIfAbsent(token, ignored -> new HashSet<>());
            if (keys.isEmpty()) addDependencyGroup(token);
            keys.add(key);
        }
    }

    private void putEntry(Object key, Object value) {
        if (dependencies != null && capacity > 0 && !map.containsKey(key)) {
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

    /** Inspect a dependency bucket without copying keys or changing LRU order.
     * Unindexed caches conservatively depend on every token while nonempty. */
    public synchronized boolean hasDependency(Object token) {
        if (dependencies == null) return !map.isEmpty();
        return dependencyKeys.containsKey(token);
    }

    /** Inspect the populated three-part (or longer) dependency prefixes. */
    public synchronized boolean hasDependencyGroup(Object group) {
        if (dependencies == null) return !map.isEmpty();
        return dependencyGroups.containsKey(group);
    }

    /** Inspect one member of a three-part (or longer) dependency prefix. */
    public synchronized boolean hasDependencyGroup(Object group, Object member) {
        if (dependencies == null) return !map.isEmpty();
        Map<Object, Integer> members = dependencyGroups.get(group);
        return members != null && members.containsKey(member);
    }

    /** Snapshot keys in the requested dependency buckets; does not affect LRU order. */
    public synchronized Set<Object> candidateKeys(Iterable<?> tokens) {
        if (dependencies == null) return keys();
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
            dependencyGroups.clear();
        }
    }
}
