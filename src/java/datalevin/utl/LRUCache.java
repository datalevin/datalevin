package datalevin.utl;

import clojure.lang.IRecord;
import clojure.lang.PersistentVector;
import clojure.lang.Sequential;
import clojure.lang.Util;
import org.eclipse.collections.api.block.HashingStrategy;
import org.eclipse.collections.impl.map.strategy.mutable.UnifiedMapWithHashingStrategy;
import org.eclipse.collections.impl.set.strategy.mutable.UnifiedSetWithHashingStrategy;

import java.util.*;
import java.util.function.Function;

public class LRUCache {
    // Dependency tokens use the same numeric equivalence as invalidation.
    // Actual result keys retain Java equality and their storage identity.
    private static final HashingStrategy<Object> DEPENDENCY_STRATEGY = new HashingStrategy<>() {
        private static final long serialVersionUID = 1L;
        @Override public int computeHashCode(Object token) { return dependencyHash(token); }
        @Override public boolean equals(Object left, Object right) { return Util.equiv(left, right); }
    };

    private static int dependencyHash(Object value) {
        if (value instanceof Float || value instanceof Double) {
            double number = ((Number) value).doubleValue();
            return Double.hashCode(number == 0.0 ? 0.0 : number);
        }
        // Tokens and exact tuple values are lists. Hash their numeric leaves
        // recursively: Clojure's Float and Double hashes can differ despite =.
        if (value instanceof List<?> parts) {
            int hash = 1;
            for (Object part : parts) hash = 31 * hash + dependencyHash(part);
            return hash;
        }
        return Util.hasheq(value);
    }

    private static int touchedValueHash(Object value) {
        if (value instanceof Float || value instanceof Double) return dependencyHash(value);
        // Most tuple values are vectors. Avoid sequence allocation while
        // hashing their numeric leaves with consistent Float/Double hashes.
        if (value instanceof PersistentVector parts && value.getClass() == PersistentVector.class) {
            int hash = 1;
            for (int i = 0; i < parts.count(); i++) hash = 31 * hash + touchedValueHash(parts.nth(i));
            return hash;
        }
        if (!structuralCollection(value)) return Util.hasheq(value);
        if (value instanceof Iterable<?> parts
                && (value instanceof List<?> || value instanceof Sequential)) {
            int hash = 1;
            for (Object part : parts) hash = 31 * hash + touchedValueHash(part);
            return hash;
        }
        if (value instanceof Map<?, ?> parts && !(value instanceof IRecord)) {
            int hash = 0;
            for (Map.Entry<?, ?> part : parts.entrySet()) {
                hash += touchedValueHash(part.getKey()) ^ touchedValueHash(part.getValue());
            }
            return hash;
        }
        if (value instanceof Set<?> parts) {
            int hash = 0;
            for (Object part : parts) hash += touchedValueHash(part);
            return hash;
        }
        return Util.hasheq(value);
    }

    private static boolean structuralCollection(Object value) {
        if (!(value instanceof List<?> || value instanceof Map<?, ?> || value instanceof Set<?>
                || value instanceof Sequential && value instanceof Iterable<?>)
                || value instanceof IRecord) return false;
        // Opaque user collections own their equality and hash implementations.
        String name = value.getClass().getName();
        return name.startsWith("clojure.lang.") || name.startsWith("java.util.");
    }

    private static <V> Map<Object, V> dependencyMap() {
        return new UnifiedMapWithHashingStrategy<>(DEPENDENCY_STRATEGY);
    }

    private static final class TouchedValueStrategy implements HashingStrategy<Object> {
        private static final long serialVersionUID = 1L;
        @Override public int computeHashCode(Object value) { return touchedValueHash(value); }
        @Override public boolean equals(Object stored, Object lookup) {
            // Clojure membership compares the lookup first. Date/Timestamp
            // equality makes that ordering observable despite equal hashes.
            return Util.equiv(lookup, stored);
        }
    }

    private static final HashingStrategy<Object> TOUCHED_VALUE_STRATEGY = new TouchedValueStrategy();

    /** Transaction-local membership with the same equality as dependency buckets. */
    public static Set<Object> dependencySet() {
        return new UnifiedSetWithHashingStrategy<>(TOUCHED_VALUE_STRATEGY);
    }

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
        dependencyKeys = dependencies == null ? null : dependencyMap();
        dependencyGroups = dependencies == null ? null : dependencyMap();
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
        dependencyGroups.computeIfAbsent(parts.get(0), ignored -> dependencyMap())
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
