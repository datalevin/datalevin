package datalevin.utl;

import clojure.lang.IRecord;
import clojure.lang.Murmur3;
import clojure.lang.PersistentVector;
import clojure.lang.Util;
import org.eclipse.collections.api.block.HashingStrategy;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.impl.map.strategy.mutable.UnifiedMapWithHashingStrategy;
import org.eclipse.collections.impl.set.strategy.mutable.UnifiedSetWithHashingStrategy;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import java.util.Arrays;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Query key semantics shared by native and source execution. Byte arrays are
 * compared by content, including inside ordinary collections. Other leaves
 * retain Java key equality (notably decimal scale and custom nominal values).
 * Keys borrow their values: callers must keep mutable leaves stable while an
 * index owns them. No canonical copies or alternate public values are built.
 */
public final class QueryKey {
    private QueryKey() {}

    /** Optional per-invocation accounting for temporary unordered-key indexes. */
    public interface Budget {
        void check();
        Workspace reserve(long bytes);
    }

    public interface Workspace extends AutoCloseable {
        @Override void close();
    }

    private static final ThreadLocal<Budget> BUDGET = new ThreadLocal<>();
    // Native queries normally have no accounting context. Avoid a ThreadLocal
    // lookup on every hash collision when no thread has installed a budget.
    // Count owners, not nesting: replacing/restoring a budget keeps ownership.
    private static final AtomicInteger BUDGET_OWNERS = new AtomicInteger();

    public static Budget setBudget(Budget budget) {
        Budget previous = BUDGET.get();
        if (budget == null) {
            BUDGET.remove();
            if (previous != null) BUDGET_OWNERS.decrementAndGet();
        } else {
            if (previous == null) BUDGET_OWNERS.incrementAndGet();
            BUDGET.set(budget);
        }
        return previous;
    }

    private static void check() {
        if (BUDGET_OWNERS.get() == 0) return;
        Budget budget = BUDGET.get();
        if (budget != null) budget.check();
    }

    // A named class keeps the serialized identities of the existing anonymous
    // hashing strategies stable (QueryKey$1 and QueryKey$2).
    private static final class StructuralCollection extends ClassValue<Boolean> {
        @Override protected Boolean computeValue(Class<?> type) {
            return !IRecord.class.isAssignableFrom(type)
                    && type.getName().startsWith("clojure.lang.");
        }
    }

    private static final ClassValue<Boolean> STRUCTURAL_COLLECTION = new StructuralCollection();

    private static boolean collection(Object value) {
        // User collection classes can define nominal equality, just like
        // records. Only the standard portable Clojure containers are structural.
        if (value instanceof List<?> || value instanceof Map<?, ?> || value instanceof Set<?>) {
            Class<?> type = value.getClass();
            return type == PersistentVector.class || STRUCTURAL_COLLECTION.get(type);
        }
        return false;
    }

    public static boolean containsBytes(Object value) {
        if (value instanceof byte[]) return true;
        if (!collection(value)) return false;
        check();
        if (value instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (containsBytes(entry.getKey()) || containsBytes(entry.getValue())) return true;
            }
        } else if (value instanceof List<?> || value instanceof Set<?>) {
            for (Object child : (Iterable<?>) value) {
                if (containsBytes(child)) return true;
            }
        }
        return false;
    }

    /** Inspect engine tuples without allocating a sequence for each row. */
    public static boolean tuplesContainBytes(List<Object[]> tuples) {
        for (Object[] tuple : tuples) {
            for (Object value : tuple) {
                if (containsBytes(value)) return true;
            }
        }
        return false;
    }

    public static int hash(Object value) {
        if (!(value instanceof byte[]) && !collection(value)) return Objects.hashCode(value);
        check();
        if (value instanceof byte[] bytes) return Arrays.hashCode(bytes);
        if (value instanceof IRecord) return value.hashCode();
        if (value instanceof List<?> list) {
            int hash = 1;
            for (Object child : list) hash = 31 * hash + hash(child);
            return hash;
        }
        if (value instanceof Map<?, ?> map) {
            int hash = 0;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                hash += hash(entry.getKey()) ^ hash(entry.getValue());
            }
            return hash;
        }
        if (value instanceof Set<?> set) {
            int hash = 0;
            for (Object child : set) hash += hash(child);
            return hash;
        }
        return Objects.hashCode(value);
    }

    public static boolean equals(Object left, Object right) {
        return equal(left, right, false);
    }

    /** Predicate/constant equality preserves Clojure numeric equivalence. */
    public static boolean equiv(Object left, Object right) {
        return equal(left, right, true);
    }

    private static boolean equal(Object left, Object right, boolean equiv) {
        if (left == right) return true;
        if (left == null || right == null) return false;
        // Entity ids dominate native tuple keys. Keep their comparison
        // monomorphic even when the same index also sees other key types.
        if (left instanceof Long a && right instanceof Long b) {
            return a.longValue() == b.longValue();
        }
        if (!(left instanceof byte[]) && !(right instanceof byte[])
                && (!collection(left) || !collection(right))) {
            return equiv ? Util.equiv(left, right) : Objects.equals(left, right);
        }
        check();
        if (left instanceof byte[] a) {
            return right instanceof byte[] b && Arrays.equals(a, b);
        }
        if (right instanceof byte[]) return false;
        // Ordered collections can compare each child once. Pre-scanning both
        // lists for bytes doubles traversal and magnifies hash collisions in
        // native result sets. Opaque children still take their own equality.
        if (left instanceof List<?> a && right instanceof List<?> b) {
            if (a.size() != b.size()) return false;
            Iterator<?> ai = a.iterator(), bi = b.iterator();
            while (ai.hasNext()) {
                if (!equal(ai.next(), bi.next(), equiv)) return false;
            }
            return true;
        }
        // Preserve the existing semantics of opaque values and collections
        // without bytes, including records with user-defined equality.
        if (left instanceof IRecord || right instanceof IRecord
                || (!containsBytes(left) && !containsBytes(right))) {
            return equiv ? Util.equiv(left, right) : Objects.equals(left, right);
        }
        if (left instanceof Map<?, ?> a && right instanceof Map<?, ?> b) {
            if (a.size() != b.size()) return false;
            return sameElements(a.entrySet(), b.entrySet(), true, equiv);
        }
        if (left instanceof Set<?> a && right instanceof Set<?> b) {
            return a.size() == b.size() && sameElements(a, b, false, equiv);
        }
        return false;
    }

    private static int equivHash(Object value) {
        if (!(value instanceof byte[]) && !collection(value)) return Util.hasheq(value);
        check();
        if (value instanceof byte[] bytes) return Arrays.hashCode(bytes);
        if (value instanceof IRecord) return Util.hasheq(value);
        if (value instanceof List<?> list) {
            int hash = 1;
            for (Object child : list) hash = 31 * hash + equivHash(child);
            return Murmur3.mixCollHash(hash, list.size());
        }
        if (value instanceof Map<?, ?> map) {
            int hash = 0;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                hash += Murmur3.mixCollHash(31 * (31 + equivHash(entry.getKey()))
                        + equivHash(entry.getValue()), 2);
            }
            return Murmur3.mixCollHash(hash, map.size());
        }
        if (value instanceof Set<?> set) {
            int hash = 0;
            for (Object child : set) hash += equivHash(child);
            return Murmur3.mixCollHash(hash, set.size());
        }
        return Util.hasheq(value);
    }

    private static final class Element {
        private final Object key;
        private final Object value;
        private final boolean entry;
        private final boolean equiv;
        private final int hash;

        Element(Object value, boolean entry, boolean equiv) {
            this.entry = entry;
            this.equiv = equiv;
            this.key = entry ? ((Map.Entry<?, ?>) value).getKey() : value;
            this.value = entry ? ((Map.Entry<?, ?>) value).getValue() : null;
            this.hash = (equiv ? equivHash(key) : QueryKey.hash(key))
                    ^ (entry ? (equiv ? equivHash(this.value) : QueryKey.hash(this.value)) : 0);
        }

        @Override public int hashCode() { return hash; }
        @Override public boolean equals(Object other) {
            return other instanceof Element that && entry == that.entry && equiv == that.equiv
                    && equal(key, that.key, equiv) && (!entry || equal(value, that.value, equiv));
        }
    }

    private static boolean sameElements(Set<?> left, Set<?> right, boolean entries, boolean equiv) {
        Budget budget = BUDGET.get();
        // Include wrappers, counts, table growth and probes. Values are borrowed.
        Workspace workspace = budget == null ? null : budget.reserve(512L + 512L * left.size());
        java.util.HashMap<Element, Integer> counts = null;
        try {
            counts = new java.util.HashMap<>();
            for (Object value : left) {
                check();
                Element key = new Element(value, entries, equiv);
                counts.merge(key, 1, Integer::sum);
            }
            for (Object value : right) {
                check();
                Element key = new Element(value, entries, equiv);
                Integer count = counts.get(key);
                if (count == null) return false;
                if (count == 1) counts.remove(key); else counts.put(key, count - 1);
            }
            return counts.isEmpty();
        } finally {
            if (counts != null) counts.clear();
            if (workspace != null) workspace.close();
        }
    }

    public static boolean arrayEquals(Object[] left, Object[] right) {
        if (left.length != right.length) return false;
        for (int i = 0; i < left.length; i++) {
            if (!equals(left[i], right[i])) return false;
        }
        return true;
    }

    private static final HashingStrategy<Object> STRATEGY = new HashingStrategy<>() {
        private static final long serialVersionUID = 1L;
        @Override public int computeHashCode(Object value) { return hash(value); }
        @Override public boolean equals(Object left, Object right) { return QueryKey.equals(left, right); }
    };

    private static final HashingStrategy<Object> EQUIV_STRATEGY = new HashingStrategy<>() {
        private static final long serialVersionUID = 1L;
        @Override public int computeHashCode(Object value) { return equivHash(value); }
        @Override public boolean equals(Object left, Object right) { return equiv(left, right); }
    };

    public static final class EquivSet extends UnifiedSetWithHashingStrategy<Object> {
        private static final long serialVersionUID = 1L;
        public EquivSet() { super(EQUIV_STRATEGY); }
    }

    /** Expose Java Map iteration to Clojure, not Eclipse's Iterable of values. */
    public static final class KeyMap extends AbstractMap<Object, Object>
            implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        private final UnifiedMapWithHashingStrategy<Object, Object> entries;

        public KeyMap() { entries = new UnifiedMapWithHashingStrategy<>(STRATEGY); }
        public KeyMap(int capacity) {
            entries = new UnifiedMapWithHashingStrategy<>(STRATEGY, capacity);
        }

        @Override public int size() { return entries.size(); }
        @Override public boolean containsKey(Object key) { return entries.containsKey(key); }
        @Override public Object get(Object key) { return entries.get(key); }
        @Override public Object put(Object key, Object value) { return entries.put(key, value); }
        @Override public Object remove(Object key) { return entries.remove(key); }
        @Override public void clear() { entries.clear(); }
        @Override public Set<Entry<Object, Object>> entrySet() { return entries.entrySet(); }
        @Override public Set<Object> keySet() { return entries.keySet(); }
        @Override public Collection<Object> values() { return entries.values(); }
    }

    /**
     * Invocation-local distinct-key tracking with a primitive path for long
     * entity ids. Other values promote once to the shared content-key set.
     */
    public static final class KeyAccumulator {
        private LongHashSet longs = new LongHashSet();
        private KeySet general;

        private KeySet general() {
            if (general == null) {
                general = new KeySet(longs.size());
                LongIterator keys = longs.longIterator();
                while (keys.hasNext()) general.add(keys.next());
                longs = null;
            }
            return general;
        }

        public int size() { return general == null ? longs.size() : general.size(); }

        public boolean add(Object key) {
            if (general == null && key instanceof Long number) return longs.add(number.longValue());
            return general().add(key);
        }

        public boolean contains(Object key) {
            if (general == null && key instanceof Long number) return longs.contains(number.longValue());
            return general().contains(key);
        }
    }

    public static final class KeySet extends UnifiedSetWithHashingStrategy<Object> {
        private static final long serialVersionUID = 1L;
        public KeySet() { super(STRATEGY); }
        public KeySet(int capacity) { super(STRATEGY, capacity); }
    }
}
