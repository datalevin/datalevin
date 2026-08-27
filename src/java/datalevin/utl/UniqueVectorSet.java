package datalevin.utl;

import clojure.lang.AFn;
import clojure.lang.IHashEq;
import clojure.lang.IObj;
import clojure.lang.IPersistentCollection;
import clojure.lang.IPersistentMap;
import clojure.lang.IPersistentSet;
import clojure.lang.IPersistentVector;
import clojure.lang.ISeq;
import clojure.lang.Murmur3;
import clojure.lang.PersistentHashSet;
import clojure.lang.PersistentVector;
import clojure.lang.RT;
import clojure.lang.Util;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * An immutable, eagerly indexed set built from tuples whose producer has
 * already proved distinctness. Construction therefore needs no duplicate
 * comparisons, while lookup retains ordinary Clojure set semantics.
 */
public final class UniqueVectorSet extends AFn
        implements IPersistentSet, IObj, IHashEq, Set<Object>, Serializable {

    private static final long serialVersionUID = 1L;
    private static final double LOAD_FACTOR = 0.65;
    private static final int MAX_TABLE_SIZE = 1 << 30;

    private final Object[] values;
    private final int[] table;
    private final int mask;
    private final IPersistentMap meta;
    private transient int hash;
    private transient int hasheq;

    private UniqueVectorSet(Object[] values, int[] table, IPersistentMap meta) {
        this.values = values;
        this.table = table;
        this.mask = table.length - 1;
        this.meta = meta;
    }

    public static boolean supportsSize(int size) {
        if (size < 0) {
            return false;
        }
        long required = Math.max(2L,
                (long) Math.ceil((double) Math.max(1, size) / LOAD_FACTOR));
        return required <= MAX_TABLE_SIZE;
    }

    public static long estimatedIndexBytes(int size) {
        return (long) requiredTableSize(size) * Integer.BYTES
                + (long) size * Long.BYTES;
    }

    /**
     * Transfers ownership of pairwise-distinct Object[] tuples into an eager
     * immutable set. Neither the list entries nor their arrays may be mutated
     * after this call.
     */
    public static UniqueVectorSet fromUniqueTuples(List<?> tuples) {
        int size = tuples.size();
        int[] table = new int[requiredTableSize(size)];
        int mask = table.length - 1;
        Object[] values = new Object[size];
        for (int i = 0; i < size; i++) {
            Object[] tuple = (Object[]) tuples.get(i);
            IPersistentVector vector = tuple.length <= 32
                    ? PersistentVector.adopt(tuple)
                    : PersistentVector.create(Arrays.asList(tuple));
            values[i] = vector;
            int slot = spread(Util.hasheq(vector)) & mask;
            while (table[slot] != 0) {
                slot = (slot + 1) & mask;
            }
            table[slot] = i + 1;
        }
        return new UniqueVectorSet(values, table, null);
    }

    private static int requiredTableSize(int size) {
        long required = Math.max(2L,
                (long) Math.ceil((double) Math.max(1, size) / LOAD_FACTOR));
        if (required > MAX_TABLE_SIZE) {
            throw new IllegalArgumentException("Unique result set is too large: "
                    + size);
        }
        int capacity = 2;
        while (capacity < required) {
            capacity <<= 1;
        }
        return capacity;
    }

    private static int spread(int hash) {
        return hash ^ (hash >>> 16);
    }

    private Object find(Object value) {
        if (values.length == 0) {
            return null;
        }
        int slot = spread(Util.hasheq(value)) & mask;
        while (true) {
            int entry = table[slot];
            if (entry == 0) {
                return null;
            }
            Object stored = values[entry - 1];
            if (Util.equiv(stored, value)) {
                return stored;
            }
            slot = (slot + 1) & mask;
        }
    }

    private IPersistentSet persistentSet() {
        PersistentHashSet set = PersistentHashSet.create(Arrays.asList(values));
        return meta == null ? set : (IPersistentSet) set.withMeta(meta);
    }

    @Override
    public int count() {
        return values.length;
    }

    @Override
    public IPersistentSet cons(Object value) {
        return contains(value)
                ? this
                : (IPersistentSet) persistentSet().cons(value);
    }

    @Override
    public IPersistentCollection empty() {
        return meta == null
                ? PersistentHashSet.EMPTY
                : (IPersistentCollection) PersistentHashSet.EMPTY.withMeta(meta);
    }

    @Override
    public boolean equiv(Object other) {
        return setEquals(other);
    }

    @Override
    public ISeq seq() {
        return RT.seq(values);
    }

    @Override
    public IPersistentSet disjoin(Object value) {
        return contains(value) ? persistentSet().disjoin(value) : this;
    }

    @Override
    public boolean contains(Object value) {
        return find(value) != null;
    }

    @Override
    public Object get(Object value) {
        return find(value);
    }

    @Override
    public Object invoke(Object value) {
        return get(value);
    }

    @Override
    public IPersistentMap meta() {
        return meta;
    }

    @Override
    public IObj withMeta(IPersistentMap newMeta) {
        return new UniqueVectorSet(values, table, newMeta);
    }

    private boolean setEquals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Set<?> set) || set.size() != values.length) {
            return false;
        }
        for (Object value : set) {
            if (!contains(value)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object other) {
        return setEquals(other);
    }

    @Override
    public int hashCode() {
        int result = hash;
        if (result == 0) {
            for (Object value : values) {
                result += Util.hash(value);
            }
            hash = result;
        }
        return result;
    }

    @Override
    public int hasheq() {
        int result = hasheq;
        if (result == 0) {
            result = Murmur3.hashUnordered(this);
            hasheq = result;
        }
        return result;
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public boolean isEmpty() {
        return values.length == 0;
    }

    @Override
    public Iterator<Object> iterator() {
        return Arrays.asList(values).iterator();
    }

    @Override
    public Object[] toArray() {
        return values.clone();
    }

    @Override
    public <T> T[] toArray(T[] target) {
        return Arrays.asList(values).toArray(target);
    }

    @Override
    public boolean containsAll(Collection<?> collection) {
        for (Object value : collection) {
            if (!contains(value)) {
                return false;
            }
        }
        return true;
    }

    private static UnsupportedOperationException immutable() {
        return new UnsupportedOperationException("UniqueVectorSet is immutable");
    }

    @Override
    public boolean add(Object value) {
        throw immutable();
    }

    @Override
    public boolean remove(Object value) {
        throw immutable();
    }

    @Override
    public boolean addAll(Collection<?> collection) {
        throw immutable();
    }

    @Override
    public boolean retainAll(Collection<?> collection) {
        throw immutable();
    }

    @Override
    public boolean removeAll(Collection<?> collection) {
        throw immutable();
    }

    @Override
    public void clear() {
        throw immutable();
    }

    @Override
    public String toString() {
        return RT.printString(this);
    }
}
