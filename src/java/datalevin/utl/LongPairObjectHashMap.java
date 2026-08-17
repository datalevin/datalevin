package datalevin.utl;

import org.eclipse.collections.impl.list.mutable.FastList;

import java.util.List;

/**
 * A compact open-addressed map for the two-long keys used by composite hash
 * joins. Values are never null, so the value table also marks occupied slots.
 */
public final class LongPairObjectHashMap {

    private static final int MAX_CAPACITY = 1 << 30;
    private static final float LOAD_FACTOR = 0.7f;

    private long[] firstKeys;
    private long[] secondKeys;
    private Object[] values;
    private int mask;
    private int threshold;
    private int size;

    public LongPairObjectHashMap(int expectedSize) {
        allocate(capacityFor(expectedSize));
    }

    public Object get(long first, long second) {
        int slot = hash(first, second) & mask;
        Object value;
        while ((value = values[slot]) != null) {
            if (firstKeys[slot] == first && secondKeys[slot] == second) {
                return value;
            }
            slot = (slot + 1) & mask;
        }
        return null;
    }

    /** Adds a relation tuple to the key's singleton-or-list bucket. */
    @SuppressWarnings("unchecked")
    public void add(long first, long second, Object[] tuple) {
        int slot = hash(first, second) & mask;
        Object value;
        while ((value = values[slot]) != null) {
            if (firstKeys[slot] == first && secondKeys[slot] == second) {
                if (value instanceof Object[]) {
                    FastList<Object[]> bucket = new FastList<>(2);
                    bucket.add((Object[]) value);
                    bucket.add(tuple);
                    values[slot] = bucket;
                } else {
                    ((List<Object[]>) value).add(tuple);
                }
                return;
            }
            slot = (slot + 1) & mask;
        }

        firstKeys[slot] = first;
        secondKeys[slot] = second;
        values[slot] = tuple;
        if (++size > threshold) {
            grow();
        }
    }

    private void grow() {
        int oldCapacity = values.length;
        if (oldCapacity == MAX_CAPACITY) {
            threshold = Integer.MAX_VALUE;
            return;
        }

        long[] oldFirstKeys = firstKeys;
        long[] oldSecondKeys = secondKeys;
        Object[] oldValues = values;
        allocate(oldCapacity << 1);
        size = 0;

        for (int i = 0; i < oldCapacity; i++) {
            Object value = oldValues[i];
            if (value != null) {
                insert(oldFirstKeys[i], oldSecondKeys[i], value);
            }
        }
    }

    private void insert(long first, long second, Object value) {
        int slot = hash(first, second) & mask;
        while (values[slot] != null) {
            slot = (slot + 1) & mask;
        }
        firstKeys[slot] = first;
        secondKeys[slot] = second;
        values[slot] = value;
        size++;
    }

    private void allocate(int capacity) {
        firstKeys = new long[capacity];
        secondKeys = new long[capacity];
        values = new Object[capacity];
        mask = capacity - 1;
        threshold = Math.min(capacity - 1, (int) (capacity * LOAD_FACTOR));
    }

    private static int capacityFor(int expectedSize) {
        long required = Math.max(2L,
                (long) Math.ceil(Math.max(0, expectedSize) / LOAD_FACTOR));
        if (required >= MAX_CAPACITY) {
            return MAX_CAPACITY;
        }
        int capacity = 2;
        while (capacity < required) {
            capacity <<= 1;
        }
        return capacity;
    }

    private static int hash(long first, long second) {
        int h = 31 * Long.hashCode(first) + Long.hashCode(second);
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        return h ^ (h >>> 13);
    }
}
