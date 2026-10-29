/*
 * Copyright (c) Huahai Yang. All rights reserved.
 * The use and distribution terms for this software are covered by the
 * Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0).
 */
package datalevin.utl;

import clojure.lang.RT;

import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.RandomAccess;

/** Stable signed-long ordering for native entity and value scan tuples. */
public final class TupleSort {
    private static final int COUNTING_MIN_SIZE = 4096;
    private static final int MAX_BUCKETS = 1 << 24;

    private TupleSort() {}

    /**
     * Sort in place, preserving tuple identity and the input order of equal keys.
     * Dense keys use a bounded counting sort; sparse keys retain comparison sort.
     */
    public static void sortByLong(List<Object[]> tuples, int column) {
        int size = tuples.size();
        Comparator<Object[]> comparator = (a, b) -> Long.compare(
                RT.longCast(a[column]), RT.longCast(b[column]));
        if (size < COUNTING_MIN_SIZE || !(tuples instanceof RandomAccess)) {
            tuples.sort(comparator);
            return;
        }

        long previous = RT.longCast(tuples.get(0)[column]);
        long minimum = previous;
        long maximum = previous;
        boolean ordered = true;
        for (int i = 1; i < size; i++) {
            long key = RT.longCast(tuples.get(i)[column]);
            ordered &= previous <= key;
            minimum = Math.min(minimum, key);
            maximum = Math.max(maximum, key);
            previous = key;
        }
        if (ordered) return;

        // A difference larger than Long.MAX_VALUE wraps negative. Check this
        // before narrowing or allocating, including for MIN_VALUE/MAX_VALUE.
        long span = maximum - minimum;
        if (span < 0 || span >= Math.min(2L * size, MAX_BUCKETS)) {
            tuples.sort(comparator);
            return;
        }
        Runtime runtime = Runtime.getRuntime();
        long headroom = runtime.maxMemory() - runtime.totalMemory() + runtime.freeMemory();
        long workspace = 4L * (span + 1) + 8L * size;
        if (headroom < 2 * workspace) {
            tuples.sort(comparator);
            return;
        }

        int[] positions = new int[(int) span + 1];
        for (int i = 0; i < size; i++) {
            positions[(int) (RT.longCast(tuples.get(i)[column]) - minimum)]++;
        }
        int offset = 0;
        for (int i = 0; i < positions.length; i++) {
            int count = positions[i];
            positions[i] = offset;
            offset += count;
        }
        Object[][] sorted = new Object[size][];
        for (int i = 0; i < size; i++) {
            Object[] tuple = tuples.get(i);
            int bucket = (int) (RT.longCast(tuple[column]) - minimum);
            sorted[positions[bucket]++] = tuple;
        }
        ListIterator<Object[]> iterator = tuples.listIterator();
        for (Object[] tuple : sorted) {
            iterator.next();
            iterator.set(tuple);
        }
    }
}
