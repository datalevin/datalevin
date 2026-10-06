/* Copyright (c) Huahai Yang. All rights reserved.
 * Distributed under the Eclipse Public License 2.0. */
package datalevin.utl;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Iterator;
import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;
import java.util.RandomAccess;

/** Ordered, owned row lists retained without copying their row references.
 * Regions must not change after attachment. The owner finishes building this
 * view before sharing it with parallel encoders.
 */
public final class RowRegions extends AbstractList<Object> implements RandomAccess {
    private final ArrayList<List<?>> regions = new ArrayList<>();
    private int[] ends = new int[4];
    private int size;

    public void append(List<?> rows) {
        if (rows.isEmpty()) return;
        if (rows == this) throw new IllegalArgumentException("Self region");
        if (rows instanceof RowRegions) {
            for (List<?> region : ((RowRegions) rows).regions) append(region);
            return;
        }
        int nextSize = Math.addExact(size, rows.size());
        int index = regions.size();
        if (index == ends.length) ends = Arrays.copyOf(ends, index * 2);
        regions.add(rows);
        ends[index] = nextSize;
        size = nextSize;
        modCount++;
    }

    @Override public int size() { return size; }

    @Override public Iterator<Object> iterator() {
        return new Iterator<>() {
            private final int expectedModCount = modCount;
            private int region;
            private int offset;
            private List<?> rows = regions.isEmpty() ? null : regions.get(0);

            @Override public boolean hasNext() { return rows != null; }

            @Override public Object next() {
                if (expectedModCount != modCount) throw new ConcurrentModificationException();
                if (!hasNext()) throw new NoSuchElementException();
                Object row = rows.get(offset++);
                if (offset == rows.size()) {
                    region++;
                    offset = 0;
                    rows = region < regions.size() ? regions.get(region) : null;
                }
                return row;
            }
        };
    }

    @Override public Object get(int index) {
        if (index < 0 || index >= size) throw new IndexOutOfBoundsException(index);
        int region = Arrays.binarySearch(ends, 0, regions.size(), index + 1);
        if (region < 0) region = -region - 1;
        int start = region == 0 ? 0 : ends[region - 1];
        return regions.get(region).get(index - start);
    }
}
