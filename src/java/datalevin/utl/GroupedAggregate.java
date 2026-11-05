/*
 * Copyright (c) Huahai Yang. All rights reserved.
 * The use and distribution terms for this software are covered by the
 * Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
 * which can be found in the file LICENSE at the root of this distribution.
 * By using this software in any fashion, you are agreeing to be bound by
 * the terms of this license.
 * You must not remove this notice.
 */
package datalevin.utl;

import clojure.lang.Numbers;

import java.util.Iterator;
import java.util.Map;
import java.util.Arrays;

/**
 * One-pass sum/avg/count over an input already proven distinct in the complete
 * find/with projection. Values retain Clojure's boxed numeric-tower arithmetic;
 * group keys use the same equality as ordinary query results.
 */
public final class GroupedAggregate {
    public static final int KEY = 0;
    public static final int SUM = 1;
    public static final int AVG = 2;
    public static final int COUNT = 3;

    private final int[] columns;
    private final int[] operations;
    private final int[] groupColumns;
    private final Map<Object, Object> groups = new QueryKey.KeyMap();
    private final GroupKey lookup;

    public GroupedAggregate(int[] columns, int[] operations) {
        this.columns = columns.clone();
        this.operations = operations.clone();
        int n = 0;
        for (int operation : operations) if (operation == KEY) n++;
        groupColumns = new int[n];
        for (int i = 0, j = 0; i < operations.length; i++) {
            if (operations[i] == KEY) groupColumns[j++] = columns[i];
        }
        lookup = new GroupKey(new Object[n]);
    }

    /** Consume a borrowed engine tuple, retaining only per-group state. */
    public void add(Object[] tuple) {
        Object key;
        if (groupColumns.length == 1) {
            key = tuple[groupColumns[0]];
        } else {
            for (int i = 0; i < groupColumns.length; i++) {
                lookup.values[i] = tuple[groupColumns[i]];
            }
            lookup.hash = ArrayUtil.hashObjectArray(lookup.values);
            key = lookup;
        }
        Group group = (Group) groups.get(key);
        if (group == null) {
            group = new Group(operations.length);
            for (int i = 0; i < operations.length; i++) {
                group.values[i] = operations[i] == KEY ? tuple[columns[i]] : 0L;
            }
            groups.put(key == lookup ? new GroupKey(lookup.values.clone()) : key, group);
        }
        group.count++;
        for (int i = 0; i < operations.length; i++) {
            if (operations[i] == SUM || operations[i] == AVG) {
                Object value = tuple[columns[i]];
                if (group.doubles != null && group.doubles[i] && value instanceof Double number) {
                    // Exactly the same left-to-right DoubleOps addition, with
                    // boxing deferred until output or a numeric-tower change.
                    group.sums[i] += number.doubleValue();
                } else {
                    Object previous = group.doubles != null && group.doubles[i]
                        ? Double.valueOf(group.sums[i]) : group.values[i];
                    Object sum = Numbers.add(previous, value);
                    // Small groups keep the compact boxed state. Allocate
                    // primitive storage only when a group repeatedly adds.
                    if (sum instanceof Double number && group.count >= 32) {
                        if (group.doubles == null) {
                            group.doubles = new boolean[operations.length];
                            group.sums = new double[operations.length];
                        }
                        group.doubles[i] = true;
                        group.sums[i] = number.doubleValue();
                    } else {
                        if (group.doubles != null) group.doubles[i] = false;
                        group.values[i] = sum;
                    }
                }
            }
        }
    }

    /** Number of groups retained, without materializing the final rows. */
    public int groupCount() { return groups.size(); }

    /** Combine disjoint input partitions before finalizing averages. */
    public void merge(GroupedAggregate other) {
        if (other == this || !Arrays.equals(columns, other.columns)
                || !Arrays.equals(operations, other.operations)) {
            throw new IllegalArgumentException("Incompatible aggregate states");
        }
        for (Map.Entry<Object, Object> entry : other.groups.entrySet()) {
            Group source = (Group) entry.getValue();
            Group target = (Group) groups.get(entry.getKey());
            if (target == null) {
                target = new Group(operations.length);
                System.arraycopy(source.values, 0, target.values, 0, operations.length);
                groups.put(entry.getKey(), target);
            }
            for (int i = 0; i < operations.length; i++) {
                if (operations[i] == SUM || operations[i] == AVG) {
                    Object a = target.count == 0 ? 0L : target.value(i);
                    target.values[i] = Numbers.add(a, source.value(i));
                    if (target.doubles != null) target.doubles[i] = false;
                }
            }
            target.count = Math.addExact(target.count, source.count);
        }
    }

    /** Iterate final rows after consumption, preserving built-in average rounding. */
    public Iterable<Object[]> results() {
        return () -> new Iterator<>() {
            private final Iterator<Object> iterator = groups.values().iterator();

            @Override public boolean hasNext() { return iterator.hasNext(); }

            @Override public Object[] next() {
                Group group = (Group) iterator.next();
                Object[] row = group.values.clone();
                for (int i = 0; i < operations.length; i++) {
                    if (group.doubles != null && group.doubles[i]) {
                        row[i] = Double.valueOf(group.sums[i]);
                    }
                    if (operations[i] == COUNT) row[i] = Math.toIntExact(group.count);
                    else if (operations[i] == AVG) {
                        row[i] = ((Number) Numbers.divide(row[i],
                                (Object) Integer.valueOf(Math.toIntExact(group.count)))).doubleValue();
                    }
                }
                return row;
            }
        };
    }

    private static final class Group {
        private final Object[] values;
        private double[] sums;
        private boolean[] doubles;
        private long count;
        private Group(int width) {
            values = new Object[width];
        }
        private Object value(int i) {
            return doubles != null && doubles[i] ? Double.valueOf(sums[i]) : values[i];
        }
    }

    private static final class GroupKey {
        private final Object[] values;
        private int hash;
        private GroupKey(Object[] values) {
            this.values = values;
            hash = ArrayUtil.hashObjectArray(values);
        }
        @Override public int hashCode() { return hash; }
        @Override public boolean equals(Object other) {
            return other instanceof GroupKey key && QueryKey.arrayEquals(values, key.values);
        }
    }
}
