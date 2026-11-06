/* Copyright (c) Huahai Yang. All rights reserved.
 * Distributed under the Eclipse Public License 2.0. */
package datalevin.utl;

import clojure.lang.Numbers;
import java.util.function.Consumer;

/**
 * Sum/avg/count for proven-distinct rows with one ascending long group key.
 * Retains one group and lends its result array to the consumer until return.
 * Order and optional partition bounds are checked before any row is accepted.
 */
public final class OrderedAggregate {
    public static final class OrderViolation extends RuntimeException {
        public OrderViolation() {
            super("Aggregate input is outside its ordered group partition");
        }
    }

    private final int[] columns;
    private final int[] operations;
    private final int keyColumn;
    private final Long lower;
    private final Long upper;
    private final Consumer<Object[]> consume;
    private final Object[] values;
    private long key;
    private long count;
    private long inputCount;
    private long groupCount;
    private boolean finished;

    public OrderedAggregate(int[] columns, int[] operations, Long lower, Long upper,
                            Consumer<Object[]> consume) {
        if (columns.length != operations.length) throw new IllegalArgumentException("Aggregate width");
        this.columns = columns.clone();
        this.operations = operations.clone();
        this.lower = lower;
        this.upper = upper;
        this.consume = consume;
        values = new Object[columns.length];
        int keyIndex = -1;
        for (int i = 0; i < operations.length; i++) {
            if (operations[i] == GroupedAggregate.KEY) {
                if (keyIndex != -1) throw new IllegalArgumentException("One ordered group key required");
                keyIndex = columns[i];
            } else if (operations[i] < GroupedAggregate.SUM || operations[i] > GroupedAggregate.COUNT) {
                throw new IllegalArgumentException("Unsupported aggregate");
            }
        }
        if (keyIndex < 0) throw new IllegalArgumentException("Missing ordered group key");
        keyColumn = keyIndex;
    }

    /** Consume a borrowed tuple. Failure requires discarding all emitted groups. */
    public void add(Object[] tuple) {
        if (finished) throw new IllegalStateException("Aggregate already finished");
        long next = (Long) tuple[keyColumn];
        if (count == 0 || next != key) {
            if ((count != 0 && next < key)
                    || (lower != null && next < lower)
                    || (upper != null && next >= upper)) throw new OrderViolation();
            if (count != 0) emit();
            key = next;
            count = 0;
            groupCount++;
            for (int i = 0; i < operations.length; i++) {
                values[i] = operations[i] == GroupedAggregate.KEY ? tuple[columns[i]] : 0L;
            }
        }
        count++;
        inputCount++;
        for (int i = 0; i < operations.length; i++) {
            if (operations[i] == GroupedAggregate.SUM || operations[i] == GroupedAggregate.AVG) {
                values[i] = Numbers.add(values[i], tuple[columns[i]]);
            }
        }
    }

    private void emit() {
        for (int i = 0; i < operations.length; i++) {
            if (operations[i] == GroupedAggregate.COUNT) values[i] = Math.toIntExact(count);
            else if (operations[i] == GroupedAggregate.AVG) {
                values[i] = ((Number) Numbers.divide(values[i],
                        (Object) Integer.valueOf(Math.toIntExact(count)))).doubleValue();
            }
        }
        consume.accept(values);
    }

    public void finish() {
        if (!finished) {
            finished = true;
            if (count != 0) emit();
        }
    }

    public long inputCount() { return inputCount; }
    public long groupCount() { return groupCount; }
}
