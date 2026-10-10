/*
 * Copyright (c) Huahai Yang. All rights reserved.
 * The use and distribution terms for this software are covered by the
 * Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0).
 */
package datalevin.utl;

import java.util.Arrays;

/** Stable unsigned byte-key sort; equal keys retain their input row order. */
public final class ByteKeySort {
    private static final int INSERTION_LIMIT = 48;

    private ByteKeySort() {}

    /**
     * Sort the first count rows and their corresponding keys in place.
     * Keys must be non-null. Entries beyond count remain unchanged.
     */
    public static void sort(Object[] rows, byte[][] keys, int count) {
        if (count > 1) {
            sort(rows, keys, new Object[count], new byte[count][], 0, count, 0);
        }
    }

    private static void sort(Object[] rows, byte[][] keys, Object[] workRows,
                             byte[][] workKeys, int start, int end, int depth) {
        if (end - start < INSERTION_LIMIT) {
            for (int i = start + 1; i < end; ++i) {
                Object row = rows[i];
                byte[] key = keys[i];
                int j = i;
                while (j > start && Arrays.compareUnsigned(keys[j - 1], key) > 0) {
                    rows[j] = rows[j - 1];
                    keys[j] = keys[j - 1];
                    --j;
                }
                rows[j] = row;
                keys[j] = key;
            }
            return;
        }
        int[] counts = new int[257];
        for (;;) {
            Arrays.fill(counts, 0);
            int distinct = 0;
            for (int i = start; i < end; ++i) {
                byte[] key = keys[i];
                int bucket = depth >= key.length ? 0 : (key[depth] & 255) + 1;
                if (counts[bucket]++ == 0) {
                    ++distinct;
                }
            }
            if (distinct > 1) {
                break;
            }
            if (counts[0] != 0) {
                return;
            }
            ++depth;
        }
        int[] positions = new int[257];
        int offset = start;
        for (int bucket = 0; bucket < 257; ++bucket) {
            positions[bucket] = offset;
            offset += counts[bucket];
        }
        for (int i = start; i < end; ++i) {
            byte[] key = keys[i];
            int bucket = depth >= key.length ? 0 : (key[depth] & 255) + 1;
            int destination = positions[bucket]++;
            workRows[destination] = rows[i];
            workKeys[destination] = key;
        }
        System.arraycopy(workRows, start, rows, start, end - start);
        System.arraycopy(workKeys, start, keys, start, end - start);
        offset = start + counts[0];
        for (int bucket = 1; bucket < 257; ++bucket) {
            int next = offset + counts[bucket];
            if (next - offset > 1) {
                sort(rows, keys, workRows, workKeys, offset, next, depth + 1);
            }
            offset = next;
        }
    }

}
