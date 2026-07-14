package datalevin.utl;

import clojure.lang.Murmur3;

/**
 * Allocation-free helpers for object-array relation tuples.
 */
public final class ArrayUtil {

    private static final int HASH_OFFSET = 0x811c9dc5;
    private static final int HASH_PRIME = 16777619;

    private ArrayUtil() {
    }

    private static int addHash(int hash, Object value) {
        int elementHash = value == null ? 0 : value.hashCode();
        return (hash ^ elementHash) * HASH_PRIME;
    }

    /** Returns the mixed hash used by relation tuple wrappers. */
    public static int hashObjectArray(Object[] values) {
        int hash = HASH_OFFSET;
        for (Object value : values) {
            hash = addHash(hash, value);
        }
        return Murmur3.mixCollHash(hash, values.length);
    }

    /**
     * Fills a recursive-rule output tuple and returns its relation tuple hash.
     */
    public static int fillRuleOutputAndHash(
            Object[] output,
            long[] headTypes,
            int[] headIndexes,
            Object[] deltaTuple,
            int[] callRelationIndexes,
            Object eav0,
            Object eav1) {
        int hash = HASH_OFFSET;
        for (int i = 0; i < output.length; i++) {
            int index = headIndexes[i];
            Object value = headTypes[i] == 0
                    ? deltaTuple[callRelationIndexes[index]]
                    : (index == 0 ? eav0 : eav1);
            output[i] = value;
            hash = addHash(hash, value);
        }
        return Murmur3.mixCollHash(hash, output.length);
    }
}
