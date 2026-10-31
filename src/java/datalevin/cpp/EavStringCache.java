package datalevin.cpp;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Bounded, single-scan dictionary of short inline EAV string values. Keys are
 * copied into primitive words, so no borrowed buffer or native address survives
 * a cursor advance. One instance belongs to one column of one active scan.
 */
public final class EavStringCache {
    public static final int MAX_ENTRIES = 4096;
    public static final int MAX_KEY_BYTES = 24;
    private static final int OVERFLOW_MISSES = 64;

    private final int capacity;
    private final int mask;
    private final byte stringHeader;
    private final byte inlineFlag;
    private final long[] first;
    private final long[] second;
    private final long[] third;
    private final byte[] lengths;
    private final String[] values;
    private int size;
    private int overflowMisses;
    private boolean enabled = true;

    private int pending = -1;
    private int pendingLength;
    private long pendingFirst, pendingSecond, pendingThird;
    private int lastLength;
    private long lastFirst, lastSecond, lastThird;
    private String lastValue;

    /** Codec bytes come from the engine rather than a second format definition. */
    public EavStringCache(int capacity, byte stringHeader, byte inlineFlag) {
        if (capacity < 1 || capacity > MAX_ENTRIES) {
            throw new IllegalArgumentException("String cache capacity must be 1.." + MAX_ENTRIES);
        }
        this.capacity = capacity;
        this.stringHeader = stringHeader;
        this.inlineFlag = inlineFlag;
        int slots = 2;
        while (slots < capacity * 2) slots <<= 1;
        mask = slots - 1;
        first = new long[slots];
        second = new long[slots];
        third = new long[slots];
        lengths = new byte[slots];
        values = new String[slots];
    }

    private static long word(ByteBuffer buffer, int offset, int remaining) {
        if (remaining >= Long.BYTES) return buffer.getLong(offset);
        long value = 0;
        for (int i = 0; i < remaining; i++) {
            value = (value << 8) | (buffer.get(offset + i) & 0xffL);
        }
        return value;
    }

    private static int hash(long a, long b, long c, int length) {
        long value = a ^ Long.rotateLeft(b, 23) ^ Long.rotateLeft(c, 41) ^ length;
        value ^= value >>> 33;
        value *= 0xff51afd7ed558ccdL;
        value ^= value >>> 33;
        return (int) value;
    }

    private String last(long a, long b, long c, int length, String value) {
        lastFirst = a;
        lastSecond = b;
        lastThird = c;
        lastLength = length;
        lastValue = value;
        return value;
    }

    /**
     * Look up bytes [0, limit), preserving the buffer's position and limit.
     * Return null for a miss or an unsupported encoding. Call remember directly
     * after decoding a miss; the decoder may change or reuse the input buffer.
     */
    public String lookup(ByteBuffer buffer) {
        pending = -1;
        if (!enabled) return null;
        int length = buffer.limit();
        // Four aid bytes, one type byte, and the separator/inline trailer.
        if (length < 7 || length > MAX_KEY_BYTES ||
                buffer.order() != ByteOrder.BIG_ENDIAN ||
                buffer.get(4) != stringHeader || buffer.get(length - 1) != inlineFlag) {
            // The metadata probe was advisory. Retire a mixed-width or changed
            // column instead of adding a cache check to every ordinary decode.
            enabled = false;
            lastValue = null;
            return null;
        }
        long a = word(buffer, 0, length);
        long b = length > 8 ? word(buffer, 8, length - 8) : 0;
        long c = length > 16 ? word(buffer, 16, length - 16) : 0;
        if (lastValue != null && lastLength == length &&
                lastFirst == a && lastSecond == b && lastThird == c) {
            return lastValue;
        }
        int slot = hash(a, b, c, length) & mask;
        while (values[slot] != null) {
            if (lengths[slot] == length && first[slot] == a &&
                    second[slot] == b && third[slot] == c) {
                return last(a, b, c, length, values[slot]);
            }
            slot = (slot + 1) & mask;
        }
        if (size < capacity) {
            pending = slot;
            pendingLength = length;
            pendingFirst = a;
            pendingSecond = b;
            pendingThird = c;
        } else if (++overflowMisses >= OVERFLOW_MISSES) {
            // Cardinality is advisory: stop probing if the snapshot outgrew it.
            enabled = false;
            lastValue = null;
        }
        return null;
    }

    /** Publish an ordinary decoder result after lookup; only strings are held. */
    public Object remember(Object value) {
        int slot = pending;
        pending = -1;
        if (slot >= 0 && value instanceof String string) {
            first[slot] = pendingFirst;
            second[slot] = pendingSecond;
            third[slot] = pendingThird;
            lengths[slot] = (byte) pendingLength;
            values[slot] = string;
            size++;
            last(pendingFirst, pendingSecond, pendingThird, pendingLength, string);
        }
        return value;
    }

    /** Number of retained values, never greater than the constructor's limit. */
    public int size() { return size; }

    /** Callers can drop a retired dictionary and resume their ordinary loop. */
    public boolean isEnabled() { return enabled; }
}
