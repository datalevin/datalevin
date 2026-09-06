package datalevin.codec;

import clojure.lang.BigInt;
import clojure.lang.IPersistentList;
import clojure.lang.IPersistentMap;
import clojure.lang.IPersistentSet;
import clojure.lang.ITransientMap;
import clojure.lang.ITransientSet;
import clojure.lang.Keyword;
import clojure.lang.Numbers;
import clojure.lang.PersistentArrayMap;
import clojure.lang.PersistentHashSet;
import clojure.lang.PersistentList;
import clojure.lang.PersistentQueue;
import clojure.lang.PersistentVector;
import clojure.lang.Ratio;
import clojure.lang.Sorted;
import clojure.lang.Symbol;
import clojure.lang.Util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Purpose-built DL-CBOR v1 Phase 0 codec.
 *
 * <p>This codec intentionally is not connected to durable storage. The
 * normative format is {@code doc/dl-cbor/v1.md} and its shared golden corpus.
 */
public final class DLCbor {

    private static final long TAG_POSITIVE_BIGNUM = 2;
    private static final long TAG_NEGATIVE_BIGNUM = 3;
    private static final long TAG_DECIMAL = 4;
    private static final long TAG_RATIO = 30;
    private static final long TAG_URI = 32;
    private static final long TAG_UUID = 37;
    private static final long TAG_UINT16_LE_ARRAY = 69;
    private static final long TAG_SINT16_LE_ARRAY = 77;
    private static final long TAG_SINT32_LE_ARRAY = 78;
    private static final long TAG_SINT64_LE_ARRAY = 79;
    private static final long TAG_FLOAT32_LE_ARRAY = 85;
    private static final long TAG_FLOAT64_LE_ARRAY = 86;
    private static final long TAG_SET = 258;
    private static final long TAG_EXTENDED_TIME = 1001;

    /**
     * Non-durable Phase 0 stand-in for the not-yet-assigned Datalevin
     * extension tag. It is deliberately public so draft fixtures and the
     * independent Rust implementation cannot acquire a hidden second value.
     */
    public static final long DRAFT_EXTENSION_TAG = 0x444c;

    private static final long EXT_KEYWORD = 1;
    private static final long EXT_SYMBOL = 2;
    private static final long EXT_CHARACTER = 3;
    private static final long EXT_LIST = 4;
    private static final long EXT_QUEUE = 5;
    private static final long EXT_REGEX = 6;

    // Subtypes inside DL-CBOR byte strings. FB, FC, FD and FE reuse typed-data
    // headers; E0/E1 distinguish qualified names without a separator scan.
    private static final int SUBTYPE_QUALIFIED_KEYWORD = 0xe0;
    private static final int SUBTYPE_QUALIFIED_SYMBOL = 0xe1;
    private static final int SUBTYPE_CHARACTER = 0xe2;
    private static final int SUBTYPE_JAVA_REGEX = 0xe3;
    private static final int SUBTYPE_KEYWORD = 0xfb;
    private static final int SUBTYPE_SYMBOL = 0xfc;
    private static final int SUBTYPE_BOOLEANS = 0xfd;
    private static final int SUBTYPE_BYTES = 0xfe;

    private static final int REGEX_FLAGS = Pattern.UNIX_LINES
        | Pattern.CASE_INSENSITIVE
        | Pattern.COMMENTS
        | Pattern.MULTILINE
        | Pattern.LITERAL
        | Pattern.DOTALL
        | Pattern.UNICODE_CASE
        | Pattern.UNICODE_CHARACTER_CLASS;

    private static final Object NOT_FOUND = new Object();

    private static final int CANONICAL_FLOAT_NAN = 0x7fc00000;
    private static final long CANONICAL_DOUBLE_NAN = 0x7ff8000000000000L;

    private static final BigInteger LONG_MIN = BigInteger.valueOf(Long.MIN_VALUE);
    private static final BigInteger LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE);
    private static final ThreadLocal<BufferSink> BUFFER_SINK =
        ThreadLocal.withInitial(BufferSink::new);
    private static final ThreadLocal<GrowableSink> GROWABLE_SINK =
        ThreadLocal.withInitial(GrowableSink::new);
    private static final ThreadLocal<Decoder> DECODER =
        ThreadLocal.withInitial(Decoder::new);
    private static final ThreadLocal<byte[]> DECODE_BYTES =
        ThreadLocal.withInitial(() -> new byte[256]);

    private DLCbor() {
    }

    public enum Mode {
        CANONICAL,
        FAST
    }

    public enum ErrorCode {
        INPUT_TOO_LARGE,
        OUTPUT_TOO_SMALL,
        TRUNCATED,
        TRAILING_BYTES,
        INVALID_ADDITIONAL_INFO,
        INDEFINITE_LENGTH,
        NON_SHORTEST,
        NON_CANONICAL,
        UNSUPPORTED_SIMPLE_VALUE,
        INTEGER_OUT_OF_RANGE,
        LENGTH_OUT_OF_RANGE,
        INVALID_UTF8,
        INVALID_UNICODE,
        DEPTH_LIMIT,
        COLLECTION_LIMIT,
        STRING_LIMIT,
        BIGNUM_LIMIT,
        INVALID_BIGNUM,
        INVALID_DECIMAL,
        INVALID_RATIO,
        INVALID_URI,
        INVALID_REGEX,
        INVALID_UUID,
        INVALID_INSTANT,
        INVALID_TYPED_ARRAY,
        INVALID_EXTENSION,
        EXTENSION_LIMIT,
        DUPLICATE_KEY,
        DUPLICATE_SET_MEMBER,
        UNESCAPED_TYPED_HEADER,
        UNNECESSARY_STORAGE_ESCAPE,
        UNSUPPORTED_VALUE
    }

    public static final class CodecException extends IllegalArgumentException {
        private final ErrorCode code;
        private final int offset;

        private CodecException(ErrorCode code, int offset) {
            super("DL-CBOR " + code + " at byte " + offset);
            this.code = code;
            this.offset = offset;
        }

        public ErrorCode code() {
            return code;
        }

        public int offset() {
            return offset;
        }
    }

    public static final class Limits {
        public static final Limits DEFAULT = new Limits(64 * 1024 * 1024,
                                                        256,
                                                        1_000_000,
                                                        16 * 1024 * 1024,
                                                        4 * 1024,
                                                        16 * 1024 * 1024);

        public final int maxInputBytes;
        public final int maxDepth;
        public final int maxCollectionLength;
        public final int maxStringBytes;
        public final int maxBignumBytes;
        public final int maxExtensionBytes;

        public Limits(int maxInputBytes,
                      int maxDepth,
                      int maxCollectionLength,
                      int maxStringBytes,
                      int maxBignumBytes) {
            this(maxInputBytes, maxDepth, maxCollectionLength, maxStringBytes,
                 maxBignumBytes, 16 * 1024 * 1024);
        }

        public Limits(int maxInputBytes,
                      int maxDepth,
                      int maxCollectionLength,
                      int maxStringBytes,
                      int maxBignumBytes,
                      int maxExtensionBytes) {
            this.maxInputBytes = positive(maxInputBytes, "maxInputBytes");
            this.maxDepth = positive(maxDepth, "maxDepth");
            this.maxCollectionLength = positive(maxCollectionLength,
                                                "maxCollectionLength");
            this.maxStringBytes = positive(maxStringBytes, "maxStringBytes");
            this.maxBignumBytes = positive(maxBignumBytes, "maxBignumBytes");
            this.maxExtensionBytes = positive(maxExtensionBytes,
                                              "maxExtensionBytes");
        }

        private static int positive(int value, String name) {
            if (value <= 0) {
                throw new IllegalArgumentException(name + " must be positive");
            }
            return value;
        }
    }

    /**
     * A neutral, non-executable Datalevin extension. Integer identifiers are
     * reserved for built-ins; user identifiers are globally unique non-empty
     * strings. Arguments are DL-CBOR values and may be empty.
     */
    public static final class ExtensionValue {
        private final Object typeId;
        private final List<?> arguments;

        public ExtensionValue(long typeId, List<?> arguments) {
            this((Object) extensionTypeId(typeId), arguments);
        }

        public ExtensionValue(String typeId, List<?> arguments) {
            this((Object) extensionTypeId(typeId), arguments);
        }

        private ExtensionValue(Object typeId, List<?> arguments) {
            this.typeId = typeId;
            Objects.requireNonNull(arguments, "arguments");
            this.arguments = Collections.unmodifiableList(
                new ArrayList<>(arguments));
        }

        private static Long extensionTypeId(long typeId) {
            if (typeId < 0) {
                throw new IllegalArgumentException(
                    "extension integer type ID must be non-negative");
            }
            return Long.valueOf(typeId);
        }

        private static String extensionTypeId(String typeId) {
            Objects.requireNonNull(typeId, "typeId");
            if (typeId.isEmpty()) {
                throw new IllegalArgumentException(
                    "extension string type ID must be non-empty");
            }
            return typeId;
        }

        public Object typeId() {
            return typeId;
        }

        public List<?> arguments() {
            return arguments;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof ExtensionValue that
                && Objects.equals(typeId, that.typeId)
                && Objects.equals(arguments, that.arguments);
        }

        @Override
        public int hashCode() {
            return Objects.hash(typeId, arguments);
        }

        @Override
        public String toString() {
            return "ExtensionValue[" + typeId + ", " + arguments + "]";
        }
    }

    /** A neutral holder for a recognized but uninterpreted CBOR tag. */
    public static final class TaggedValue {
        private final long tag;
        private final Object value;

        public TaggedValue(long tag, Object value) {
            if (tag < 0) {
                throw new IllegalArgumentException("tag must be non-negative");
            }
            this.tag = tag;
            this.value = value;
        }

        public long tag() {
            return tag;
        }

        public Object value() {
            return value;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof TaggedValue that
                && tag == that.tag
                && Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tag, value);
        }

        @Override
        public String toString() {
            return "TaggedValue[" + tag + ", " + value + "]";
        }
    }

    /**
     * A lossless map value for keys that a host map would merge. Entries are
     * exposed as a list so no host equality rule can discard a key. The list
     * and entries are immutable; contained values have their usual mutability.
     * This holder encodes as an ordinary CBOR map, with no extra wire wrapper.
     */
    public static final class MapValue {
        private final List<Map.Entry<?, ?>> entries;

        public MapValue(Collection<? extends Map.Entry<?, ?>> entries) {
            Objects.requireNonNull(entries, "entries");
            List<Map.Entry<?, ?>> copy = new ArrayList<>(entries.size());
            for (Map.Entry<?, ?> entry : entries) {
                copy.add(new SimpleImmutableEntry<>(entry));
            }
            this.entries = Collections.unmodifiableList(copy);
        }

        public List<Map.Entry<?, ?>> entries() {
            return entries;
        }

        @Override
        public boolean equals(Object other) {
            return this == other || other instanceof MapValue that
                && Arrays.equals(encode(this, Mode.CANONICAL),
                                 encode(that, Mode.CANONICAL));
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(encode(this, Mode.CANONICAL));
        }

        @Override
        public String toString() {
            return "MapValue" + entries;
        }
    }

    /**
     * A lossless set value for members that a host set would merge. Members
     * are exposed as an immutable list, and encode with the normal set tag
     * and array. Equality of these holders uses canonical DL-CBOR bytes.
     */
    public static final class SetValue {
        private final List<?> members;

        public SetValue(Collection<?> members) {
            Objects.requireNonNull(members, "members");
            this.members = Collections.unmodifiableList(new ArrayList<>(members));
        }

        public List<?> members() {
            return members;
        }

        @Override
        public boolean equals(Object other) {
            return this == other || other instanceof SetValue that
                && Arrays.equals(encode(this, Mode.CANONICAL),
                                 encode(that, Mode.CANONICAL));
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(encode(this, Mode.CANONICAL));
        }

        @Override
        public String toString() {
            return "SetValue" + members;
        }
    }

    /** Return the exact encoded length for an item. */
    public static int encodedSize(Object value, Mode mode) {
        Objects.requireNonNull(mode, "mode");
        return sizeOf(value);
    }

    /** Write one bare item into caller-owned storage and return bytes written. */
    public static int write(ByteBuffer output, Object value, Mode mode) {
        Objects.requireNonNull(output, "output");
        int start = output.position();
        BufferSink sink = BUFFER_SINK.get();
        sink.reset(output);
        try {
            writeValue(sink, value, Objects.requireNonNull(mode, "mode"));
        } catch (BufferOverflowException exception) {
            throw error(ErrorCode.OUTPUT_TOO_SMALL, output.position() - start);
        } finally {
            sink.clear();
        }
        return output.position() - start;
    }

    /** Allocating convenience wrapper for one bare item. */
    public static byte[] encode(Object value, Mode mode) {
        Objects.requireNonNull(mode, "mode");
        GrowableSink sink = GROWABLE_SINK.get();
        if (!sink.acquire()) {
            byte[] result = new byte[encodedSize(value, mode)];
            writeValue(new ArraySink(result), value, mode);
            return result;
        }
        try {
            writeValue(sink, value, mode);
            return sink.toByteArray();
        } finally {
            sink.release();
        }
    }

    /** Write the storage-only typed-header collision wrapper. */
    public static int writeStorage(ByteBuffer output, Object value, Mode mode) {
        int start = output.position();
        try {
            if (needsStorageEscape(value)) {
                output.put((byte) 0xff);
            }
            write(output, value, mode);
        } catch (BufferOverflowException exception) {
            throw error(ErrorCode.OUTPUT_TOO_SMALL, output.position() - start);
        }
        return output.position() - start;
    }

    /** Allocating convenience wrapper for an untyped storage field. */
    public static byte[] encodeStorage(Object value, Mode mode) {
        int size = encodedSize(value, mode);
        boolean escaped = needsStorageEscape(value);
        byte[] result = new byte[size + (escaped ? 1 : 0)];
        writeStorage(ByteBuffer.wrap(result), value, mode);
        return result;
    }

    /** Decode one complete bare item from a byte array. */
    public static Object decode(byte[] input, boolean canonical) {
        Objects.requireNonNull(input, "input");
        return decodeArray(input, 0, input.length, canonical, Limits.DEFAULT);
    }

    /** Decode one complete bare item, consuming the input buffer. */
    public static Object decode(ByteBuffer input,
                                boolean canonical,
                                Limits limits) {
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(limits, "limits");
        if (input.remaining() > limits.maxInputBytes) {
            throw error(ErrorCode.INPUT_TOO_LARGE, 0);
        }
        Decoder decoder = DECODER.get();
        decoder.reset(input, canonical, limits);
        try {
            Object result = decoder.readValue(0);
            if (input.hasRemaining()) {
                throw error(ErrorCode.TRAILING_BYTES, decoder.offset());
            }
            return result;
        } finally {
            decoder.clear();
        }
    }

    /** Decode one complete storage-wrapped untyped item. */
    public static Object decodeStorage(byte[] input, boolean canonical) {
        Objects.requireNonNull(input, "input");
        if (input.length == 0) {
            throw error(ErrorCode.TRUNCATED, 0);
        }
        int first = input[0] & 0xff;
        if (first == 0xff) {
            if (input.length == 1) {
                throw error(ErrorCode.TRUNCATED, 1);
            }
            if (!isTypedHeader(input[1] & 0xff)) {
                throw error(ErrorCode.UNNECESSARY_STORAGE_ESCAPE, 0);
            }
            return decodeArray(input, 1, input.length - 1,
                               canonical, Limits.DEFAULT);
        }
        if (isTypedHeader(first)) {
            throw error(ErrorCode.UNESCAPED_TYPED_HEADER, 0);
        }
        return decode(input, canonical);
    }

    private static Object decodeArray(byte[] input,
                                      int start,
                                      int length,
                                      boolean canonical,
                                      Limits limits) {
        if (length > limits.maxInputBytes) {
            throw error(ErrorCode.INPUT_TOO_LARGE, 0);
        }
        Decoder decoder = DECODER.get();
        decoder.reset(input, start, length, canonical, limits);
        try {
            Object result = decoder.readValue(0);
            if (decoder.hasRemaining()) {
                throw error(ErrorCode.TRAILING_BYTES, decoder.offset());
            }
            return result;
        } finally {
            decoder.clear();
        }
    }

    public static boolean isTypedHeader(int value) {
        return value == 0xc0 || value == 0xc1 || (value >= 0xf1 && value <= 0xfe);
    }

    public static boolean needsStorageEscape(Object value) {
        if (value == null
            || value instanceof Boolean
            || value instanceof Float
            || value instanceof Double) {
            return true;
        }
        return value instanceof TaggedValue tagged
            && (tagged.tag == 0 || tagged.tag == 1);
    }

    private interface Sink {
        int position();

        void putByte(int value);

        void putBytes(byte[] bytes);

        void putText(String value, int utf8Length);

        void putShortLittleEndian(int value);

        void putIntLittleEndian(int value);

        void putLongLittleEndian(long value);

        default void putCharsLittleEndian(char[] values) {
            for (char value : values) {
                putShortLittleEndian(value);
            }
        }

        default void putShortsLittleEndian(short[] values) {
            for (short value : values) {
                putShortLittleEndian(value);
            }
        }

        default void putIntsLittleEndian(int[] values) {
            for (int value : values) {
                putIntLittleEndian(value);
            }
        }

        default void putLongsLittleEndian(long[] values) {
            for (long value : values) {
                putLongLittleEndian(value);
            }
        }

        default void putFloatsLittleEndian(float[] values) {
            for (float value : values) {
                int bits = Float.isNaN(value)
                    ? CANONICAL_FLOAT_NAN
                    : Float.floatToRawIntBits(value);
                putIntLittleEndian(bits);
            }
        }

        default void putDoublesLittleEndian(double[] values) {
            for (double value : values) {
                long bits = Double.isNaN(value)
                    ? CANONICAL_DOUBLE_NAN
                    : Double.doubleToRawLongBits(value);
                putLongLittleEndian(bits);
            }
        }
    }

    private static final class ArraySink implements Sink {
        private final byte[] output;
        private int position;

        private ArraySink(byte[] output) {
            this.output = output;
        }

        @Override
        public int position() {
            return position;
        }

        @Override
        public void putByte(int value) {
            if (position >= output.length) {
                throw error(ErrorCode.OUTPUT_TOO_SMALL, position);
            }
            output[position++] = (byte) value;
        }

        @Override
        public void putBytes(byte[] bytes) {
            int end;
            try {
                end = Math.addExact(position, bytes.length);
            } catch (ArithmeticException ignored) {
                throw error(ErrorCode.LENGTH_OUT_OF_RANGE, position);
            }
            if (end > output.length) {
                throw error(ErrorCode.OUTPUT_TOO_SMALL, position);
            }
            System.arraycopy(bytes, 0, output, position, bytes.length);
            position = end;
        }

        @Override
        public void putText(String value, int utf8Length) {
            int end = checkedEnd(position, utf8Length);
            if (end > output.length) {
                throw error(ErrorCode.OUTPUT_TOO_SMALL, position);
            }
            encodeUtf8(value, output, position);
            position = end;
        }

        @Override
        public void putShortLittleEndian(int value) {
            int end = checkedEnd(position, 2);
            if (end > output.length) {
                throw error(ErrorCode.OUTPUT_TOO_SMALL, position);
            }
            output[position] = (byte) value;
            output[position + 1] = (byte) (value >>> 8);
            position = end;
        }

        @Override
        public void putIntLittleEndian(int value) {
            int end = checkedEnd(position, 4);
            if (end > output.length) {
                throw error(ErrorCode.OUTPUT_TOO_SMALL, position);
            }
            output[position] = (byte) value;
            output[position + 1] = (byte) (value >>> 8);
            output[position + 2] = (byte) (value >>> 16);
            output[position + 3] = (byte) (value >>> 24);
            position = end;
        }

        @Override
        public void putLongLittleEndian(long value) {
            int end = checkedEnd(position, 8);
            if (end > output.length) {
                throw error(ErrorCode.OUTPUT_TOO_SMALL, position);
            }
            output[position] = (byte) value;
            output[position + 1] = (byte) (value >>> 8);
            output[position + 2] = (byte) (value >>> 16);
            output[position + 3] = (byte) (value >>> 24);
            output[position + 4] = (byte) (value >>> 32);
            output[position + 5] = (byte) (value >>> 40);
            output[position + 6] = (byte) (value >>> 48);
            output[position + 7] = (byte) (value >>> 56);
            position = end;
        }
    }

    private static final class GrowableSink implements Sink {
        private byte[] output = new byte[256];
        private int position;
        private boolean active;

        private boolean acquire() {
            if (active) {
                return false;
            }
            active = true;
            position = 0;
            return true;
        }

        private void release() {
            active = false;
            position = 0;
        }

        private byte[] toByteArray() {
            return Arrays.copyOf(output, position);
        }

        private void require(int length) {
            int end = checkedEnd(position, length);
            if (end <= output.length) {
                return;
            }
            int capacity = output.length;
            while (capacity < end) {
                int grown = capacity + (capacity >>> 1) + 1;
                capacity = grown > capacity ? grown : end;
            }
            output = Arrays.copyOf(output, capacity);
        }

        @Override
        public int position() {
            return position;
        }

        @Override
        public void putByte(int value) {
            require(1);
            output[position++] = (byte) value;
        }

        @Override
        public void putBytes(byte[] bytes) {
            require(bytes.length);
            System.arraycopy(bytes, 0, output, position, bytes.length);
            position += bytes.length;
        }

        @Override
        public void putText(String value, int utf8Length) {
            require(utf8Length);
            encodeUtf8(value, output, position);
            position += utf8Length;
        }

        @Override
        public void putShortLittleEndian(int value) {
            require(2);
            output[position] = (byte) value;
            output[position + 1] = (byte) (value >>> 8);
            position += 2;
        }

        @Override
        public void putIntLittleEndian(int value) {
            require(4);
            output[position] = (byte) value;
            output[position + 1] = (byte) (value >>> 8);
            output[position + 2] = (byte) (value >>> 16);
            output[position + 3] = (byte) (value >>> 24);
            position += 4;
        }

        @Override
        public void putLongLittleEndian(long value) {
            require(8);
            output[position] = (byte) value;
            output[position + 1] = (byte) (value >>> 8);
            output[position + 2] = (byte) (value >>> 16);
            output[position + 3] = (byte) (value >>> 24);
            output[position + 4] = (byte) (value >>> 32);
            output[position + 5] = (byte) (value >>> 40);
            output[position + 6] = (byte) (value >>> 48);
            output[position + 7] = (byte) (value >>> 56);
            position += 8;
        }
    }

    private static final class BufferSink implements Sink {
        private ByteBuffer output;
        private int origin;
        private long textWord;
        private int textWidth;
        private boolean textBigEndian;
        private boolean littleEndian;

        private BufferSink() {
        }

        private void reset(ByteBuffer output) {
            this.output = output;
            this.origin = output.position();
            this.littleEndian = output.order() == ByteOrder.LITTLE_ENDIAN;
        }

        private void clear() {
            output = null;
            origin = 0;
            littleEndian = false;
        }

        @Override
        public int position() {
            return output.position() - origin;
        }

        @Override
        public void putByte(int value) {
            output.put((byte) value);
        }

        @Override
        public void putBytes(byte[] bytes) {
            output.put(bytes);
        }

        @Override
        public void putText(String value, int utf8Length) {
            if (output.remaining() < utf8Length) {
                throw new BufferOverflowException();
            }
            textWord = 0;
            textWidth = 0;
            textBigEndian = output.order() == ByteOrder.BIG_ENDIAN;
            for (int index = 0; index < value.length(); index++) {
                char character = value.charAt(index);
                if (character <= 0x7f) {
                    appendTextByte(character);
                } else if (character <= 0x7ff) {
                    appendTextByte(0xc0 | (character >>> 6));
                    appendTextByte(0x80 | (character & 0x3f));
                } else if (Character.isHighSurrogate(character)) {
                    int codePoint = Character.toCodePoint(character,
                                                          value.charAt(++index));
                    appendTextByte(0xf0 | (codePoint >>> 18));
                    appendTextByte(0x80 | ((codePoint >>> 12) & 0x3f));
                    appendTextByte(0x80 | ((codePoint >>> 6) & 0x3f));
                    appendTextByte(0x80 | (codePoint & 0x3f));
                } else {
                    appendTextByte(0xe0 | (character >>> 12));
                    appendTextByte(0x80 | ((character >>> 6) & 0x3f));
                    appendTextByte(0x80 | (character & 0x3f));
                }
            }
            for (int shift = (textWidth - 1) * 8; shift >= 0; shift -= 8) {
                output.put((byte) (textWord >>> shift));
            }
            textWord = 0;
            textWidth = 0;
        }

        @Override
        public void putShortLittleEndian(int value) {
            short item = (short) value;
            output.putShort(littleEndian ? item : Short.reverseBytes(item));
        }

        @Override
        public void putIntLittleEndian(int value) {
            output.putInt(littleEndian ? value : Integer.reverseBytes(value));
        }

        @Override
        public void putLongLittleEndian(long value) {
            output.putLong(littleEndian ? value : Long.reverseBytes(value));
        }

        @Override
        public void putCharsLittleEndian(char[] values) {
            int byteLength = Math.multiplyExact(values.length, 2);
            requireBuffer(byteLength);
            int start = output.position();
            ByteOrder original = output.order();
            try {
                output.order(ByteOrder.LITTLE_ENDIAN);
                output.asCharBuffer().put(values);
                output.position(start + byteLength);
            } finally {
                output.order(original);
            }
        }

        @Override
        public void putShortsLittleEndian(short[] values) {
            int byteLength = Math.multiplyExact(values.length, 2);
            requireBuffer(byteLength);
            int start = output.position();
            ByteOrder original = output.order();
            try {
                output.order(ByteOrder.LITTLE_ENDIAN);
                output.asShortBuffer().put(values);
                output.position(start + byteLength);
            } finally {
                output.order(original);
            }
        }

        @Override
        public void putIntsLittleEndian(int[] values) {
            int byteLength = Math.multiplyExact(values.length, 4);
            requireBuffer(byteLength);
            int start = output.position();
            ByteOrder original = output.order();
            try {
                output.order(ByteOrder.LITTLE_ENDIAN);
                output.asIntBuffer().put(values);
                output.position(start + byteLength);
            } finally {
                output.order(original);
            }
        }

        @Override
        public void putLongsLittleEndian(long[] values) {
            int byteLength = Math.multiplyExact(values.length, 8);
            requireBuffer(byteLength);
            int start = output.position();
            ByteOrder original = output.order();
            try {
                output.order(ByteOrder.LITTLE_ENDIAN);
                output.asLongBuffer().put(values);
                output.position(start + byteLength);
            } finally {
                output.order(original);
            }
        }

        @Override
        public void putFloatsLittleEndian(float[] values) {
            for (float value : values) {
                if (Float.isNaN(value)) {
                    Sink.super.putFloatsLittleEndian(values);
                    return;
                }
            }
            int byteLength = Math.multiplyExact(values.length, 4);
            requireBuffer(byteLength);
            int start = output.position();
            ByteOrder original = output.order();
            try {
                output.order(ByteOrder.LITTLE_ENDIAN);
                output.asFloatBuffer().put(values);
                output.position(start + byteLength);
            } finally {
                output.order(original);
            }
        }

        @Override
        public void putDoublesLittleEndian(double[] values) {
            for (double value : values) {
                if (Double.isNaN(value)) {
                    Sink.super.putDoublesLittleEndian(values);
                    return;
                }
            }
            int byteLength = Math.multiplyExact(values.length, 8);
            requireBuffer(byteLength);
            int start = output.position();
            ByteOrder original = output.order();
            try {
                output.order(ByteOrder.LITTLE_ENDIAN);
                output.asDoubleBuffer().put(values);
                output.position(start + byteLength);
            } finally {
                output.order(original);
            }
        }

        private void requireBuffer(int length) {
            if (output.remaining() < length) {
                throw new BufferOverflowException();
            }
        }

        private void appendTextByte(int value) {
            textWord = (textWord << 8) | (value & 0xff);
            if (++textWidth == 8) {
                output.putLong(textBigEndian
                               ? textWord
                               : Long.reverseBytes(textWord));
                textWord = 0;
                textWidth = 0;
            }
        }
    }

    private static int checkedEnd(int position, int length) {
        try {
            return Math.addExact(position, length);
        } catch (ArithmeticException exception) {
            throw error(ErrorCode.LENGTH_OUT_OF_RANGE, position);
        }
    }

    private static int sizeOf(Object value) {
        if (value == null || value instanceof Boolean) {
            return 1;
        }
        if (value instanceof Byte
            || value instanceof Short
            || value instanceof Integer
            || value instanceof Long) {
            return integerSize(((Number) value).longValue());
        }
        if (value instanceof BigInt bigInt) {
            return bigIntegerSize(bigInt.toBigInteger());
        }
        if (value instanceof BigInteger bigInteger) {
            return bigIntegerSize(bigInteger);
        }
        if (value instanceof Float) {
            return 5;
        }
        if (value instanceof Double) {
            return 9;
        }
        if (value instanceof BigDecimal input) {
            BigDecimal decimal = normalizedDecimal(input);
            long exponent = -(long) decimal.scale();
            checkDecimalExponent(exponent, 0);
            return checkedSize(headSize(TAG_DECIMAL),
                               headSize(2),
                               integerSize(exponent),
                               bigIntegerSize(decimal.unscaledValue()));
        }
        if (value instanceof Ratio ratio) {
            validateRatio(ratio.numerator, ratio.denominator, 0);
            return checkedSize(headSize(TAG_RATIO),
                               headSize(2),
                               bigIntegerSize(ratio.numerator),
                               bigIntegerSize(ratio.denominator));
        }
        if (value instanceof Date date) {
            return instantSize(date.getTime());
        }
        if (value instanceof Instant instant) {
            return instantSize(instantMillis(instant, 0));
        }
        if (value instanceof byte[] bytes) {
            int length = checkedSize(1, bytes.length);
            return checkedSize(headSize(length), length);
        }
        if (value instanceof boolean[] values) {
            int length = booleanPayloadSize(values.length);
            return checkedSize(headSize(length), length);
        }
        if (value instanceof char[] values) {
            return typedArraySize(TAG_UINT16_LE_ARRAY, values.length, 2);
        }
        if (value instanceof short[] values) {
            return typedArraySize(TAG_SINT16_LE_ARRAY, values.length, 2);
        }
        if (value instanceof int[] values) {
            return typedArraySize(TAG_SINT32_LE_ARRAY, values.length, 4);
        }
        if (value instanceof long[] values) {
            return typedArraySize(TAG_SINT64_LE_ARRAY, values.length, 8);
        }
        if (value instanceof float[] values) {
            return typedArraySize(TAG_FLOAT32_LE_ARRAY, values.length, 4);
        }
        if (value instanceof double[] values) {
            return typedArraySize(TAG_FLOAT64_LE_ARRAY, values.length, 8);
        }
        if (value instanceof Keyword keyword) {
            return namedValueSize(keyword.getNamespace(), keyword.getName());
        }
        if (value instanceof Symbol symbol) {
            return namedValueSize(symbol.getNamespace(), symbol.getName());
        }
        if (value instanceof Character character) {
            return character <= 0xff ? 3 : 4;
        }
        if (value instanceof PersistentQueue queue) {
            return collectionExtensionSize(EXT_QUEUE, queue);
        }
        if (value instanceof IPersistentList list) {
            return collectionExtensionSize(EXT_LIST,
                                           (Collection<?>) list);
        }
        if (value instanceof Pattern pattern) {
            int flags = validatedRegexFlags(pattern.flags(), 0);
            int length = checkedSize(1, unsignedVarintSize(flags),
                                     utf8Length(pattern.pattern(), 0));
            return checkedSize(headSize(length), length);
        }
        if (value instanceof String text) {
            int length = utf8Length(text, 0);
            return checkedSize(headSize(length), length);
        }
        if (value instanceof URI uri) {
            String text = uri.toASCIIString();
            int length = utf8Length(text, 0);
            return checkedSize(headSize(TAG_URI), headSize(length), length);
        }
        if (value instanceof UUID) {
            return checkedSize(headSize(TAG_UUID), headSize(16), 16);
        }
        if (value instanceof ExtensionValue extension) {
            validateExtensionId(extension.typeId, 0);
            int size = checkedSize(extensionHeadSize(
                                       extensionItemCount(
                                           extension.arguments.size())),
                                   sizeOf(extension.typeId));
            for (Object argument : extension.arguments) {
                size = checkedSize(size, sizeOf(argument));
            }
            return size;
        }
        if (value instanceof TaggedValue tagged) {
            if (tagged.tag == DRAFT_EXTENSION_TAG) {
                throw error(ErrorCode.INVALID_EXTENSION, 0);
            }
            if (rawByteStringTag(tagged.tag) && tagged.value instanceof byte[] bytes) {
                return checkedSize(headSize(tagged.tag), headSize(bytes.length),
                                   bytes.length);
            }
            return checkedSize(headSize(tagged.tag), sizeOf(tagged.value));
        }
        if (value instanceof MapValue map) {
            return mapSize(map.entries);
        }
        if (value instanceof SetValue set) {
            return setSize(set.members);
        }
        if (value instanceof Sorted
            || value instanceof SortedMap<?, ?>
            || value instanceof SortedSet<?>
            || value instanceof Collection<?>
               && !(value instanceof List<?>)
               && !(value instanceof Set<?>)) {
            throw error(ErrorCode.UNSUPPORTED_VALUE, 0);
        }
        if (value instanceof Map<?, ?> map) {
            return mapSize(map.entrySet());
        }
        if (value instanceof Set<?> set) {
            return setSize(set);
        }
        if (value instanceof List<?> list) {
            int size = headSize(list.size());
            for (Object item : list) {
                size = checkedSize(size, sizeOf(item));
            }
            return size;
        }
        throw error(ErrorCode.UNSUPPORTED_VALUE, 0);
    }

    private static int mapSize(Collection<? extends Map.Entry<?, ?>> entries) {
        int size = headSize(entries.size());
        for (Map.Entry<?, ?> entry : entries) {
            size = checkedSize(size, sizeOf(entry.getKey()),
                               sizeOf(entry.getValue()));
        }
        return size;
    }

    private static int setSize(Collection<?> members) {
        int size = checkedSize(headSize(TAG_SET), headSize(members.size()));
        for (Object member : members) {
            size = checkedSize(size, sizeOf(member));
        }
        return size;
    }

    private static int integerSize(long value) {
        return headSize(value >= 0 ? value : ~value);
    }

    private static int instantSize(long milliseconds) {
        long seconds = Math.floorDiv(milliseconds, 1000);
        int remainder = Math.floorMod(milliseconds, 1000);
        int result = checkedSize(headSize(TAG_EXTENDED_TIME),
                                 headSize(remainder == 0 ? 1 : 2),
                                 integerSize(1),
                                 integerSize(seconds));
        return remainder == 0
            ? result
            : checkedSize(result, integerSize(-3), integerSize(remainder));
    }

    private static int typedArraySize(long tag, int length, int width) {
        final int byteLength;
        try {
            byteLength = Math.multiplyExact(length, width);
        } catch (ArithmeticException exception) {
            throw error(ErrorCode.LENGTH_OUT_OF_RANGE, 0);
        }
        return checkedSize(headSize(tag), headSize(byteLength), byteLength);
    }

    private static int extensionHeadSize(int itemCount) {
        return checkedSize(headSize(DRAFT_EXTENSION_TAG),
                           headSize(itemCount));
    }

    private static int extensionItemCount(int argumentCount) {
        if (argumentCount == Integer.MAX_VALUE) {
            throw error(ErrorCode.LENGTH_OUT_OF_RANGE, 0);
        }
        return argumentCount + 1;
    }

    private static int namedValueSize(String namespace, String name) {
        int length = checkedSize(1, utf8Length(name, 0));
        if (namespace != null) {
            int namespaceLength = utf8Length(namespace, 0);
            length = checkedSize(length, unsignedVarintSize(namespaceLength),
                                 namespaceLength);
        }
        return checkedSize(headSize(length), length);
    }

    private static int unsignedVarintSize(int value) {
        int length = 1;
        while ((value >>>= 7) != 0) {
            length++;
        }
        return length;
    }

    private static void validateExtensionId(Object typeId, int offset) {
        if (typeId instanceof Long id
            && (id == EXT_KEYWORD || id == EXT_SYMBOL
                || id == EXT_CHARACTER || id == EXT_REGEX)) {
            throw error(ErrorCode.INVALID_EXTENSION, offset);
        }
    }

    private static int collectionExtensionSize(long typeId,
                                               Collection<?> values) {
        int size = checkedSize(extensionHeadSize(
                                   extensionItemCount(values.size())),
                               integerSize(typeId));
        for (Object value : values) {
            size = checkedSize(size, sizeOf(value));
        }
        return size;
    }

    private static int validatedRegexFlags(long rawFlags, int offset) {
        if (rawFlags < 0 || rawFlags > Integer.MAX_VALUE) {
            throw error(ErrorCode.INVALID_REGEX, offset);
        }
        int flags = (int) rawFlags;
        if ((flags & ~REGEX_FLAGS) != 0
            || (flags & Pattern.UNICODE_CHARACTER_CLASS) != 0
               && (flags & Pattern.UNICODE_CASE) == 0) {
            throw error(ErrorCode.INVALID_REGEX, offset);
        }
        return flags;
    }

    private static int bigIntegerSize(BigInteger value) {
        if (value.compareTo(LONG_MIN) >= 0 && value.compareTo(LONG_MAX) <= 0) {
            return integerSize(value.longValue());
        }
        BigInteger magnitude = value.signum() < 0
            ? value.negate().subtract(BigInteger.ONE)
            : value;
        int byteLength = Math.max(1, (magnitude.bitLength() + 7) >>> 3);
        return checkedSize(headSize(value.signum() < 0
                                    ? TAG_NEGATIVE_BIGNUM
                                    : TAG_POSITIVE_BIGNUM),
                           headSize(byteLength),
                           byteLength);
    }

    private static int headSize(long argument) {
        if (argument < 0) {
            throw error(ErrorCode.INTEGER_OUT_OF_RANGE, 0);
        }
        if (argument <= 23) {
            return 1;
        }
        if (argument <= 0xff) {
            return 2;
        }
        if (argument <= 0xffff) {
            return 3;
        }
        if (argument <= 0xffffffffL) {
            return 5;
        }
        return 9;
    }

    private static int checkedSize(int... parts) {
        int result = 0;
        try {
            for (int part : parts) {
                result = Math.addExact(result, part);
            }
            return result;
        } catch (ArithmeticException ignored) {
            throw error(ErrorCode.LENGTH_OUT_OF_RANGE, 0);
        }
    }

    private static void writeValue(Sink sink, Object value, Mode mode) {
        if (value == null) {
            sink.putByte(0xf6);
        } else if (value instanceof Boolean bool) {
            sink.putByte(bool ? 0xf5 : 0xf4);
        } else if (value instanceof Byte
                   || value instanceof Short
                   || value instanceof Integer
                   || value instanceof Long) {
            writeLong(sink, ((Number) value).longValue());
        } else if (value instanceof BigInt bigInt) {
            writeBigInteger(sink, bigInt.toBigInteger());
        } else if (value instanceof BigInteger bigInteger) {
            writeBigInteger(sink, bigInteger);
        } else if (value instanceof Float number) {
            sink.putByte(0xfa);
            int bits = Float.isNaN(number)
                ? CANONICAL_FLOAT_NAN
                : Float.floatToRawIntBits(number);
            putInt(sink, bits);
        } else if (value instanceof Double number) {
            sink.putByte(0xfb);
            long bits = Double.isNaN(number)
                ? CANONICAL_DOUBLE_NAN
                : Double.doubleToRawLongBits(number);
            putLong(sink, bits);
        } else if (value instanceof BigDecimal decimal) {
            writeDecimal(sink, decimal);
        } else if (value instanceof Ratio ratio) {
            writeRatio(sink, ratio.numerator, ratio.denominator);
        } else if (value instanceof Date date) {
            writeInstant(sink, date.getTime());
        } else if (value instanceof Instant instant) {
            writeInstant(sink, instantMillis(instant, sink.position()));
        } else if (value instanceof byte[] bytes) {
            writeHead(sink, 2, checkedSize(1, bytes.length));
            sink.putByte(SUBTYPE_BYTES);
            sink.putBytes(bytes);
        } else if (value instanceof boolean[] values) {
            writeBooleans(sink, values);
        } else if (value instanceof char[] values) {
            writeTypedArrayHead(sink, TAG_UINT16_LE_ARRAY, values.length, 2);
            sink.putCharsLittleEndian(values);
        } else if (value instanceof short[] values) {
            writeTypedArrayHead(sink, TAG_SINT16_LE_ARRAY, values.length, 2);
            sink.putShortsLittleEndian(values);
        } else if (value instanceof int[] values) {
            writeTypedArrayHead(sink, TAG_SINT32_LE_ARRAY, values.length, 4);
            sink.putIntsLittleEndian(values);
        } else if (value instanceof long[] values) {
            writeTypedArrayHead(sink, TAG_SINT64_LE_ARRAY, values.length, 8);
            sink.putLongsLittleEndian(values);
        } else if (value instanceof float[] values) {
            writeTypedArrayHead(sink, TAG_FLOAT32_LE_ARRAY, values.length, 4);
            sink.putFloatsLittleEndian(values);
        } else if (value instanceof double[] values) {
            writeTypedArrayHead(sink, TAG_FLOAT64_LE_ARRAY, values.length, 8);
            sink.putDoublesLittleEndian(values);
        } else if (value instanceof Keyword keyword) {
            writeNamedValue(sink, SUBTYPE_KEYWORD, SUBTYPE_QUALIFIED_KEYWORD,
                            keyword.getNamespace(), keyword.getName());
        } else if (value instanceof Symbol symbol) {
            writeNamedValue(sink, SUBTYPE_SYMBOL, SUBTYPE_QUALIFIED_SYMBOL,
                            symbol.getNamespace(), symbol.getName());
        } else if (value instanceof Character character) {
            writeHead(sink, 2, character <= 0xff ? 2 : 3);
            sink.putByte(SUBTYPE_CHARACTER);
            if (character > 0xff) {
                sink.putByte(character >>> 8);
            }
            sink.putByte(character);
        } else if (value instanceof PersistentQueue queue) {
            writeCollectionExtension(sink, EXT_QUEUE, queue, mode);
        } else if (value instanceof IPersistentList list) {
            writeCollectionExtension(sink, EXT_LIST,
                                     (Collection<?>) list, mode);
        } else if (value instanceof Pattern pattern) {
            int flags = validatedRegexFlags(pattern.flags(), sink.position());
            int sourceLength = utf8Length(pattern.pattern(), sink.position());
            writeHead(sink, 2, checkedSize(1, unsignedVarintSize(flags),
                                         sourceLength));
            sink.putByte(SUBTYPE_JAVA_REGEX);
            writeUnsignedVarint(sink, flags);
            sink.putText(pattern.pattern(), sourceLength);
        } else if (value instanceof String text) {
            writeText(sink, text);
        } else if (value instanceof URI uri) {
            writeHead(sink, 6, TAG_URI);
            writeText(sink, uri.toASCIIString());
        } else if (value instanceof UUID uuid) {
            writeHead(sink, 6, TAG_UUID);
            writeHead(sink, 2, 16);
            putLong(sink, uuid.getMostSignificantBits());
            putLong(sink, uuid.getLeastSignificantBits());
        } else if (value instanceof ExtensionValue extension) {
            validateExtensionId(extension.typeId, sink.position());
            writeExtensionHead(sink,
                               extensionItemCount(extension.arguments.size()));
            writeValue(sink, extension.typeId, mode);
            for (Object argument : extension.arguments) {
                writeValue(sink, argument, mode);
            }
        } else if (value instanceof TaggedValue tagged) {
            if (tagged.tag == DRAFT_EXTENSION_TAG) {
                throw error(ErrorCode.INVALID_EXTENSION, sink.position());
            }
            writeHead(sink, 6, tagged.tag);
            if (rawByteStringTag(tagged.tag) && tagged.value instanceof byte[] bytes) {
                writeHead(sink, 2, bytes.length);
                sink.putBytes(bytes);
            } else {
                writeValue(sink, tagged.value, mode);
            }
        } else if (value instanceof MapValue map) {
            writeMapEntries(sink, map.entries, mode);
        } else if (value instanceof SetValue set) {
            writeHead(sink, 6, TAG_SET);
            writeSetMembers(sink, set.members, mode);
        } else if (value instanceof Sorted
                   || value instanceof SortedMap<?, ?>
                   || value instanceof SortedSet<?>) {
            throw error(ErrorCode.UNSUPPORTED_VALUE, sink.position());
        } else if (value instanceof Map<?, ?> map) {
            writeMap(sink, map, mode);
        } else if (value instanceof Set<?> set) {
            writeHead(sink, 6, TAG_SET);
            writeSet(sink, set, mode);
        } else if (value instanceof List<?> list) {
            writeArray(sink, list, mode);
        } else if (value instanceof Collection<?>) {
            throw error(ErrorCode.UNSUPPORTED_VALUE, sink.position());
        } else {
            throw error(ErrorCode.UNSUPPORTED_VALUE, sink.position());
        }
    }

    private static int booleanPayloadSize(int length) {
        return checkedSize(2, length / 8, length % 8 == 0 ? 0 : 1);
    }

    private static void writeBooleans(Sink sink, boolean[] values) {
        int padding = (8 - values.length % 8) % 8;
        writeHead(sink, 2, booleanPayloadSize(values.length));
        sink.putByte(SUBTYPE_BOOLEANS);
        sink.putByte(padding);
        int index = 0;
        int complete = values.length & ~7;
        while (index < complete) {
            int bits = (values[index] ? 1 : 0)
                | (values[index + 1] ? 2 : 0)
                | (values[index + 2] ? 4 : 0)
                | (values[index + 3] ? 8 : 0)
                | (values[index + 4] ? 16 : 0)
                | (values[index + 5] ? 32 : 0)
                | (values[index + 6] ? 64 : 0)
                | (values[index + 7] ? 128 : 0);
            sink.putByte(bits);
            index += 8;
        }
        if (padding != 0) {
            int bits = 0;
            for (int bit = 0; index < values.length; bit++, index++) {
                bits |= (values[index] ? 1 : 0) << bit;
            }
            sink.putByte(bits);
        }
    }

    private static void writeLong(Sink sink, long value) {
        if (value >= 0) {
            writeHead(sink, 0, value);
        } else {
            writeHead(sink, 1, ~value);
        }
    }

    private static void writeBigInteger(Sink sink, BigInteger value) {
        if (value.compareTo(LONG_MIN) >= 0 && value.compareTo(LONG_MAX) <= 0) {
            writeLong(sink, value.longValue());
            return;
        }

        boolean negative = value.signum() < 0;
        BigInteger magnitude = negative
            ? value.negate().subtract(BigInteger.ONE)
            : value;
        byte[] bytes = unsignedMagnitude(magnitude);
        writeHead(sink, 6, negative ? TAG_NEGATIVE_BIGNUM : TAG_POSITIVE_BIGNUM);
        writeHead(sink, 2, bytes.length);
        sink.putBytes(bytes);
    }

    private static byte[] unsignedMagnitude(BigInteger value) {
        byte[] bytes = value.toByteArray();
        return bytes.length > 1 && bytes[0] == 0
            ? Arrays.copyOfRange(bytes, 1, bytes.length)
            : bytes;
    }

    private static void writeDecimal(Sink sink, BigDecimal input) {
        BigDecimal value = normalizedDecimal(input);
        long exponent = -(long) value.scale();
        checkDecimalExponent(exponent, sink.position());
        writeHead(sink, 6, TAG_DECIMAL);
        writeHead(sink, 4, 2);
        writeLong(sink, exponent);
        writeBigInteger(sink, value.unscaledValue());
    }

    private static void writeRatio(Sink sink,
                                   BigInteger numerator,
                                   BigInteger denominator) {
        validateRatio(numerator, denominator, sink.position());
        writeHead(sink, 6, TAG_RATIO);
        writeHead(sink, 4, 2);
        writeBigInteger(sink, numerator);
        writeBigInteger(sink, denominator);
    }

    private static void writeInstant(Sink sink, long milliseconds) {
        long seconds = Math.floorDiv(milliseconds, 1000);
        int remainder = Math.floorMod(milliseconds, 1000);
        writeHead(sink, 6, TAG_EXTENDED_TIME);
        writeHead(sink, 5, remainder == 0 ? 1 : 2);
        writeLong(sink, 1);
        writeLong(sink, seconds);
        if (remainder != 0) {
            writeLong(sink, -3);
            writeLong(sink, remainder);
        }
    }

    private static void writeExtensionHead(Sink sink, int itemCount) {
        writeHead(sink, 6, DRAFT_EXTENSION_TAG);
        writeHead(sink, 4, itemCount);
    }

    private static void writeNamedValue(Sink sink,
                                        int subtype,
                                        int qualifiedSubtype,
                                        String namespace,
                                        String name) {
        int nameLength = utf8Length(name, sink.position());
        int namespaceLength = namespace == null
            ? 0 : utf8Length(namespace, sink.position());
        int length = checkedSize(1, nameLength);
        if (namespace != null) {
            length = checkedSize(length, unsignedVarintSize(namespaceLength),
                                 namespaceLength);
        }
        writeHead(sink, 2, length);
        sink.putByte(namespace == null ? subtype : qualifiedSubtype);
        if (namespace != null) {
            writeUnsignedVarint(sink, namespaceLength);
            sink.putText(namespace, namespaceLength);
        }
        sink.putText(name, nameLength);
    }

    private static void writeUnsignedVarint(Sink sink, int value) {
        while (value >= 128) {
            sink.putByte((value & 0x7f) | 0x80);
            value >>>= 7;
        }
        sink.putByte(value);
    }

    private static void writeCollectionExtension(Sink sink,
                                                 long typeId,
                                                 Collection<?> values,
                                                 Mode mode) {
        writeExtensionHead(sink, extensionItemCount(values.size()));
        writeLong(sink, typeId);
        for (Object value : values) {
            writeValue(sink, value, mode);
        }
    }

    private static long instantMillis(Instant instant, int offset) {
        try {
            return instant.toEpochMilli();
        } catch (ArithmeticException exception) {
            throw error(ErrorCode.INVALID_INSTANT, offset);
        }
    }

    private static void writeTypedArrayHead(Sink sink,
                                            long tag,
                                            int length,
                                            int width) {
        final int byteLength;
        try {
            byteLength = Math.multiplyExact(length, width);
        } catch (ArithmeticException exception) {
            throw error(ErrorCode.LENGTH_OUT_OF_RANGE, sink.position());
        }
        writeHead(sink, 6, tag);
        writeHead(sink, 2, byteLength);
    }

    private static BigDecimal normalizedDecimal(BigDecimal input) {
        return input.signum() == 0 ? BigDecimal.ZERO : input.stripTrailingZeros();
    }

    private static void checkDecimalExponent(long exponent, int offset) {
        if (exponent < Integer.MIN_VALUE || exponent > Integer.MAX_VALUE) {
            throw error(ErrorCode.INVALID_DECIMAL, offset);
        }
    }

    private static void validateRatio(BigInteger numerator,
                                      BigInteger denominator,
                                      int offset) {
        if (denominator.signum() <= 0
            || !numerator.gcd(denominator).equals(BigInteger.ONE)) {
            throw error(ErrorCode.INVALID_RATIO, offset);
        }
    }

    private static void writeText(Sink sink, String value) {
        int length = utf8Length(value, sink.position());
        writeHead(sink, 3, length);
        sink.putText(value, length);
    }

    private static void encodeUtf8(String value, byte[] output, int position) {
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (character <= 0x7f) {
                output[position++] = (byte) character;
            } else if (character <= 0x7ff) {
                output[position++] = (byte) (0xc0 | (character >>> 6));
                output[position++] = (byte) (0x80 | (character & 0x3f));
            } else if (Character.isHighSurrogate(character)) {
                int codePoint = Character.toCodePoint(character,
                                                      value.charAt(++index));
                output[position++] = (byte) (0xf0 | (codePoint >>> 18));
                output[position++] = (byte) (0x80 | ((codePoint >>> 12) & 0x3f));
                output[position++] = (byte) (0x80 | ((codePoint >>> 6) & 0x3f));
                output[position++] = (byte) (0x80 | (codePoint & 0x3f));
            } else {
                output[position++] = (byte) (0xe0 | (character >>> 12));
                output[position++] = (byte) (0x80 | ((character >>> 6) & 0x3f));
                output[position++] = (byte) (0x80 | (character & 0x3f));
            }
        }
    }

    private static int utf8Length(String value, int offset) {
        int length = 0;
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            int width;
            if (character <= 0x7f) {
                width = 1;
            } else if (character <= 0x7ff) {
                width = 2;
            } else if (Character.isHighSurrogate(character)) {
                if (index + 1 >= value.length()
                    || !Character.isLowSurrogate(value.charAt(index + 1))) {
                    throw error(ErrorCode.INVALID_UNICODE, offset);
                }
                index++;
                width = 4;
            } else if (Character.isLowSurrogate(character)) {
                throw error(ErrorCode.INVALID_UNICODE, offset);
            } else {
                width = 3;
            }
            try {
                length = Math.addExact(length, width);
            } catch (ArithmeticException exception) {
                throw error(ErrorCode.LENGTH_OUT_OF_RANGE, offset);
            }
        }
        return length;
    }

    private static void validateUtf8(ByteBuffer input,
                                     int start,
                                     int length,
                                     int errorOffset) {
        int index = start;
        int end = start + length;
        while (index < end) {
            int first = input.get(index) & 0xff;
            if (first <= 0x7f) {
                index++;
                continue;
            }
            if (first >= 0xc2 && first <= 0xdf) {
                if (index + 1 >= end
                    || !isUtf8Continuation(input.get(index + 1) & 0xff)) {
                    throw error(ErrorCode.INVALID_UTF8, errorOffset);
                }
                index += 2;
                continue;
            }
            if (first >= 0xe0 && first <= 0xef) {
                if (index + 2 >= end) {
                    throw error(ErrorCode.INVALID_UTF8, errorOffset);
                }
                int second = input.get(index + 1) & 0xff;
                int third = input.get(index + 2) & 0xff;
                if (!isUtf8Continuation(second)
                    || !isUtf8Continuation(third)
                    || first == 0xe0 && second < 0xa0
                    || first == 0xed && second >= 0xa0) {
                    throw error(ErrorCode.INVALID_UTF8, errorOffset);
                }
                index += 3;
                continue;
            }
            if (first >= 0xf0 && first <= 0xf4) {
                if (index + 3 >= end) {
                    throw error(ErrorCode.INVALID_UTF8, errorOffset);
                }
                int second = input.get(index + 1) & 0xff;
                int third = input.get(index + 2) & 0xff;
                int fourth = input.get(index + 3) & 0xff;
                if (!isUtf8Continuation(second)
                    || !isUtf8Continuation(third)
                    || !isUtf8Continuation(fourth)
                    || first == 0xf0 && second < 0x90
                    || first == 0xf4 && second > 0x8f) {
                    throw error(ErrorCode.INVALID_UTF8, errorOffset);
                }
                index += 4;
                continue;
            }
            throw error(ErrorCode.INVALID_UTF8, errorOffset);
        }
    }

    private static void validateUtf8(byte[] input,
                                     int start,
                                     int length,
                                     int errorOffset) {
        int index = start;
        int end = start + length;
        while (index < end) {
            int first = input[index] & 0xff;
            if (first <= 0x7f) {
                index++;
                continue;
            }
            if (first >= 0xc2 && first <= 0xdf) {
                if (index + 1 >= end
                    || !isUtf8Continuation(input[index + 1] & 0xff)) {
                    throw error(ErrorCode.INVALID_UTF8, errorOffset);
                }
                index += 2;
                continue;
            }
            if (first >= 0xe0 && first <= 0xef) {
                if (index + 2 >= end) {
                    throw error(ErrorCode.INVALID_UTF8, errorOffset);
                }
                int second = input[index + 1] & 0xff;
                int third = input[index + 2] & 0xff;
                if (!isUtf8Continuation(second)
                    || !isUtf8Continuation(third)
                    || first == 0xe0 && second < 0xa0
                    || first == 0xed && second >= 0xa0) {
                    throw error(ErrorCode.INVALID_UTF8, errorOffset);
                }
                index += 3;
                continue;
            }
            if (first >= 0xf0 && first <= 0xf4) {
                if (index + 3 >= end) {
                    throw error(ErrorCode.INVALID_UTF8, errorOffset);
                }
                int second = input[index + 1] & 0xff;
                int third = input[index + 2] & 0xff;
                int fourth = input[index + 3] & 0xff;
                if (!isUtf8Continuation(second)
                    || !isUtf8Continuation(third)
                    || !isUtf8Continuation(fourth)
                    || first == 0xf0 && second < 0x90
                    || first == 0xf4 && second > 0x8f) {
                    throw error(ErrorCode.INVALID_UTF8, errorOffset);
                }
                index += 4;
                continue;
            }
            throw error(ErrorCode.INVALID_UTF8, errorOffset);
        }
    }

    private static boolean isUtf8Continuation(int value) {
        return value >= 0x80 && value <= 0xbf;
    }

    private static byte[] decodeBytes(int length) {
        byte[] bytes = DECODE_BYTES.get();
        if (bytes.length < length) {
            bytes = new byte[length];
            DECODE_BYTES.set(bytes);
        }
        return bytes;
    }

    private static void writeArray(Sink sink, List<?> values, Mode mode) {
        writeHead(sink, 4, values.size());
        for (Object value : values) {
            writeValue(sink, value, mode);
        }
    }

    private static void writeMap(Sink sink, Map<?, ?> map, Mode mode) {
        if (mode == Mode.CANONICAL && asciiStringKeys(map)) {
            Object[] entries = map.entrySet().toArray();
            insertionSort(entries, (left, right) -> {
                String leftKey = (String) ((Map.Entry<?, ?>) left).getKey();
                String rightKey = (String) ((Map.Entry<?, ?>) right).getKey();
                int length = Integer.compare(encodedAsciiSize(leftKey),
                                             encodedAsciiSize(rightKey));
                return length != 0 ? length : leftKey.compareTo(rightKey);
            });
            writeHead(sink, 5, entries.length);
            String previous = null;
            for (Object item : entries) {
                Map.Entry<?, ?> entry = (Map.Entry<?, ?>) item;
                String key = (String) entry.getKey();
                if (key.equals(previous)) {
                    throw error(ErrorCode.DUPLICATE_KEY, sink.position());
                }
                previous = key;
                writeText(sink, key);
                writeValue(sink, entry.getValue(), mode);
            }
            return;
        }

        if (mode == Mode.FAST && hostMapKeysGuaranteeUniqueness(map)) {
            writeHead(sink, 5, map.size());
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                writeValue(sink, entry.getKey(), mode);
                writeValue(sink, entry.getValue(), mode);
            }
            return;
        }

        writeMapEntries(sink, map.entrySet(), mode);
    }

    private static void writeMapEntries(
        Sink sink, Collection<? extends Map.Entry<?, ?>> input, Mode mode) {
        List<PreparedEntry> entries = new ArrayList<>(input.size());
        for (Map.Entry<?, ?> entry : input) {
            entries.add(new PreparedEntry(encode(entry.getKey(), Mode.CANONICAL),
                                          entry.getKey(),
                                          entry.getValue()));
        }
        if (mode == Mode.CANONICAL) {
            entries.sort((left, right) -> compareCanonical(left.keyBytes,
                                                           right.keyBytes));
            rejectAdjacentEntryDuplicates(entries, sink.position());
        } else {
            rejectDuplicateEntries(entries, ErrorCode.DUPLICATE_KEY,
                                   sink.position());
        }
        writeHead(sink, 5, entries.size());
        for (PreparedEntry entry : entries) {
            if (mode == Mode.CANONICAL) {
                sink.putBytes(entry.keyBytes);
            } else {
                writeValue(sink, entry.key, mode);
            }
            writeValue(sink, entry.value, mode);
        }
    }

    private static void writeSet(Sink sink, Set<?> set, Mode mode) {
        long[] integerValues = mode == Mode.CANONICAL
            ? fixedIntegerValues(set)
            : null;
        if (integerValues != null) {
            insertionSortCanonicalLongs(integerValues);
            writeHead(sink, 4, integerValues.length);
            for (int index = 0; index < integerValues.length; index++) {
                long value = integerValues[index];
                if (index > 0 && value == integerValues[index - 1]) {
                    throw error(ErrorCode.DUPLICATE_SET_MEMBER, sink.position());
                }
                writeLong(sink, value);
            }
            return;
        }

        if (mode == Mode.FAST && hostSetValuesGuaranteeUniqueness(set)) {
            writeHead(sink, 4, set.size());
            for (Object value : set) {
                writeValue(sink, value, mode);
            }
            return;
        }

        writeSetMembers(sink, set, mode);
    }

    private static void writeSetMembers(Sink sink, Collection<?> input, Mode mode) {
        List<PreparedValue> values = new ArrayList<>(input.size());
        for (Object value : input) {
            values.add(new PreparedValue(encode(value, Mode.CANONICAL), value));
        }
        if (mode == Mode.CANONICAL) {
            values.sort((left, right) -> compareCanonical(left.bytes, right.bytes));
            for (int index = 1; index < values.size(); index++) {
                if (Arrays.equals(values.get(index - 1).bytes,
                                  values.get(index).bytes)) {
                    throw error(ErrorCode.DUPLICATE_SET_MEMBER,
                                sink.position());
                }
            }
        } else {
            HashSet<ByteArrayKey> seen = new HashSet<>();
            for (PreparedValue value : values) {
                if (!seen.add(new ByteArrayKey(value.bytes))) {
                    throw error(ErrorCode.DUPLICATE_SET_MEMBER,
                                sink.position());
                }
            }
        }
        writeHead(sink, 4, values.size());
        for (PreparedValue value : values) {
            if (mode == Mode.CANONICAL) {
                sink.putBytes(value.bytes);
            } else {
                writeValue(sink, value.value, mode);
            }
        }
    }

    private static void rejectDuplicateEntries(List<PreparedEntry> entries,
                                               ErrorCode code,
                                               int offset) {
        HashSet<ByteArrayKey> seen = new HashSet<>();
        for (PreparedEntry entry : entries) {
            if (!seen.add(new ByteArrayKey(entry.keyBytes))) {
                throw error(code, offset);
            }
        }
    }

    private static void rejectAdjacentEntryDuplicates(List<PreparedEntry> entries,
                                                      int offset) {
        for (int index = 1; index < entries.size(); index++) {
            if (Arrays.equals(entries.get(index - 1).keyBytes,
                              entries.get(index).keyBytes)) {
                throw error(ErrorCode.DUPLICATE_KEY, offset);
            }
        }
    }

    private static boolean hostMapKeysGuaranteeUniqueness(Map<?, ?> map) {
        // Identity maps and arbitrary comparators cannot guarantee wire
        // uniqueness even for otherwise safe scalar classes.
        if (!(map instanceof IPersistentMap || map instanceof HashMap<?, ?>)) {
            return false;
        }
        Class<?> valueClass = null;
        boolean sawNull = false;
        for (Object key : map.keySet()) {
            if (key == null) {
                if (valueClass != null) {
                    return false;
                }
                sawNull = true;
            } else {
                if (sawNull || !safeHostEqualityClass(key.getClass())) {
                    return false;
                }
                if (valueClass == null) {
                    valueClass = key.getClass();
                } else if (valueClass != key.getClass()) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean asciiStringKeys(Map<?, ?> map) {
        for (Object key : map.keySet()) {
            if (!(key instanceof String text) || !isAscii(text)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isAscii(String value) {
        for (int index = 0; index < value.length(); index++) {
            if (value.charAt(index) > 0x7f) {
                return false;
            }
        }
        return true;
    }

    private static int encodedAsciiSize(String value) {
        return checkedSize(headSize(value.length()), value.length());
    }

    private static long[] fixedIntegerValues(Set<?> set) {
        long[] values = new long[set.size()];
        Class<?> valueClass = null;
        int index = 0;
        for (Object value : set) {
            if (!(value instanceof Byte
                  || value instanceof Short
                  || value instanceof Integer
                  || value instanceof Long)) {
                return null;
            }
            if (valueClass == null) {
                valueClass = value.getClass();
            } else if (valueClass != value.getClass()) {
                return null;
            }
            values[index++] = ((Number) value).longValue();
        }
        return values;
    }

    private static int compareCanonicalLongs(long left, long right) {
        long leftArgument = left >= 0 ? left : ~left;
        long rightArgument = right >= 0 ? right : ~right;
        int length = Integer.compare(headSize(leftArgument),
                                     headSize(rightArgument));
        if (length != 0) {
            return length;
        }
        int major = Boolean.compare(left < 0, right < 0);
        return major != 0 ? major : Long.compare(leftArgument, rightArgument);
    }

    private static void insertionSortCanonicalLongs(long[] values) {
        for (int index = 1; index < values.length; index++) {
            long value = values[index];
            int insertion = index;
            while (insertion > 0
                   && compareCanonicalLongs(values[insertion - 1], value) > 0) {
                values[insertion] = values[insertion - 1];
                insertion--;
            }
            values[insertion] = value;
        }
    }

    private static void insertionSort(Object[] values,
                                      Comparator<Object> comparator) {
        for (int index = 1; index < values.length; index++) {
            Object value = values[index];
            int insertion = index;
            while (insertion > 0
                   && comparator.compare(values[insertion - 1], value) > 0) {
                values[insertion] = values[insertion - 1];
                insertion--;
            }
            values[insertion] = value;
        }
    }

    private static boolean hostSetValuesGuaranteeUniqueness(Set<?> set) {
        if (!(set instanceof IPersistentSet || set instanceof HashSet<?>)) {
            return false;
        }
        Class<?> valueClass = null;
        boolean sawNull = false;
        for (Object value : set) {
            if (value == null) {
                if (valueClass != null) {
                    return false;
                }
                sawNull = true;
            } else {
                if (sawNull || !safeHostEqualityClass(value.getClass())) {
                    return false;
                }
                if (valueClass == null) {
                    valueClass = value.getClass();
                } else if (valueClass != value.getClass()) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean safeHostEqualityClass(Class<?> valueClass) {
        return valueClass == String.class
            || valueClass == Boolean.class
            || valueClass == Byte.class
            || valueClass == Short.class
            || valueClass == Integer.class
            || valueClass == Long.class
            || valueClass == BigInt.class
            || valueClass == BigInteger.class
            || valueClass == Character.class
            || valueClass == Keyword.class
            || valueClass == Symbol.class
            || valueClass == UUID.class;
    }

    private static boolean safeDecodedSetMemberEquality(Object value) {
        return value == null || safeHostEqualityClass(value.getClass());
    }

    private static int compareCanonical(byte[] left, byte[] right) {
        int length = Integer.compare(left.length, right.length);
        return length != 0 ? length : Arrays.compareUnsigned(left, right);
    }

    private static void writeHead(Sink sink, int major, long argument) {
        if (argument < 0) {
            throw error(ErrorCode.INTEGER_OUT_OF_RANGE, sink.position());
        }
        int prefix = major << 5;
        if (argument <= 23) {
            sink.putByte(prefix | (int) argument);
        } else if (argument <= 0xff) {
            sink.putByte(prefix | 24);
            sink.putByte((int) argument);
        } else if (argument <= 0xffff) {
            sink.putByte(prefix | 25);
            putShort(sink, (int) argument);
        } else if (argument <= 0xffffffffL) {
            sink.putByte(prefix | 26);
            putInt(sink, (int) argument);
        } else {
            sink.putByte(prefix | 27);
            putLong(sink, argument);
        }
    }

    private static void putShort(Sink sink, int value) {
        sink.putByte(value >>> 8);
        sink.putByte(value);
    }

    private static void putInt(Sink sink, int value) {
        sink.putByte(value >>> 24);
        sink.putByte(value >>> 16);
        sink.putByte(value >>> 8);
        sink.putByte(value);
    }

    private static void putLong(Sink sink, long value) {
        sink.putByte((int) (value >>> 56));
        sink.putByte((int) (value >>> 48));
        sink.putByte((int) (value >>> 40));
        sink.putByte((int) (value >>> 32));
        sink.putByte((int) (value >>> 24));
        sink.putByte((int) (value >>> 16));
        sink.putByte((int) (value >>> 8));
        sink.putByte((int) value);
    }

    private static final class PreparedEntry {
        private final byte[] keyBytes;
        private final Object key;
        private final Object value;

        private PreparedEntry(byte[] keyBytes, Object key, Object value) {
            this.keyBytes = keyBytes;
            this.key = key;
            this.value = value;
        }
    }

    private static final class PreparedValue {
        private final byte[] bytes;
        private final Object value;

        private PreparedValue(byte[] bytes, Object value) {
            this.bytes = bytes;
            this.value = value;
        }
    }

    private static final class ByteArrayKey {
        private final byte[] bytes;
        private final int hash;

        private ByteArrayKey(byte[] bytes) {
            this.bytes = bytes;
            this.hash = Arrays.hashCode(bytes);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof ByteArrayKey that
                && Arrays.equals(bytes, that.bytes);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    private static final class Decoder {
        private ByteBuffer input;
        private byte[] arrayInput;
        private int arrayPosition;
        private int arrayLimit;
        private boolean canonical;
        private Limits limits;
        private int origin;
        private boolean littleEndian;
        private int activeLimit;
        private ErrorCode activeLimitCode;

        private Decoder() {
        }

        private void reset(ByteBuffer input, boolean canonical, Limits limits) {
            this.input = input;
            this.arrayInput = null;
            this.canonical = canonical;
            this.limits = limits;
            this.origin = input.position();
            this.littleEndian = input.order() == ByteOrder.LITTLE_ENDIAN;
            this.activeLimit = -1;
            this.activeLimitCode = null;
        }

        private void reset(byte[] input,
                           int start,
                           int length,
                           boolean canonical,
                           Limits limits) {
            this.input = null;
            this.arrayInput = input;
            this.arrayPosition = start;
            this.arrayLimit = start + length;
            this.canonical = canonical;
            this.limits = limits;
            this.origin = start;
            this.littleEndian = true;
            this.activeLimit = -1;
            this.activeLimitCode = null;
        }

        private void clear() {
            input = null;
            arrayInput = null;
            arrayPosition = 0;
            arrayLimit = 0;
            limits = null;
            origin = 0;
            littleEndian = false;
            activeLimit = -1;
            activeLimitCode = null;
        }

        private int offset() {
            return inputPosition() - origin;
        }

        private Object readValue(int depth) {
            if (depth > limits.maxDepth) {
                throw error(ErrorCode.DEPTH_LIMIT, offset());
            }
            int headOffset = offset();
            int head = readUnsignedByte();
            int major = head >>> 5;
            int additional = head & 0x1f;
            return switch (major) {
                case 0 -> readPositiveInteger(additional, headOffset);
                case 1 -> readNegativeInteger(additional, headOffset);
                case 2 -> readByteStringValue(additional, headOffset);
                case 3 -> readText(additional, headOffset);
                case 4 -> readArray(additional, headOffset, depth);
                case 5 -> readMap(additional, headOffset, depth);
                case 6 -> readTagged(additional, headOffset, depth);
                case 7 -> readSimple(additional, headOffset);
                default -> throw new AssertionError("unreachable CBOR major type");
            };
        }

        private Long readPositiveInteger(int additional, int headOffset) {
            return argument(additional, headOffset);
        }

        private Long readNegativeInteger(int additional, int headOffset) {
            return ~argument(additional, headOffset);
        }

        private byte[] readByteString(int additional,
                                      int headOffset,
                                      int limit,
                                      ErrorCode limitCode) {
            int length = length(additional, headOffset, limit, limitCode);
            require(length);
            byte[] bytes = new byte[length];
            getBytes(bytes, 0, length);
            return bytes;
        }

        private Object readByteStringValue(int additional, int headOffset) {
            int length = length(additional, headOffset, limits.maxStringBytes,
                                ErrorCode.STRING_LIMIT);
            require(length);
            if (length == 0) {
                throw error(ErrorCode.INVALID_EXTENSION, headOffset);
            }
            int end = inputPosition() + length;
            int subtype = readUnsignedByte();
            if (subtype == SUBTYPE_BYTES) {
                byte[] bytes = new byte[length - 1];
                getBytes(bytes, 0, bytes.length);
                return bytes;
            }
            if (subtype != SUBTYPE_KEYWORD && subtype != SUBTYPE_SYMBOL
                && subtype != SUBTYPE_QUALIFIED_KEYWORD
                && subtype != SUBTYPE_QUALIFIED_SYMBOL
                && subtype != SUBTYPE_CHARACTER && subtype != SUBTYPE_JAVA_REGEX
                && subtype != SUBTYPE_BOOLEANS) {
                throw error(ErrorCode.INVALID_EXTENSION, headOffset);
            }
            if (length > limits.maxExtensionBytes) {
                throw error(ErrorCode.EXTENSION_LIMIT, headOffset);
            }
            if (subtype == SUBTYPE_BOOLEANS) {
                return readBooleans(length, headOffset);
            }
            if (subtype == SUBTYPE_CHARACTER) {
                if (length != 2 && length != 3) {
                    throw error(ErrorCode.INVALID_EXTENSION, headOffset);
                }
                int codeUnit = readUnsignedByte();
                if (length == 3) {
                    if (codeUnit == 0) {
                        throw error(ErrorCode.NON_SHORTEST, headOffset);
                    }
                    codeUnit = (codeUnit << 8) | readUnsignedByte();
                }
                return Character.valueOf((char) codeUnit);
            }
            if (subtype == SUBTYPE_JAVA_REGEX) {
                int flags = validatedRegexFlags(
                    readUnsignedVarint(end, headOffset, ErrorCode.INVALID_REGEX,
                                       ErrorCode.INVALID_REGEX), headOffset);
                String source = readTextBytes(end - inputPosition());
                return compileRegex(source, flags, headOffset);
            }
            String namespace = null;
            if (subtype == SUBTYPE_QUALIFIED_KEYWORD
                || subtype == SUBTYPE_QUALIFIED_SYMBOL) {
                int namespaceLength = readUnsignedVarint(
                    end, headOffset, ErrorCode.INVALID_EXTENSION,
                    ErrorCode.LENGTH_OUT_OF_RANGE);
                if (namespaceLength > end - inputPosition()) {
                    throw error(ErrorCode.INVALID_EXTENSION, headOffset);
                }
                namespace = readTextBytes(namespaceLength);
            }
            String name = readTextBytes(end - inputPosition());
            return subtype == SUBTYPE_KEYWORD
                || subtype == SUBTYPE_QUALIFIED_KEYWORD
                ? Keyword.intern(namespace, name)
                : Symbol.intern(namespace, name);
        }

        private int readUnsignedVarint(int end, int headOffset,
                                       ErrorCode missingCode,
                                       ErrorCode overflowCode) {
            int value = 0;
            for (int shift = 0; shift <= 28; shift += 7) {
                if (inputPosition() == end) {
                    throw error(missingCode, headOffset);
                }
                int next = readUnsignedByte();
                if (shift == 28 && next > 7) {
                    throw error(overflowCode, headOffset);
                }
                value |= (next & 0x7f) << shift;
                if ((next & 0x80) == 0) {
                    if (shift != 0 && next == 0) {
                        throw error(ErrorCode.NON_SHORTEST, headOffset);
                    }
                    return value;
                }
            }
            throw new AssertionError("unreachable unsigned varint");
        }

        private String readText(int additional, int headOffset) {
            int length = length(additional, headOffset,
                                limits.maxStringBytes,
                                ErrorCode.STRING_LIMIT);
            return readTextBytes(length);
        }

        private String readTextBytes(int length) {
            require(length);
            int start = inputPosition();
            validateInputUtf8(start, length, offset());
            if (arrayInput != null) {
                String result = new String(arrayInput, start, length,
                                           StandardCharsets.UTF_8);
                arrayPosition = start + length;
                return result;
            }
            if (input.hasArray()) {
                String result = new String(input.array(),
                                           input.arrayOffset() + start,
                                           length,
                                           StandardCharsets.UTF_8);
                input.position(start + length);
                return result;
            }
            byte[] bytes = decodeBytes(length);
            getBytes(bytes, 0, length);
            return new String(bytes, 0, length, StandardCharsets.UTF_8);
        }

        private boolean[] readBooleans(int length, int headOffset) {
            // The byte-string dispatcher has already bounded the full payload.
            if (length < 2) {
                throw error(ErrorCode.INVALID_TYPED_ARRAY, headOffset);
            }
            int padding = nextUnsignedByte();
            int payload = length - 2;
            if (padding > 7 || payload == 0 && padding != 0) {
                throw error(ErrorCode.INVALID_TYPED_ARRAY, headOffset);
            }
            long count = (long) payload * 8 - padding;
            if (count > limits.maxCollectionLength) {
                throw error(ErrorCode.COLLECTION_LIMIT, headOffset);
            }
            if (padding != 0
                && (inputByteAt(inputPosition() + payload - 1) >>> (8 - padding)) != 0) {
                throw error(ErrorCode.INVALID_TYPED_ARRAY, headOffset);
            }
            boolean[] values = new boolean[(int) count];
            int index = 0;
            int complete = values.length & ~7;
            while (index < complete) {
                int bits = nextUnsignedByte();
                values[index] = (bits & 1) != 0;
                values[index + 1] = (bits & 2) != 0;
                values[index + 2] = (bits & 4) != 0;
                values[index + 3] = (bits & 8) != 0;
                values[index + 4] = (bits & 16) != 0;
                values[index + 5] = (bits & 32) != 0;
                values[index + 6] = (bits & 64) != 0;
                values[index + 7] = (bits & 128) != 0;
                index += 8;
            }
            if (padding != 0) {
                int bits = nextUnsignedByte();
                for (int bit = 0; index < values.length; bit++, index++) {
                    values[index] = (bits & (1 << bit)) != 0;
                }
            }
            return values;
        }

        private PersistentVector readArray(int additional,
                                           int headOffset,
                                           int depth) {
            int length = collectionLength(additional, headOffset);
            Object[] values = new Object[length];
            for (int index = 0; index < length; index++) {
                values[index] = readValue(depth + 1);
            }
            return PersistentVector.adopt(values);
        }

        private Object readMap(int additional,
                               int headOffset,
                               int depth) {
            int length = collectionLength(additional, headOffset);
            HashSet<ByteArrayKey> seen = canonical
                ? null
                : new HashSet<>(Math.min(length, 1024));
            int previousStart = -1;
            int previousEnd = -1;
            boolean small = length <= 8;
            Object[] keyValues = small ? new Object[length * 2] : null;
            ITransientMap largeResult = small
                ? null
                : PersistentArrayMap.EMPTY.asTransient();
            boolean lossless = false;
            List<Map.Entry<?, ?>> entries = null;
            for (int index = 0; index < length; index++) {
                int keyStart = inputPosition();
                int keyOffset = offset();
                Object key = readValue(depth + 1);
                int keyEnd = inputPosition();
                byte[] canonicalKey = canonical ? null : encode(key, Mode.CANONICAL);
                if (canonical && previousStart >= 0) {
                    int comparison = compareInputRanges(previousStart,
                                                        previousEnd,
                                                        keyStart,
                                                        keyEnd);
                    if (comparison == 0) {
                        throw error(ErrorCode.DUPLICATE_KEY, keyOffset);
                    }
                    if (comparison > 0) {
                        throw error(ErrorCode.NON_CANONICAL, keyOffset);
                    }
                } else if (!canonical
                           && !seen.add(new ByteArrayKey(canonicalKey))) {
                    throw error(ErrorCode.DUPLICATE_KEY, keyOffset);
                }
                previousStart = keyStart;
                previousEnd = keyEnd;
                if (small && !lossless) {
                    for (int prior = 0; prior < index; prior++) {
                        if (Util.equiv(keyValues[prior * 2], key)) {
                            lossless = true;
                            break;
                        }
                    }
                } else if (!small && !lossless
                           && largeResult.valAt(key, NOT_FOUND) != NOT_FOUND) {
                    // Preserve all prior entries before an assoc could merge
                    // this wire-distinct key with a host-equivalent key.
                    Map<?, ?> prior = (Map<?, ?>) largeResult.persistent();
                    entries = new ArrayList<>(length);
                    entries.addAll(prior.entrySet());
                    largeResult = null;
                    lossless = true;
                }
                Object value = readValue(depth + 1);
                if (small) {
                    keyValues[index * 2] = key;
                    keyValues[index * 2 + 1] = value;
                } else if (lossless) {
                    entries.add(new SimpleImmutableEntry<>(key, value));
                } else {
                    largeResult = largeResult.assoc(key, value);
                }
            }
            if (lossless) {
                if (small) {
                    entries = new ArrayList<>(length);
                    for (int index = 0; index < length; index++) {
                        entries.add(new SimpleImmutableEntry<>(
                            keyValues[index * 2], keyValues[index * 2 + 1]));
                    }
                }
                return new MapValue(entries);
            }
            return small
                ? PersistentArrayMap.createAsIfByAssoc(keyValues)
                : largeResult.persistent();
        }

        private Object readTagged(int additional,
                                  int headOffset,
                                  int depth) {
            long tag = argument(additional, headOffset);
            if (tag == TAG_POSITIVE_BIGNUM || tag == TAG_NEGATIVE_BIGNUM) {
                return readBignum(tag == TAG_NEGATIVE_BIGNUM, headOffset);
            }
            if (tag == TAG_UUID) {
                if (depth + 1 > limits.maxDepth) {
                    throw error(ErrorCode.DEPTH_LIMIT, offset());
                }
                int payloadOffset = offset();
                int payloadHead = readUnsignedByte();
                if ((payloadHead >>> 5) != 2) {
                    throw error(ErrorCode.INVALID_UUID, headOffset);
                }
                int length = length(payloadHead & 0x1f, payloadOffset,
                                    limits.maxStringBytes, ErrorCode.STRING_LIMIT);
                require(length);
                if (length != 16) {
                    throw error(ErrorCode.INVALID_UUID, headOffset);
                }
                return new UUID(readLong(), readLong());
            }
            if (tag == TAG_SET) {
                return readSet(headOffset, depth + 1);
            }
            if (typedArrayWidth(tag) != 0) {
                return readTypedArray(tag, headOffset);
            }
            if (tag == TAG_EXTENDED_TIME) {
                return readInstant(headOffset, depth + 1);
            }
            if (tag == DRAFT_EXTENSION_TAG) {
                return readExtension(headOffset, depth + 1);
            }
            Object value = readValue(depth + 1);
            if (tag == TAG_DECIMAL) {
                return decodeDecimal(value, headOffset);
            }
            if (tag == TAG_RATIO) {
                return decodeRatio(value, headOffset);
            }
            if (tag == TAG_URI) {
                if (!(value instanceof String text) || text.isEmpty()) {
                    throw error(ErrorCode.INVALID_URI, headOffset);
                }
                try {
                    URI uri = URI.create(text);
                    if (!uri.toASCIIString().equals(text)) {
                        throw error(ErrorCode.INVALID_URI, headOffset);
                    }
                    return uri;
                } catch (IllegalArgumentException exception) {
                    throw error(ErrorCode.INVALID_URI, headOffset);
                }
            }
            return new TaggedValue(tag, value);
        }

        private Object readExtension(int tagOffset, int depth) {
            int previousLimit = activeLimit;
            ErrorCode previousLimitCode = activeLimitCode;
            long candidate = (long) inputPosition() + limits.maxExtensionBytes;
            int candidateLimit = candidate > Integer.MAX_VALUE
                ? Integer.MAX_VALUE
                : (int) candidate;
            if (activeLimit < 0 || candidateLimit < activeLimit) {
                activeLimit = candidateLimit;
                activeLimitCode = ErrorCode.EXTENSION_LIMIT;
            }
            try {
                return readExtensionWithinLimit(tagOffset, depth);
            } finally {
                activeLimit = previousLimit;
                activeLimitCode = previousLimitCode;
            }
        }

        private Object readExtensionWithinLimit(int tagOffset, int depth) {
            if (depth > limits.maxDepth) {
                throw error(ErrorCode.DEPTH_LIMIT, offset());
            }
            int arrayOffset = offset();
            int arrayHead = readUnsignedByte();
            if ((arrayHead >>> 5) != 4) {
                throw error(ErrorCode.INVALID_EXTENSION, tagOffset);
            }
            int length = collectionLength(arrayHead & 0x1f, arrayOffset);
            if (length == 0) {
                throw error(ErrorCode.INVALID_EXTENSION, tagOffset);
            }
            Object typeId = readValue(depth + 1);
            if (typeId instanceof Long integerId) {
                if (integerId < 0) {
                    throw error(ErrorCode.INVALID_EXTENSION, tagOffset);
                }
                if (integerId > Integer.MAX_VALUE) {
                    return readUnknownExtension(typeId, length, depth);
                }
                return switch (integerId.intValue()) {
                    case (int) EXT_KEYWORD, (int) EXT_SYMBOL,
                         (int) EXT_CHARACTER, (int) EXT_REGEX ->
                        throw error(ErrorCode.INVALID_EXTENSION, tagOffset);
                    case (int) EXT_LIST ->
                        readListExtension(length, depth);
                    case (int) EXT_QUEUE ->
                        readQueueExtension(length, depth);
                    default -> readUnknownExtension(typeId, length, depth);
                };
            }
            if (typeId instanceof String name && !name.isEmpty()) {
                return readUnknownExtension(name, length, depth);
            }
            throw error(ErrorCode.INVALID_EXTENSION, tagOffset);
        }

        private IPersistentList readListExtension(int length, int depth) {
            Object[] values = readExtensionArguments(length, depth);
            return PersistentList.create(Arrays.asList(values));
        }

        private PersistentQueue readQueueExtension(int length, int depth) {
            PersistentQueue result = PersistentQueue.EMPTY;
            for (int index = 1; index < length; index++) {
                result = result.cons(readValue(depth + 1));
            }
            return result;
        }

        private Pattern compileRegex(String source, int flags, int headOffset) {
            try {
                Pattern result = Pattern.compile(source, flags);
                if (result.flags() != flags) {
                    throw error(ErrorCode.INVALID_REGEX, headOffset);
                }
                return result;
            } catch (IllegalArgumentException | StackOverflowError exception) {
                throw error(ErrorCode.INVALID_REGEX, headOffset);
            }
        }

        private ExtensionValue readUnknownExtension(Object typeId,
                                                    int length,
                                                    int depth) {
            List<?> arguments = PersistentVector.adopt(
                readExtensionArguments(length, depth));
            return typeId instanceof Long integerId
                ? new ExtensionValue(integerId, arguments)
                : new ExtensionValue((String) typeId, arguments);
        }

        private Object[] readExtensionArguments(int length, int depth) {
            int argumentCount = length - 1;
            if (activeLimit >= 0
                && argumentCount > activeLimit - inputPosition()) {
                throw error(activeLimitCode, offset());
            }
            Object[] arguments = new Object[argumentCount];
            for (int index = 0; index < arguments.length; index++) {
                arguments[index] = readValue(depth + 1);
            }
            return arguments;
        }

        private Date readInstant(int tagOffset, int depth) {
            if (depth > limits.maxDepth) {
                throw error(ErrorCode.DEPTH_LIMIT, offset());
            }
            int mapHeadOffset = offset();
            int mapHead = readUnsignedByte();
            if ((mapHead >>> 5) != 5) {
                throw error(ErrorCode.INVALID_INSTANT, tagOffset);
            }
            int length = collectionLength(mapHead & 0x1f, mapHeadOffset);
            int previousStart = -1;
            int previousEnd = -1;
            HashSet<ByteArrayKey> seen = canonical || length <= 2
                ? null
                : new HashSet<>(Math.min(length, 4));
            Object previousFastKey = null;
            boolean previousFastKeyPresent = false;
            long seconds = 0;
            long remainder = 0;
            boolean secondsPresent = false;
            boolean remainderPresent = false;
            boolean invalid = length < 1 || length > 2;
            for (int index = 0; index < length; index++) {
                int keyStart = inputPosition();
                int keyOffset = offset();
                Object key = readValue(depth + 1);
                int keyEnd = inputPosition();
                if (canonical && previousStart >= 0) {
                    int comparison = compareInputRanges(previousStart,
                                                        previousEnd,
                                                        keyStart,
                                                        keyEnd);
                    if (comparison == 0) {
                        throw error(ErrorCode.DUPLICATE_KEY, keyOffset);
                    }
                    if (comparison > 0) {
                        throw error(ErrorCode.NON_CANONICAL, keyOffset);
                    }
                } else if (!canonical && seen != null) {
                    byte[] canonicalKey = encode(key, Mode.CANONICAL);
                    if (!seen.add(new ByteArrayKey(canonicalKey))) {
                        throw error(ErrorCode.DUPLICATE_KEY, keyOffset);
                    }
                } else if (!canonical) {
                    if (previousFastKeyPresent) {
                        boolean duplicate = previousFastKey instanceof Long
                            && key instanceof Long
                            ? previousFastKey.equals(key)
                            : Arrays.equals(encode(previousFastKey,
                                                  Mode.CANONICAL),
                                            encode(key, Mode.CANONICAL));
                        if (duplicate) {
                            throw error(ErrorCode.DUPLICATE_KEY, keyOffset);
                        }
                    }
                    previousFastKey = key;
                    previousFastKeyPresent = true;
                }
                previousStart = keyStart;
                previousEnd = keyEnd;
                int componentHead = hasRemaining()
                    ? inputByteAt(inputPosition())
                    : -1;
                int componentMajor = componentHead >>> 5;
                if (Long.valueOf(1).equals(key)
                    && (componentMajor == 0 || componentMajor == 1)) {
                    seconds = readSignedInteger(depth + 1);
                    secondsPresent = true;
                } else if (Long.valueOf(-3).equals(key)
                           && (componentMajor == 0 || componentMajor == 1)) {
                    remainder = readSignedInteger(depth + 1);
                    remainderPresent = true;
                    if (remainder < 1 || remainder > 999) {
                        invalid = true;
                    }
                } else {
                    readValue(depth + 1);
                    invalid = true;
                }
            }
            if (invalid
                || !secondsPresent
                || length != 1 + (remainderPresent ? 1 : 0)) {
                throw error(ErrorCode.INVALID_INSTANT, tagOffset);
            }
            long fraction = remainderPresent ? remainder : 0;
            long minimumSeconds = Math.floorDiv(Long.MIN_VALUE, 1000);
            long maximumSeconds = Math.floorDiv(Long.MAX_VALUE, 1000);
            if (seconds < minimumSeconds
                || seconds > maximumSeconds
                || seconds == minimumSeconds
                   && fraction < Math.floorMod(Long.MIN_VALUE, 1000)
                || seconds == maximumSeconds
                   && fraction > Math.floorMod(Long.MAX_VALUE, 1000)) {
                throw error(ErrorCode.INVALID_INSTANT, tagOffset);
            }
            if (seconds == minimumSeconds) {
                return new Date(Long.MIN_VALUE
                                + fraction
                                - Math.floorMod(Long.MIN_VALUE, 1000));
            }
            return new Date(seconds * 1000 + fraction);
        }

        private long readSignedInteger(int depth) {
            if (depth > limits.maxDepth) {
                throw error(ErrorCode.DEPTH_LIMIT, offset());
            }
            int headOffset = offset();
            int head = readUnsignedByte();
            int major = head >>> 5;
            long value = argument(head & 0x1f, headOffset);
            if (value > Long.MAX_VALUE) {
                throw error(ErrorCode.INTEGER_OUT_OF_RANGE, headOffset);
            }
            return major == 0 ? value : ~value;
        }

        private Object readTypedArray(long tag, int tagOffset) {
            int payloadHeadOffset = offset();
            int payloadHead = readUnsignedByte();
            if ((payloadHead >>> 5) != 2) {
                throw error(ErrorCode.INVALID_TYPED_ARRAY, tagOffset);
            }
            int byteLength = length(payloadHead & 0x1f,
                                    payloadHeadOffset,
                                    limits.maxStringBytes,
                                    ErrorCode.STRING_LIMIT);
            int width = typedArrayWidth(tag);
            if (byteLength % width != 0) {
                throw error(ErrorCode.INVALID_TYPED_ARRAY, tagOffset);
            }
            int elementCount = byteLength / width;
            if (elementCount > limits.maxCollectionLength) {
                throw error(ErrorCode.COLLECTION_LIMIT, tagOffset);
            }
            require(byteLength);
            int valueOffset = offset();
            if (tag == TAG_UINT16_LE_ARRAY) {
                char[] values = new char[elementCount];
                for (int index = 0; index < elementCount; index++) {
                    values[index] = (char) readLittleEndianUnsignedShort();
                }
                return values;
            }
            if (tag == TAG_SINT16_LE_ARRAY) {
                short[] values = new short[elementCount];
                for (int index = 0; index < elementCount; index++) {
                    values[index] = (short) readLittleEndianUnsignedShort();
                }
                return values;
            }
            if (tag == TAG_SINT32_LE_ARRAY) {
                int[] values = new int[elementCount];
                for (int index = 0; index < elementCount; index++) {
                    values[index] = readLittleEndianInt();
                }
                return values;
            }
            if (tag == TAG_SINT64_LE_ARRAY) {
                long[] values = new long[elementCount];
                for (int index = 0; index < elementCount; index++) {
                    values[index] = readLittleEndianLong();
                }
                return values;
            }
            if (tag == TAG_FLOAT32_LE_ARRAY) {
                float[] values = new float[elementCount];
                for (int index = 0; index < elementCount; index++) {
                    int bits = readLittleEndianInt();
                    if (canonical
                        && Float.isNaN(Float.intBitsToFloat(bits))
                        && bits != CANONICAL_FLOAT_NAN) {
                        throw error(ErrorCode.NON_CANONICAL,
                                    valueOffset + index * width);
                    }
                    values[index] = Float.intBitsToFloat(bits);
                }
                return values;
            }
            if (tag == TAG_FLOAT64_LE_ARRAY) {
                double[] values = new double[elementCount];
                for (int index = 0; index < elementCount; index++) {
                    long bits = readLittleEndianLong();
                    if (canonical
                        && Double.isNaN(Double.longBitsToDouble(bits))
                        && bits != CANONICAL_DOUBLE_NAN) {
                        throw error(ErrorCode.NON_CANONICAL,
                                    valueOffset + index * width);
                    }
                    values[index] = Double.longBitsToDouble(bits);
                }
                return values;
            }
            throw new AssertionError("unreachable typed-array tag");
        }

        private Object readSet(int headOffset, int depth) {
            int arrayHeadOffset = offset();
            int arrayHead = readUnsignedByte();
            if ((arrayHead >>> 5) != 4) {
                throw error(ErrorCode.UNSUPPORTED_VALUE, headOffset);
            }
            int length = collectionLength(arrayHead & 0x1f, arrayHeadOffset);
            ITransientSet values = (ITransientSet)
                PersistentHashSet.EMPTY.asTransient();
            List<Object> members = null;
            HashSet<ByteArrayKey> seen = null;
            int previousStart = -1;
            int previousEnd = -1;
            for (int index = 0; index < length; index++) {
                int valueStart = inputPosition();
                int valueOffset = offset();
                Object value = readValue(depth + 1);
                int valueEnd = inputPosition();
                if (canonical && previousStart >= 0) {
                    int comparison = compareInputRanges(previousStart,
                                                        previousEnd,
                                                        valueStart,
                                                        valueEnd);
                    if (comparison == 0) {
                        throw error(ErrorCode.DUPLICATE_SET_MEMBER, valueOffset);
                    }
                    if (comparison > 0) {
                        throw error(ErrorCode.NON_CANONICAL, valueOffset);
                    }
                } else if (!canonical
                           && (members != null
                               || !safeDecodedSetMemberEquality(value))) {
                    byte[] encoded = encode(value, Mode.CANONICAL);
                    if (seen == null) {
                        seen = new HashSet<>(Math.min(length, 1024));
                    }
                    if (!seen.add(new ByteArrayKey(encoded))) {
                        throw error(ErrorCode.DUPLICATE_SET_MEMBER, valueOffset);
                    }
                }
                previousStart = valueStart;
                previousEnd = valueEnd;
                if (members == null && values.contains(value)) {
                    Set<?> prior = (Set<?>) values.persistent();
                    members = new ArrayList<>(length);
                    members.addAll(prior);
                    values = null;
                    if (!canonical) {
                        // Once host equality collides, canonical bytes must
                        // track every member, including the safe scalars.
                        seen = new HashSet<>(Math.min(length, 1024));
                        for (Object member : members) {
                            seen.add(new ByteArrayKey(
                                encode(member, Mode.CANONICAL)));
                        }
                        if (!seen.add(new ByteArrayKey(
                                encode(value, Mode.CANONICAL)))) {
                            throw error(ErrorCode.DUPLICATE_SET_MEMBER,
                                        valueOffset);
                        }
                    }
                }
                if (members != null) {
                    members.add(value);
                } else {
                    values = (ITransientSet) values.conj(value);
                }
            }
            return members == null ? values.persistent() : new SetValue(members);
        }

        private BigInt readBignum(boolean negative, int headOffset) {
            int innerHead = readUnsignedByte();
            if ((innerHead >>> 5) != 2) {
                throw error(ErrorCode.INVALID_BIGNUM, headOffset);
            }
            byte[] magnitude = readByteString(innerHead & 0x1f,
                                              headOffset,
                                              limits.maxBignumBytes,
                                              ErrorCode.BIGNUM_LIMIT);
            validateBignum(magnitude, headOffset);
            BigInteger value = new BigInteger(1, magnitude);
            if (negative) {
                value = value.negate().subtract(BigInteger.ONE);
            }
            return BigInt.fromBigInteger(value);
        }

        private BigDecimal decodeDecimal(Object value, int headOffset) {
            List<?> pair = pair(value, ErrorCode.INVALID_DECIMAL, headOffset);
            BigInteger exponentInteger = integerValue(pair.get(0),
                                                      ErrorCode.INVALID_DECIMAL,
                                                      headOffset);
            final int exponent;
            final int scale;
            try {
                exponent = exponentInteger.intValueExact();
                scale = Math.negateExact(exponent);
            } catch (ArithmeticException exception) {
                throw error(ErrorCode.INVALID_DECIMAL, headOffset);
            }
            BigInteger mantissa = integerValue(pair.get(1),
                                               ErrorCode.INVALID_DECIMAL,
                                               headOffset);
            if ((mantissa.signum() == 0 && exponent != 0)
                || (mantissa.signum() != 0
                    && mantissa.remainder(BigInteger.TEN).signum() == 0)) {
                throw error(ErrorCode.INVALID_DECIMAL, headOffset);
            }
            return new BigDecimal(mantissa, scale);
        }

        private Number decodeRatio(Object value, int headOffset) {
            List<?> pair = pair(value, ErrorCode.INVALID_RATIO, headOffset);
            BigInteger numerator = integerValue(pair.get(0),
                                                ErrorCode.INVALID_RATIO,
                                                headOffset);
            BigInteger denominator = integerValue(pair.get(1),
                                                  ErrorCode.INVALID_RATIO,
                                                  headOffset);
            if (denominator.signum() <= 0
                || !numerator.gcd(denominator).equals(BigInteger.ONE)) {
                throw error(ErrorCode.INVALID_RATIO, headOffset);
            }
            return Numbers.divide(numerator, denominator);
        }

        private Object readSimple(int additional, int headOffset) {
            return switch (additional) {
                case 20 -> Boolean.FALSE;
                case 21 -> Boolean.TRUE;
                case 22 -> null;
                case 25 -> throw error(ErrorCode.UNSUPPORTED_SIMPLE_VALUE,
                                       headOffset);
                case 26 -> {
                    int bits = readInt();
                    if (canonical
                        && Float.isNaN(Float.intBitsToFloat(bits))
                        && bits != CANONICAL_FLOAT_NAN) {
                        throw error(ErrorCode.NON_CANONICAL, headOffset);
                    }
                    yield Float.intBitsToFloat(bits);
                }
                case 27 -> {
                    long bits = readLong();
                    if (canonical
                        && Double.isNaN(Double.longBitsToDouble(bits))
                        && bits != CANONICAL_DOUBLE_NAN) {
                        throw error(ErrorCode.NON_CANONICAL, headOffset);
                    }
                    yield Double.longBitsToDouble(bits);
                }
                case 31 -> throw error(ErrorCode.INDEFINITE_LENGTH, headOffset);
                default -> throw error(ErrorCode.UNSUPPORTED_SIMPLE_VALUE,
                                       headOffset);
            };
        }

        private long argument(int additional, int headOffset) {
            long value;
            if (additional <= 23) {
                return additional;
            }
            value = switch (additional) {
                case 24 -> readUnsignedByte();
                case 25 -> readUnsignedShort();
                case 26 -> readUnsignedInt();
                case 27 -> {
                    long result = readLong();
                    if (result < 0) {
                        throw error(ErrorCode.INTEGER_OUT_OF_RANGE, headOffset);
                    }
                    yield result;
                }
                case 31 -> throw error(ErrorCode.INDEFINITE_LENGTH, headOffset);
                default -> throw error(ErrorCode.INVALID_ADDITIONAL_INFO,
                                       headOffset);
            };
            if (canonical
                && ((additional == 24 && value < 24)
                    || (additional == 25 && value <= 0xff)
                    || (additional == 26 && value <= 0xffff)
                    || (additional == 27 && value <= 0xffffffffL))) {
                throw error(ErrorCode.NON_SHORTEST, headOffset);
            }
            return value;
        }

        private int collectionLength(int additional, int headOffset) {
            return length(additional, headOffset,
                          limits.maxCollectionLength,
                          ErrorCode.COLLECTION_LIMIT);
        }

        private int length(int additional,
                           int headOffset,
                           int limit,
                           ErrorCode limitCode) {
            long value = argument(additional, headOffset);
            if (value > limit) {
                throw error(limitCode, headOffset);
            }
            if (value > Integer.MAX_VALUE) {
                throw error(ErrorCode.LENGTH_OUT_OF_RANGE, headOffset);
            }
            return (int) value;
        }

        private int readUnsignedByte() {
            require(1);
            return nextUnsignedByte();
        }

        private int readUnsignedShort() {
            require(2);
            return (nextUnsignedByte() << 8) | nextUnsignedByte();
        }

        private long readUnsignedInt() {
            return readInt() & 0xffffffffL;
        }

        private int readInt() {
            require(4);
            return (nextUnsignedByte() << 24)
                | (nextUnsignedByte() << 16)
                | (nextUnsignedByte() << 8)
                | nextUnsignedByte();
        }

        private long readLong() {
            require(8);
            return ((long) nextUnsignedByte() << 56)
                | ((long) nextUnsignedByte() << 48)
                | ((long) nextUnsignedByte() << 40)
                | ((long) nextUnsignedByte() << 32)
                | ((long) nextUnsignedByte() << 24)
                | ((long) nextUnsignedByte() << 16)
                | ((long) nextUnsignedByte() << 8)
                | nextUnsignedByte();
        }

        private int readLittleEndianUnsignedShort() {
            if (arrayInput != null) {
                return nextUnsignedByte() | (nextUnsignedByte() << 8);
            }
            short value = input.getShort();
            return Short.toUnsignedInt(littleEndian
                                       ? value
                                       : Short.reverseBytes(value));
        }

        private int readLittleEndianInt() {
            if (arrayInput != null) {
                return nextUnsignedByte()
                    | (nextUnsignedByte() << 8)
                    | (nextUnsignedByte() << 16)
                    | (nextUnsignedByte() << 24);
            }
            int value = input.getInt();
            return littleEndian ? value : Integer.reverseBytes(value);
        }

        private long readLittleEndianLong() {
            if (arrayInput != null) {
                return nextUnsignedByte()
                    | ((long) nextUnsignedByte() << 8)
                    | ((long) nextUnsignedByte() << 16)
                    | ((long) nextUnsignedByte() << 24)
                    | ((long) nextUnsignedByte() << 32)
                    | ((long) nextUnsignedByte() << 40)
                    | ((long) nextUnsignedByte() << 48)
                    | ((long) nextUnsignedByte() << 56);
            }
            long value = input.getLong();
            return littleEndian ? value : Long.reverseBytes(value);
        }

        private void require(int length) {
            int position = inputPosition();
            if (length < 0) {
                throw error(ErrorCode.TRUNCATED, offset());
            }
            if (activeLimit >= 0 && length > activeLimit - position) {
                throw error(activeLimitCode, offset());
            }
            if (remaining() < length) {
                throw error(ErrorCode.TRUNCATED, offset());
            }
        }

        private int inputPosition() {
            return arrayInput == null ? input.position() : arrayPosition;
        }

        private int remaining() {
            return arrayInput == null
                ? input.remaining()
                : arrayLimit - arrayPosition;
        }

        private boolean hasRemaining() {
            return remaining() != 0;
        }

        private int inputByteAt(int index) {
            return (arrayInput == null ? input.get(index) : arrayInput[index])
                & 0xff;
        }

        private int nextUnsignedByte() {
            return (arrayInput == null ? input.get() : arrayInput[arrayPosition++])
                & 0xff;
        }

        private void getBytes(byte[] output, int start, int length) {
            if (arrayInput == null) {
                input.get(output, start, length);
            } else {
                System.arraycopy(arrayInput, arrayPosition,
                                 output, start, length);
                arrayPosition += length;
            }
        }

        private void validateInputUtf8(int start, int length, int errorOffset) {
            if (arrayInput == null) {
                validateUtf8(input, start, length, errorOffset);
            } else {
                validateUtf8(arrayInput, start, length, errorOffset);
            }
        }

        private int compareInputRanges(int leftStart,
                                       int leftEnd,
                                       int rightStart,
                                       int rightEnd) {
            return arrayInput == null
                ? compareCanonicalRanges(input, leftStart, leftEnd,
                                         rightStart, rightEnd)
                : compareCanonicalRanges(arrayInput, leftStart, leftEnd,
                                         rightStart, rightEnd);
        }
    }

    private static boolean rawByteStringTag(long tag) {
        return tag == TAG_POSITIVE_BIGNUM || tag == TAG_NEGATIVE_BIGNUM
            || tag == TAG_UUID || typedArrayWidth(tag) != 0;
    }

    private static int typedArrayWidth(long tag) {
        if (tag == TAG_UINT16_LE_ARRAY || tag == TAG_SINT16_LE_ARRAY) {
            return 2;
        }
        if (tag == TAG_SINT32_LE_ARRAY || tag == TAG_FLOAT32_LE_ARRAY) {
            return 4;
        }
        if (tag == TAG_SINT64_LE_ARRAY || tag == TAG_FLOAT64_LE_ARRAY) {
            return 8;
        }
        return 0;
    }

    private static List<?> pair(Object value,
                                ErrorCode code,
                                int offset) {
        if (!(value instanceof List<?> values) || values.size() != 2) {
            throw error(code, offset);
        }
        return values;
    }

    private static BigInteger integerValue(Object value,
                                           ErrorCode code,
                                           int offset) {
        if (value instanceof Long number) {
            return BigInteger.valueOf(number);
        }
        if (value instanceof BigInt number) {
            return number.toBigInteger();
        }
        if (value instanceof BigInteger number) {
            return number;
        }
        throw error(code, offset);
    }

    private static void validateBignum(byte[] magnitude, int offset) {
        if (magnitude.length == 0 || magnitude[0] == 0) {
            throw error(ErrorCode.INVALID_BIGNUM, offset);
        }
        BigInteger value = new BigInteger(1, magnitude);
        if (value.compareTo(LONG_MAX) <= 0) {
            throw error(ErrorCode.INVALID_BIGNUM, offset);
        }
    }

    private static int compareCanonicalRanges(ByteBuffer source,
                                              int leftStart,
                                              int leftEnd,
                                              int rightStart,
                                              int rightEnd) {
        int leftLength = leftEnd - leftStart;
        int rightLength = rightEnd - rightStart;
        int length = Integer.compare(leftLength, rightLength);
        if (length != 0) {
            return length;
        }
        for (int index = 0; index < leftLength; index++) {
            int left = source.get(leftStart + index) & 0xff;
            int right = source.get(rightStart + index) & 0xff;
            if (left != right) {
                return Integer.compare(left, right);
            }
        }
        return 0;
    }

    private static int compareCanonicalRanges(byte[] source,
                                              int leftStart,
                                              int leftEnd,
                                              int rightStart,
                                              int rightEnd) {
        int leftLength = leftEnd - leftStart;
        int rightLength = rightEnd - rightStart;
        int length = Integer.compare(leftLength, rightLength);
        if (length != 0) {
            return length;
        }
        for (int index = 0; index < leftLength; index++) {
            int left = source[leftStart + index] & 0xff;
            int right = source[rightStart + index] & 0xff;
            if (left != right) {
                return Integer.compare(left, right);
            }
        }
        return 0;
    }

    private static CodecException error(ErrorCode code, int offset) {
        return new CodecException(code, offset);
    }
}
