package datalevin.codec;

import clojure.lang.BigInt;
import clojure.lang.IPersistentList;
import clojure.lang.IPersistentMap;
import clojure.lang.ITransientCollection;
import clojure.lang.ITransientMap;
import clojure.lang.Numbers;
import clojure.lang.PersistentArrayMap;
import clojure.lang.PersistentHashSet;
import clojure.lang.PersistentVector;
import clojure.lang.Ratio;
import clojure.lang.Sorted;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.UUID;

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
    private static final long TAG_SET = 258;

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
        INVALID_UUID,
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
                                                        4 * 1024);

        public final int maxInputBytes;
        public final int maxDepth;
        public final int maxCollectionLength;
        public final int maxStringBytes;
        public final int maxBignumBytes;

        public Limits(int maxInputBytes,
                      int maxDepth,
                      int maxCollectionLength,
                      int maxStringBytes,
                      int maxBignumBytes) {
            this.maxInputBytes = positive(maxInputBytes, "maxInputBytes");
            this.maxDepth = positive(maxDepth, "maxDepth");
            this.maxCollectionLength = positive(maxCollectionLength,
                                                "maxCollectionLength");
            this.maxStringBytes = positive(maxStringBytes, "maxStringBytes");
            this.maxBignumBytes = positive(maxBignumBytes, "maxBignumBytes");
        }

        private static int positive(int value, String name) {
            if (value <= 0) {
                throw new IllegalArgumentException(name + " must be positive");
            }
            return value;
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
        return decode(ByteBuffer.wrap(Objects.requireNonNull(input, "input")),
                      canonical,
                      Limits.DEFAULT);
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
            return decode(ByteBuffer.wrap(input, 1, input.length - 1).slice(),
                          canonical,
                          Limits.DEFAULT);
        }
        if (isTypedHeader(first)) {
            throw error(ErrorCode.UNESCAPED_TYPED_HEADER, 0);
        }
        return decode(input, canonical);
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
    }

    private static final class BufferSink implements Sink {
        private ByteBuffer output;
        private int origin;
        private long textWord;
        private int textWidth;
        private boolean textBigEndian;

        private BufferSink() {
        }

        private void reset(ByteBuffer output) {
            this.output = output;
            this.origin = output.position();
        }

        private void clear() {
            output = null;
            origin = 0;
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
        if (value instanceof byte[] bytes) {
            return checkedSize(headSize(bytes.length), bytes.length);
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
        if (value instanceof TaggedValue tagged) {
            return checkedSize(headSize(tagged.tag), sizeOf(tagged.value));
        }
        if (value instanceof Sorted
            || value instanceof SortedMap<?, ?>
            || value instanceof SortedSet<?>
            || value instanceof IPersistentList
            || value instanceof Collection<?>
               && !(value instanceof List<?>)
               && !(value instanceof Set<?>)) {
            throw error(ErrorCode.UNSUPPORTED_VALUE, 0);
        }
        if (value instanceof Map<?, ?> map) {
            int size = headSize(map.size());
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                size = checkedSize(size, sizeOf(entry.getKey()),
                                   sizeOf(entry.getValue()));
            }
            return size;
        }
        if (value instanceof Set<?> set) {
            int size = checkedSize(headSize(TAG_SET), headSize(set.size()));
            for (Object item : set) {
                size = checkedSize(size, sizeOf(item));
            }
            return size;
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

    private static int integerSize(long value) {
        return headSize(value >= 0 ? value : ~value);
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
        } else if (value instanceof byte[] bytes) {
            writeHead(sink, 2, bytes.length);
            sink.putBytes(bytes);
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
        } else if (value instanceof TaggedValue tagged) {
            writeHead(sink, 6, tagged.tag);
            writeValue(sink, tagged.value, mode);
        } else if (value instanceof Sorted
                   || value instanceof SortedMap<?, ?>
                   || value instanceof SortedSet<?>) {
            throw error(ErrorCode.UNSUPPORTED_VALUE, sink.position());
        } else if (value instanceof IPersistentList) {
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
            for (Object item : entries) {
                Map.Entry<?, ?> entry = (Map.Entry<?, ?>) item;
                writeText(sink, (String) entry.getKey());
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

        List<PreparedEntry> entries = new ArrayList<>(map.size());
        for (Map.Entry<?, ?> entry : map.entrySet()) {
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
            for (long value : integerValues) {
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

        List<PreparedValue> values = new ArrayList<>(set.size());
        for (Object value : set) {
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
            || valueClass == Float.class
            || valueClass == Double.class
            || valueClass == UUID.class;
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
        private boolean canonical;
        private Limits limits;
        private int origin;

        private Decoder() {
        }

        private void reset(ByteBuffer input, boolean canonical, Limits limits) {
            this.input = input;
            this.canonical = canonical;
            this.limits = limits;
            this.origin = input.position();
        }

        private void clear() {
            input = null;
            limits = null;
            origin = 0;
        }

        private int offset() {
            return input.position() - origin;
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
                case 2 -> readByteString(additional, headOffset,
                                         limits.maxStringBytes,
                                         ErrorCode.STRING_LIMIT);
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
            byte[] bytes = new byte[length];
            require(length);
            input.get(bytes);
            return bytes;
        }

        private String readText(int additional, int headOffset) {
            int length = length(additional, headOffset,
                                limits.maxStringBytes,
                                ErrorCode.STRING_LIMIT);
            require(length);
            int start = input.position();
            validateUtf8(input, start, length, headOffset);
            if (input.hasArray()) {
                String result = new String(input.array(),
                                           input.arrayOffset() + start,
                                           length,
                                           StandardCharsets.UTF_8);
                input.position(start + length);
                return result;
            }
            byte[] bytes = decodeBytes(length);
            input.get(bytes, 0, length);
            return new String(bytes, 0, length, StandardCharsets.UTF_8);
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

        private IPersistentMap readMap(int additional,
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
            for (int index = 0; index < length; index++) {
                int keyStart = input.position();
                int keyOffset = offset();
                Object key = readValue(depth + 1);
                int keyEnd = input.position();
                byte[] canonicalKey = canonical ? null : encode(key, Mode.CANONICAL);
                if (canonical && previousStart >= 0) {
                    int comparison = compareCanonicalRanges(input,
                                                              previousStart,
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
                Object value = readValue(depth + 1);
                if (small) {
                    keyValues[index * 2] = key;
                    keyValues[index * 2 + 1] = value;
                } else {
                    largeResult = largeResult.assoc(key, value);
                }
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
            if (tag == TAG_SET) {
                return readSet(headOffset, depth + 1);
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
                    return URI.create(text);
                } catch (IllegalArgumentException exception) {
                    throw error(ErrorCode.INVALID_URI, headOffset);
                }
            }
            if (tag == TAG_UUID) {
                if (!(value instanceof byte[] bytes) || bytes.length != 16) {
                    throw error(ErrorCode.INVALID_UUID, headOffset);
                }
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                return new UUID(buffer.getLong(), buffer.getLong());
            }
            return new TaggedValue(tag, value);
        }

        private Object readSet(int headOffset, int depth) {
            int arrayHeadOffset = offset();
            int arrayHead = readUnsignedByte();
            if ((arrayHead >>> 5) != 4) {
                throw error(ErrorCode.UNSUPPORTED_VALUE, headOffset);
            }
            int length = collectionLength(arrayHead & 0x1f, arrayHeadOffset);
            ITransientCollection values = PersistentHashSet.EMPTY.asTransient();
            HashSet<ByteArrayKey> seen = canonical
                ? null
                : new HashSet<>(Math.min(length, 1024));
            int previousStart = -1;
            int previousEnd = -1;
            for (int index = 0; index < length; index++) {
                int valueStart = input.position();
                int valueOffset = offset();
                Object value = readValue(depth + 1);
                int valueEnd = input.position();
                if (canonical && previousStart >= 0) {
                    int comparison = compareCanonicalRanges(input,
                                                              previousStart,
                                                              previousEnd,
                                                              valueStart,
                                                              valueEnd);
                    if (comparison == 0) {
                        throw error(ErrorCode.DUPLICATE_SET_MEMBER, valueOffset);
                    }
                    if (comparison > 0) {
                        throw error(ErrorCode.NON_CANONICAL, valueOffset);
                    }
                } else if (!canonical) {
                    byte[] encoded = encode(value, Mode.CANONICAL);
                    if (!seen.add(new ByteArrayKey(encoded))) {
                        throw error(ErrorCode.DUPLICATE_SET_MEMBER, valueOffset);
                    }
                }
                previousStart = valueStart;
                previousEnd = valueEnd;
                values = values.conj(value);
            }
            return values.persistent();
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
            return input.get() & 0xff;
        }

        private int readUnsignedShort() {
            require(2);
            return (readUnsignedByte() << 8) | readUnsignedByte();
        }

        private long readUnsignedInt() {
            return readInt() & 0xffffffffL;
        }

        private int readInt() {
            require(4);
            return (readUnsignedByte() << 24)
                | (readUnsignedByte() << 16)
                | (readUnsignedByte() << 8)
                | readUnsignedByte();
        }

        private long readLong() {
            require(8);
            return ((long) readUnsignedByte() << 56)
                | ((long) readUnsignedByte() << 48)
                | ((long) readUnsignedByte() << 40)
                | ((long) readUnsignedByte() << 32)
                | ((long) readUnsignedByte() << 24)
                | ((long) readUnsignedByte() << 16)
                | ((long) readUnsignedByte() << 8)
                | readUnsignedByte();
        }

        private void require(int length) {
            if (length < 0 || input.remaining() < length) {
                throw error(ErrorCode.TRUNCATED, offset());
            }
        }
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

    private static CodecException error(ErrorCode code, int offset) {
        return new CodecException(code, offset);
    }
}
