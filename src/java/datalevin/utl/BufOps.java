package datalevin.utl;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

public class BufOps {

    /** Request-owned UTF-8 scratch; uses the JDK array encoder and replacement rules. */
    public static final class Utf8Encoder {
        private CharBuffer chars;
        private CharsetEncoder encoder;

        /** Append a complete string within the byte budget, leaving position unchanged on overflow. */
        public boolean put(ByteBuffer target, String value, int maxBytes) {
            int available = Math.min(maxBytes, target.remaining());
            int length = value.length();
            if (length > available) return false;
            if (length <= 32) {
                int bits = 0;
                for (int i = 0; i < length; i++) bits |= value.charAt(i);
                if (bits < 0x80) {
                    // The JDK range copy is UTF-8 for this verified ASCII span.
                    value.getBytes(0, length, target.array(),
                                   target.arrayOffset() + target.position());
                    target.position(target.position() + length);
                    return true;
                }
            }
            if (chars == null) {
                chars = CharBuffer.allocate(Math.max(64, length));
                encoder = StandardCharsets.UTF_8.newEncoder()
                        .onMalformedInput(CodingErrorAction.REPLACE)
                        .onUnmappableCharacter(CodingErrorAction.REPLACE);
            }
            if (length > chars.capacity()) {
                chars = CharBuffer.allocate(Math.max(length, chars.capacity() * 2));
            }
            chars.clear();
            chars.put(value).flip();
            encoder.reset();
            int start = target.position(), limit = target.limit();
            boolean complete = false;
            try {
                target.limit(start + available);
                complete = encoder.encode(chars, target, true).isUnderflow()
                        && encoder.flush(target).isUnderflow();
                return complete;
            } finally {
                target.limit(limit);
                if (!complete) target.position(start);
            }
        }
    }

    /** Frame a settled Nippy payload as an owned, non-giant AVG row. */
    public static byte[] avgBytes(int aid, ByteBuffer payload,
                                  byte separator, byte nonGiant) {
        int size = payload.position();
        byte[] row = new byte[size + 6];
        row[0] = (byte) (aid >>> 24);
        row[1] = (byte) (aid >>> 16);
        row[2] = (byte) (aid >>> 8);
        row[3] = (byte) aid;
        System.arraycopy(payload.array(), payload.arrayOffset(), row, 4, size);
        row[size + 4] = separator;
        row[size + 5] = nonGiant;
        return row;
    }

    public static int compareByteBuf(final ByteBuffer a, final ByteBuffer b) {
      if (a.equals(b)) {
        return 0;
      }

      final int minLength = Math.min(a.limit(), b.limit());
      final int minWords = minLength / Long.BYTES;

      // assume Big Endian (JVM default)
      for (int i = 0; i < minWords * Long.BYTES; i += Long.BYTES) {
        final long lw = a.getLong(i);
        final long rw = b.getLong(i);
        final int diff = Long.compareUnsigned(lw, rw);
        if (diff != 0) {
          return diff;
        }
      }

      for (int i = minWords * Long.BYTES; i < minLength; i++) {
        final int lw = Byte.toUnsignedInt(a.get(i));
        final int rw = Byte.toUnsignedInt(b.get(i));
        final int result = Integer.compareUnsigned(lw, rw);
        if (result != 0) {
          return result;
        }
      }

      return a.remaining() - b.remaining();
    }
}
