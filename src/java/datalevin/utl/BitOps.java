package datalevin.utl;

import java.util.Arrays;

public class BitOps {
    public static int intNot (int x) {
        return ~x;
    }

    public static int intFlip (int x, long n) {
        return x ^ (1 << n);
    }

    public static int intAnd (int x, int y) {
        return x & y;
    }

    public static int intOr (int x, int y) {
        return x | y;
    }

    public static int compareBytes(byte[] left, byte[] right) {
        return Arrays.compareUnsigned(left, right);
    }
}
