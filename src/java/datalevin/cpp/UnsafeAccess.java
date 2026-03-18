package datalevin.cpp;

import java.lang.reflect.Field;
import java.lang.reflect.InaccessibleObjectException;
import java.lang.reflect.Method;

import sun.misc.Unsafe;
import java.nio.Buffer;
import java.nio.ByteBuffer;

@SuppressWarnings("removal")
final class UnsafeAccess {

    static Unsafe UNSAFE = null;
    static Method INVOKE_CLEANER = null;

    static boolean available = false;

    static {
        try {
            final Field u = Unsafe.class.getDeclaredField("theUnsafe");
            u.setAccessible(true);
            UNSAFE = (Unsafe) u.get(null);

            final Field c = Buffer.class.getDeclaredField("capacity");
            c.setAccessible(true);
            UNSAFE.objectFieldOffset(c);

            final Field a = Buffer.class.getDeclaredField("address");
            a.setAccessible(true);
            UNSAFE.objectFieldOffset(a);

            INVOKE_CLEANER = Unsafe.class.getMethod("invokeCleaner", ByteBuffer.class);

            available = true;
        } catch (final NoSuchFieldException | SecurityException
                | IllegalArgumentException | IllegalAccessException
                | InaccessibleObjectException | NoSuchMethodException e) {
            // don't throw, as Unsafe use is optional
            available = false;
            INVOKE_CLEANER = null;
        }
    }

    public static boolean isAvailable() {
        return available;
    }

    static void clean(final ByteBuffer buffer) {
        if (!available || INVOKE_CLEANER == null || buffer == null || !buffer.isDirect()) {
            return;
        }
        try {
            INVOKE_CLEANER.invoke(UNSAFE, buffer);
        } catch (final Exception e) {
            throw new RuntimeException("Unable to clean direct buffer", e);
        }
    }

    private UnsafeAccess() {
    }
}
