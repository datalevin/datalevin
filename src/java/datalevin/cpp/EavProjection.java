package datalevin.cpp;

import datalevin.dtlvnative.DTLV;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.bytedeco.javacpp.IntPointer;
import org.bytedeco.javacpp.Pointer;
import org.bytedeco.javacpp.PointerPointer;

/** Borrow one entity's projected EAV values until the next cursor advance. */
public final class EavProjection implements AutoCloseable {
    private static MethodHandle find(String name, Class<?> result, Class<?>... args) {
        try {
            return MethodHandles.publicLookup().findStatic(
                DTLV.class, name, MethodType.methodType(result, args));
        } catch (NoSuchMethodException e) {
            return null;
        } catch (IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static final MethodHandle LEGACY_NEXT = find("dtlv_eav_project_next",
        int.class, DTLV.MDB_cursor.class, DTLV.MDB_val.class,
        DTLV.MDB_val.class, IntPointer.class, int.class, int.class);
    private static final MethodHandle CREATE = find("dtlv_eav_projector_create",
        int.class, PointerPointer.class, DTLV.MDB_cursor.class,
        DTLV.MDB_val.class, DTLV.MDB_val.class, IntPointer.class, int.class);
    private static final MethodHandle NEXT = find("dtlv_eav_projector_next",
        int.class, Pointer.class);
    private static final MethodHandle DESTROY = find("dtlv_eav_projector_destroy",
        void.class, Pointer.class);

    private static final class Layout {
        // MDB_val's constructor must load JNI before querying its layout.
        private static final int VALUE_SIZE = Pointer.sizeof(DTLV.MDB_val.class);
    }

    public static boolean selectiveAvailable() {
        return CREATE != null && NEXT != null && DESTROY != null;
    }

    public static boolean available() {
        return selectiveAvailable() || LEGACY_NEXT != null;
    }

    private static RuntimeException failure(Throwable error) {
        if (error instanceof Error fatal) throw fatal;
        return error instanceof RuntimeException runtime ? runtime
            : new IllegalStateException("Native projection failed", error);
    }

    private final Cursor cursor;
    private final BufVal key;
    private final IntPointer aids;
    private final DTLV.MDB_val values;
    private final ByteBuffer buffer;
    private final int count;
    private final Pointer projector;
    private boolean first = true;
    private boolean closed;

    public EavProjection(Cursor cursor, int[] attributes) {
        if (!available()) throw new IllegalStateException("Native projection unavailable");
        if (attributes.length == 0) throw new IllegalArgumentException("Empty projection");
        for (int i = 0; i < attributes.length; i++) {
            if (attributes[i] < 0 || (i > 0 && attributes[i - 1] >= attributes[i])) {
                throw new IllegalArgumentException("Attribute IDs must be sorted and distinct");
            }
        }
        this.cursor = cursor;
        count = attributes.length;
        buffer = ByteBuffer.allocateDirect(0).order(ByteOrder.BIG_ENDIAN);
        aids = new IntPointer(attributes);
        try {
            key = new BufVal(0);
        } catch (Throwable e) {
            aids.close();
            throw e;
        }
        try {
            values = new DTLV.MDB_val(count);
        } catch (Throwable e) {
            key.close();
            aids.close();
            throw e;
        }
        try {
            if (selectiveAvailable()) {
                try (PointerPointer<Pointer> out = new PointerPointer<>(1)) {
                    int rc = (int) CREATE.invokeExact(out, cursor.ptr(), key.ptr(),
                        values, aids, count);
                    Util.checkRc(rc);
                    projector = out.get();
                    if (projector == null || projector.isNull()) {
                        throw new IllegalStateException("Native projector was not created");
                    }
                }
            } else {
                projector = null;
            }
        } catch (Throwable e) {
            values.close();
            key.close();
            aids.close();
            throw failure(e);
        }
    }

    /** Return 1 for a complete entity, 0 for an incomplete one, or -1 at EOF. */
    public int advance() {
        if (closed) throw new IllegalStateException("Projection is closed");
        final int rc;
        try {
            rc = projector != null ? (int) NEXT.invokeExact(projector)
                : (int) LEGACY_NEXT.invokeExact(cursor.ptr(), key.ptr(),
                    values, aids, count, first ? 1 : 0);
        } catch (Throwable e) {
            throw failure(e);
        }
        first = false;
        if (rc == DTLV.MDB_NOTFOUND) return -1;
        if (rc == DTLV.DTLV_TRUE) return 1;
        if (rc == DTLV.DTLV_FALSE) return 0;
        Util.checkRc(rc);
        throw new IllegalStateException("Unexpected projection result: " + rc);
    }

    public long entity() {
        return key.outBuf().getLong(0);
    }

    /** The returned buffer is reused by every call; decode it immediately. */
    public ByteBuffer value(int index) {
        if (index < 0 || index >= count) throw new IndexOutOfBoundsException(index);
        if (BufVal.UNSAFE_AVAILABLE) {
            return cursor.val().unsafeOut(buffer, values.address() + (long) index * Layout.VALUE_SIZE);
        }
        try {
            values.position(index);
            return values.mv_data().position(0).limit(values.mv_size())
                .asByteBuffer().order(ByteOrder.BIG_ENDIAN);
        } finally {
            values.position(0);
        }
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        try {
            if (projector != null) {
                try {
                    DESTROY.invokeExact(projector);
                } catch (Throwable e) {
                    throw failure(e);
                } finally {
                    projector.close();
                }
            }
        } finally {
            try {
                values.close();
            } finally {
                try { key.close(); } finally { aids.close(); }
            }
        }
    }
}
