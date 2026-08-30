package datalevin.cpp;

import org.bytedeco.javacpp.*;
import datalevin.dtlvnative.DTLV;
import java.nio.ByteBuffer;

/**
 * Wrap a MDB_cursor
 */
public class Cursor {

    private DTLV.MDB_cursor ptr;

    private BufVal key;
    private BufVal val;
    private DTLV.MDB_val multipleVals;

    public Cursor(DTLV.MDB_cursor ptr, BufVal key, BufVal val) {
        this.ptr = ptr;
        this.key = key;
        this.val = val;
    }

    /**
     * Factory method to create an instance
     */
    public static Cursor create(Txn txn, Dbi dbi, BufVal key, BufVal val) {

        DTLV.MDB_cursor ptr = new DTLV.MDB_cursor();

        Util.checkRc(DTLV.mdb_cursor_open(txn.get(), dbi.get(), ptr));

        return new Cursor(ptr, key, val);
    }

    /**
     * Return the MDB_cursor pointer to be used in DTLV calls
     */
    public DTLV.MDB_cursor ptr() {
        return ptr;
    }

    /**
     * Position cursor
     */
    public boolean seek(int op) {
        int rc = DTLV.mdb_cursor_get(ptr(), key.ptr(), val.ptr(), op);
        Util.checkRc(rc);
        if (rc == DTLV.MDB_NOTFOUND) {
            return false;
        } else {
            return true;
        }
    }

    public boolean get(BufVal k, int op) {

        key.in(k);

        int rc = DTLV.mdb_cursor_get(ptr(), key.ptr(), val.ptr(), op);
        Util.checkRc(rc);
        if (rc == DTLV.MDB_NOTFOUND) {
            return false;
        } else {
            return true;
        }
    }

    public boolean get(BufVal k, BufVal v, int op) {

        key.in(k);
        val.in(v);

        int rc = DTLV.mdb_cursor_get(ptr(), key.ptr(), val.ptr(), op);
        Util.checkRc(rc);
        if (rc == DTLV.MDB_NOTFOUND) {
            return false;
        } else {
            return true;
        }
    }

    public BufVal key() {
        return key;
    }

    public BufVal val() {
        return val;
    }

    /**
     * Store the key/value currently held by this cursor's buffers.
     */
    public void put(int flags) {
        Util.checkRc(DTLV.mdb_cursor_put(ptr(), key.ptr(), val.ptr(), flags));
    }

    /**
     * Store the current key/value unless the key already exists.
     *
     * MDB_NOOVERWRITE replaces the supplied data MDB_val with the existing
     * value on MDB_KEYEXIST. Restore our reusable value buffer before
     * returning so it never retains an LMDB-owned pointer.
     */
    public boolean tryPut(int flags) {
        int rc = DTLV.mdb_cursor_put(ptr(), key.ptr(), val.ptr(), flags);
        if (rc == DTLV.MDB_KEYEXIST) {
            val.reset();
            return false;
        }
        Util.checkRc(rc);
        return true;
    }

    /**
     * Store contiguous fixed-size duplicate values with MDB_MULTIPLE.
     *
     * The values buffer must contain {@code itemCount} adjacent items of
     * {@code itemSize} bytes. The buffer belongs to the caller and remains
     * valid for the duration of this call.
     */
    public long putMultiple(BufVal values, long itemSize, long itemCount,
                            int flags) {
        if (values == null) {
            throw new IllegalArgumentException("Values buffer cannot be null");
        }
        if (itemSize <= 0 || itemCount <= 0) {
            throw new IllegalArgumentException(
                "Item size and count must both be positive");
        }

        final long required;
        try {
            required = Math.multiplyExact(itemSize, itemCount);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("Multiple value buffer is too large", e);
        }
        if (values.size() < required) {
            throw new IllegalArgumentException(
                "Values buffer is smaller than itemSize * itemCount");
        }

        if (multipleVals == null) {
            multipleVals = new DTLV.MDB_val(2);
        }

        final long firstAddress = multipleVals.position(0).address();
        final long secondAddress = multipleVals.position(1).address();
        if (BufVal.UNSAFE_AVAILABLE) {
            values.unsafeIn(firstAddress, values.inAddr(), itemSize);
            UnsafeAccess.UNSAFE.putLong(
                secondAddress + BufVal.STRUCT_FIELD_OFFSET_SIZE, itemCount);
            UnsafeAccess.UNSAFE.putLong(
                secondAddress + BufVal.STRUCT_FIELD_OFFSET_DATA, 0L);
        } else {
            multipleVals.position(0).mv_size(itemSize).mv_data(values.data());
            multipleVals.position(1).mv_size(itemCount);
        }

        multipleVals.position(0);
        final int rc = DTLV.mdb_cursor_put(
            ptr(), key.ptr(), multipleVals, flags | DTLV.MDB_MULTIPLE);
        final long processed;
        if (BufVal.UNSAFE_AVAILABLE) {
            processed = UnsafeAccess.UNSAFE.getLong(
                secondAddress + BufVal.STRUCT_FIELD_OFFSET_SIZE);
        } else {
            processed = multipleVals.position(1).mv_size();
        }
        multipleVals.position(0);
        Util.checkRc(rc);
        return processed;
    }

    /**
     * Close and free memory
     */
    public void close() {
        DTLV.mdb_cursor_close(ptr);
        ptr.close();
        if (multipleVals != null) {
            multipleVals.position(0).close();
            multipleVals = null;
        }
    }

    /**
     * Return count of duplicates for current key.
     */
    public long count() {
        SizeTPointer cPtr = new SizeTPointer(1);
        Util.checkRc(DTLV.mdb_cursor_count(ptr, cPtr));
        long res = (long)cPtr.get();
        cPtr.close();
        return res;
    }

    /**
     * Delete the key/data pair to which the cursor refers.
     */
    public void delete(int flags) {
        Util.checkRc(DTLV.mdb_cursor_del(ptr(), flags));
    }

    /**
     * Renew cursor.
     */
    public Cursor renew(Txn txn) {
        Util.checkRc(DTLV.mdb_cursor_renew(txn.get(), ptr));
        return this;
    }
}
