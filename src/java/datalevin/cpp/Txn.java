package datalevin.cpp;

import static datalevin.cpp.Txn.State.DONE;
import static datalevin.cpp.Txn.State.READY;
import static datalevin.cpp.Txn.State.RELEASED;
import static datalevin.cpp.Txn.State.RESET;

import org.bytedeco.javacpp.*;
import datalevin.dtlvnative.DTLV;

/**
 * Wrap a DTLV MDB_txn
 */
public class Txn {

    private State state;

    private DTLV.MDB_txn ptr;
    private final boolean readOnly;

    public Txn(DTLV.MDB_txn ptr, boolean readOnly) {
        this.ptr = ptr;
        this.readOnly = readOnly;
        state = READY;
    }

    /**
     * Factory method to create a read only transaction instance
     */
    public static Txn createReadOnly(Env env) {
        return create(env, DTLV.MDB_RDONLY);
    }

    /**
     * Factory method to create a read/write transaction instance
     */
    public static Txn create(Env env) {
        return create(env, 0);
    }

    public static Txn create(Env env, int flags) {
        DTLV.MDB_txn ptr = new DTLV.MDB_txn();
        Util.checkRc(DTLV.mdb_txn_begin(env.get(), null, flags, ptr));

        if ((flags & DTLV.MDB_RDONLY) == DTLV.MDB_RDONLY) {
            return new Txn(ptr, true);
        } else {
            return new Txn(ptr, false);
        }
    }

    void checkReady() {
        if (state != READY)
            throw new Util.NotReadyException("Txn not in READY state");
    }

    /**
     * Return the MDB_txn pointer to be used in DTLV calls
     */
    public DTLV.MDB_txn get() {
        return ptr;
    }

    public void abort() {
        checkReady();
        state = DONE;
        DTLV.mdb_txn_abort(ptr);
    }

    public void close() {
        if (state == RELEASED) return;
        if (state == READY || state == RESET) DTLV.mdb_txn_abort(ptr);
        state = RELEASED;
    }

    public void commit() {
        checkReady();
        state = DONE;
        Util.checkRc(DTLV.mdb_txn_commit(ptr));
    }

    public void reset() {
        if (state != READY && state != DONE)
            throw new Util.ResetException("Txn cannot be reset");
        state = RESET;
        DTLV.mdb_txn_reset(ptr);
    }

    public void renew() {
        if (state != RESET)
            throw new Util.NotResetException("Txn cannot be renew");
        state = DONE;
        Util.checkRc(DTLV.mdb_txn_renew(ptr));
        state = READY;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    enum State {
        READY, DONE, RESET, RELEASED
    }
}
