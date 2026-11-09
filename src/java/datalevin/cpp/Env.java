package datalevin.cpp;

import org.bytedeco.javacpp.*;
import datalevin.dtlvnative.DTLV;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Wrap a MDB_env
 */
public class Env {

    private volatile boolean closed;
    private DTLV.MDB_env env;
    // A prepared reader holds pointers into the current mapping. Writers may
    // commit while it is alive, but close and map growth must wait for it.
    private final ReentrantReadWriteLock copyLifetime = new ReentrantReadWriteLock(true);

    public Env(DTLV.MDB_env env) {
        this.closed = false;
        this.env = env;
    }

    /**
     * Factory method to create an instance
     */
    public static Env create(String dir, long size, int maxReaders, int maxDbs,
                             int flags) {

        DTLV.MDB_env env = new DTLV.MDB_env();
        Util.checkRc(DTLV.mdb_env_create(env));

        try {
            Util.checkRc(DTLV.mdb_env_set_mapsize(env, size));
            Util.checkRc(DTLV.mdb_env_set_maxreaders(env, maxReaders));
            Util.checkRc(DTLV.mdb_env_set_maxdbs(env, maxDbs));

            Util.checkRc(DTLV.mdb_env_open(env, dir, flags, 0664));

            return new Env(env);
        } catch (RuntimeException | Error e) {
            DTLV.mdb_env_close(env);
            throw e;
        }
    }

    /**
     * Return the MDB_env pointer to be used in DTLV calls
     */
    public DTLV.MDB_env get() {
        return env;
    }

    /**
     * Close and free memory
     */
    public void close() {
        if (copyLifetime.getReadHoldCount() != 0) {
            throw new IllegalStateException("Close prepared copies before closing the environment");
        }
        copyLifetime.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            DTLV.mdb_env_close(env);
        } finally {
            copyLifetime.writeLock().unlock();
        }
    }

    /**
     * return closed status
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * sync to disk, non-zero force will force a synchronous flush. Otherwise,
     * respect env flags.
     */
    public void sync(final int force) {
        if (closed) {
            return;
        }
        Util.checkRc(DTLV.mdb_env_sync(env, force));
    }

    /**
     * Check reader lock table, clean it up. Return the number of dead readers.
     */
    public int readerCheck() {
        try (IntPointer dPtr = new IntPointer(1)) {
            if (closed) return (int)0;
            Util.checkRc(DTLV.mdb_reader_check(env, dPtr));
            return (int) dPtr.get();
        }
    }

    /**
     * Set flags, non-zero onOff set the flags. Otherwise, clear the flags.
     */
    public void setFlags(final int toChange, final int onOff) {
        Util.checkRc(DTLV.mdb_env_set_flags(env, toChange, onOff));
    }


    /**
     * Get flags.
     */
    public int getFlags() {
        int[] flags = new int[1];
        Util.checkRc(DTLV.mdb_env_get_flags(env, flags));
        return flags[0];
    }

    public void setMapSize(long size) {
        if (copyLifetime.getReadHoldCount() != 0) {
            throw new IllegalStateException("Close prepared copies before remapping the environment");
        }
        copyLifetime.writeLock().lock();
        try {
            Util.checkRc(DTLV.mdb_env_set_mapsize(env, size));
        } finally {
            copyLifetime.writeLock().unlock();
        }
    }

    /** A fixed, single-use snapshot owned by the preparing Java thread. */
    public final class PreparedCopy implements AutoCloseable {
        private final Thread thread = Thread.currentThread();
        private DTLV.MDB_copy handle;
        private boolean executed;

        private PreparedCopy(DTLV.MDB_copy handle) {
            this.handle = handle;
        }

        private void checkThread() {
            if (thread != Thread.currentThread()) {
                throw new IllegalStateException("Use prepared copy on its preparing thread");
            }
        }

        private void execute() {
            checkThread();
            if (handle == null || executed) {
                throw new IllegalStateException("Prepared copy is closed or already executed");
            }
            executed = true;
            Util.checkRc(DTLV.mdb_env_copy_execute(handle));
        }

        @Override
        public void close() {
            checkThread();
            if (handle != null) {
                DTLV.MDB_copy copy = handle;
                handle = null;
                try {
                    Util.checkRc(DTLV.mdb_env_copy_close(copy));
                } finally {
                    copyLifetime.readLock().unlock();
                }
            }
        }
    }

    /** Fix the reader and metadata without copying payload bytes. The caller
     * must close the result on this thread, including after cancellation. */
    public PreparedCopy prepareCopy(String dest, boolean compact) {
        copyLifetime.readLock().lock();
        DTLV.MDB_copy copy = null;
        boolean prepared = false;
        try {
            if (closed) {
                throw new IllegalStateException("LMDB env is closed");
            }
            copy = new DTLV.MDB_copy();
            Util.checkRc(DTLV.mdb_env_copy_prepare(env, dest,
                         compact ? DTLV.MDB_CP_COMPACT : 0, copy));
            prepared = true;
            return new PreparedCopy(copy);
        } catch (RuntimeException | Error e) {
            if (prepared) {
                try {
                    Util.checkRc(DTLV.mdb_env_copy_close(copy));
                } catch (RuntimeException | Error cleanup) {
                    e.addSuppressed(cleanup);
                }
            }
            copyLifetime.readLock().unlock();
            throw e;
        }
    }

    /** Copy exactly the prepared state; foreground commits remain allowed. */
    public void copyPrepared(PreparedCopy copy) {
        copy.execute();
    }

    /** Release the fixed reader, destination descriptor, and mapping lease. */
    public void closePrepared(PreparedCopy copy) {
        copy.close();
    }

    public void copy(String dest, boolean compact) {
        try (PreparedCopy copy = prepareCopy(dest, compact)) {
            copyPrepared(copy);
        }
    }

}
