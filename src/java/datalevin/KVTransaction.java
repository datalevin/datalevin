package datalevin;

/**
 * Explicit KV write transaction handle.
 *
 * <p>Use this handle for reads and writes inside the transaction, then call
 * {@link #commit()} to persist changes or {@link #abort()} to roll them back.
 * Closing an active transaction aborts it.
 */
public final class KVTransaction extends KV {

    private final KV owner;
    private boolean finished;

    KVTransaction(KV owner, Object transactionKv) {
        super(transactionKv, false);
        this.owner = owner;
        this.finished = false;
    }

    /**
     * Returns whether this transaction is still active.
     */
    public synchronized boolean active() {
        return !finished && !isReleased();
    }

    /**
     * Commits this transaction.
     */
    public synchronized Object commit() {
        ensureActive();
        try {
            return ClojureRuntime.core("commit-kv-transaction", owner.resource());
        } finally {
            finish();
        }
    }

    /**
     * Aborts this transaction.
     */
    public synchronized Object abort() {
        ensureActive();
        try {
            return ClojureRuntime.core("abort-kv-transaction", owner.resource());
        } finally {
            finish();
        }
    }

    /**
     * Aborts this transaction if it is still active.
     */
    @Override
    public synchronized void close() {
        if (active()) {
            abort();
        }
    }

    private void ensureActive() {
        if (!active()) {
            throw new IllegalStateException("KV transaction is closed.");
        }
    }

    private void finish() {
        finished = true;
        super.close();
    }
}
