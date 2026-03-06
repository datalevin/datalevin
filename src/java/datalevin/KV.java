package datalevin;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Handle for a local key-value store.
 *
 * <p>Use instances with try-with-resources when you own the handle lifecycle.
 * Methods in this class expose common DBI operations and range lookups.
 */
public final class KV extends HandleResource {

    KV(String handle) {
        super(handle, "close-kv", "kv");
    }

    /**
     * Returns whether this handle has been closed.
     */
    public boolean closed() {
        return isReleased() || JsonBridge.asBoolean(call("closed-kv?"));
    }

    /**
     * Returns the root directory backing this KV store.
     */
    public String dir() {
        return JsonBridge.asString(call("dir"));
    }

    /**
     * Opens a regular DBI with default options.
     */
    public void openDbi(String dbiName) {
        call("open-dbi", Datalevin.mapOf("dbi-name", dbiName));
    }

    /**
     * Opens a regular DBI with explicit options.
     */
    public void openDbi(String dbiName, Map<String, ?> opts) {
        call("open-dbi", Datalevin.mapOf("dbi-name", dbiName, "opts", opts));
    }

    /**
     * Opens a list DBI with default options.
     */
    public void openListDbi(String listName) {
        call("open-list-dbi", Datalevin.mapOf("list-name", listName));
    }

    /**
     * Opens a list DBI with explicit options.
     */
    public void openListDbi(String listName, Map<String, ?> opts) {
        call("open-list-dbi", Datalevin.mapOf("list-name", listName, "opts", opts));
    }

    /**
     * Clears all entries from the named DBI.
     */
    public void clearDbi(String dbiName) {
        call("clear-dbi", Datalevin.mapOf("dbi-name", dbiName));
    }

    /**
     * Drops the named DBI.
     */
    public void dropDbi(String dbiName) {
        call("drop-dbi", Datalevin.mapOf("dbi-name", dbiName));
    }

    /**
     * Lists all DBIs in the store.
     */
    public List<Object> listDbis() {
        return JsonBridge.asList(call("list-dbis"));
    }

    /**
     * Returns environment statistics.
     */
    public Map<String, Object> stat() {
        return JsonBridge.asMap(call("stat"));
    }

    /**
     * Returns statistics for the named DBI.
     */
    public Map<String, Object> stat(String dbiName) {
        return JsonBridge.asMap(call("stat", Datalevin.mapOf("dbi-name", dbiName)));
    }

    /**
     * Returns the number of entries in the named DBI.
     */
    public long entries(String dbiName) {
        return JsonBridge.asLong(call("entries", Datalevin.mapOf("dbi-name", dbiName)));
    }

    /**
     * Applies KV transactions with default type inference.
     */
    public Object transact(Object txs) {
        return call("transact-kv", Datalevin.mapOf("txs", txs));
    }

    /**
     * Applies KV transactions against the named DBI.
     */
    public Object transact(String dbiName, Object txs) {
        return call("transact-kv", Datalevin.mapOf("dbi-name", dbiName, "txs", txs));
    }

    /**
     * Applies KV transactions with an explicit key type.
     */
    public Object transact(String dbiName, Object txs, String kType) {
        return call("transact-kv", Datalevin.mapOf("dbi-name", dbiName, "txs", txs, "k-type", kType));
    }

    /**
     * Applies KV transactions with explicit key and value types.
     */
    public Object transact(String dbiName, Object txs, String kType, String vType) {
        return call("transact-kv", Datalevin.mapOf(
                "dbi-name", dbiName,
                "txs", txs,
                "k-type", kType,
                "v-type", vType
        ));
    }

    /**
     * Returns the value for {@code key} from the named DBI.
     */
    public Object getValue(String dbi, Object key) {
        return call("get-value", Datalevin.mapOf("dbi", dbi, "k", key));
    }

    /**
     * Returns the value for {@code key} from the named DBI with explicit types.
     */
    public Object getValue(String dbi, Object key, String kType, String vType, boolean ignoreKey) {
        return call("get-value", Datalevin.mapOf(
                "dbi", dbi,
                "k", key,
                "k-type", kType,
                "v-type", vType,
                "ignore-key?", ignoreKey
        ));
    }

    /**
     * Returns the sorted rank of {@code key}, or {@code null} when absent.
     */
    public Long getRank(String dbi, Object key) {
        return JsonBridge.asNullableLong(call("get-rank", Datalevin.mapOf("dbi", dbi, "k", key)));
    }

    /**
     * Returns the sorted rank of {@code key} with an explicit key type.
     */
    public Long getRank(String dbi, Object key, String kType) {
        return JsonBridge.asNullableLong(call("get-rank", Datalevin.mapOf("dbi", dbi, "k", key, "k-type", kType)));
    }

    /**
     * Returns the entry at the given rank.
     */
    public Object getByRank(String dbi, long rank) {
        return call("get-by-rank", Datalevin.mapOf("dbi", dbi, "rank", rank));
    }

    /**
     * Returns the entry at the given rank with explicit types.
     */
    public Object getByRank(String dbi, long rank, String kType, String vType, boolean ignoreKey) {
        return call("get-by-rank", Datalevin.mapOf(
                "dbi", dbi,
                "rank", rank,
                "k-type", kType,
                "v-type", vType,
                "ignore-key?", ignoreKey
        ));
    }

    /**
     * Returns entries in the given key range.
     */
    public List<Object> getRange(String dbi, List<?> kRange) {
        return JsonBridge.asList(call("get-range", Datalevin.mapOf("dbi", dbi, "k-range", kRange)));
    }

    /**
     * Returns entries in the given key range with explicit types and paging.
     */
    public List<Object> getRange(String dbi,
                                 List<?> kRange,
                                 String kType,
                                 String vType,
                                 Integer limit,
                                 Integer offset) {
        LinkedHashMap<String, Object> args = Datalevin.mapOf("dbi", dbi, "k-range", kRange);
        if (kType != null) {
            args.put("k-type", kType);
        }
        if (vType != null) {
            args.put("v-type", vType);
        }
        if (limit != null) {
            args.put("limit", limit);
        }
        if (offset != null) {
            args.put("offset", offset);
        }
        return JsonBridge.asList(call("get-range", args));
    }

    /**
     * Returns keys in the given range.
     */
    public List<Object> keyRange(String dbi, List<?> kRange, String kType, Integer limit, Integer offset) {
        LinkedHashMap<String, Object> args = Datalevin.mapOf("dbi", dbi, "k-range", kRange);
        if (kType != null) {
            args.put("k-type", kType);
        }
        if (limit != null) {
            args.put("limit", limit);
        }
        if (offset != null) {
            args.put("offset", offset);
        }
        return JsonBridge.asList(call("key-range", args));
    }

    /**
     * Returns the number of keys in the given range.
     */
    public long keyRangeCount(String dbi, List<?> kRange, String kType) {
        LinkedHashMap<String, Object> args = Datalevin.mapOf("dbi", dbi, "k-range", kRange);
        if (kType != null) {
            args.put("k-type", kType);
        }
        return JsonBridge.asLong(call("key-range-count", args));
    }

    /**
     * Returns the approximate number of entries in the given range.
     */
    public long rangeCount(String dbi, List<?> kRange, String kType) {
        LinkedHashMap<String, Object> args = Datalevin.mapOf("dbi", dbi, "k-range", kRange);
        if (kType != null) {
            args.put("k-type", kType);
        }
        return JsonBridge.asLong(call("range-count", args));
    }

    /**
     * Flushes store state to disk.
     */
    public void sync() {
        call("sync");
    }

    /**
     * Flushes store state to disk using the given force flag.
     */
    public void sync(long force) {
        call("sync", Datalevin.mapOf("force", force));
    }

    /**
     * Escape hatch for calling a KV-scoped JSON API operation directly.
     */
    public Object exec(String op, Map<String, ?> args) {
        return super.call(op, args);
    }
}
