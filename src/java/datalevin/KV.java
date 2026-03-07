package datalevin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Handle for a local key-value store.
 *
 * <p>Use instances with try-with-resources when you own the handle lifecycle.
 * Methods in this class expose common DBI operations and range lookups.
 */
public final class KV extends HandleResource {

    KV(Object kv) {
        super(kv,
              resource -> ClojureBridge.core("close-kv", resource),
              "kv",
              "kv");
    }

    /**
     * Returns whether this handle has been closed.
     */
    public boolean closed() {
        return isReleased() || ClojureBridge.javaBoolean(ClojureBridge.core("closed-kv?", resource()));
    }

    /**
     * Returns the root directory backing this KV store.
     */
    public String dir() {
        return ClojureBridge.javaString(ClojureBridge.core("dir", resource()));
    }

    /**
     * Opens a regular DBI with default options.
     */
    public void openDbi(String dbiName) {
        ClojureBridge.core("open-dbi", resource(), dbiName);
    }

    /**
     * Opens a regular DBI with explicit options.
     */
    public void openDbi(String dbiName, Map<String, ?> opts) {
        ClojureBridge.core("open-dbi", resource(), dbiName, ClojureBridge.optionsInput(opts));
    }

    /**
     * Opens a list DBI with default options.
     */
    public void openListDbi(String listName) {
        ClojureBridge.core("open-list-dbi", resource(), listName);
    }

    /**
     * Opens a list DBI with explicit options.
     */
    public void openListDbi(String listName, Map<String, ?> opts) {
        ClojureBridge.core("open-list-dbi", resource(), listName, ClojureBridge.optionsInput(opts));
    }

    /**
     * Clears all entries from the named DBI.
     */
    public void clearDbi(String dbiName) {
        ClojureBridge.core("clear-dbi", resource(), dbiName);
    }

    /**
     * Drops the named DBI.
     */
    public void dropDbi(String dbiName) {
        ClojureBridge.core("drop-dbi", resource(), dbiName);
    }

    /**
     * Lists all DBIs in the store.
     */
    public List<Object> listDbis() {
        return ClojureBridge.javaList(ClojureBridge.core("list-dbis", resource()));
    }

    /**
     * Returns environment statistics.
     */
    public Map<String, Object> stat() {
        return ClojureBridge.javaMap(ClojureBridge.core("stat", resource()));
    }

    /**
     * Returns statistics for the named DBI.
     */
    public Map<String, Object> stat(String dbiName) {
        return ClojureBridge.javaMap(ClojureBridge.core("stat", resource(), dbiName));
    }

    /**
     * Returns the number of entries in the named DBI.
     */
    public long entries(String dbiName) {
        return ClojureBridge.javaLong(ClojureBridge.core("entries", resource(), dbiName));
    }

    /**
     * Applies KV transactions with default type inference.
     */
    public Object transact(Object txs) {
        return ClojureBridge.toJava(ClojureBridge.core("transact-kv",
                                                       resource(),
                                                       ClojureBridge.kvTxsInput(txs)));
    }

    /**
     * Applies KV transactions against the named DBI.
     */
    public Object transact(String dbiName, Object txs) {
        return ClojureBridge.toJava(ClojureBridge.core("transact-kv",
                                                       resource(),
                                                       dbiName,
                                                       ClojureBridge.kvTxsInput(txs)));
    }

    /**
     * Applies KV transactions with an explicit key type.
     */
    public Object transact(String dbiName, Object txs, String kType) {
        return ClojureBridge.toJava(ClojureBridge.core("transact-kv",
                                                       resource(),
                                                       dbiName,
                                                       ClojureBridge.kvTxsInput(txs),
                                                       ClojureBridge.typeKeyword(kType)));
    }

    /**
     * Applies KV transactions with explicit key and value types.
     */
    public Object transact(String dbiName, Object txs, String kType, String vType) {
        return ClojureBridge.toJava(ClojureBridge.core("transact-kv",
                                                       resource(),
                                                       dbiName,
                                                       ClojureBridge.kvTxsInput(txs),
                                                       ClojureBridge.typeKeyword(kType),
                                                       ClojureBridge.typeKeyword(vType)));
    }

    /**
     * Returns the value for {@code key} from the named DBI.
     */
    public Object getValue(String dbi, Object key) {
        return ClojureBridge.toJava(ClojureBridge.core("get-value",
                                                       resource(),
                                                       dbi,
                                                       ClojureBridge.genericInput(key)));
    }

    /**
     * Returns the value for {@code key} from the named DBI with explicit types.
     */
    public Object getValue(String dbi, Object key, String kType, String vType, boolean ignoreKey) {
        return ClojureBridge.toJava(ClojureBridge.core("get-value",
                                                       resource(),
                                                       dbi,
                                                       ClojureBridge.genericInput(key),
                                                       ClojureBridge.typeKeyword(kType),
                                                       ClojureBridge.typeKeyword(vType),
                                                       ignoreKey));
    }

    /**
     * Returns the sorted rank of {@code key}, or {@code null} when absent.
     */
    public Long getRank(String dbi, Object key) {
        return ClojureBridge.javaNullableLong(ClojureBridge.core("get-rank",
                                                                 resource(),
                                                                 dbi,
                                                                 ClojureBridge.genericInput(key)));
    }

    /**
     * Returns the sorted rank of {@code key} with an explicit key type.
     */
    public Long getRank(String dbi, Object key, String kType) {
        return ClojureBridge.javaNullableLong(ClojureBridge.core("get-rank",
                                                                 resource(),
                                                                 dbi,
                                                                 ClojureBridge.genericInput(key),
                                                                 ClojureBridge.typeKeyword(kType)));
    }

    /**
     * Returns the entry at the given rank.
     */
    public Object getByRank(String dbi, long rank) {
        return ClojureBridge.toJava(ClojureBridge.core("get-by-rank", resource(), dbi, rank));
    }

    /**
     * Returns the entry at the given rank with explicit types.
     */
    public Object getByRank(String dbi, long rank, String kType, String vType, boolean ignoreKey) {
        return ClojureBridge.toJava(ClojureBridge.core("get-by-rank",
                                                       resource(),
                                                       dbi,
                                                       rank,
                                                       ClojureBridge.typeKeyword(kType),
                                                       ClojureBridge.typeKeyword(vType),
                                                       ignoreKey));
    }

    /**
     * Returns entries in the given key range.
     */
    public List<Object> getRange(String dbi, List<?> kRange) {
        return ClojureBridge.javaList(ClojureBridge.core("get-range",
                                                         resource(),
                                                         dbi,
                                                         ClojureBridge.rangeInput(kRange)));
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
        ArrayList<Object> args = new ArrayList<>();
        args.add(resource());
        args.add(dbi);
        args.add(ClojureBridge.rangeInput(kRange));
        if (kType != null || vType != null) {
            args.add(ClojureBridge.typeKeyword(kType));
        }
        if (vType != null) {
            args.add(ClojureBridge.typeKeyword(vType));
        }
        return page(ClojureBridge.javaList(ClojureBridge.core("get-range", args.toArray())),
                    limit,
                    offset);
    }

    /**
     * Returns keys in the given range.
     */
    public List<Object> keyRange(String dbi, List<?> kRange, String kType, Integer limit, Integer offset) {
        ArrayList<Object> args = new ArrayList<>();
        args.add(resource());
        args.add(dbi);
        args.add(ClojureBridge.rangeInput(kRange));
        if (kType != null) {
            args.add(ClojureBridge.typeKeyword(kType));
        }
        return page(ClojureBridge.javaList(ClojureBridge.core("key-range", args.toArray())),
                    limit,
                    offset);
    }

    /**
     * Returns the number of keys in the given range.
     */
    public long keyRangeCount(String dbi, List<?> kRange, String kType) {
        if (kType == null) {
            return ClojureBridge.javaLong(ClojureBridge.core("key-range-count",
                                                             resource(),
                                                             dbi,
                                                             ClojureBridge.rangeInput(kRange)));
        }
        return ClojureBridge.javaLong(ClojureBridge.core("key-range-count",
                                                         resource(),
                                                         dbi,
                                                         ClojureBridge.rangeInput(kRange),
                                                         ClojureBridge.typeKeyword(kType)));
    }

    /**
     * Returns the approximate number of entries in the given range.
     */
    public long rangeCount(String dbi, List<?> kRange, String kType) {
        if (kType == null) {
            return ClojureBridge.javaLong(ClojureBridge.core("range-count",
                                                             resource(),
                                                             dbi,
                                                             ClojureBridge.rangeInput(kRange)));
        }
        return ClojureBridge.javaLong(ClojureBridge.core("range-count",
                                                         resource(),
                                                         dbi,
                                                         ClojureBridge.rangeInput(kRange),
                                                         ClojureBridge.typeKeyword(kType)));
    }

    /**
     * Flushes store state to disk.
     */
    public void sync() {
        ClojureBridge.core("sync", resource());
    }

    /**
     * Flushes store state to disk using the given force flag.
     */
    public void sync(long force) {
        ClojureBridge.core("sync", resource(), force);
    }

    /**
     * Escape hatch for calling a KV-scoped JSON API operation directly.
     */
    public Object exec(String op, Map<String, ?> args) {
        return execJson(op, args);
    }

    private List<Object> page(List<Object> items, Integer limit, Integer offset) {
        int start = offset == null ? 0 : Math.max(offset, 0);
        if (start >= items.size()) {
            return List.of();
        }
        int end = limit == null ? items.size() : Math.min(items.size(), start + Math.max(limit, 0));
        if (end <= start) {
            return List.of();
        }
        return new ArrayList<>(items.subList(start, end));
    }
}
