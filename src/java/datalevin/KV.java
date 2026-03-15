package datalevin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

/**
 * Handle for a local key-value store.
 *
 * <p>Use instances with try-with-resources when you own the handle lifecycle.
 * Methods in this class expose common DBI operations and range lookups.
 */
public final class KV extends HandleResource {

    KV(Object kv) {
        super(kv,
              resource -> ClojureRuntime.core("close-kv", resource),
              "kv",
              "kv");
    }

    /**
     * Returns whether this handle has been closed.
     */
    public boolean closed() {
        return isReleased() || ClojureCodec.javaBoolean(ClojureRuntime.core("closed-kv?", resource()));
    }

    /**
     * Returns the root directory backing this KV store.
     */
    public String dir() {
        return ClojureCodec.javaString(ClojureRuntime.core("dir", resource()));
    }

    /**
     * Opens a regular DBI with default options.
     */
    public void openDbi(String dbiName) {
        ClojureRuntime.core("open-dbi", resource(), dbiName);
    }

    /**
     * Opens a regular DBI with explicit options.
     */
    public void openDbi(String dbiName, Map<?, ?> opts) {
        ClojureRuntime.core("open-dbi", resource(), dbiName, DatalevinForms.optionsInput(opts));
    }

    /**
     * Opens a list DBI with default options.
     */
    public void openListDbi(String listName) {
        ClojureRuntime.core("open-list-dbi", resource(), listName);
    }

    /**
     * Opens a list DBI with explicit options.
     */
    public void openListDbi(String listName, Map<?, ?> opts) {
        ClojureRuntime.core("open-list-dbi", resource(), listName, DatalevinForms.optionsInput(opts));
    }

    /**
     * Adds values to the list associated with {@code key}.
     */
    public void putListItems(String listName, Object key, Object values, String kType, String vType) {
        putListItemsInternal(listName, key, values, kType, vType);
    }

    /**
     * Adds values to the list associated with {@code key}.
     */
    public void putListItems(String listName, Object key, Object values, KVType kType, KVType vType) {
        putListItemsInternal(listName, key, values, kType, vType);
    }

    /**
     * Deletes the entire list associated with {@code key}.
     */
    public void delListItems(String listName, Object key, String kType) {
        delListItemsInternal(listName, key, kType);
    }

    /**
     * Deletes the entire list associated with {@code key}.
     */
    public void deleteListItems(String listName, Object key, String kType) {
        delListItems(listName, key, kType);
    }

    /**
     * Deletes the entire list associated with {@code key}.
     */
    public void delListItems(String listName, Object key, KVType kType) {
        delListItemsInternal(listName, key, kType);
    }

    /**
     * Deletes the entire list associated with {@code key}.
     */
    public void deleteListItems(String listName, Object key, KVType kType) {
        delListItems(listName, key, kType);
    }

    /**
     * Deletes the provided values from the list associated with {@code key}.
     */
    public void delListItems(String listName, Object key, Object values, String kType, String vType) {
        delListItemsInternal(listName, key, values, kType, vType);
    }

    /**
     * Deletes the provided values from the list associated with {@code key}.
     */
    public void deleteListItems(String listName, Object key, Object values, String kType, String vType) {
        delListItems(listName, key, values, kType, vType);
    }

    /**
     * Deletes the provided values from the list associated with {@code key}.
     */
    public void delListItems(String listName, Object key, Object values, KVType kType, KVType vType) {
        delListItemsInternal(listName, key, values, kType, vType);
    }

    /**
     * Deletes the provided values from the list associated with {@code key}.
     */
    public void deleteListItems(String listName, Object key, Object values, KVType kType, KVType vType) {
        delListItems(listName, key, values, kType, vType);
    }

    /**
     * Returns the values in the list associated with {@code key}.
     */
    public List<?> getList(String listName, Object key, String kType, String vType) {
        return getListInternal(listName, key, kType, vType);
    }

    /**
     * Returns the values in the list associated with {@code key}.
     */
    public List<?> getList(String listName, Object key, KVType kType, KVType vType) {
        return getListInternal(listName, key, kType, vType);
    }

    /**
     * Returns the values in the list associated with {@code key}, paged in memory.
     */
    public List<?> getList(String listName,
                           Object key,
                           String kType,
                           String vType,
                           Integer limit,
                           Integer offset) {
        return page(getList(listName, key, kType, vType), limit, offset);
    }

    /**
     * Returns the values in the list associated with {@code key}, paged in memory.
     */
    public List<?> getList(String listName,
                           Object key,
                           KVType kType,
                           KVType vType,
                           Integer limit,
                           Integer offset) {
        return page(getList(listName, key, kType, vType), limit, offset);
    }

    /**
     * Visits the values in the list associated with {@code key}.
     */
    public void visitList(String listName,
                          Consumer<Object> visitor,
                          Object key,
                          String kType,
                          String vType) {
        visitListInternal(listName, visitor, key, kType, vType);
    }

    /**
     * Visits the values in the list associated with {@code key}.
     */
    public void visitList(String listName,
                          Consumer<Object> visitor,
                          Object key,
                          KVType kType,
                          KVType vType) {
        visitListInternal(listName, visitor, key, kType, vType);
    }

    /**
     * Returns the number of items in the list associated with {@code key}.
     */
    public long listCount(String listName, Object key, String kType) {
        return listCountInternal(listName, key, kType);
    }

    /**
     * Returns the number of items in the list associated with {@code key}.
     */
    public long listCount(String listName, Object key, KVType kType) {
        return listCountInternal(listName, key, kType);
    }

    /**
     * Returns whether {@code value} is in the list associated with {@code key}.
     */
    public boolean inList(String listName, Object key, Object value, String kType, String vType) {
        return inListInternal(listName, key, value, kType, vType);
    }

    /**
     * Returns whether {@code value} is in the list associated with {@code key}.
     */
    public boolean inList(String listName, Object key, Object value, KVType kType, KVType vType) {
        return inListInternal(listName, key, value, kType, vType);
    }

    /**
     * Returns list-backed key-value pairs in the given key and value ranges.
     */
    public List<?> listRange(String listName,
                             List<?> kRange,
                             String kType,
                             List<?> vRange,
                             String vType,
                             Integer limit,
                             Integer offset) {
        return listRangeInternal(listName, kRange, kType, vRange, vType, limit, offset);
    }

    /**
     * Returns list-backed key-value pairs in the given key and value ranges.
     */
    public List<?> listRange(String listName,
                             RangeSpec kRange,
                             String kType,
                             RangeSpec vRange,
                             String vType,
                             Integer limit,
                             Integer offset) {
        return listRangeInternal(listName, kRange, kType, vRange, vType, limit, offset);
    }

    /**
     * Returns list-backed key-value pairs in the given key and value ranges.
     */
    public List<?> listRange(String listName,
                             List<?> kRange,
                             KVType kType,
                             List<?> vRange,
                             KVType vType,
                             Integer limit,
                             Integer offset) {
        return listRangeInternal(listName, kRange, kType, vRange, vType, limit, offset);
    }

    /**
     * Returns list-backed key-value pairs in the given key and value ranges.
     */
    public List<?> listRange(String listName,
                             RangeSpec kRange,
                             KVType kType,
                             RangeSpec vRange,
                             KVType vType,
                             Integer limit,
                             Integer offset) {
        return listRangeInternal(listName, kRange, kType, vRange, vType, limit, offset);
    }

    /**
     * Returns the approximate number of list-backed key-values in the given key range.
     *
     * <p>This count ignores the value-range boundary.
     */
    public long listRangeCount(String listName, List<?> kRange, String kType) {
        return listRangeCountInternal(listName, kRange, kType);
    }

    /**
     * Returns the approximate number of list-backed key-values in the given key range.
     *
     * <p>This count ignores the value-range boundary.
     */
    public long listRangeCount(String listName, RangeSpec kRange, String kType) {
        return listRangeCountInternal(listName, kRange, kType);
    }

    /**
     * Returns the approximate number of list-backed key-values in the given key range.
     *
     * <p>This count ignores the value-range boundary.
     */
    public long listRangeCount(String listName, List<?> kRange, KVType kType) {
        return listRangeCountInternal(listName, kRange, kType);
    }

    /**
     * Returns the approximate number of list-backed key-values in the given key range.
     *
     * <p>This count ignores the value-range boundary.
     */
    public long listRangeCount(String listName, RangeSpec kRange, KVType kType) {
        return listRangeCountInternal(listName, kRange, kType);
    }

    /**
     * Returns list-backed key-value pairs in the given key and value ranges that satisfy {@code predicate}.
     */
    public List<?> listRangeFilter(String listName,
                                   BiPredicate<Object, Object> predicate,
                                   List<?> kRange,
                                   String kType,
                                   List<?> vRange,
                                   String vType,
                                   Integer limit,
                                   Integer offset) {
        return listRangeFilterInternal(listName, predicate, kRange, kType, vRange, vType, limit, offset);
    }

    /**
     * Returns list-backed key-value pairs in the given key and value ranges that satisfy {@code predicate}.
     */
    public List<?> listRangeFilter(String listName,
                                   BiPredicate<Object, Object> predicate,
                                   RangeSpec kRange,
                                   String kType,
                                   RangeSpec vRange,
                                   String vType,
                                   Integer limit,
                                   Integer offset) {
        return listRangeFilterInternal(listName, predicate, kRange, kType, vRange, vType, limit, offset);
    }

    /**
     * Returns list-backed key-value pairs in the given key and value ranges that satisfy {@code predicate}.
     */
    public List<?> listRangeFilter(String listName,
                                   BiPredicate<Object, Object> predicate,
                                   List<?> kRange,
                                   KVType kType,
                                   List<?> vRange,
                                   KVType vType,
                                   Integer limit,
                                   Integer offset) {
        return listRangeFilterInternal(listName, predicate, kRange, kType, vRange, vType, limit, offset);
    }

    /**
     * Returns list-backed key-value pairs in the given key and value ranges that satisfy {@code predicate}.
     */
    public List<?> listRangeFilter(String listName,
                                   BiPredicate<Object, Object> predicate,
                                   RangeSpec kRange,
                                   KVType kType,
                                   RangeSpec vRange,
                                   KVType vType,
                                   Integer limit,
                                   Integer offset) {
        return listRangeFilterInternal(listName, predicate, kRange, kType, vRange, vType, limit, offset);
    }

    /**
     * Returns the truthy results of applying {@code fn} to list-backed key-values in the given ranges.
     */
    public List<?> listRangeKeep(String listName,
                                 BiFunction<Object, Object, ?> fn,
                                 List<?> kRange,
                                 String kType,
                                 List<?> vRange,
                                 String vType,
                                 Integer limit,
                                 Integer offset) {
        return listRangeKeepInternal(listName, fn, kRange, kType, vRange, vType, limit, offset);
    }

    /**
     * Returns the truthy results of applying {@code fn} to list-backed key-values in the given ranges.
     */
    public List<?> listRangeKeep(String listName,
                                 BiFunction<Object, Object, ?> fn,
                                 RangeSpec kRange,
                                 String kType,
                                 RangeSpec vRange,
                                 String vType,
                                 Integer limit,
                                 Integer offset) {
        return listRangeKeepInternal(listName, fn, kRange, kType, vRange, vType, limit, offset);
    }

    /**
     * Returns the truthy results of applying {@code fn} to list-backed key-values in the given ranges.
     */
    public List<?> listRangeKeep(String listName,
                                 BiFunction<Object, Object, ?> fn,
                                 List<?> kRange,
                                 KVType kType,
                                 List<?> vRange,
                                 KVType vType,
                                 Integer limit,
                                 Integer offset) {
        return listRangeKeepInternal(listName, fn, kRange, kType, vRange, vType, limit, offset);
    }

    /**
     * Returns the truthy results of applying {@code fn} to list-backed key-values in the given ranges.
     */
    public List<?> listRangeKeep(String listName,
                                 BiFunction<Object, Object, ?> fn,
                                 RangeSpec kRange,
                                 KVType kType,
                                 RangeSpec vRange,
                                 KVType vType,
                                 Integer limit,
                                 Integer offset) {
        return listRangeKeepInternal(listName, fn, kRange, kType, vRange, vType, limit, offset);
    }

    /**
     * Returns the first truthy result of applying {@code fn} to list-backed key-values in the given ranges.
     */
    public Object listRangeSome(String listName,
                                BiFunction<Object, Object, ?> fn,
                                List<?> kRange,
                                String kType,
                                List<?> vRange,
                                String vType) {
        return listRangeSomeInternal(listName, fn, kRange, kType, vRange, vType);
    }

    /**
     * Returns the first truthy result of applying {@code fn} to list-backed key-values in the given ranges.
     */
    public Object listRangeSome(String listName,
                                BiFunction<Object, Object, ?> fn,
                                RangeSpec kRange,
                                String kType,
                                RangeSpec vRange,
                                String vType) {
        return listRangeSomeInternal(listName, fn, kRange, kType, vRange, vType);
    }

    /**
     * Returns the first truthy result of applying {@code fn} to list-backed key-values in the given ranges.
     */
    public Object listRangeSome(String listName,
                                BiFunction<Object, Object, ?> fn,
                                List<?> kRange,
                                KVType kType,
                                List<?> vRange,
                                KVType vType) {
        return listRangeSomeInternal(listName, fn, kRange, kType, vRange, vType);
    }

    /**
     * Returns the first truthy result of applying {@code fn} to list-backed key-values in the given ranges.
     */
    public Object listRangeSome(String listName,
                                BiFunction<Object, Object, ?> fn,
                                RangeSpec kRange,
                                KVType kType,
                                RangeSpec vRange,
                                KVType vType) {
        return listRangeSomeInternal(listName, fn, kRange, kType, vRange, vType);
    }

    /**
     * Returns the number of list-backed key-values in the given ranges that satisfy {@code predicate}.
     */
    public long listRangeFilterCount(String listName,
                                     BiPredicate<Object, Object> predicate,
                                     List<?> kRange,
                                     String kType,
                                     List<?> vRange,
                                     String vType) {
        return listRangeFilterCountInternal(listName, predicate, kRange, kType, vRange, vType);
    }

    /**
     * Returns the number of list-backed key-values in the given ranges that satisfy {@code predicate}.
     */
    public long listRangeFilterCount(String listName,
                                     BiPredicate<Object, Object> predicate,
                                     RangeSpec kRange,
                                     String kType,
                                     RangeSpec vRange,
                                     String vType) {
        return listRangeFilterCountInternal(listName, predicate, kRange, kType, vRange, vType);
    }

    /**
     * Returns the number of list-backed key-values in the given ranges that satisfy {@code predicate}.
     */
    public long listRangeFilterCount(String listName,
                                     BiPredicate<Object, Object> predicate,
                                     List<?> kRange,
                                     KVType kType,
                                     List<?> vRange,
                                     KVType vType) {
        return listRangeFilterCountInternal(listName, predicate, kRange, kType, vRange, vType);
    }

    /**
     * Returns the number of list-backed key-values in the given ranges that satisfy {@code predicate}.
     */
    public long listRangeFilterCount(String listName,
                                     BiPredicate<Object, Object> predicate,
                                     RangeSpec kRange,
                                     KVType kType,
                                     RangeSpec vRange,
                                     KVType vType) {
        return listRangeFilterCountInternal(listName, predicate, kRange, kType, vRange, vType);
    }

    /**
     * Visits list-backed key-values in the given ranges.
     */
    public void visitListRange(String listName,
                               BiConsumer<Object, Object> visitor,
                               List<?> kRange,
                               String kType,
                               List<?> vRange,
                               String vType) {
        visitListRangeInternal(listName, visitor, kRange, kType, vRange, vType);
    }

    /**
     * Visits list-backed key-values in the given ranges.
     */
    public void visitListRange(String listName,
                               BiConsumer<Object, Object> visitor,
                               RangeSpec kRange,
                               String kType,
                               RangeSpec vRange,
                               String vType) {
        visitListRangeInternal(listName, visitor, kRange, kType, vRange, vType);
    }

    /**
     * Visits list-backed key-values in the given ranges.
     */
    public void visitListRange(String listName,
                               BiConsumer<Object, Object> visitor,
                               List<?> kRange,
                               KVType kType,
                               List<?> vRange,
                               KVType vType) {
        visitListRangeInternal(listName, visitor, kRange, kType, vRange, vType);
    }

    /**
     * Visits list-backed key-values in the given ranges.
     */
    public void visitListRange(String listName,
                               BiConsumer<Object, Object> visitor,
                               RangeSpec kRange,
                               KVType kType,
                               RangeSpec vRange,
                               KVType vType) {
        visitListRangeInternal(listName, visitor, kRange, kType, vRange, vType);
    }

    /**
     * Returns the first list-backed key-value in the given key and value ranges.
     */
    public Object listRangeFirst(String listName,
                                 List<?> kRange,
                                 String kType,
                                 List<?> vRange,
                                 String vType) {
        return listRangeFirstInternal(listName, kRange, kType, vRange, vType);
    }

    /**
     * Returns the first list-backed key-value in the given key and value ranges.
     */
    public Object listRangeFirst(String listName,
                                 RangeSpec kRange,
                                 String kType,
                                 RangeSpec vRange,
                                 String vType) {
        return listRangeFirstInternal(listName, kRange, kType, vRange, vType);
    }

    /**
     * Returns the first list-backed key-value in the given key and value ranges.
     */
    public Object listRangeFirst(String listName,
                                 List<?> kRange,
                                 KVType kType,
                                 List<?> vRange,
                                 KVType vType) {
        return listRangeFirstInternal(listName, kRange, kType, vRange, vType);
    }

    /**
     * Returns the first list-backed key-value in the given key and value ranges.
     */
    public Object listRangeFirst(String listName,
                                 RangeSpec kRange,
                                 KVType kType,
                                 RangeSpec vRange,
                                 KVType vType) {
        return listRangeFirstInternal(listName, kRange, kType, vRange, vType);
    }

    /**
     * Returns the first {@code n} list-backed key-values in the given key and value ranges.
     */
    public List<?> listRangeFirstN(String listName,
                                   long n,
                                   List<?> kRange,
                                   String kType,
                                   List<?> vRange,
                                   String vType) {
        return listRangeFirstNInternal(listName, n, kRange, kType, vRange, vType);
    }

    /**
     * Returns the first {@code n} list-backed key-values in the given key and value ranges.
     */
    public List<?> listRangeFirstN(String listName,
                                   long n,
                                   RangeSpec kRange,
                                   String kType,
                                   RangeSpec vRange,
                                   String vType) {
        return listRangeFirstNInternal(listName, n, kRange, kType, vRange, vType);
    }

    /**
     * Returns the first {@code n} list-backed key-values in the given key and value ranges.
     */
    public List<?> listRangeFirstN(String listName,
                                   long n,
                                   List<?> kRange,
                                   KVType kType,
                                   List<?> vRange,
                                   KVType vType) {
        return listRangeFirstNInternal(listName, n, kRange, kType, vRange, vType);
    }

    /**
     * Returns the first {@code n} list-backed key-values in the given key and value ranges.
     */
    public List<?> listRangeFirstN(String listName,
                                   long n,
                                   RangeSpec kRange,
                                   KVType kType,
                                   RangeSpec vRange,
                                   KVType vType) {
        return listRangeFirstNInternal(listName, n, kRange, kType, vRange, vType);
    }

    /**
     * Returns the total number of list items in the given key range.
     */
    public long keyRangeListCount(String listName, List<?> kRange, String kType) {
        return keyRangeListCountInternal(listName, kRange, kType);
    }

    /**
     * Returns the total number of list items in the given key range.
     */
    public long keyRangeListCount(String listName, RangeSpec kRange, String kType) {
        return keyRangeListCountInternal(listName, kRange, kType);
    }

    /**
     * Returns the total number of list items in the given key range.
     */
    public long keyRangeListCount(String listName, List<?> kRange, KVType kType) {
        return keyRangeListCountInternal(listName, kRange, kType);
    }

    /**
     * Returns the total number of list items in the given key range.
     */
    public long keyRangeListCount(String listName, RangeSpec kRange, KVType kType) {
        return keyRangeListCountInternal(listName, kRange, kType);
    }

    /**
     * Clears all entries from the named DBI.
     */
    public void clearDbi(String dbiName) {
        ClojureRuntime.core("clear-dbi", resource(), dbiName);
    }

    /**
     * Drops the named DBI.
     */
    public void dropDbi(String dbiName) {
        ClojureRuntime.core("drop-dbi", resource(), dbiName);
    }

    /**
     * Lists all DBIs in the store.
     */
    public List<?> listDbis() {
        return ResultSupport.sequence(ClojureRuntime.core("list-dbis", resource()));
    }

    /**
     * Returns environment statistics.
     */
    public Map<?, ?> stat() {
        return (Map<?, ?>) ClojureRuntime.core("stat", resource());
    }

    /**
     * Returns statistics for the named DBI.
     */
    public Map<?, ?> stat(String dbiName) {
        return (Map<?, ?>) ClojureRuntime.core("stat", resource(), dbiName);
    }

    /**
     * Returns the number of entries in the named DBI.
     */
    public long entries(String dbiName) {
        return ClojureCodec.javaLong(ClojureRuntime.core("entries", resource(), dbiName));
    }

    /**
     * Applies KV transactions with default type inference.
     */
    public Object transact(Object txs) {
        return ClojureRuntime.core("transact-kv",
                                  resource(),
                                  DatalevinForms.kvTxsInput(txs));
    }

    /**
     * Applies KV transactions against the named DBI.
     */
    public Object transact(String dbiName, Object txs) {
        return ClojureRuntime.core("transact-kv",
                                  resource(),
                                  dbiName,
                                  DatalevinForms.kvTxsInput(txs));
    }

    /**
     * Applies KV transactions with an explicit key type.
     */
    public Object transact(String dbiName, Object txs, String kType) {
        return transactInternal(dbiName, txs, kType, null);
    }

    /**
     * Applies KV transactions with an explicit key type.
     */
    public Object transact(String dbiName, Object txs, KVType kType) {
        return transactInternal(dbiName, txs, kType, null);
    }

    /**
     * Applies KV transactions with explicit key and value types.
     */
    public Object transact(String dbiName, Object txs, String kType, String vType) {
        return transactInternal(dbiName, txs, kType, vType);
    }

    /**
     * Applies KV transactions with explicit key and value types.
     */
    public Object transact(String dbiName, Object txs, KVType kType, KVType vType) {
        return transactInternal(dbiName, txs, kType, vType);
    }

    /**
     * Returns the value for {@code key} from the named DBI.
     */
    public Object getValue(String dbi, Object key) {
        return ClojureRuntime.core("get-value",
                                  resource(),
                                  dbi,
                                  ClojureCodec.runtimeInput(key));
    }

    /**
     * Returns the value for {@code key} from the named DBI with explicit types.
     */
    public Object getValue(String dbi, Object key, String kType, String vType, boolean ignoreKey) {
        return getValueInternal(dbi, key, kType, vType, ignoreKey);
    }

    /**
     * Returns the value for {@code key} from the named DBI with explicit types.
     */
    public Object getValue(String dbi, Object key, KVType kType, KVType vType, boolean ignoreKey) {
        return getValueInternal(dbi, key, kType, vType, ignoreKey);
    }

    /**
     * Returns the sorted rank of {@code key}, or {@code null} when absent.
     */
    public Long getRank(String dbi, Object key) {
        return ClojureCodec.javaNullableLong(ClojureRuntime.core("get-rank",
                                                                 resource(),
                                                                 dbi,
                                                                 ClojureCodec.runtimeInput(key)));
    }

    /**
     * Returns the sorted rank of {@code key} with an explicit key type.
     */
    public Long getRank(String dbi, Object key, String kType) {
        return getRankInternal(dbi, key, kType);
    }

    /**
     * Returns the sorted rank of {@code key} with an explicit key type.
     */
    public Long getRank(String dbi, Object key, KVType kType) {
        return getRankInternal(dbi, key, kType);
    }

    /**
     * Returns the entry at the given rank.
     */
    public Object getByRank(String dbi, long rank) {
        return ClojureRuntime.core("get-by-rank", resource(), dbi, rank);
    }

    /**
     * Returns the entry at the given rank with explicit types.
     */
    public Object getByRank(String dbi, long rank, String kType, String vType, boolean ignoreKey) {
        return getByRankInternal(dbi, rank, kType, vType, ignoreKey);
    }

    /**
     * Returns the entry at the given rank with explicit types.
     */
    public Object getByRank(String dbi, long rank, KVType kType, KVType vType, boolean ignoreKey) {
        return getByRankInternal(dbi, rank, kType, vType, ignoreKey);
    }

    /**
     * Returns entries in the given key range.
     */
    public List<?> getRange(String dbi, List<?> kRange) {
        return ResultSupport.sequence(ClojureRuntime.core("get-range",
                                                         resource(),
                                                         dbi,
                                                         DatalevinForms.rangeInput(kRange)));
    }

    /**
     * Returns entries in the given key range.
     */
    public List<?> getRange(String dbi, RangeSpec kRange) {
        return getRange(dbi, buildRange(kRange));
    }

    /**
     * Returns entries in the given key range with explicit types and paging.
     */
    public List<?> getRange(String dbi,
                            List<?> kRange,
                            String kType,
                            String vType,
                            Integer limit,
                            Integer offset) {
        return getRangeInternal(dbi, kRange, kType, vType, limit, offset);
    }

    /**
     * Returns entries in the given key range with explicit types and paging.
     */
    public List<?> getRange(String dbi,
                            RangeSpec kRange,
                            String kType,
                            String vType,
                            Integer limit,
                            Integer offset) {
        return getRangeInternal(dbi, kRange, kType, vType, limit, offset);
    }

    /**
     * Returns entries in the given key range with explicit types and paging.
     */
    public List<?> getRange(String dbi,
                            List<?> kRange,
                            KVType kType,
                            KVType vType,
                            Integer limit,
                            Integer offset) {
        return getRangeInternal(dbi, kRange, kType, vType, limit, offset);
    }

    /**
     * Returns entries in the given key range with explicit types and paging.
     */
    public List<?> getRange(String dbi,
                            RangeSpec kRange,
                            KVType kType,
                            KVType vType,
                            Integer limit,
                            Integer offset) {
        return getRangeInternal(dbi, kRange, kType, vType, limit, offset);
    }

    /**
     * Returns the key-value entry at the given rank with explicit types.
     */
    public Object getEntryByRank(String dbi, long rank, String kType, String vType) {
        return getEntryByRankInternal(dbi, rank, kType, vType);
    }

    /**
     * Returns the key-value entry at the given rank with explicit types.
     */
    public Object getEntryByRank(String dbi, long rank, KVType kType, KVType vType) {
        return getEntryByRankInternal(dbi, rank, kType, vType);
    }

    /**
     * Returns keys in the given range.
     */
    public List<?> keyRange(String dbi, List<?> kRange, String kType, Integer limit, Integer offset) {
        return keyRangeInternal(dbi, kRange, kType, limit, offset);
    }

    /**
     * Returns keys in the given range.
     */
    public List<?> keyRange(String dbi, RangeSpec kRange, String kType, Integer limit, Integer offset) {
        return keyRangeInternal(dbi, kRange, kType, limit, offset);
    }

    /**
     * Returns keys in the given range.
     */
    public List<?> keyRange(String dbi, List<?> kRange, KVType kType, Integer limit, Integer offset) {
        return keyRangeInternal(dbi, kRange, kType, limit, offset);
    }

    /**
     * Returns keys in the given range.
     */
    public List<?> keyRange(String dbi, RangeSpec kRange, KVType kType, Integer limit, Integer offset) {
        return keyRangeInternal(dbi, kRange, kType, limit, offset);
    }

    /**
     * Returns the number of keys in the given range.
     */
    public long keyRangeCount(String dbi, List<?> kRange, String kType) {
        return keyRangeCountInternal(dbi, kRange, kType);
    }

    /**
     * Returns the number of keys in the given range.
     */
    public long keyRangeCount(String dbi, RangeSpec kRange, String kType) {
        return keyRangeCountInternal(dbi, kRange, kType);
    }

    /**
     * Returns the number of keys in the given range.
     */
    public long keyRangeCount(String dbi, List<?> kRange, KVType kType) {
        return keyRangeCountInternal(dbi, kRange, kType);
    }

    /**
     * Returns the number of keys in the given range.
     */
    public long keyRangeCount(String dbi, RangeSpec kRange, KVType kType) {
        return keyRangeCountInternal(dbi, kRange, kType);
    }

    /**
     * Returns the approximate number of entries in the given range.
     */
    public long rangeCount(String dbi, List<?> kRange, String kType) {
        return rangeCountInternal(dbi, kRange, kType);
    }

    /**
     * Returns the number of entries in the given range.
     */
    public long rangeCount(String dbi, RangeSpec kRange, String kType) {
        return rangeCountInternal(dbi, kRange, kType);
    }

    /**
     * Returns the approximate number of entries in the given range.
     */
    public long rangeCount(String dbi, List<?> kRange, KVType kType) {
        return rangeCountInternal(dbi, kRange, kType);
    }

    /**
     * Returns the number of entries in the given range.
     */
    public long rangeCount(String dbi, RangeSpec kRange, KVType kType) {
        return rangeCountInternal(dbi, kRange, kType);
    }

    /**
     * Flushes store state to disk.
     */
    public void sync() {
        ClojureRuntime.core("sync", resource());
    }

    /**
     * Flushes store state to disk using the given force flag.
     */
    public void sync(long force) {
        ClojureRuntime.core("sync", resource(), force);
    }

    /**
     * Escape hatch for calling a KV-scoped JSON API operation directly.
     */
    public Object exec(String op, Map<String, ?> args) {
        return execJson(op, args);
    }

    private static List<?> buildRange(RangeSpec range) {
        return range == null ? null : range.build();
    }

    private static Object typeArg(Object type) {
        return DatalevinForms.typeInput(type);
    }

    private static Object rangeArg(Object range) {
        if (range instanceof RangeSpec spec) {
            return DatalevinForms.rangeInput(spec.build());
        }
        return DatalevinForms.rangeInput((List<?>) range);
    }

    private static Object typedRangeArg(Object range, Object type) {
        if (range instanceof RangeSpec spec) {
            return DatalevinForms.rangeInput(spec.build(), type);
        }
        return DatalevinForms.rangeInput((List<?>) range, type);
    }

    private void putListItemsInternal(String listName, Object key, Object values, Object kType, Object vType) {
        ClojureRuntime.core("put-list-items",
                           resource(),
                           listName,
                           ClojureCodec.runtimeInput(key),
                           ClojureCodec.runtimeInput(values),
                           typeArg(kType),
                           typeArg(vType));
    }

    private void delListItemsInternal(String listName, Object key, Object kType) {
        ClojureRuntime.core("del-list-items",
                           resource(),
                           listName,
                           ClojureCodec.runtimeInput(key),
                           typeArg(kType));
    }

    private void delListItemsInternal(String listName, Object key, Object values, Object kType, Object vType) {
        ClojureRuntime.core("del-list-items",
                           resource(),
                           listName,
                           ClojureCodec.runtimeInput(key),
                           ClojureCodec.runtimeInput(values),
                           typeArg(kType),
                           typeArg(vType));
    }

    private List<?> getListInternal(String listName, Object key, Object kType, Object vType) {
        return ResultSupport.sequence(ClojureRuntime.core("get-list",
                                                          resource(),
                                                          listName,
                                                          ClojureCodec.runtimeInput(key),
                                                          typeArg(kType),
                                                          typeArg(vType)));
    }

    private void visitListInternal(String listName,
                                   Consumer<Object> visitor,
                                   Object key,
                                   Object kType,
                                   Object vType) {
        runVisitList(listName,
                     ClojureFns.consumer(visitor),
                     ClojureCodec.runtimeInput(key),
                     typeArg(kType),
                     typeArg(vType));
    }

    private long listCountInternal(String listName, Object key, Object kType) {
        return ClojureCodec.javaLong(ClojureRuntime.core("list-count",
                                                         resource(),
                                                         listName,
                                                         ClojureCodec.runtimeInput(key),
                                                         typeArg(kType)));
    }

    private boolean inListInternal(String listName, Object key, Object value, Object kType, Object vType) {
        return ClojureCodec.javaBoolean(ClojureRuntime.core("in-list?",
                                                            resource(),
                                                            listName,
                                                            ClojureCodec.runtimeInput(key),
                                                            ClojureCodec.runtimeInput(value),
                                                            typeArg(kType),
                                                            typeArg(vType)));
    }

    private List<?> listRangeInternal(String listName,
                                      Object kRange,
                                      Object kType,
                                      Object vRange,
                                      Object vType,
                                      Integer limit,
                                      Integer offset) {
        return page(ResultSupport.sequence(runListRangeOp("list-range",
                                                         listName,
                                                         rangeArg(kRange),
                                                         typeArg(kType),
                                                         rangeArg(vRange),
                                                         typeArg(vType))),
                    limit,
                    offset);
    }

    private long listRangeCountInternal(String listName, Object kRange, Object kType) {
        return ClojureCodec.javaLong(ClojureRuntime.core("list-range-count",
                                                         resource(),
                                                         listName,
                                                         rangeArg(kRange),
                                                         typeArg(kType)));
    }

    private List<?> listRangeFilterInternal(String listName,
                                            BiPredicate<Object, Object> predicate,
                                            Object kRange,
                                            Object kType,
                                            Object vRange,
                                            Object vType,
                                            Integer limit,
                                            Integer offset) {
        return runListRangeFilter(listName,
                                  predicate,
                                  rangeArg(kRange),
                                  typeArg(kType),
                                  rangeArg(vRange),
                                  typeArg(vType),
                                  limit,
                                  offset);
    }

    private List<?> listRangeKeepInternal(String listName,
                                          BiFunction<Object, Object, ?> fn,
                                          Object kRange,
                                          Object kType,
                                          Object vRange,
                                          Object vType,
                                          Integer limit,
                                          Integer offset) {
        return runListRangeKeep(listName,
                                fn,
                                rangeArg(kRange),
                                typeArg(kType),
                                rangeArg(vRange),
                                typeArg(vType),
                                limit,
                                offset);
    }

    private Object listRangeSomeInternal(String listName,
                                         BiFunction<Object, Object, ?> fn,
                                         Object kRange,
                                         Object kType,
                                         Object vRange,
                                         Object vType) {
        return runListRangeFnOp("list-range-some",
                                listName,
                                ClojureFns.biFunction(fn),
                                rangeArg(kRange),
                                typeArg(kType),
                                rangeArg(vRange),
                                typeArg(vType));
    }

    private long listRangeFilterCountInternal(String listName,
                                              BiPredicate<Object, Object> predicate,
                                              Object kRange,
                                              Object kType,
                                              Object vRange,
                                              Object vType) {
        return ClojureCodec.javaLong(runListRangeFnOp("list-range-filter-count",
                                                      listName,
                                                      ClojureFns.biPredicate(predicate),
                                                      rangeArg(kRange),
                                                      typeArg(kType),
                                                      rangeArg(vRange),
                                                      typeArg(vType)));
    }

    private void visitListRangeInternal(String listName,
                                        BiConsumer<Object, Object> visitor,
                                        Object kRange,
                                        Object kType,
                                        Object vRange,
                                        Object vType) {
        runListRangeFnOp("visit-list-range",
                         listName,
                         ClojureFns.biConsumer(visitor),
                         rangeArg(kRange),
                         typeArg(kType),
                         rangeArg(vRange),
                         typeArg(vType));
    }

    private Object listRangeFirstInternal(String listName,
                                          Object kRange,
                                          Object kType,
                                          Object vRange,
                                          Object vType) {
        return runListRangeOp("list-range-first",
                              listName,
                              rangeArg(kRange),
                              typeArg(kType),
                              rangeArg(vRange),
                              typeArg(vType));
    }

    private List<?> listRangeFirstNInternal(String listName,
                                            long n,
                                            Object kRange,
                                            Object kType,
                                            Object vRange,
                                            Object vType) {
        return ResultSupport.sequence(runListRangeFirstN(listName,
                                                         n,
                                                         rangeArg(kRange),
                                                         typeArg(kType),
                                                         rangeArg(vRange),
                                                         typeArg(vType)));
    }

    private long keyRangeListCountInternal(String listName, Object kRange, Object kType) {
        return ClojureCodec.javaLong(ClojureRuntime.core("key-range-list-count",
                                                         resource(),
                                                         listName,
                                                         rangeArg(kRange),
                                                         typeArg(kType)));
    }

    private Object transactInternal(String dbiName, Object txs, Object kType, Object vType) {
        Object normalizedKType = typeArg(kType);
        Object normalizedVType = typeArg(vType);
        if (normalizedKType == null && normalizedVType == null) {
            return ClojureRuntime.core("transact-kv",
                                      resource(),
                                      dbiName,
                                      DatalevinForms.kvTxsInput(txs));
        }
        if (normalizedVType == null) {
            return ClojureRuntime.core("transact-kv",
                                      resource(),
                                      dbiName,
                                      DatalevinForms.kvTxsInput(txs, normalizedKType, null),
                                      normalizedKType);
        }
        return ClojureRuntime.core("transact-kv",
                                  resource(),
                                  dbiName,
                                  DatalevinForms.kvTxsInput(txs, normalizedKType, normalizedVType),
                                  normalizedKType,
                                  normalizedVType);
    }

    private Object getValueInternal(String dbi, Object key, Object kType, Object vType, boolean ignoreKey) {
        Object normalizedKType = typeArg(kType);
        Object normalizedVType = typeArg(vType);
        return ClojureRuntime.core("get-value",
                                  resource(),
                                  dbi,
                                  DatalevinForms.kvInput(key, normalizedKType),
                                  normalizedKType,
                                  normalizedVType,
                                  ignoreKey);
    }

    private Long getRankInternal(String dbi, Object key, Object kType) {
        return ClojureCodec.javaNullableLong(ClojureRuntime.core("get-rank",
                                                                 resource(),
                                                                 dbi,
                                                                 ClojureCodec.runtimeInput(key),
                                                                 typeArg(kType)));
    }

    private Object getByRankInternal(String dbi, long rank, Object kType, Object vType, boolean ignoreKey) {
        return ClojureRuntime.core("get-by-rank",
                                  resource(),
                                  dbi,
                                  rank,
                                  typeArg(kType),
                                  typeArg(vType),
                                  ignoreKey);
    }

    private List<?> getRangeInternal(String dbi,
                                     Object kRange,
                                     Object kType,
                                     Object vType,
                                     Integer limit,
                                     Integer offset) {
        Object normalizedKType = typeArg(kType);
        Object normalizedVType = typeArg(vType);
        return page(ResultSupport.sequence(runGetRange(dbi,
                                                       typedRangeArg(kRange, normalizedKType),
                                                       normalizedKType,
                                                       normalizedVType)),
                    limit,
                    offset);
    }

    private Object getEntryByRankInternal(String dbi, long rank, Object kType, Object vType) {
        return getByRankInternal(dbi, rank, kType, vType, false);
    }

    private List<?> keyRangeInternal(String dbi, Object kRange, Object kType, Integer limit, Integer offset) {
        return page(ResultSupport.sequence(runKeyRange(dbi,
                                                       rangeArg(kRange),
                                                       typeArg(kType))),
                    limit,
                    offset);
    }

    private long keyRangeCountInternal(String dbi, Object kRange, Object kType) {
        Object normalizedKType = typeArg(kType);
        if (normalizedKType == null) {
            return ClojureCodec.javaLong(ClojureRuntime.core("key-range-count",
                                                             resource(),
                                                             dbi,
                                                             rangeArg(kRange)));
        }
        return ClojureCodec.javaLong(ClojureRuntime.core("key-range-count",
                                                         resource(),
                                                         dbi,
                                                         rangeArg(kRange),
                                                         normalizedKType));
    }

    private long rangeCountInternal(String dbi, Object kRange, Object kType) {
        Object normalizedKType = typeArg(kType);
        if (normalizedKType == null) {
            return ClojureCodec.javaLong(ClojureRuntime.core("range-count",
                                                             resource(),
                                                             dbi,
                                                             rangeArg(kRange)));
        }
        return ClojureCodec.javaLong(ClojureRuntime.core("range-count",
                                                         resource(),
                                                         dbi,
                                                         rangeArg(kRange),
                                                         normalizedKType));
    }

    private List<?> page(List<?> items, Integer limit, Integer offset) {
        int start = offset == null ? 0 : Math.max(offset, 0);
        if (start >= items.size()) {
            return List.of();
        }
        int end = limit == null ? items.size() : Math.min(items.size(), start + Math.max(limit, 0));
        if (end <= start) {
            return List.of();
        }
        if (start == 0 && end == items.size()) {
            return items;
        }
        return new PagedListView(items, start, end - start);
    }

    private void runVisitList(String listName, Object visitor, Object key, Object kType, Object vType) {
        ClojureRuntime.core("visit-list", resource(), listName, visitor, key, kType, vType, false);
    }

    private Object runListRangeOp(String op,
                                  String listName,
                                  Object kRange,
                                  Object kType,
                                  Object vRange,
                                  Object vType) {
        return ClojureRuntime.core(op, resource(), listName, kRange, kType, vRange, vType);
    }

    private Object runListRangeFnOp(String op,
                                    String listName,
                                    Object fn,
                                    Object kRange,
                                    Object kType,
                                    Object vRange,
                                    Object vType) {
        return ClojureRuntime.core(op, resource(), listName, fn, kRange, kType, vRange, vType, false);
    }

    private List<?> runListRangeFilter(String listName,
                                       BiPredicate<Object, Object> predicate,
                                       Object kRange,
                                       Object kType,
                                       Object vRange,
                                       Object vType,
                                       Integer limit,
                                       Integer offset) {
        if (limit != null && limit <= 0) {
            return List.of();
        }
        if (!useBoundedPage(limit, offset)) {
            return page(ResultSupport.sequence(runListRangeFnOp("list-range-filter",
                                                                listName,
                                                                ClojureFns.biPredicate(predicate),
                                                                kRange,
                                                                kType,
                                                                vRange,
                                                                vType)),
                        limit,
                        offset);
        }
        ArrayList<Object> results = new ArrayList<>(initialPageCapacity(limit));
        runListRangeFnOp("visit-list-range",
                         listName,
                         ClojureFns.pagedFilter(predicate,
                                                normalizedOffset(offset),
                                                normalizedLimit(limit),
                                                results),
                         kRange,
                         kType,
                         vRange,
                         vType);
        return results;
    }

    private List<?> runListRangeKeep(String listName,
                                     BiFunction<Object, Object, ?> fn,
                                     Object kRange,
                                     Object kType,
                                     Object vRange,
                                     Object vType,
                                     Integer limit,
                                     Integer offset) {
        if (limit != null && limit <= 0) {
            return List.of();
        }
        if (!useBoundedPage(limit, offset)) {
            return page(ResultSupport.sequence(runListRangeFnOp("list-range-keep",
                                                                listName,
                                                                ClojureFns.biFunction(fn),
                                                                kRange,
                                                                kType,
                                                                vRange,
                                                                vType)),
                        limit,
                        offset);
        }
        ArrayList<Object> results = new ArrayList<>(initialPageCapacity(limit));
        runListRangeFnOp("visit-list-range",
                         listName,
                         ClojureFns.pagedKeep(fn,
                                              normalizedOffset(offset),
                                              normalizedLimit(limit),
                                              results),
                         kRange,
                         kType,
                         vRange,
                         vType);
        return results;
    }

    private static boolean useBoundedPage(Integer limit, Integer offset) {
        return (limit != null && limit > 0) || normalizedOffset(offset) > 0;
    }

    private static int normalizedOffset(Integer offset) {
        return offset == null ? 0 : Math.max(offset, 0);
    }

    private static int normalizedLimit(Integer limit) {
        return limit == null ? Integer.MAX_VALUE : Math.max(limit, 0);
    }

    private static int initialPageCapacity(Integer limit) {
        return limit == null ? 16 : Math.max(limit, 0);
    }

    private Object runListRangeFirstN(String listName,
                                      long n,
                                      Object kRange,
                                      Object kType,
                                      Object vRange,
                                      Object vType) {
        return ClojureRuntime.core("list-range-first-n", resource(), listName, n, kRange, kType, vRange, vType);
    }

    private Object runGetRange(String dbi, Object kRange, Object kType, Object vType) {
        if (kType == null) {
            return ClojureRuntime.core("get-range", resource(), dbi, kRange);
        }
        if (vType == null) {
            return ClojureRuntime.core("get-range", resource(), dbi, kRange, kType);
        }
        return ClojureRuntime.core("get-range", resource(), dbi, kRange, kType, vType);
    }

    private Object runKeyRange(String dbi, Object kRange, Object kType) {
        if (kType == null) {
            return ClojureRuntime.core("key-range", resource(), dbi, kRange);
        }
        return ClojureRuntime.core("key-range", resource(), dbi, kRange, kType);
    }

    private static final class PagedListView extends java.util.AbstractList<Object> {

        private final List<?> items;
        private final int start;
        private final int size;

        private PagedListView(List<?> items, int start, int size) {
            this.items = items;
            this.start = start;
            this.size = size;
        }

        @Override
        public Object get(int index) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException(index);
            }
            return items.get(start + index);
        }

        @Override
        public int size() {
            return size;
        }
    }
}
