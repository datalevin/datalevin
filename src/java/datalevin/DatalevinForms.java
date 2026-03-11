package datalevin;

import clojure.lang.IPersistentMap;
import clojure.lang.IPersistentCollection;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;
import clojure.lang.PersistentHashSet;
import clojure.lang.PersistentVector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Datalevin-specific input and form shaping on top of the shared runtime and
 * codec layers.
 *
 * <p>This layer knows how to build lookup refs, schema maps, tx data, query
 * forms, and other Datalevin domain values, but it does not expose the Java
 * ergonomic wrapper API.
 */
final class DatalevinForms {

    private DatalevinForms() {
    }

    static Object queryForm(String queryEdn) {
        Objects.requireNonNull(queryEdn, "queryEdn");
        return ClojureRuntime.readEdn(queryEdn);
    }

    static Object explainOpts(String optsEdn) {
        return optsEdn == null ? PersistentArrayMap.EMPTY : ClojureRuntime.readEdn(optsEdn);
    }

    static Object pullSelectorInput(Object selector) {
        if (selector == null) {
            return null;
        }
        if (selector instanceof PullSelector pullSelector) {
            return pullSelector.buildForm();
        }
        if (selector instanceof EdnLiteral literal) {
            return ClojureRuntime.readEdn(literal.value());
        }
        if (selector instanceof String s) {
            return ClojureRuntime.readEdn(s);
        }
        return ClojureCodec.runtimeInput(selector);
    }

    static Object rangeInput(List<?> rangeSpec) {
        Objects.requireNonNull(rangeSpec, "rangeSpec");
        if (rangeSpec.isEmpty()) {
            throw new IllegalArgumentException("rangeSpec must not be empty.");
        }
        if (rangeSpec instanceof IPersistentCollection && normalizedRangeSpec(rangeSpec)) {
            return rangeSpec;
        }
        return rangeInput(rangeSpec.get(0), rangeSpec.subList(1, rangeSpec.size()));
    }

    static Object lookupRefInput(Object value) {
        if (isNormalizedLookupRef(value)) {
            return value;
        }
        if (value instanceof List<?> list && list.size() == 2) {
            return PersistentVector.create(Arrays.asList(
                    keywordFromAttr(list.get(0)),
                    ClojureCodec.runtimeInput(list.get(1))
            ));
        }
        if (value instanceof Object[] array && array.length == 2) {
            return PersistentVector.create(Arrays.asList(
                    keywordFromAttr(array[0]),
                    ClojureCodec.runtimeInput(array[1])
            ));
        }
        return ClojureCodec.runtimeInput(value);
    }

    static Object entityIdsInput(List<?> eids) {
        Objects.requireNonNull(eids, "eids");
        if (eids instanceof IPersistentCollection && normalizedEntityIds(eids)) {
            return eids;
        }
        ArrayList<Object> normalized = new ArrayList<>(eids.size());
        for (Object eid : eids) {
            normalized.add(lookupRefInput(eid));
        }
        return PersistentVector.create(normalized);
    }

    static Object schemaInput(Map<?, ?> schema) {
        if (schema == null) {
            return null;
        }
        if (schema instanceof IPersistentMap && normalizedKeywordMap(schema, true)) {
            return schema;
        }
        return keywordMap(schema, true);
    }

    static Object optionsInput(Map<?, ?> opts) {
        if (opts == null) {
            return null;
        }
        if (opts instanceof IPersistentMap && normalizedKeywordMap(opts, true)) {
            return opts;
        }
        return keywordMap(opts, true);
    }

    static Object udfDescriptorInput(Map<?, ?> descriptor) {
        if (descriptor == null) {
            return null;
        }
        if (descriptor instanceof IPersistentMap
                && normalizedKeywordMap(descriptor, true)) {
            return descriptor;
        }
        return keywordMap(descriptor, true);
    }

    static Object renameMapInput(Map<?, ?> renameMap) {
        if (renameMap == null) {
            return null;
        }
        if (renameMap instanceof IPersistentMap && normalizedRenameMap(renameMap)) {
            return renameMap;
        }

        IPersistentMap result = PersistentArrayMap.EMPTY;
        for (Map.Entry<?, ?> entry : renameMap.entrySet()) {
            result = result.assoc(keywordFromAttr(entry.getKey()),
                                  keywordFromAttr(entry.getValue()));
        }
        return result;
    }

    static Object deleteAttrsInput(Collection<?> attrs) {
        if (attrs == null) {
            return null;
        }
        if (attrs instanceof IPersistentCollection && normalizedDeleteAttrs(attrs)) {
            return attrs;
        }

        ArrayList<Object> values = new ArrayList<>(attrs.size());
        for (Object attr : attrs) {
            values.add(keywordFromAttr(attr));
        }
        return PersistentHashSet.create(values);
    }

    static Object createDatabaseType(String dbType) {
        if (dbType == null) {
            return null;
        }
        String normalized = stripLeadingColon(dbType);
        return switch (normalized) {
            case "datalog" -> ClojureCodec.keyword(":datalog");
            case "kv", "key-value" -> ClojureCodec.keyword(":key-value");
            default -> throw new IllegalArgumentException("Unsupported database type: " + dbType);
        };
    }

    static Object createDatabaseType(DatabaseType dbType) {
        return DatabaseType.createArg(dbType);
    }

    static Object roleInput(String role) {
        return ClojureCodec.keyword(role);
    }

    static Object permissionKeyword(String value) {
        return ClojureCodec.keyword(value);
    }

    static Object permissionTarget(String obj, Object tgt) {
        if (tgt == null) {
            return null;
        }
        String normalized = stripLeadingColon(obj);
        if ("datalevin.server/role".equals(normalized) && tgt instanceof String s) {
            return ClojureCodec.keyword(s);
        }
        return ClojureCodec.runtimeInput(tgt);
    }

    static Object typeInput(KVType value) {
        return value == null ? null : value.build();
    }

    static Object typeInput(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof KVType type) {
            return type.build();
        }
        if (value instanceof Keyword) {
            return value;
        }
        if (value instanceof IPersistentCollection collection && normalizedTypeInput(collection)) {
            return collection;
        }
        if (value instanceof EdnLiteral literal) {
            return ClojureRuntime.readEdn(literal.value());
        }
        if (value instanceof String s) {
            return ClojureCodec.keyword(s);
        }
        if (value instanceof Collection<?> collection) {
            ArrayList<Object> converted = new ArrayList<>(collection.size());
            for (Object item : collection) {
                converted.add(typeInput(item));
            }
            return PersistentVector.create(converted);
        }
        if (value instanceof Object[] array) {
            ArrayList<Object> converted = new ArrayList<>(array.length);
            for (Object item : array) {
                converted.add(typeInput(item));
            }
            return PersistentVector.create(converted);
        }
        return ClojureCodec.runtimeInput(value);
    }

    static Object txDataInput(Object txData) {
        if (txData == null) {
            return null;
        }
        if (txData instanceof Collection<?> collection
                && txData instanceof IPersistentCollection
                && normalizedTxData(collection)) {
            return txData;
        }
        ArrayList<Object> items = new ArrayList<>();
        if (txData instanceof Collection<?> collection) {
            for (Object item : collection) {
                items.add(txItemInput(item));
            }
            return PersistentVector.create(items);
        }
        if (txData instanceof Object[] array) {
            for (Object item : array) {
                items.add(txItemInput(item));
            }
            return PersistentVector.create(items);
        }
        throw new IllegalArgumentException("Transaction data must be a collection.");
    }

    static Object kvTxsInput(Object txs) {
        if (txs == null) {
            return null;
        }
        if (txs instanceof Collection<?> collection
                && txs instanceof IPersistentCollection
                && normalizedKvTxs(collection)) {
            return txs;
        }
        ArrayList<Object> items = new ArrayList<>();
        if (txs instanceof Collection<?> collection) {
            for (Object item : collection) {
                items.add(kvTxItemInput(item));
            }
            return PersistentVector.create(items);
        }
        if (txs instanceof Object[] array) {
            for (Object item : array) {
                items.add(kvTxItemInput(item));
            }
            return PersistentVector.create(items);
        }
        throw new IllegalArgumentException("KV transaction data must be a collection.");
    }

    private static Object txItemInput(Object item) {
        if (item instanceof IPersistentMap && item instanceof Map<?, ?> map
                && normalizedKeywordMap(map, false)) {
            return item;
        }
        if (item instanceof IPersistentCollection collection && normalizedTxItem(collection)) {
            return item;
        }
        if (item instanceof Map<?, ?> map) {
            return txEntityMap(map);
        }
        if (item instanceof Collection<?> collection) {
            return txVector(collection);
        }
        if (item instanceof Object[] array) {
            return txVector(Arrays.asList(array));
        }
        return ClojureCodec.runtimeInput(item);
    }

    private static Object txVector(Collection<?> collection) {
        ArrayList<?> list = collection instanceof List<?> existing
                ? new ArrayList<>(existing)
                : new ArrayList<>(collection);
        ArrayList<Object> converted = new ArrayList<>(list.size());
        String op = list.isEmpty() ? null : extractKeywordString(list.get(0));
        for (int i = 0; i < list.size(); i++) {
            Object value = list.get(i);
            if (i == 0 && op != null) {
                converted.add(ClojureCodec.keyword(op));
            } else if (i == 1 && ":db.fn/call".equals(op)) {
                converted.add(callableTargetInput(value));
            } else if (i == 1) {
                converted.add(lookupRefInput(value));
            } else if (i == 2 && op != null && expectsAttrInThirdPosition(op, list.size())) {
                converted.add(keywordFromAttr(value));
            } else {
                converted.add(ClojureCodec.runtimeInput(value));
            }
        }
        return PersistentVector.create(converted);
    }

    private static boolean expectsAttrInThirdPosition(String op, int size) {
        return size >= 3 && (":db/add".equals(op)
                || ":db/retract".equals(op)
                || ":db/retractAttribute".equals(op)
                || ":db.fn/retractAttribute".equals(op));
    }

    private static Object txEntityMap(Map<?, ?> map) {
        IPersistentMap result = PersistentArrayMap.EMPTY;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            result = result.assoc(keywordFromAttr(entry.getKey()),
                                  txEntityValue(entry.getValue()));
        }
        return result;
    }

    private static Object txEntityValue(Object value) {
        if (value instanceof Map<?, ?> map) {
            if (looksLikeUdfDescriptor(map)) {
                return udfDescriptorInput(map);
            }
            if (looksLikeNestedEntity(map)) {
                return txEntityMap(map);
            }
        }
        if (value instanceof Collection<?> collection) {
            ArrayList<Object> converted = new ArrayList<>(collection.size());
            for (Object item : collection) {
                converted.add(txEntityValue(item));
            }
            return PersistentVector.create(converted);
        }
        if (value instanceof Object[] array) {
            ArrayList<Object> converted = new ArrayList<>(array.length);
            for (Object item : array) {
                converted.add(txEntityValue(item));
            }
            return PersistentVector.create(converted);
        }
        return ClojureCodec.runtimeInput(value);
    }

    private static Object callableTargetInput(Object value) {
        if (value instanceof Map<?, ?> map && looksLikeUdfDescriptor(map)) {
            return udfDescriptorInput(map);
        }
        String keyword = extractKeywordString(value);
        if (keyword != null) {
            return ClojureCodec.keyword(keyword);
        }
        return ClojureCodec.runtimeInput(value);
    }

    private static boolean looksLikeNestedEntity(Map<?, ?> map) {
        for (Object key : map.keySet()) {
            if (key instanceof String s && s.startsWith(":")) {
                return true;
            }
            if (key instanceof Keyword || key instanceof EdnLiteral) {
                return true;
            }
        }
        return false;
    }

    private static boolean looksLikeUdfDescriptor(Map<?, ?> map) {
        return containsKeywordLikeKey(map, ":udf/lang")
                && containsKeywordLikeKey(map, ":udf/kind")
                && containsKeywordLikeKey(map, ":udf/id");
    }

    private static boolean containsKeywordLikeKey(Map<?, ?> map, String keyName) {
        for (Object key : map.keySet()) {
            if (keyName.equals(extractKeywordString(key))) {
                return true;
            }
        }
        return false;
    }

    private static Object kvTxItemInput(Object item) {
        if (item instanceof Collection<?> collection) {
            ArrayList<?> list = collection instanceof List<?> existing
                    ? new ArrayList<>(existing)
                    : new ArrayList<>(collection);
            ArrayList<Object> converted = new ArrayList<>(list.size());
            for (int i = 0; i < list.size(); i++) {
                Object value = list.get(i);
                if (i == 0) {
                    String op = extractKeywordString(value);
                    converted.add(op == null ? ClojureCodec.runtimeInput(value) : ClojureCodec.keyword(op));
                } else {
                    converted.add(ClojureCodec.runtimeInput(value));
                }
            }
            return PersistentVector.create(converted);
        }
        if (item instanceof Object[] array) {
            return kvTxItemInput(Arrays.asList(array));
        }
        return ClojureCodec.runtimeInput(item);
    }

    private static IPersistentMap keywordMap(Map<?, ?> map, boolean keywordizeColonValues) {
        IPersistentMap result = PersistentArrayMap.EMPTY;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            result = result.assoc(keywordFromAttr(entry.getKey()),
                                  keywordValue(entry.getValue(), keywordizeColonValues));
        }
        return result;
    }

    private static Object keywordValue(Object value, boolean keywordizeColonValues) {
        if (value instanceof String s && keywordizeColonValues && s.startsWith(":")) {
            return ClojureCodec.keyword(s);
        }
        if (value instanceof Map<?, ?> map) {
            return keywordMap(map, keywordizeColonValues);
        }
        if (value instanceof java.util.Set<?> set) {
            ArrayList<Object> converted = new ArrayList<>(set.size());
            for (Object item : set) {
                converted.add(keywordValue(item, keywordizeColonValues));
            }
            return PersistentHashSet.create(converted);
        }
        if (value instanceof Collection<?> collection) {
            ArrayList<Object> converted = new ArrayList<>(collection.size());
            for (Object item : collection) {
                converted.add(keywordValue(item, keywordizeColonValues));
            }
            return PersistentVector.create(converted);
        }
        if (value instanceof Object[] array) {
            ArrayList<Object> converted = new ArrayList<>(array.length);
            for (Object item : array) {
                converted.add(keywordValue(item, keywordizeColonValues));
            }
            return PersistentVector.create(converted);
        }
        return ClojureCodec.runtimeInput(value);
    }

    private static Object rangeKeyword(Object value) {
        if (value instanceof Keyword) {
            return value;
        }
        if (value instanceof String s) {
            return ClojureCodec.keyword(s);
        }
        if (value instanceof EdnLiteral literal) {
            return ClojureRuntime.readEdn(literal.value());
        }
        return ClojureCodec.runtimeInput(value);
    }

    private static Object rangeInput(Object rangeType, List<?> bounds) {
        ArrayList<Object> converted = new ArrayList<>(1 + bounds.size());
        converted.add(rangeKeyword(rangeType));
        for (Object bound : bounds) {
            converted.add(ClojureCodec.runtimeInput(bound));
        }
        return PersistentVector.create(converted);
    }

    private static Object keywordFromAttr(Object value) {
        if (value instanceof Keyword) {
            return value;
        }
        if (value instanceof EdnLiteral literal) {
            return ClojureRuntime.readEdn(literal.value());
        }
        if (value instanceof String s) {
            return ClojureCodec.keyword(s);
        }
        return ClojureCodec.runtimeInput(value);
    }

    private static String extractKeywordString(Object value) {
        if (value instanceof Keyword keyword) {
            return keyword.toString();
        }
        if (value instanceof String s && s.startsWith(":")) {
            return s;
        }
        return null;
    }

    private static String stripLeadingColon(String value) {
        return value.startsWith(":") ? value.substring(1) : value;
    }

    private static boolean normalizedKeywordMap(Map<?, ?> map, boolean keywordizeColonValues) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!(entry.getKey() instanceof Keyword)) {
                return false;
            }
            if (!normalizedKeywordValue(entry.getValue(), keywordizeColonValues)) {
                return false;
            }
        }
        return true;
    }

    private static boolean normalizedKeywordValue(Object value, boolean keywordizeColonValues) {
        if (value instanceof String s) {
            return !keywordizeColonValues || !s.startsWith(":");
        }
        if (value instanceof Map<?, ?> map) {
            return normalizedKeywordMap(map, keywordizeColonValues);
        }
        if (value instanceof java.util.Set<?> set) {
            for (Object item : set) {
                if (!normalizedKeywordValue(item, keywordizeColonValues)) {
                    return false;
                }
            }
            return true;
        }
        if (value instanceof Collection<?> collection) {
            for (Object item : collection) {
                if (!normalizedKeywordValue(item, keywordizeColonValues)) {
                    return false;
                }
            }
            return true;
        }
        return ClojureCodec.isRuntimeInput(value);
    }

    private static boolean normalizedRenameMap(Map<?, ?> map) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!(entry.getKey() instanceof Keyword) || !(entry.getValue() instanceof Keyword)) {
                return false;
            }
        }
        return true;
    }

    private static boolean normalizedDeleteAttrs(Collection<?> attrs) {
        for (Object attr : attrs) {
            if (!(attr instanceof Keyword)) {
                return false;
            }
        }
        return true;
    }

    private static boolean normalizedRangeSpec(List<?> rangeSpec) {
        if (rangeSpec.isEmpty() || !(rangeSpec.get(0) instanceof Keyword)) {
            return false;
        }
        for (int i = 1; i < rangeSpec.size(); i++) {
            if (!ClojureCodec.isRuntimeInput(rangeSpec.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean normalizedTypeInput(Object value) {
        if (value instanceof Keyword) {
            return true;
        }
        if (!(value instanceof List<?> list)) {
            return false;
        }
        for (Object item : list) {
            if (!normalizedTypeInput(item)) {
                return false;
            }
        }
        return true;
    }

    private static boolean normalizedTxData(Iterable<?> txData) {
        for (Object item : txData) {
            if (item instanceof Map<?, ?> map) {
                if (!normalizedKeywordMap(map, false)) {
                    return false;
                }
                continue;
            }
            if (!(item instanceof IPersistentCollection collection) || !normalizedTxItem(collection)) {
                return false;
            }
        }
        return true;
    }

    private static boolean normalizedKvTxs(Iterable<?> txs) {
        for (Object item : txs) {
            if (!(item instanceof IPersistentCollection collection) || !normalizedKvTxItem(collection)) {
                return false;
            }
        }
        return true;
    }

    private static boolean normalizedKvTxItem(IPersistentCollection item) {
        if (!(item instanceof List<?> list) || list.isEmpty() || !(list.get(0) instanceof Keyword)) {
            return false;
        }
        for (int i = 1; i < list.size(); i++) {
            if (!ClojureCodec.isRuntimeInput(list.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean normalizedTxItem(IPersistentCollection item) {
        if (!(item instanceof List<?> list) || list.isEmpty()) {
            return false;
        }
        Object op = list.get(0);
        if (!(op instanceof Keyword keyword)) {
            return false;
        }
        if (list.size() >= 2 && !normalizedEntityId(list.get(1))) {
            return false;
        }
        String opName = keyword.toString();
        if (expectsAttrInThirdPosition(opName, list.size())) {
            return list.get(2) instanceof Keyword;
        }
        return true;
    }

    private static boolean isNormalizedLookupRef(Object value) {
        if (!(value instanceof IPersistentCollection)
                || !(value instanceof List<?> list)
                || list.size() != 2) {
            return false;
        }
        return list.get(0) instanceof Keyword;
    }

    private static boolean normalizedEntityId(Object value) {
        return !(value instanceof List<?>) || isNormalizedLookupRef(value);
    }

    private static boolean normalizedEntityIds(List<?> values) {
        for (Object value : values) {
            if (!normalizedEntityId(value)) {
                return false;
            }
        }
        return true;
    }
}
