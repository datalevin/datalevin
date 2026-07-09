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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Datalevin-specific input and form shaping on top of the shared runtime and
 * codec layers.
 *
 * <p>This layer knows how to build lookup refs, schema maps, tx data, query
 * forms, and other Datalevin domain values, but it does not expose the Java
 * ergonomic wrapper API.
 */
final class DatalevinForms {
    private static final Object MISSING = new Object();

    private DatalevinForms() {
    }

    static Object queryForm(String queryEdn) {
        Objects.requireNonNull(queryEdn, "queryEdn");
        return ClojureRuntime.readEdn(queryEdn);
    }

    static Object queryFormInput(Object query) {
        Objects.requireNonNull(query, "query");
        if (query instanceof EdnLiteral literal) {
            return ClojureRuntime.readEdn(literal.value());
        }
        if (query instanceof String s) {
            return ClojureRuntime.readEdn(s);
        }
        return ClojureCodec.runtimeInput(query);
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
        return rangeInput(rangeSpec, null);
    }

    static Object rangeInput(List<?> rangeSpec, Object boundType) {
        Objects.requireNonNull(rangeSpec, "rangeSpec");
        if (rangeSpec.isEmpty()) {
            throw new IllegalArgumentException("rangeSpec must not be empty.");
        }
        Object normalizedType = normalizedKvType(boundType);
        if (!needsTypedNormalization(normalizedType)
                && rangeSpec instanceof IPersistentCollection
                && normalizedRangeSpec(rangeSpec)) {
            return rangeSpec;
        }
        return rangeInput(rangeSpec.get(0), rangeSpec.subList(1, rangeSpec.size()), normalizedType);
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
        if (opts instanceof IPersistentMap && normalizedOptionsMap(opts)) {
            return opts;
        }
        return optionsMap(opts);
    }

    static Object envFlagsInput(Collection<?> flags) {
        Objects.requireNonNull(flags, "flags");
        ArrayList<Object> converted = new ArrayList<>(flags.size());
        for (Object flag : flags) {
            converted.add(envFlagInput(flag));
        }
        return PersistentHashSet.create(converted);
    }

    static Object txReportOutput(Object report) {
        return txReportOutput(report, false);
    }

    static Object txReportOutput(Object report, boolean includeDbValues) {
        return txReportOutput(report, includeDbValues, Function.identity());
    }

    static Object txReportOutput(Object report,
                                 boolean includeDbValues,
                                 Function<Object, Object> dbValueOutput) {
        if (!(report instanceof Map<?, ?> map)) {
            return ClojureCodec.bridgeOutput(report);
        }

        Object dbBefore = map.get(ClojureCodec.keyword(":db-before"));
        Object dbAfter = map.get(ClojureCodec.keyword(":db-after"));
        Object txData = map.get(ClojureCodec.keyword(":tx-data"));
        Object tempids = map.get(ClojureCodec.keyword(":tempids"));
        Object txMeta = map.get(ClojureCodec.keyword(":tx-meta"));

        LinkedHashMap<Object, Object> result = new LinkedHashMap<>();
        if (includeDbValues) {
            result.put(ClojureCodec.keyword(":db-before"),
                       dbBefore == null ? null : dbValueOutput.apply(dbBefore));
            result.put(ClojureCodec.keyword(":db-after"),
                       dbAfter == null ? null : dbValueOutput.apply(dbAfter));
        }
        result.put(ClojureCodec.keyword(":tx-data"),
                   ClojureCodec.bridgeOutput(txData == null ? PersistentVector.EMPTY : txData));
        result.put(ClojureCodec.keyword(":tempids"),
                   ClojureCodec.bridgeOutput(tempids == null ? PersistentArrayMap.EMPTY : tempids));
        result.put(ClojureCodec.keyword(":tx-id"), ClojureCodec.bridgeOutput(txReportId(txData)));
        result.put(ClojureCodec.keyword(":tx-meta"), ClojureCodec.bridgeOutput(txMeta));
        return result;
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

    static Object datomInput(Object datom) {
        if (datom == null || ClojureCodec.isDatom(datom)) {
            return datom;
        }
        if (datom instanceof Map<?, ?> map) {
            return datomFromMap(map);
        }
        List<?> values = toList(datom);
        if (values != null) {
            return datomFromList(values);
        }
        throw new IllegalArgumentException(
                "Datom data must contain Datom values, 3/4/5-element collections, or maps with :e/:a/:v keys.");
    }

    static Object datomsInput(Object datoms) {
        if (datoms == null) {
            return null;
        }
        if (datoms instanceof Collection<?> collection) {
            ArrayList<Object> converted = new ArrayList<>(collection.size());
            for (Object item : collection) {
                converted.add(datomInput(item));
            }
            return PersistentVector.create(converted);
        }
        if (datoms instanceof Object[] array) {
            ArrayList<Object> converted = new ArrayList<>(array.length);
            for (Object item : array) {
                converted.add(datomInput(item));
            }
            return PersistentVector.create(converted);
        }
        throw new IllegalArgumentException("Datoms must be a collection.");
    }

    static Object datom(Object e, Object attr, Object value) {
        return ClojureRuntime.datom("datom",
                                    ClojureCodec.runtimeInput(e),
                                    keywordFromAttr(attr),
                                    ClojureCodec.runtimeInput(value));
    }

    static Object datom(Object e, Object attr, Object value, Object tx) {
        return ClojureRuntime.datom("datom",
                                    ClojureCodec.runtimeInput(e),
                                    keywordFromAttr(attr),
                                    ClojureCodec.runtimeInput(value),
                                    ClojureCodec.runtimeInput(tx));
    }

    static Object datom(Object e, Object attr, Object value, Object tx, Object added) {
        return ClojureRuntime.datom("datom",
                                    ClojureCodec.runtimeInput(e),
                                    keywordFromAttr(attr),
                                    ClojureCodec.runtimeInput(value),
                                    ClojureCodec.runtimeInput(tx),
                                    ClojureCodec.runtimeInput(added));
    }

    static boolean datomIs(Object value) {
        if (ClojureCodec.isDatom(value)) {
            return true;
        }
        if (value instanceof Map<?, ?> map) {
            return datomMapValue(map, "e") != MISSING
                    && datomMapValue(map, "a") != MISSING
                    && datomMapValue(map, "v") != MISSING;
        }
        List<?> values = toList(value);
        return values != null && values.size() >= 3 && values.size() <= 5;
    }

    static Object datomE(Object datom) {
        return datomField(datom, "e", 0, "datom-e");
    }

    static Object datomA(Object datom) {
        return datomField(datom, "a", 1, "datom-a");
    }

    static Object datomV(Object datom) {
        return datomField(datom, "v", 2, "datom-v");
    }

    static Object datomTx(Object datom) {
        return datomField(datom, "tx", 3, "datom-tx");
    }

    static Object datomAdded(Object datom) {
        return datomField(datom, "added", 4, "datom-added");
    }

    private static Object datomField(Object datom, String key, int index, String function) {
        if (ClojureCodec.isDatom(datom)) {
            return ClojureRuntime.datom(function, datom);
        }
        if (datom instanceof Map<?, ?> map) {
            Object value = datomMapValue(map, key);
            return value == MISSING ? null : value;
        }
        List<?> values = toList(datom);
        if (values != null) {
            return index < values.size() ? values.get(index) : null;
        }
        throw new IllegalArgumentException("Expected Datom, datom map, or datom vector, got: " + datom);
    }

    static Object datalogIndexInput(Object index) {
        if (index instanceof Keyword) {
            return index;
        }
        if (index instanceof EdnLiteral literal) {
            return ClojureRuntime.readEdn(literal.value());
        }
        if (index instanceof String s) {
            return ClojureCodec.keyword(s);
        }
        return ClojureCodec.runtimeInput(index);
    }

    static Object datalogAttrInput(Object attr) {
        return attr == null ? null : keywordFromAttr(attr);
    }

    static Object datalogIndexComponentInput(Object index, int position, Object value) {
        if (value == null) {
            return null;
        }
        if (datalogAttributePosition(index, position)) {
            return keywordFromAttr(value);
        }
        return ClojureCodec.runtimeInput(value);
    }

    static Object kvTxsInput(Object txs) {
        return kvTxsInput(txs, null, null);
    }

    static Object kvTxsInput(Object txs, Object defaultKType, Object defaultVType) {
        if (txs == null) {
            return null;
        }
        Object normalizedKType = normalizedKvType(defaultKType);
        Object normalizedVType = normalizedKvType(defaultVType);
        if (txs instanceof Collection<?> collection
                && !needsTypedKvTxNormalization(collection, normalizedKType, normalizedVType)
                && txs instanceof IPersistentCollection
                && normalizedKvTxs(collection)) {
            return txs;
        }
        ArrayList<Object> items = new ArrayList<>();
        if (txs instanceof Collection<?> collection) {
            for (Object item : collection) {
                items.add(kvTxItemInput(item, normalizedKType, normalizedVType));
            }
            return PersistentVector.create(items);
        }
        if (txs instanceof Object[] array) {
            for (Object item : array) {
                items.add(kvTxItemInput(item, normalizedKType, normalizedVType));
            }
            return PersistentVector.create(items);
        }
        throw new IllegalArgumentException("KV transaction data must be a collection.");
    }

    static Object kvInput(Object value, Object type) {
        return typedKvInput(value, normalizedKvType(type));
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

    private static Object datomFromList(List<?> values) {
        int size = values.size();
        if (size < 3 || size > 5) {
            throw new IllegalArgumentException(
                    "Datom collection values must have 3, 4, or 5 elements, got: " + size);
        }
        if (size == 3) {
            return datom(values.get(0), values.get(1), values.get(2));
        }
        if (size == 4) {
            return datom(values.get(0), values.get(1), values.get(2), values.get(3));
        }
        return datom(values.get(0), values.get(1), values.get(2), values.get(3), values.get(4));
    }

    private static Object datomFromMap(Map<?, ?> map) {
        Object e = datomMapValue(map, "e");
        Object a = datomMapValue(map, "a");
        Object v = datomMapValue(map, "v");
        if (e == MISSING || a == MISSING || v == MISSING) {
            throw new IllegalArgumentException("Datom maps must contain :e, :a, and :v keys.");
        }
        Object tx = datomMapValue(map, "tx");
        Object added = datomMapValue(map, "added");
        if (added != MISSING && tx == MISSING) {
            throw new IllegalArgumentException("Datom maps with :added must also contain :tx.");
        }
        if (added != MISSING) {
            return datom(e, a, v, tx, added);
        }
        if (tx != MISSING) {
            return datom(e, a, v, tx);
        }
        return datom(e, a, v);
    }

    private static Object datomMapValue(Map<?, ?> map, String key) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (key.equals(datomMapKey(entry.getKey()))) {
                return entry.getValue();
            }
        }
        return MISSING;
    }

    private static String datomMapKey(Object key) {
        if (key instanceof Keyword keyword) {
            return stripLeadingColon(keyword.toString());
        }
        if (key instanceof String s) {
            return stripLeadingColon(s);
        }
        return null;
    }

    private static Object envFlagInput(Object flag) {
        if (flag instanceof Keyword) {
            return flag;
        }
        if (flag instanceof String s) {
            return ClojureCodec.keyword(s);
        }
        if (flag instanceof EdnLiteral literal) {
            return ClojureRuntime.readEdn(literal.value());
        }
        return ClojureCodec.runtimeInput(flag);
    }

    private static Object txReportId(Object txData) {
        if (txData instanceof Collection<?> collection && !collection.isEmpty()) {
            return ClojureRuntime.datom("datom-tx", collection.iterator().next());
        }
        return null;
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
            } else if (i == 1 && (":db.fn/call".equals(op) || ":db/ensure".equals(op))) {
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

    private static Object kvTxItemInput(Object item, Object defaultKType, Object defaultVType) {
        if (item instanceof Collection<?> collection) {
            ArrayList<?> list = collection instanceof List<?> existing
                    ? new ArrayList<>(existing)
                    : new ArrayList<>(collection);
            ArrayList<Object> converted = new ArrayList<>(list.size());
            String op = list.isEmpty() ? null : extractKeywordString(list.get(0));
            int keyIndex = defaultKType == null && defaultVType == null ? 2 : 1;
            int valueIndex = kvDeleteOp(op) ? -1 : keyIndex + 1;
            int itemKTypeIndex = defaultKType == null && defaultVType == null ? keyIndex + 2 : -1;
            int itemVTypeIndex = defaultKType == null && defaultVType == null && valueIndex >= 0
                    ? valueIndex + 2
                    : -1;
            Object itemKType = typeAt(list, itemKTypeIndex);
            Object itemVType = typeAt(list, itemVTypeIndex);
            Object resolvedKType = itemKType == null ? defaultKType : itemKType;
            Object resolvedVType = itemVType == null ? defaultVType : itemVType;
            for (int i = 0; i < list.size(); i++) {
                Object value = list.get(i);
                if (i == 0) {
                    converted.add(op == null ? ClojureCodec.runtimeInput(value) : ClojureCodec.keyword(op));
                } else if (i == keyIndex) {
                    converted.add(typedKvInput(value, resolvedKType));
                } else if (i == valueIndex) {
                    converted.add(kvListOp(op)
                                  ? typedKvValuesInput(value, resolvedVType)
                                  : typedKvInput(value, resolvedVType));
                } else if (i == itemKTypeIndex) {
                    converted.add(typeInput(value));
                } else if (i == itemVTypeIndex) {
                    converted.add(typeInput(value));
                } else {
                    converted.add(ClojureCodec.runtimeInput(value));
                }
            }
            return PersistentVector.create(converted);
        }
        if (item instanceof Object[] array) {
            return kvTxItemInput(Arrays.asList(array), defaultKType, defaultVType);
        }
        return typedKvInput(item, defaultVType == null ? defaultKType : defaultVType);
    }

    private static IPersistentMap keywordMap(Map<?, ?> map, boolean keywordizeColonValues) {
        IPersistentMap result = PersistentArrayMap.EMPTY;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            Object key = keywordFromAttr(entry.getKey());
            Object value = rawOptionValueKey(key)
                    ? ClojureCodec.runtimeInput(entry.getValue())
                    : keywordValue(entry.getValue(), keywordizeColonValues);
            result = result.assoc(key, value);
        }
        return result;
    }

    private static IPersistentMap optionsMap(Map<?, ?> map) {
        IPersistentMap result = PersistentArrayMap.EMPTY;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            Object key = keywordFromAttr(entry.getKey());
            Object value;
            if (domainOptionsKey(key)) {
                value = domainOptionsMap(entry.getValue());
            } else if (rawOptionValueKey(key)) {
                value = ClojureCodec.runtimeInput(entry.getValue());
            } else {
                value = keywordValue(entry.getValue(), true);
            }
            result = result.assoc(key, value);
        }
        return result;
    }

    private static Object domainOptionsMap(Object value) {
        if (value == null) {
            return null;
        }
        if (!(value instanceof Map<?, ?> map)) {
            return keywordValue(value, true);
        }
        IPersistentMap result = PersistentArrayMap.EMPTY;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            result = result.assoc(domainName(entry.getKey()),
                                  keywordValue(entry.getValue(), true));
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

    private static boolean datalogAttributePosition(Object index, int position) {
        String indexName = stripLeadingColon(String.valueOf(index));
        return ("eav".equals(indexName) && position == 1)
                || ("ave".equals(indexName) && position == 0);
    }

    private static Object rangeInput(Object rangeType, List<?> bounds, Object boundType) {
        ArrayList<Object> converted = new ArrayList<>(1 + bounds.size());
        converted.add(rangeKeyword(rangeType));
        for (Object bound : bounds) {
            converted.add(typedKvInput(bound, boundType));
        }
        return PersistentVector.create(converted);
    }

    private static Object normalizedKvType(Object type) {
        return type == null ? null : typeInput(type);
    }

    private static boolean needsTypedNormalization(Object type) {
        if (isBytesType(type)) {
            return true;
        }
        if (type instanceof Collection<?> collection) {
            for (Object item : collection) {
                if (needsTypedNormalization(item)) {
                    return true;
                }
            }
        }
        if (type instanceof Object[] array) {
            for (Object item : array) {
                if (needsTypedNormalization(item)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean needsTypedKvTxNormalization(Iterable<?> txs,
                                                       Object defaultKType,
                                                       Object defaultVType) {
        if (needsTypedNormalization(defaultKType) || needsTypedNormalization(defaultVType)) {
            return true;
        }
        for (Object item : txs) {
            if (kvTxUsesTypedNormalization(item)) {
                return true;
            }
        }
        return false;
    }

    private static boolean kvTxUsesTypedNormalization(Object item) {
        List<?> list = toList(item);
        if (list == null || list.isEmpty()) {
            return false;
        }
        String op = extractKeywordString(list.get(0));
        if (op == null) {
            return false;
        }
        int keyTypeIndex = kvDeleteOp(op) ? 3 : 4;
        int valueTypeIndex = kvDeleteOp(op) ? -1 : 5;
        return needsTypedNormalization(typeAt(list, keyTypeIndex))
                || needsTypedNormalization(typeAt(list, valueTypeIndex));
    }

    private static boolean kvDeleteOp(String op) {
        return ":del".equals(op);
    }

    private static boolean kvListOp(String op) {
        return ":put-list".equals(op) || ":del-list".equals(op);
    }

    private static Object typeAt(List<?> list, int index) {
        if (index < 0 || index >= list.size()) {
            return null;
        }
        return normalizedKvType(list.get(index));
    }

    private static List<?> toList(Object value) {
        if (value instanceof List<?> list) {
            return list;
        }
        if (value instanceof Collection<?> collection) {
            return new ArrayList<>(collection);
        }
        if (value instanceof Object[] array) {
            return Arrays.asList(array);
        }
        return null;
    }

    private static Object typedKvInput(Object value, Object type) {
        if (isBytesType(type)) {
            return bytesInput(value);
        }
        List<?> typeList = toList(type);
        if (typeList != null) {
            return typedKvTupleInput(value, typeList);
        }
        return ClojureCodec.runtimeInput(value);
    }

    private static Object typedKvTupleInput(Object value, List<?> typeSpec) {
        List<?> values = toList(value);
        if (values == null) {
            return ClojureCodec.runtimeInput(value);
        }
        ArrayList<Object> converted = new ArrayList<>(values.size());
        if (typeSpec.size() == 1) {
            Object elementType = typeSpec.get(0);
            for (Object item : values) {
                converted.add(typedKvInput(item, elementType));
            }
            return PersistentVector.create(converted);
        }
        for (int i = 0; i < values.size(); i++) {
            Object elementType = i < typeSpec.size() ? typeSpec.get(i) : null;
            converted.add(typedKvInput(values.get(i), elementType));
        }
        return PersistentVector.create(converted);
    }

    private static Object typedKvValuesInput(Object value, Object type) {
        List<?> values = toList(value);
        if (values == null) {
            return typedKvInput(value, type);
        }
        ArrayList<Object> converted = new ArrayList<>(values.size());
        for (Object item : values) {
            converted.add(typedKvInput(item, type));
        }
        return PersistentVector.create(converted);
    }

    private static boolean isBytesType(Object type) {
        String keyword = extractKeywordString(type);
        return ":bytes".equals(keyword) || ":db.type/bytes".equals(keyword);
    }

    private static Object bytesInput(Object value) {
        if (value == null || value instanceof byte[]) {
            return value;
        }
        if (value instanceof Collection<?> collection) {
            return bytesInput(collection.toArray());
        }
        if (value instanceof Object[] array) {
            byte[] bytes = new byte[array.length];
            for (int i = 0; i < array.length; i++) {
                bytes[i] = byteValue(array[i]);
            }
            return bytes;
        }
        return ClojureCodec.runtimeInput(value);
    }

    private static byte byteValue(Object value) {
        if (value instanceof Number number) {
            int intValue = number.intValue();
            if (intValue < -128 || intValue > 255) {
                throw new IllegalArgumentException("Byte value out of range: " + intValue);
            }
            return (byte) intValue;
        }
        throw new IllegalArgumentException("Expected byte value, got: " + value);
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

    private static boolean domainOptionsKey(Object key) {
        String name = key instanceof Keyword ? key.toString() : String.valueOf(key);
        return ":search-domains".equals(name)
                || ":vector-domains".equals(name)
                || ":embedding-domains".equals(name);
    }

    private static boolean rawOptionValueKey(Object key) {
        String name = key instanceof Keyword ? key.toString() : String.valueOf(key);
        return ":headers".equals(name);
    }

    private static String domainName(Object key) {
        if (key instanceof Keyword keyword) {
            return stripLeadingColon(keyword.toString());
        }
        return String.valueOf(key);
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

    private static boolean normalizedOptionsMap(Map<?, ?> map) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!(entry.getKey() instanceof Keyword)) {
                return false;
            }
            if (domainOptionsKey(entry.getKey())) {
                if (!normalizedDomainOptionsMap(entry.getValue())) {
                    return false;
                }
            } else if (!normalizedKeywordValue(entry.getValue(), true)) {
                return false;
            }
        }
        return true;
    }

    private static boolean normalizedDomainOptionsMap(Object value) {
        if (value == null) {
            return true;
        }
        if (!(value instanceof Map<?, ?> map)) {
            return false;
        }
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!(entry.getKey() instanceof String s) || s.startsWith(":")) {
                return false;
            }
            if (!normalizedKeywordValue(entry.getValue(), true)) {
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
