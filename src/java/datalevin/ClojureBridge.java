package datalevin;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;
import clojure.lang.PersistentHashSet;
import clojure.lang.PersistentVector;
import clojure.lang.RT;
import clojure.lang.Symbol;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

final class ClojureBridge {

    private static final ConcurrentHashMap<String, IFn> VARS = new ConcurrentHashMap<>();
    private static final IFn REQUIRE = Clojure.var("clojure.core", "require");

    static {
        require("clojure.edn");
        require("datalevin.core");
        require("datalevin.client");
        require("datalevin.datom");
        require("datalevin.json-api");
    }

    private ClojureBridge() {
    }

    static Object core(String name, Object... args) {
        return invoke("datalevin.core", name, args);
    }

    static Object client(String name, Object... args) {
        return invoke("datalevin.client", name, args);
    }

    static Object readEdn(String edn) {
        Objects.requireNonNull(edn, "edn");
        return invoke("clojure.edn", "read-string", edn);
    }

    static Object execWithJsonHandle(String handlePrefix,
                                     String handleArg,
                                     Object resource,
                                     String op,
                                     Map<String, ?> args) {
        String handle = (String) invoke("datalevin.json-api",
                                        "register!",
                                        handlePrefix,
                                        keyword(handlePrefix),
                                        resource);
        try {
            LinkedHashMap<String, Object> request = new LinkedHashMap<>();
            request.put(handleArg, handle);
            if (args != null) {
                request.putAll(args);
            }
            return JsonBridge.call(op, request);
        } finally {
            invoke("datalevin.json-api", "unregister!", handle);
        }
    }

    static Object queryForm(String queryEdn) {
        return readEdn(queryEdn);
    }

    static Object explainOpts(String optsEdn) {
        return optsEdn == null ? PersistentArrayMap.EMPTY : readEdn(optsEdn);
    }

    static Object pullSelectorInput(Object selector) {
        if (selector == null) {
            return null;
        }
        if (selector instanceof String s) {
            return readEdn(s);
        }
        return readEdn(Edn.render(selector));
    }

    static Object rangeInput(List<?> rangeSpec) {
        Objects.requireNonNull(rangeSpec, "rangeSpec");
        return readEdn(Edn.render(rangeSpec));
    }

    static Object lookupRefInput(Object value) {
        if (value instanceof List<?> list && list.size() == 2) {
            return PersistentVector.create(Arrays.asList(
                    keywordFromAttr(list.get(0)),
                    genericInput(list.get(1))
            ));
        }
        if (value instanceof Object[] array && array.length == 2) {
            return PersistentVector.create(Arrays.asList(
                    keywordFromAttr(array[0]),
                    genericInput(array[1])
            ));
        }
        return genericInput(value);
    }

    static Object schemaInput(Map<String, ?> schema) {
        return schema == null ? null : keywordMap(schema, true);
    }

    static Object optionsInput(Map<String, ?> opts) {
        return opts == null ? null : keywordMap(opts, true);
    }

    static Object renameMapInput(Map<?, ?> renameMap) {
        if (renameMap == null) {
            return null;
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
            case "datalog" -> keyword(":datalog");
            case "kv", "key-value" -> keyword(":key-value");
            default -> throw new IllegalArgumentException("Unsupported database type: " + dbType);
        };
    }

    static Object roleInput(String role) {
        return keyword(role);
    }

    static Object permissionKeyword(String value) {
        return keyword(value);
    }

    static Object permissionTarget(String obj, Object tgt) {
        if (tgt == null) {
            return null;
        }
        String normalized = stripLeadingColon(obj);
        if ("datalevin.server/role".equals(normalized) && tgt instanceof String s) {
            return keyword(s);
        }
        return genericInput(tgt);
    }

    static Object typeKeyword(String value) {
        return value == null ? null : keyword(value);
    }

    static Object txDataInput(Object txData) {
        if (txData == null) {
            return null;
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

    static Object genericInput(Object value) {
        if (value == null
                || value instanceof String
                || value instanceof Boolean
                || value instanceof Integer
                || value instanceof Long
                || value instanceof Short
                || value instanceof Byte
                || value instanceof Double
                || value instanceof Float
                || value instanceof BigInteger
                || value instanceof BigDecimal
                || value instanceof UUID
                || value instanceof byte[]) {
            return value;
        }

        if (value instanceof Instant instant) {
            return Date.from(instant);
        }

        if (value instanceof Date) {
            return value;
        }

        if (value instanceof EdnLiteral literal) {
            return readEdn(literal.value());
        }

        if (value instanceof Set<?> set) {
            ArrayList<Object> values = new ArrayList<>(set.size());
            for (Object item : set) {
                values.add(genericInput(item));
            }
            return PersistentHashSet.create(values);
        }

        if (value instanceof Collection<?> collection) {
            ArrayList<Object> values = new ArrayList<>(collection.size());
            for (Object item : collection) {
                values.add(genericInput(item));
            }
            return PersistentVector.create(values);
        }

        if (value instanceof Map<?, ?> map) {
            IPersistentMap result = PersistentArrayMap.EMPTY;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                result = result.assoc(genericInput(entry.getKey()),
                                      genericInput(entry.getValue()));
            }
            return result;
        }

        if (value instanceof Object[] array) {
            ArrayList<Object> values = new ArrayList<>(array.length);
            for (Object item : array) {
                values.add(genericInput(item));
            }
            return PersistentVector.create(values);
        }

        return value;
    }

    static Object toJava(Object value) {
        if (value == null
                || value instanceof String
                || value instanceof Boolean
                || value instanceof Integer
                || value instanceof Long
                || value instanceof Short
                || value instanceof Byte
                || value instanceof Double
                || value instanceof Float
                || value instanceof BigInteger
                || value instanceof BigDecimal
                || value instanceof UUID
                || value instanceof byte[]) {
            return value;
        }

        if (value instanceof Instant instant) {
            return instant;
        }

        if (value instanceof Date date) {
            return date.toInstant();
        }

        if (value instanceof Keyword || value instanceof Symbol) {
            return value.toString();
        }

        if (Boolean.TRUE.equals(invoke("datalevin.datom", "datom?", value))) {
            return datomToJava(value);
        }

        if (value instanceof Map<?, ?> map) {
            LinkedHashMap<Object, Object> converted = new LinkedHashMap<>(map.size());
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                converted.put(toJavaMapKey(entry.getKey()), toJava(entry.getValue()));
            }
            return converted;
        }

        if (value instanceof Set<?> set) {
            LinkedHashSet<Object> converted = new LinkedHashSet<>(set.size());
            for (Object item : set) {
                converted.add(toJava(item));
            }
            return converted;
        }

        if (value instanceof Iterable<?> iterable) {
            ArrayList<Object> converted = new ArrayList<>();
            for (Object item : iterable) {
                converted.add(toJava(item));
            }
            return converted;
        }

        if (value instanceof Object[] array) {
            ArrayList<Object> converted = new ArrayList<>(array.length);
            for (Object item : array) {
                converted.add(toJava(item));
            }
            return converted;
        }

        return value;
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> javaMap(Object value) {
        return (Map<String, Object>) toJava(value);
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> javaMapOrNull(Object value) {
        return value == null ? null : (Map<String, Object>) toJava(value);
    }

    @SuppressWarnings("unchecked")
    static List<Object> javaList(Object value) {
        return (List<Object>) toJava(value);
    }

    @SuppressWarnings("unchecked")
    static Map<Object, Object> javaAnyMap(Object value) {
        return (Map<Object, Object>) toJava(value);
    }

    static String javaString(Object value) {
        Object converted = toJava(value);
        return converted == null ? null : String.valueOf(converted);
    }

    static long javaLong(Object value) {
        Object converted = toJava(value);
        if (converted instanceof Number number) {
            return number.longValue();
        }
        throw new IllegalArgumentException("Expected integer result, got: " + converted);
    }

    static Long javaNullableLong(Object value) {
        return value == null ? null : javaLong(value);
    }

    static boolean javaBoolean(Object value) {
        Object converted = toJava(value);
        if (converted instanceof Boolean bool) {
            return bool;
        }
        throw new IllegalArgumentException("Expected boolean result, got: " + converted);
    }

    static UUID javaUuid(Object value) {
        Object converted = toJava(value);
        if (converted instanceof UUID uuid) {
            return uuid;
        }
        throw new IllegalArgumentException("Expected UUID result, got: " + converted);
    }

    private static void require(String ns) {
        REQUIRE.invoke(Clojure.read(ns));
    }

    private static Object invoke(String ns, String name, Object... args) {
        try {
            IFn fn = VARS.computeIfAbsent(ns + "/" + name, ignored -> Clojure.var(ns, name));
            if (args == null || args.length == 0) {
                return fn.invoke();
            }
            return fn.applyTo(RT.seq(Arrays.asList(args)));
        } catch (RuntimeException e) {
            throw translateException(e);
        }
    }

    private static RuntimeException translateException(RuntimeException e) {
        if (e instanceof DatalevinException de) {
            return de;
        }
        if (e instanceof clojure.lang.ExceptionInfo info) {
            Object data = toJava(info.getData());
            String errorType = null;
            if (info.getData() != null) {
                Object raw = info.getData().valAt(keyword(":error"));
                if (raw != null) {
                    errorType = String.valueOf(toJava(raw));
                }
            }
            return new DatalevinException(info.getMessage(), errorType, data);
        }
        return e;
    }

    private static Object datomToJava(Object datom) {
        LinkedHashMap<String, Object> converted = new LinkedHashMap<>(5);
        converted.put("e", toJava(invoke("datalevin.datom", "datom-e", datom)));
        converted.put("a", toJava(invoke("datalevin.datom", "datom-a", datom)));
        converted.put("v", toJava(invoke("datalevin.datom", "datom-v", datom)));
        converted.put("tx", toJava(invoke("datalevin.datom", "datom-tx", datom)));
        converted.put("added", toJava(invoke("datalevin.datom", "datom-added", datom)));
        return converted;
    }

    private static Object toJavaMapKey(Object key) {
        Object converted = toJava(key);
        if (converted instanceof String || converted instanceof UUID) {
            return converted;
        }
        return converted;
    }

    private static Object txItemInput(Object item) {
        if (item instanceof Map<?, ?> map) {
            return txEntityMap(map);
        }
        if (item instanceof Collection<?> collection) {
            return txVector(collection);
        }
        if (item instanceof Object[] array) {
            return txVector(Arrays.asList(array));
        }
        return genericInput(item);
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
                converted.add(keyword(op));
            } else if (i == 1) {
                converted.add(lookupRefInput(value));
            } else if (i == 2 && op != null && expectsAttrInThirdPosition(op, list.size())) {
                converted.add(keywordFromAttr(value));
            } else {
                converted.add(genericInput(value));
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
        if (value instanceof Map<?, ?> map && looksLikeNestedEntity(map)) {
            return txEntityMap(map);
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
        return genericInput(value);
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
                    converted.add(op == null ? genericInput(value) : keyword(op));
                } else {
                    converted.add(genericInput(value));
                }
            }
            return PersistentVector.create(converted);
        }
        if (item instanceof Object[] array) {
            return kvTxItemInput(Arrays.asList(array));
        }
        return genericInput(item);
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
            return keyword(s);
        }
        if (value instanceof Map<?, ?> map) {
            return keywordMap(map, keywordizeColonValues);
        }
        if (value instanceof Set<?> set) {
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
        return genericInput(value);
    }

    private static Object keywordFromAttr(Object value) {
        if (value instanceof Keyword) {
            return value;
        }
        if (value instanceof EdnLiteral literal) {
            return readEdn(literal.value());
        }
        if (value instanceof String s) {
            return keyword(s);
        }
        return genericInput(value);
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

    private static Keyword keyword(String value) {
        String normalized = stripLeadingColon(value);
        int slash = normalized.indexOf('/');
        if (slash < 0) {
            return Keyword.intern(normalized);
        }
        return Keyword.intern(normalized.substring(0, slash), normalized.substring(slash + 1));
    }

    private static String stripLeadingColon(String value) {
        return value.startsWith(":") ? value.substring(1) : value;
    }
}
