package datalevin;

import clojure.lang.IPersistentCollection;
import clojure.lang.IPersistentList;
import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;
import clojure.lang.PersistentHashSet;
import clojure.lang.PersistentList;
import clojure.lang.PersistentVector;
import clojure.lang.Symbol;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Generic JVM object and Clojure data conversion layer.
 *
 * <p>This layer is intentionally Datalevin-agnostic aside from datom decoding.
 * Higher-level bindings can reuse it without pulling in the Java-friendly API.
 */
final class ClojureCodec {

    private ClojureCodec() {
    }

    static Object toClojure(Object value) {
        if (value == null
                || value instanceof Keyword
                || value instanceof Symbol
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

        if (value instanceof LiteralString literal) {
            return literal.value();
        }

        if (value instanceof Instant instant) {
            return Date.from(instant);
        }

        if (value instanceof Date) {
            return value;
        }

        if (value instanceof EdnLiteral literal) {
            return ClojureRuntime.readEdn(literal.value());
        }

        if (value instanceof IPersistentMap) {
            return value;
        }

        if (value instanceof IPersistentList list) {
            ArrayList<Object> values = new ArrayList<>();
            for (Object item : (java.util.List<?>) list) {
                values.add(toClojure(item));
            }
            return PersistentList.create(values);
        }

        if (value instanceof Set<?> set) {
            ArrayList<Object> values = new ArrayList<>(set.size());
            for (Object item : set) {
                values.add(toClojure(item));
            }
            return PersistentHashSet.create(values);
        }

        if (value instanceof java.util.Collection<?> collection) {
            ArrayList<Object> values = new ArrayList<>(collection.size());
            for (Object item : collection) {
                values.add(toClojure(item));
            }
            return PersistentVector.create(values);
        }

        if (value instanceof Map<?, ?> map) {
            IPersistentMap result = PersistentArrayMap.EMPTY;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                result = result.assoc(toClojure(entry.getKey()),
                                      toClojure(entry.getValue()));
            }
            return result;
        }

        if (value instanceof Object[] array) {
            ArrayList<Object> values = new ArrayList<>(array.length);
            for (Object item : array) {
                values.add(toClojure(item));
            }
            return PersistentVector.create(values);
        }

        return value;
    }

    static Object runtimeInput(Object value) {
        return isRuntimeInput(value) ? value : toClojure(value);
    }

    static Object bridgeOutput(Object value) {
        if (value == null
                || value instanceof Keyword
                || value instanceof Symbol
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

        if (value instanceof LiteralString literal) {
            return literal.value();
        }

        if (value instanceof Instant instant) {
            return Date.from(instant);
        }

        if (value instanceof Date) {
            return value;
        }

        if (isDatom(value)) {
            LinkedHashMap<Object, Object> result = new LinkedHashMap<>();
            result.put(keyword(":e"), bridgeOutput(ClojureRuntime.datom("datom-e", value)));
            result.put(keyword(":a"), bridgeOutput(ClojureRuntime.datom("datom-a", value)));
            result.put(keyword(":v"), bridgeOutput(ClojureRuntime.datom("datom-v", value)));
            result.put(keyword(":tx"), bridgeOutput(ClojureRuntime.datom("datom-tx", value)));
            result.put(keyword(":added"), bridgeOutput(ClojureRuntime.datom("datom-added", value)));
            return result;
        }

        if (value instanceof Map<?, ?> map) {
            LinkedHashMap<Object, Object> result = new LinkedHashMap<>(map.size());
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                result.put(bridgeOutput(entry.getKey()), bridgeOutput(entry.getValue()));
            }
            return result;
        }

        if (value instanceof Set<?> set) {
            LinkedHashSet<Object> result = new LinkedHashSet<>(set.size());
            for (Object item : set) {
                result.add(bridgeOutput(item));
            }
            return result;
        }

        if (value instanceof Collection<?> collection) {
            ArrayList<Object> result = new ArrayList<>(collection.size());
            for (Object item : collection) {
                result.add(bridgeOutput(item));
            }
            return result;
        }

        if (value instanceof Iterable<?> iterable) {
            ArrayList<Object> result = new ArrayList<>();
            for (Object item : iterable) {
                result.add(bridgeOutput(item));
            }
            return result;
        }

        if (value instanceof Object[] array) {
            ArrayList<Object> result = new ArrayList<>(array.length);
            for (Object item : array) {
                result.add(bridgeOutput(item));
            }
            return result;
        }

        return String.valueOf(value);
    }

    static boolean isRuntimeInput(Object value) {
        return value == null
                || value instanceof Keyword
                || value instanceof Symbol
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
                || value instanceof byte[]
                || value instanceof Date
                || value instanceof IPersistentMap
                || value instanceof IPersistentCollection;
    }

    static String javaString(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    static long javaLong(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        throw new IllegalArgumentException("Expected integer result, got: " + value);
    }

    static Long javaNullableLong(Object value) {
        return value == null ? null : javaLong(value);
    }

    static boolean javaBoolean(Object value) {
        if (value instanceof Boolean bool) {
            return bool;
        }
        throw new IllegalArgumentException("Expected boolean result, got: " + value);
    }

    static UUID javaUuid(Object value) {
        if (value instanceof UUID uuid) {
            return uuid;
        }
        throw new IllegalArgumentException("Expected UUID result, got: " + value);
    }

    static Keyword keyword(String value) {
        String normalized = stripLeadingColon(value);
        int slash = normalized.indexOf('/');
        if (slash < 0) {
            return Keyword.intern(normalized);
        }
        return Keyword.intern(normalized.substring(0, slash), normalized.substring(slash + 1));
    }

    static Symbol symbol(String value) {
        return Symbol.intern(value);
    }

    private static boolean isDatom(Object value) {
        return value != null && javaBoolean(ClojureRuntime.datom("datom?", value));
    }

    private static String stripLeadingColon(String value) {
        return value.startsWith(":") ? value.substring(1) : value;
    }
}
