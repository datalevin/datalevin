package datalevin;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

final class JsonBridge {

    private static final long MAX_JS_SAFE_INT = 9_007_199_254_740_991L;
    private static final Set<String> TAG_KEYS = Set.of(
            "~str",
            "~i64",
            "~map",
            "~set",
            "~date",
            "~uuid",
            "~bytes",
            "~bigdec",
            "~bigint",
            "~edn",
            "~handle",
            "~pull"
    );

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonBridge() {
    }

    static Object call(String op) {
        return call(op, Map.of());
    }

    static Object call(String op, Map<String, ?> args) {
        Objects.requireNonNull(op, "op");

        LinkedHashMap<String, Object> request = new LinkedHashMap<>();
        request.put("op", op);
        request.put("args", args == null ? Map.of() : args);

        try {
            String requestJson = MAPPER.writeValueAsString(encode(request));
            Object envelope = decode(MAPPER.readValue(JSONApi.exec(requestJson), Object.class));
            if (!(envelope instanceof Map<?, ?> map)) {
                throw new IllegalStateException("Datalevin JSON API returned a non-object response.");
            }

            if (Boolean.TRUE.equals(map.get("ok"))) {
                return map.get("result");
            }

            Object message = map.get("error");
            Object type = map.get("type");
            Object data = map.get("data");
            throw new DatalevinException(
                    message == null ? "Datalevin request failed." : String.valueOf(message),
                    type == null ? null : String.valueOf(type),
                    data
            );
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to encode/decode Datalevin JSON payload.", e);
        }
    }

    static Object encode(Object value) {
        if (value == null || value instanceof String || value instanceof Boolean) {
            return value;
        }

        if (value instanceof Character c) {
            return String.valueOf(c);
        }

        if (value instanceof Enum<?> e) {
            return e.toString();
        }

        if (value instanceof Long l) {
            if (l >= -MAX_JS_SAFE_INT && l <= MAX_JS_SAFE_INT) {
                return l;
            }
            return tagged("~i64", Long.toString(l));
        }

        if (value instanceof Integer || value instanceof Short || value instanceof Byte) {
            return value;
        }

        if (value instanceof BigInteger bigInteger) {
            return tagged("~bigint", bigInteger.toString());
        }

        if (value instanceof BigDecimal bigDecimal) {
            return tagged("~bigdec", bigDecimal.toPlainString());
        }

        if (value instanceof Double d) {
            return d;
        }

        if (value instanceof Float f) {
            return f.doubleValue();
        }

        if (value instanceof UUID uuid) {
            return tagged("~uuid", uuid.toString());
        }

        if (value instanceof Instant instant) {
            return tagged("~date", instant.toString());
        }

        if (value instanceof Date date) {
            return tagged("~date", date.toInstant().toString());
        }

        if (value instanceof EdnLiteral ednLiteral) {
            return tagged("~edn", ednLiteral.value());
        }

        if (value instanceof byte[] bytes) {
            return tagged("~bytes", Base64.getEncoder().encodeToString(bytes));
        }

        if (value instanceof Map<?, ?> map) {
            return encodeMap(map);
        }

        if (value instanceof Set<?> set) {
            ArrayList<Object> items = new ArrayList<>(set.size());
            for (Object item : set) {
                items.add(encode(item));
            }
            return tagged("~set", items);
        }

        if (value instanceof Collection<?> collection) {
            ArrayList<Object> items = new ArrayList<>(collection.size());
            for (Object item : collection) {
                items.add(encode(item));
            }
            return items;
        }

        if (value instanceof Iterable<?> iterable) {
            ArrayList<Object> items = new ArrayList<>();
            for (Object item : iterable) {
                items.add(encode(item));
            }
            return items;
        }

        if (value.getClass().isArray()) {
            ArrayList<Object> items = new ArrayList<>(Array.getLength(value));
            for (int i = 0; i < Array.getLength(value); i++) {
                items.add(encode(Array.get(value, i)));
            }
            return items;
        }

        throw new IllegalArgumentException(
                "Unsupported Java value for Datalevin JSON encoding: " + value.getClass().getName()
        );
    }

    private static Object encodeMap(Map<?, ?> map) {
        boolean plainObject = true;
        for (Object key : map.keySet()) {
            if (!(key instanceof String s) || TAG_KEYS.contains(s)) {
                plainObject = false;
                break;
            }
        }

        if (plainObject) {
            LinkedHashMap<String, Object> encoded = new LinkedHashMap<>(map.size());
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                encoded.put((String) entry.getKey(), encode(entry.getValue()));
            }
            return encoded;
        }

        ArrayList<Object> entries = new ArrayList<>(map.size());
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            ArrayList<Object> pair = new ArrayList<>(2);
            pair.add(encode(entry.getKey()));
            pair.add(encode(entry.getValue()));
            entries.add(pair);
        }
        return tagged("~map", entries);
    }

    private static LinkedHashMap<String, Object> tagged(String tag, Object value) {
        LinkedHashMap<String, Object> tagged = new LinkedHashMap<>(1);
        tagged.put(tag, value);
        return tagged;
    }

    static Object decode(Object value) {
        if (value == null || value instanceof String || value instanceof Boolean) {
            return value;
        }

        if (value instanceof Integer || value instanceof Long || value instanceof Double
                || value instanceof Float || value instanceof BigDecimal
                || value instanceof BigInteger) {
            return value;
        }

        if (value instanceof Map<?, ?> map) {
            if (map.size() == 1) {
                Object firstKey = map.keySet().iterator().next();
                if (firstKey instanceof String tag && TAG_KEYS.contains(tag)) {
                    return decodeTagged(tag, map.get(firstKey));
                }
            }

            LinkedHashMap<String, Object> decoded = new LinkedHashMap<>(map.size());
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                decoded.put(String.valueOf(entry.getKey()), decode(entry.getValue()));
            }
            return decoded;
        }

        if (value instanceof List<?> list) {
            ArrayList<Object> decoded = new ArrayList<>(list.size());
            for (Object item : list) {
                decoded.add(decode(item));
            }
            return decoded;
        }

        return value;
    }

    private static Object decodeTagged(String tag, Object value) {
        return switch (tag) {
            case "~str", "~edn", "~handle" -> value;
            case "~i64" -> Long.parseLong(String.valueOf(value));
            case "~uuid" -> UUID.fromString(String.valueOf(value));
            case "~date" -> Instant.parse(String.valueOf(value));
            case "~bytes" -> Base64.getDecoder().decode(String.valueOf(value));
            case "~bigdec" -> new BigDecimal(String.valueOf(value));
            case "~bigint" -> new BigInteger(String.valueOf(value));
            case "~pull" -> decode(value);
            case "~set" -> decodeSet(value);
            case "~map" -> decodeMapPairs(value);
            default -> throw new IllegalArgumentException("Unsupported Datalevin JSON tag: " + tag);
        };
    }

    private static LinkedHashSet<Object> decodeSet(Object value) {
        if (!(value instanceof List<?> list)) {
            throw new IllegalArgumentException("Expected JSON array for ~set tag.");
        }

        LinkedHashSet<Object> decoded = new LinkedHashSet<>(list.size());
        for (Object item : list) {
            decoded.add(decode(item));
        }
        return decoded;
    }

    private static LinkedHashMap<Object, Object> decodeMapPairs(Object value) {
        if (!(value instanceof List<?> list)) {
            throw new IllegalArgumentException("Expected JSON array for ~map tag.");
        }

        LinkedHashMap<Object, Object> decoded = new LinkedHashMap<>(list.size());
        for (Object item : list) {
            if (!(item instanceof List<?> pair) || pair.size() != 2) {
                throw new IllegalArgumentException("Expected [k, v] pairs for ~map tag.");
            }
            decoded.put(decode(pair.get(0)), decode(pair.get(1)));
        }
        return decoded;
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> asMap(Object value) {
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> asMapOrNull(Object value) {
        return value == null ? null : (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    static List<Object> asList(Object value) {
        return (List<Object>) value;
    }

    static String asString(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    static boolean asBoolean(Object value) {
        if (!(value instanceof Boolean b)) {
            throw new IllegalArgumentException("Expected boolean result, got: " + value);
        }
        return b;
    }

    static long asLong(Object value) {
        if (value instanceof Byte b) {
            return b.longValue();
        }
        if (value instanceof Short s) {
            return s.longValue();
        }
        if (value instanceof Integer i) {
            return i.longValue();
        }
        if (value instanceof Long l) {
            return l;
        }
        if (value instanceof BigInteger bi) {
            return bi.longValueExact();
        }
        throw new IllegalArgumentException("Expected integer result, got: " + value);
    }

    static Long asNullableLong(Object value) {
        return value == null ? null : asLong(value);
    }

    static UUID asUuid(Object value) {
        if (!(value instanceof UUID uuid)) {
            throw new IllegalArgumentException("Expected UUID result, got: " + value);
        }
        return uuid;
    }
}
