package datalevin;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import clojure.lang.Keyword;
import clojure.lang.IPersistentList;
import clojure.lang.Symbol;

final class Edn {

    private enum StringMode {
        AUTO_EDN,
        QUOTED,
        RAW
    }

    private Edn() {
    }

    static String render(Object value) {
        return render(value, StringMode.AUTO_EDN);
    }

    static String renderQuery(Object value) {
        return render(value, StringMode.QUOTED);
    }

    static String renderPattern(Object value) {
        return render(value, StringMode.RAW);
    }

    private static String render(Object value, StringMode stringMode) {
        if (value == null) {
            return "nil";
        }

        if (value instanceof EdnLiteral ednLiteral) {
            return ednLiteral.value();
        }

        if (value instanceof LiteralString literalString) {
            return quote(literalString.value());
        }

        if (value instanceof Keyword || value instanceof Symbol) {
            return value.toString();
        }

        if (value instanceof String s) {
            if (stringMode == StringMode.RAW) {
                return s;
            }
            if (stringMode == StringMode.AUTO_EDN && looksLikeRawEdn(s)) {
                return s;
            }
            return quote(s);
        }

        if (value instanceof Character c) {
            return quote(String.valueOf(c));
        }

        if (value instanceof Boolean || value instanceof Integer || value instanceof Long
                || value instanceof Short || value instanceof Byte
                || value instanceof Double || value instanceof Float
                || value instanceof BigInteger || value instanceof BigDecimal) {
            return String.valueOf(value);
        }

        if (value instanceof UUID uuid) {
            return "#uuid " + quote(uuid.toString());
        }

        if (value instanceof Instant instant) {
            return "#inst " + quote(instant.toString());
        }

        if (value instanceof Date date) {
            return "#inst " + quote(date.toInstant().toString());
        }

        if (value instanceof Map<?, ?> map) {
            return renderMap(map, stringMode);
        }

        if (value instanceof IPersistentList list) {
            return "(" + join((java.util.List<?>) list, " ", item -> render(item, stringMode)) + ")";
        }

        if (value instanceof Set<?> set) {
            return "#{" + join(set, " ", item -> render(item, stringMode)) + "}";
        }

        if (value instanceof Collection<?> collection) {
            return "[" + join(collection, " ", item -> render(item, stringMode)) + "]";
        }

        if (value.getClass().isArray()) {
            int length = java.lang.reflect.Array.getLength(value);
            StringBuilder builder = new StringBuilder("[");
            for (int i = 0; i < length; i++) {
                if (i > 0) {
                    builder.append(' ');
                }
                builder.append(render(java.lang.reflect.Array.get(value, i), stringMode));
            }
            return builder.append(']').toString();
        }

        return quote(String.valueOf(value));
    }

    static String renderAttribute(Object value) {
        if (value instanceof EdnLiteral ednLiteral) {
            return ednLiteral.value();
        }
        if (value instanceof String s && !looksLikeRawEdn(s)) {
            return Datalevin.kw(s).toString();
        }
        return render(value);
    }

    private static String renderMap(Map<?, ?> map, StringMode stringMode) {
        StringBuilder builder = new StringBuilder("{");
        Iterator<? extends Map.Entry<?, ?>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<?, ?> entry = iterator.next();
            builder.append(render(entry.getKey(), stringMode))
                    .append(' ')
                    .append(render(entry.getValue(), stringMode));
            if (iterator.hasNext()) {
                builder.append(' ');
            }
        }
        return builder.append('}').toString();
    }

    private static boolean looksLikeRawEdn(String value) {
        return value.startsWith(":")
                || value.startsWith("?")
                || value.startsWith("$")
                || value.startsWith("%")
                || value.startsWith("_")
                || value.startsWith("(")
                || value.startsWith("[")
                || value.startsWith("{")
                || value.startsWith("#")
                || value.startsWith("'")
                || value.equals("...")
                || value.equals(".");
    }

    private static String quote(String value) {
        StringBuilder builder = new StringBuilder("\"");
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '\\' -> builder.append("\\\\");
                case '"' -> builder.append("\\\"");
                case '\n' -> builder.append("\\n");
                case '\r' -> builder.append("\\r");
                case '\t' -> builder.append("\\t");
                default -> builder.append(ch);
            }
        }
        return builder.append('"').toString();
    }

    private interface Renderer {
        String render(Object value);
    }

    private static String join(Iterable<?> values, String separator, Renderer renderer) {
        StringBuilder builder = new StringBuilder();
        Iterator<?> iterator = values.iterator();
        while (iterator.hasNext()) {
            builder.append(renderer.render(iterator.next()));
            if (iterator.hasNext()) {
                builder.append(separator);
            }
        }
        return builder.toString();
    }
}
