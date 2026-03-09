package datalevin;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

final class ResultSupport {

    private ResultSupport() {
    }

    @SuppressWarnings("unchecked")
    static <T> T coerce(Object value, Class<T> type) {
        if (value == null) {
            return null;
        }
        if (type.isInstance(value)) {
            return type.cast(value);
        }
        if (type == String.class) {
            return (T) String.valueOf(value);
        }
        if (value instanceof Number number) {
            if (type == Long.class) {
                return (T) Long.valueOf(number.longValue());
            }
            if (type == Integer.class) {
                return (T) Integer.valueOf(number.intValue());
            }
            if (type == Double.class) {
                return (T) Double.valueOf(number.doubleValue());
            }
            if (type == Float.class) {
                return (T) Float.valueOf(number.floatValue());
            }
            if (type == Short.class) {
                return (T) Short.valueOf(number.shortValue());
            }
            if (type == Byte.class) {
                return (T) Byte.valueOf(number.byteValue());
            }
        }
        throw new IllegalArgumentException("Cannot coerce value " + value + " to " + type.getName());
    }

    @SuppressWarnings("unchecked")
    static List<Object> sequence(Object value) {
        if (value == null) {
            return List.of();
        }
        if (value instanceof List<?> list) {
            return (List<Object>) list;
        }
        if (value instanceof Collection<?> collection) {
            return freezeList(new ArrayList<>((Collection<Object>) collection));
        }
        if (value instanceof Iterable<?> iterable) {
            ArrayList<Object> items = new ArrayList<>();
            for (Object item : iterable) {
                items.add(item);
            }
            return freezeList(items);
        }
        if (value instanceof Object[] array) {
            ArrayList<Object> items = new ArrayList<>(array.length);
            Collections.addAll(items, array);
            return freezeList(items);
        }
        throw new IllegalArgumentException("Expected sequential value, got: " + value);
    }

    @SuppressWarnings("unchecked")
    static <T> List<T> typedSequence(Object value, Class<T> type) {
        if (value == null) {
            return List.of();
        }
        if (value instanceof List<?> list) {
            return typedView((List<Object>) list, type);
        }
        if (value instanceof Collection<?> collection) {
            ArrayList<T> values = new ArrayList<>(collection.size());
            for (Object item : collection) {
                values.add(coerce(item, type));
            }
            return freezeList(values);
        }
        if (value instanceof Iterable<?> iterable) {
            ArrayList<T> values = new ArrayList<>();
            for (Object item : iterable) {
                values.add(coerce(item, type));
            }
            return freezeList(values);
        }
        if (value instanceof Object[] array) {
            ArrayList<T> values = new ArrayList<>(array.length);
            for (Object item : array) {
                values.add(coerce(item, type));
            }
            return freezeList(values);
        }
        throw new IllegalArgumentException("Expected sequential value, got: " + value);
    }

    static <T> List<T> freezeList(List<T> value) {
        if (value == null) {
            return List.of();
        }
        int size = value.size();
        if (size == 0) {
            return List.of();
        }
        return Collections.unmodifiableList(value);
    }

    static <T> List<T> copyList(List<T> value) {
        return value == null ? List.of() : Collections.unmodifiableList(new ArrayList<>(value));
    }

    private static <T> List<T> typedView(List<Object> list, Class<T> type) {
        int size = list.size();
        if (size == 0) {
            return List.of();
        }
        return new TypedListView<>(list, type);
    }

    private static final class TypedListView<T> extends AbstractList<T> {

        private final List<Object> values;
        private final Class<T> type;

        private TypedListView(List<Object> values, Class<T> type) {
            this.values = values;
            this.type = type;
        }

        @Override
        public T get(int index) {
            return coerce(values.get(index), type);
        }

        @Override
        public int size() {
            return values.size();
        }
    }
}
