package datalevin;

import clojure.lang.PersistentVector;

import java.util.ArrayList;
import java.util.Objects;

/**
 * Type specification for KV key and value encoding.
 *
 * <p>Use the built-in constants for scalar types, {@link #of(String)} for
 * uncommon keyword-based types, and {@link #tuple(KVType...)} for tuple specs.
 */
public final class KVType {

    public static final KVType DATA = scalar(":data");
    public static final KVType STRING = scalar(":string");
    public static final KVType LONG = scalar(":long");
    public static final KVType FLOAT = scalar(":float");
    public static final KVType DOUBLE = scalar(":double");
    public static final KVType BIGINT = scalar(":bigint");
    public static final KVType BIGDEC = scalar(":bigdec");
    public static final KVType BYTES = scalar(":bytes");
    public static final KVType KEYWORD = scalar(":keyword");
    public static final KVType SYMBOL = scalar(":symbol");
    public static final KVType BOOLEAN = scalar(":boolean");
    public static final KVType INSTANT = scalar(":instant");
    public static final KVType UUID = scalar(":uuid");
    public static final KVType ID = scalar(":id");
    public static final KVType IGNORE = scalar(":ignore");
    public static final KVType DATOM = scalar(":datom");

    private final Object form;

    private KVType(Object form) {
        this.form = form;
    }

    /**
     * Creates a scalar type from a keyword-like string such as
     * {@code "string"} or {@code ":string"}.
     */
    public static KVType of(String keyword) {
        return scalar(keyword);
    }

    /**
     * Creates a tuple type specification.
     */
    public static KVType tuple(KVType... elementTypes) {
        Objects.requireNonNull(elementTypes, "elementTypes");
        ArrayList<Object> values = new ArrayList<>(elementTypes.length);
        for (KVType elementType : elementTypes) {
            if (elementType == null) {
                throw new NullPointerException("elementTypes contains null");
            }
            values.add(elementType.form);
        }
        return new KVType(PersistentVector.create(values));
    }

    @Override
    public String toString() {
        return Edn.render(form);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof KVType that)) {
            return false;
        }
        return Objects.equals(form, that.form);
    }

    @Override
    public int hashCode() {
        return Objects.hash(form);
    }

    Object build() {
        return form;
    }

    private static KVType scalar(String keyword) {
        return new KVType(ClojureCodec.keyword(keyword));
    }
}
