package datalevin;

import clojure.lang.IHashEq;

import java.util.Objects;
import java.util.function.BiPredicate;

/**
 * An immutable byte snapshot of a value owned by an embedded language runtime.
 *
 * <p>The bridge supplies logical equality over decoded values. Serialized byte
 * equality is insufficient: equal values can have different payloads. A constant
 * hash keeps that contract even for host values without a stable hash function.
 * This object is a runtime bridge value, not a persistent or wire codec.
 */
public final class NativeValue implements IHashEq {
    private final String codecId;
    private final String typeName;
    private final byte[] payload;
    private final BiPredicate<NativeValue, NativeValue> equality;

    public NativeValue(String codecId, String typeName, byte[] payload,
                       BiPredicate<NativeValue, NativeValue> equality) {
        this.codecId = Objects.requireNonNull(codecId, "codecId");
        this.typeName = Objects.requireNonNull(typeName, "typeName");
        this.payload = Objects.requireNonNull(payload, "payload").clone();
        this.equality = Objects.requireNonNull(equality, "equality");
    }

    public String codecId() {
        return codecId;
    }

    public String typeName() {
        return typeName;
    }

    public byte[] payload() {
        return payload.clone();
    }

    /** Restore a spill snapshot using this value's live runtime binding. */
    public NativeValue withPayload(byte[] bytes) {
        return new NativeValue(codecId, typeName, bytes, equality);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof NativeValue value
                && equality.test(this, value);
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public int hasheq() {
        return hashCode();
    }

    @Override
    public String toString() {
        return "#<native-value " + typeName + ">";
    }
}
